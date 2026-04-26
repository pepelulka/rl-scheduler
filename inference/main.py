"""
RL Scheduler Inference Service with online PPO fine-tuning.

Flow:
  1. POST /infer  — build observation, run policy.forward(), store
                    (obs, action, value, log_prob) under a request_id,
                    return action + request_id.
  2. POST /feedback — receive reward and next state for that request_id;
                      add the complete transition to the rollout buffer.
                      When the buffer is full, run one PPO update and reset.

PPO is on-policy: we must use the (value, log_prob) that were computed
at inference time for the same action that was actually executed.
"""

from __future__ import annotations

import os
import threading
import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional

import numpy as np
import torch as th
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from stable_baselines3 import PPO
from stable_baselines3.common.buffers import RolloutBuffer
from stable_baselines3.common.logger import configure as sb3_configure_logger
from stable_baselines3.common.utils import obs_as_tensor

# ---------------------------------------------------------------------------
# Constants (must match ClusterSchedulerEnv)
# ---------------------------------------------------------------------------
N_WORKERS     = 6
MAX_QUEUE_OBS = 5
TASK_TYPES    = ["monte_carlo", "mat_mul", "prime_sieve"]
TASK_TYPE_IDX = {t: i for i, t in enumerate(TASK_TYPES)}

_HERE        = Path(__file__).parent
_MODEL_PATH   = os.environ.get("MODEL_PATH",  str(_HERE / "../rl/models/best/best_model.zip"))
_ONLINE_BATCH = int(os.environ.get("ONLINE_BATCH", "256")) # transitions per PPO update

# ---------------------------------------------------------------------------
# Load model and replace its rollout buffer with one of the desired size
# ---------------------------------------------------------------------------
model: PPO = PPO.load(_MODEL_PATH)
# PPO.load() skips __init__, so _logger is never set; model.train() requires it.
model.set_logger(sb3_configure_logger(folder=None, format_strings=["stdout"]))
model.rollout_buffer = RolloutBuffer(
    buffer_size=_ONLINE_BATCH,
    observation_space=model.observation_space,
    action_space=model.action_space,
    device=model.device,
    gamma=model.gamma,
    gae_lambda=model.gae_lambda,
    n_envs=1,
)
model.rollout_buffer.reset()

# ---------------------------------------------------------------------------
# Online-learning state
# ---------------------------------------------------------------------------
_lock               = threading.Lock()
_pending: Dict[str, Dict[str, Any]] = {}   # request_id → stored transition data
_update_count       = 0
_next_episode_start = True                 # True for the very first transition

# ---------------------------------------------------------------------------
# Schemas
# ---------------------------------------------------------------------------

class WorkerState(BaseModel):
    active_tasks:    int   = Field(ge=0,    description="Number of active tasks on this worker")
    cpu_util_pct:    float = Field(ge=0.0, le=100.0, description="CPU utilisation, percent")
    cpu_limit_cores: float = Field(gt=0.0, description="CPU limit in cores")
    ram_usage_kib:   float = Field(ge=0.0, description="RAM usage in KiB")
    ram_max_kib:     float = Field(gt=0.0, description="RAM limit in KiB")


class TaskInfo(BaseModel):
    task_type:  str = Field(description="One of: monte_carlo, mat_mul, prime_sieve")
    size_index: int = Field(ge=0, le=2,   description="Task size: 0=small, 1=medium, 2=large")


class InferRequest(BaseModel):
    workers:              List[WorkerState] = Field(description="Exactly 6 worker states, indexed 0-5")
    queue_size:           int               = Field(ge=0, description="Total tasks in the queue")
    queue_tasks:          List[TaskInfo]    = Field(default=[], description="First up to 5 tasks (head first)")
    max_tasks_per_worker: int               = Field(default=2, ge=1, description="Concurrency limit per worker")


class InferResponse(BaseModel):
    request_id:   str           = Field(description="Pass this to /feedback together with the observed reward")
    action:       int           = Field(description="Raw action: 0-5 assign to worker, 6 wait")
    action_type:  str           = Field(description="'assign' or 'wait'")
    worker_index: Optional[int] = Field(default=None, description="Target worker when action_type='assign'")
    description:  str


class FeedbackRequest(BaseModel):
    request_id:                str             = Field(description="request_id from /infer response")
    reward:                    float           = Field(description="Observed reward for the taken action")
    done:                      bool            = Field(default=False, description="True if this ends an episode")
    next_workers:              List[WorkerState]
    next_queue_size:           int             = Field(ge=0)
    next_queue_tasks:          List[TaskInfo]  = Field(default=[])
    next_max_tasks_per_worker: int             = Field(default=2, ge=1)


class FeedbackResponse(BaseModel):
    buffered:     int  = Field(description="Transitions accumulated since last update")
    updated:      bool = Field(description="True if a PPO update was triggered by this call")
    update_count: int  = Field(description="Total PPO updates so far")


class StatusResponse(BaseModel):
    buffered:     int
    pending:      int
    update_count: int

# ---------------------------------------------------------------------------
# Observation builder
# ---------------------------------------------------------------------------

def _build_obs(
    workers: List[WorkerState],
    queue_size: int,
    queue_tasks: List[TaskInfo],
    max_tasks_per_worker: int,
) -> np.ndarray:
    obs: List[float] = []
    for w in workers:
        obs.append(w.active_tasks / max(max_tasks_per_worker, 1))
        obs.append(w.cpu_util_pct / 100.0)
        obs.append(w.cpu_limit_cores / 2.0)
        obs.append(min(w.ram_usage_kib / max(w.ram_max_kib, 1.0), 10.0))
    obs.append(min(queue_size / 20.0, 10.0))
    for i in range(MAX_QUEUE_OBS):
        if i < len(queue_tasks):
            t    = queue_tasks[i]
            tvec = [0.0] * len(TASK_TYPES)
            tvec[TASK_TYPE_IDX[t.task_type]] = 1.0
            obs.extend(tvec + [t.size_index / 2.0])
        else:
            obs.extend([0.0] * (len(TASK_TYPES) + 1))
    return np.array(obs, dtype=np.float32)

# ---------------------------------------------------------------------------
# PPO update helper (must be called while holding _lock)
# ---------------------------------------------------------------------------

def _run_ppo_update(last_obs: np.ndarray, last_done: bool) -> None:
    global _update_count

    last_tensor = obs_as_tensor(last_obs.reshape(1, -1), model.device)
    with th.no_grad():
        _, last_values, _ = model.policy.forward(last_tensor)

    model.rollout_buffer.compute_returns_and_advantage(
        last_values=last_values,
        dones=np.array([last_done]),
    )
    model.train()                          # PPO gradient update
    model.policy.set_training_mode(False)  # back to eval after update
    model.rollout_buffer.reset()
    _update_count += 1

    model.save(_MODEL_PATH)

# ---------------------------------------------------------------------------
# App
# ---------------------------------------------------------------------------

app = FastAPI(
    title="RL Scheduler Inference",
    version="2.0.0",
    description=(
        "PPO-based task scheduler with online fine-tuning.\n\n"
        "Call **POST /infer** to get a scheduling decision, then "
        "**POST /feedback** to report the reward. Every `ONLINE_BATCH` "
        f"(default: {_ONLINE_BATCH}) transitions trigger a PPO gradient update."
    ),
)


@app.get("/health")
def health() -> dict:
    return {"status": "ok"}


@app.get("/status", response_model=StatusResponse, summary="Online-learning state")
def status() -> StatusResponse:
    with _lock:
        return StatusResponse(
            buffered=model.rollout_buffer.pos,
            pending=len(_pending),
            update_count=_update_count,
        )


@app.post("/infer", response_model=InferResponse, summary="Run scheduling inference")
def infer(req: InferRequest) -> InferResponse:
    global _next_episode_start

    if len(req.workers) != N_WORKERS:
        raise HTTPException(422, f"Expected {N_WORKERS} workers, got {len(req.workers)}.")
    if len(req.queue_tasks) > MAX_QUEUE_OBS:
        raise HTTPException(422, f"queue_tasks may have at most {MAX_QUEUE_OBS} items.")
    for t in req.queue_tasks:
        if t.task_type not in TASK_TYPE_IDX:
            raise HTTPException(422, f"Unknown task_type '{t.task_type}'. Valid: {TASK_TYPES}.")

    obs = _build_obs(req.workers, req.queue_size, req.queue_tasks, req.max_tasks_per_worker)
    obs_tensor = obs_as_tensor(obs.reshape(1, -1), model.device)

    with _lock:
        model.policy.set_training_mode(False)
        with th.no_grad():
            # forward() returns (actions, values, log_probs) — all needed for PPO
            actions_t, values_t, log_probs_t = model.policy.forward(obs_tensor)

        action = int(actions_t.cpu().numpy().flatten()[0])
        rid    = str(uuid.uuid4())
        _pending[rid] = {
            "obs":           obs,
            "action":        action,
            "value":         values_t,     # kept as Tensor for RolloutBuffer.add()
            "log_prob":      log_probs_t,
            "episode_start": _next_episode_start,
        }
        _next_episode_start = False        # subsequent steps are mid-episode

    if action < N_WORKERS:
        return InferResponse(
            request_id=rid,
            action=action,
            action_type="assign",
            worker_index=action,
            description=f"Assign head-of-queue task to worker {action}",
        )
    return InferResponse(
        request_id=rid,
        action=action,
        action_type="wait",
        worker_index=None,
        description="Wait — no assignment this scheduling interval",
    )


@app.post("/feedback", response_model=FeedbackResponse, summary="Report reward and trigger learning")
def feedback(req: FeedbackRequest) -> FeedbackResponse:
    global _next_episode_start

    if len(req.next_workers) != N_WORKERS:
        raise HTTPException(422, f"Expected {N_WORKERS} next_workers, got {len(req.next_workers)}.")
    for t in req.next_queue_tasks:
        if t.task_type not in TASK_TYPE_IDX:
            raise HTTPException(422, f"Unknown task_type '{t.task_type}'.")

    next_obs = _build_obs(
        req.next_workers, req.next_queue_size,
        req.next_queue_tasks, req.next_max_tasks_per_worker,
    )

    with _lock:
        pending = _pending.pop(req.request_id, None)
        if pending is None:
            raise HTTPException(404, "request_id not found or already consumed.")

        model.rollout_buffer.add(
            obs=pending["obs"].reshape(1, -1),
            action=np.array([pending["action"]]),
            reward=np.array([req.reward]),
            episode_start=np.array([pending["episode_start"]]),
            value=pending["value"],
            log_prob=pending["log_prob"],
        )

        if req.done:
            _next_episode_start = True

        updated = model.rollout_buffer.full
        if updated:
            _run_ppo_update(last_obs=next_obs, last_done=req.done)

    return FeedbackResponse(
        buffered=model.rollout_buffer.pos,
        updated=updated,
        update_count=_update_count,
    )
