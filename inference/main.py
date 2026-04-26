"""
RL Scheduler Inference Service.

Accepts cluster state (worker metrics + queue snapshot), builds the
45-float observation vector expected by the PPO model, runs inference,
and returns the scheduling action.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import List, Optional

import numpy as np
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from stable_baselines3 import PPO

# ---------------------------------------------------------------------------
# Constants (must match ClusterSchedulerEnv)
# ---------------------------------------------------------------------------
N_WORKERS      = 6
MAX_QUEUE_OBS  = 5
TASK_TYPES     = ["monte_carlo", "mat_mul", "prime_sieve"]
TASK_TYPE_IDX  = {t: i for i, t in enumerate(TASK_TYPES)}
ACTION_WAIT    = N_WORKERS  # action index meaning "wait"

_HERE       = Path(__file__).parent
_MODEL_PATH = os.environ.get(
    "MODEL_PATH",
    str(_HERE / "../rl/models/best/best_model.zip"),
)

# ---------------------------------------------------------------------------
# Load model once at startup
# ---------------------------------------------------------------------------
model: PPO = PPO.load(_MODEL_PATH)

# ---------------------------------------------------------------------------
# Request / response schemas
# ---------------------------------------------------------------------------

class WorkerState(BaseModel):
    active_tasks:    int   = Field(ge=0,    description="Number of active tasks on this worker")
    cpu_util_pct:    float = Field(ge=0.0, le=100.0, description="CPU utilisation, percent")
    cpu_limit_cores: float = Field(gt=0.0, description="CPU limit in cores")
    ram_usage_kib:   float = Field(ge=0.0, description="RAM usage in KiB")
    ram_max_kib:     float = Field(gt=0.0, description="RAM limit in KiB")


class TaskInfo(BaseModel):
    task_type:  str = Field(description="One of: monte_carlo, mat_mul, prime_sieve")
    size_index: int = Field(ge=0, le=2, description="Task size: 0=small, 1=medium, 2=large")


class InferRequest(BaseModel):
    workers:              List[WorkerState] = Field(description="Exactly 6 worker states, indexed 0-5")
    queue_size:           int               = Field(ge=0, description="Total number of tasks currently in the queue")
    queue_tasks:          List[TaskInfo]    = Field(default=[], description="First up to 5 tasks in the queue (head first)")
    max_tasks_per_worker: int               = Field(default=2, ge=1, description="Maximum concurrent tasks allowed per worker")


class InferResponse(BaseModel):
    action:       int           = Field(description="Raw action index: 0-5 assign to worker, 6 wait")
    action_type:  str           = Field(description="'assign' or 'wait'")
    worker_index: Optional[int] = Field(default=None, description="Target worker (0-5) when action_type is 'assign', else null")
    description:  str           = Field(description="Human-readable summary of the decision")


# ---------------------------------------------------------------------------
# Observation builder
# ---------------------------------------------------------------------------

def _build_obs(req: InferRequest) -> np.ndarray:
    obs: List[float] = []

    for w in req.workers:
        obs.append(w.active_tasks / max(req.max_tasks_per_worker, 1))
        obs.append(w.cpu_util_pct / 100.0)
        obs.append(w.cpu_limit_cores / 2.0)
        obs.append(min(w.ram_usage_kib / max(w.ram_max_kib, 1.0), 10.0))

    # Queue head
    obs.append(min(req.queue_size / 20.0, 10.0))
    for i in range(MAX_QUEUE_OBS):
        if i < len(req.queue_tasks):
            t    = req.queue_tasks[i]
            tvec = [0.0] * len(TASK_TYPES)
            tvec[TASK_TYPE_IDX[t.task_type]] = 1.0
            size_norm = t.size_index / 2.0  # 0.0 / 0.5 / 1.0
            obs.extend(tvec + [size_norm])
        else:
            obs.extend([0.0] * (len(TASK_TYPES) + 1))

    return np.array(obs, dtype=np.float32)


# ---------------------------------------------------------------------------
# App
# ---------------------------------------------------------------------------

app = FastAPI(
    title="RL Scheduler Inference",
    version="1.0.0",
    description=(
        "Runs a PPO-based scheduling policy. "
        "Given cluster state, returns the next scheduling action."
    ),
)


@app.get("/health")
def health() -> dict:
    return {"status": "ok"}


@app.post("/infer", response_model=InferResponse, summary="Run scheduling inference")
def infer(req: InferRequest) -> InferResponse:
    if len(req.workers) != N_WORKERS:
        raise HTTPException(
            status_code=422,
            detail=f"Expected exactly {N_WORKERS} workers, got {len(req.workers)}.",
        )
    if len(req.queue_tasks) > MAX_QUEUE_OBS:
        raise HTTPException(
            status_code=422,
            detail=f"queue_tasks may contain at most {MAX_QUEUE_OBS} items.",
        )
    for t in req.queue_tasks:
        if t.task_type not in TASK_TYPE_IDX:
            raise HTTPException(
                status_code=422,
                detail=f"Unknown task_type '{t.task_type}'. Valid: {TASK_TYPES}.",
            )

    obs        = _build_obs(req)
    action_raw, _ = model.predict(obs, deterministic=True)
    action     = int(action_raw)

    if action < N_WORKERS:
        return InferResponse(
            action=action,
            action_type="assign",
            worker_index=action,
            description=f"Assign head-of-queue task to worker {action}",
        )
    return InferResponse(
        action=action,
        action_type="wait",
        worker_index=None,
        description="Wait — no assignment this scheduling interval",
    )
