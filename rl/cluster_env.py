"""
ClusterSchedulerEnv — Gymnasium environment for RL-based task scheduling.

Simulates a 6-worker cluster using real profiling data collected from
profile_results.csv (solo tasks) and profile_results_pairs.csv (two tasks
running simultaneously on the same worker).

Observation (45 floats):
    Workers (6 × 4):
        active_tasks / max_tasks_per_worker
        cpu_util_pct / 100
        cpu_limit_cores / 2.0          (normalised by max = 2.0)
        ram_usage_kib / ram_max_kib
    Queue (1 + max_queue_obs × 4):
        queue_size / 20                (normalised)
        per visible task: [type_one_hot(3), size_norm(1)]

Action — Discrete(N_WORKERS + 1):
    0..N_WORKERS-1  assign head-of-queue to worker i
    N_WORKERS       wait / do nothing this step

Reward:
    +1  per task completed in the current scheduler interval
    -0.02 × queue_size at end of each step   (latency penalty)
    -1.0 when agent tries to assign to a full worker (invalid action)
"""

from __future__ import annotations

import heapq
from typing import Dict, List, Optional, Tuple

import gymnasium as gym
import numpy as np
import pandas as pd
from gymnasium import spaces


class ClusterSchedulerEnv(gym.Env):
    metadata = {"render_modes": ["human"]}

    # ------------------------------------------------------------------ #
    # Static cluster configuration (mirrors local/workers/*.yaml)         #
    # ------------------------------------------------------------------ #
    N_WORKERS = 6
    WORKER_HOSTS = [
        "worker1:2001",
        "worker2:2002",
        "worker3:2003",
        "worker4:2004",
        "worker5:2005",
        "worker6:2006",
    ]
    # CPU limits and RAM limits per worker (from profile data)
    WORKER_CPU_LIMITS = [0.30, 0.50, 0.80, 1.00, 1.50, 2.00]   # cores
    WORKER_RAM_MAX    = [307200, 512000, 819200, 1048576, 1572864, 2097152]  # KiB

    TASK_TYPES    = ["monte_carlo", "mat_mul", "prime_sieve"]
    TASK_TYPE_IDX = {t: i for i, t in enumerate(TASK_TYPES)}
    TASK_SIZES    = {
        "monte_carlo":  [2_000_000, 4_000_000, 6_000_000],
        "mat_mul":      [100, 150, 200],
        "prime_sieve":  [5_000_000, 10_000_000, 20_000_000],
    }

    # ------------------------------------------------------------------ #
    def __init__(
        self,
        profile_csv: str       = "../cluster/profile_results.csv",
        profile_pairs_csv: str = "../cluster/profile_results_pairs.csv",
        max_tasks_per_worker: int   = 2,
        scheduler_interval_ms: float = 500.0,
        max_queue_obs: int    = 5,
        episode_tasks: int    = 50,
        task_arrival_rate: float = 2.0,   # tasks per second (Poisson)
        render_mode: Optional[str] = None,
    ):
        super().__init__()

        self.max_tasks_per_worker  = max_tasks_per_worker
        self.scheduler_interval_ms = scheduler_interval_ms
        self.max_queue_obs         = max_queue_obs
        self.episode_tasks         = episode_tasks
        self.task_arrival_rate     = task_arrival_rate
        self.render_mode           = render_mode

        self._load_profiles(profile_csv, profile_pairs_csv)

        # Observation: workers (N*4) + queue head (1 + max_queue_obs*4)
        worker_dim = self.N_WORKERS * 4
        queue_dim  = 1 + self.max_queue_obs * (len(self.TASK_TYPES) + 1)
        obs_dim    = worker_dim + queue_dim

        self.observation_space = spaces.Box(
            low=0.0, high=10.0, shape=(obs_dim,), dtype=np.float32
        )
        self.action_space = spaces.Discrete(self.N_WORKERS + 1)

        # Internal counters reset in reset()
        self._task_counter   = 0
        self._current_time   = 0.0
        self._completed_count = 0
        self._total_submitted = 0
        self._worker_state: List[Dict] = []
        self._task_queue: List[Dict]   = []
        self._event_heap: List         = []
        self._running_tasks: Dict[int, float] = {}

    # ------------------------------------------------------------------ #
    # Profile loading                                                      #
    # ------------------------------------------------------------------ #
    def _load_profiles(self, profile_csv: str, profile_pairs_csv: str) -> None:
        host_to_id = {h: i for i, h in enumerate(self.WORKER_HOSTS)}

        df       = pd.read_csv(profile_csv)
        df_pairs = pd.read_csv(profile_pairs_csv)

        # duration_solo[(wid, task_type, size)] -> mean ms (repeats >= 2)
        self._dur_solo:  Dict[Tuple, float] = {}
        self._cpu_solo:  Dict[Tuple, float] = {}
        self._ram_solo:  Dict[Tuple, float] = {}

        for (worker, ttype, size), grp in df.groupby(["worker", "task_type", "size"]):
            wid = host_to_id.get(worker)
            if wid is None:
                continue
            key = (wid, ttype, int(size))
            stable = grp[grp["repeat"] >= 2]
            if stable.empty:
                stable = grp
            self._dur_solo[key] = float(stable["duration_ms"].mean())
            self._cpu_solo[key] = float(stable["cpu_util_pct"].mean())
            self._ram_solo[key] = float(stable["ram_usage_kib"].mean())

        # duration_pair[(wid, task_type, size)] -> mean ms when 2 run together
        self._dur_pair: Dict[Tuple, float] = {}
        self._cpu_pair: Dict[Tuple, float] = {}
        self._ram_pair: Dict[Tuple, float] = {}

        for (worker, ttype, size), grp in df_pairs.groupby(["worker", "task_type", "size"]):
            wid = host_to_id.get(worker)
            if wid is None:
                continue
            key = (wid, ttype, int(size))
            self._dur_pair[key] = float(grp["duration_ms"].mean())
            self._cpu_pair[key] = float(grp["cpu_util_pct"].mean())
            self._ram_pair[key] = float(grp["ram_usage_kib"].mean())

    # ------------------------------------------------------------------ #
    # Duration / metric helpers                                            #
    # ------------------------------------------------------------------ #
    def _task_duration(self, wid: int, ttype: str, size: int, n_concurrent: int) -> float:
        """Return expected task duration in ms given number of concurrent tasks."""
        key = (wid, ttype, size)
        if n_concurrent <= 1:
            if key in self._dur_solo:
                return self._dur_solo[key]
        else:
            if key in self._dur_pair:
                return self._dur_pair[key]
            elif key in self._dur_solo:
                return self._dur_solo[key] * 1.5   # rough pair penalty

        # Fallback: scale from another worker with same task profile
        return self._fallback_duration(wid, ttype, size, n_concurrent)

    def _fallback_duration(self, wid: int, ttype: str, size: int, n_concurrent: int) -> float:
        base_cpu = self.WORKER_CPU_LIMITS[wid]
        for ref_wid in range(self.N_WORKERS):
            ref_key = (ref_wid, ttype, size)
            if ref_key in self._dur_solo:
                ref_cpu = self.WORKER_CPU_LIMITS[ref_wid]
                scale   = ref_cpu / base_cpu if base_cpu > 0 else 1.0
                dur     = self._dur_solo[ref_key] * scale
                return dur if n_concurrent <= 1 else dur * 1.5
        return 1000.0  # last-resort default: 1 s

    def _worker_metrics(self, wid: int) -> Tuple[float, float]:
        """Return (cpu_util_pct, ram_usage_kib) for a worker based on its tasks."""
        w = self._worker_state[wid]
        tasks = w["tasks"]
        if not tasks:
            return 0.0, 0.0

        n = len(tasks)
        cpu_total = ram_total = 0.0
        for t in tasks:
            key = (wid, t["type"], t["size"])
            if n <= 1:
                cpu_total += self._cpu_solo.get(key, 50.0)
                ram_total += self._ram_solo.get(key, 10_000.0)
            else:
                cpu_total += self._cpu_pair.get(key, self._cpu_solo.get(key, 50.0) * 1.2)
                ram_total += self._ram_pair.get(key, self._ram_solo.get(key, 10_000.0))
        return cpu_total, ram_total

    # ------------------------------------------------------------------ #
    # Task generation                                                      #
    # ------------------------------------------------------------------ #
    def _sample_task(self) -> Dict:
        self._task_counter += 1
        ttype = self.np_random.choice(self.TASK_TYPES)
        size  = int(self.np_random.choice(self.TASK_SIZES[ttype]))
        return {
            "id":           self._task_counter,
            "type":         ttype,
            "size":         size,
            "arrival_time": self._current_time,
        }

    def _normalize_size(self, ttype: str, size: int) -> float:
        sizes = self.TASK_SIZES[ttype]
        try:
            idx = sizes.index(size)
        except ValueError:
            idx = 0
        return idx / max(len(sizes) - 1, 1)

    # ------------------------------------------------------------------ #
    # Observation                                                          #
    # ------------------------------------------------------------------ #
    def _get_obs(self) -> np.ndarray:
        obs: List[float] = []

        for wid in range(self.N_WORKERS):
            w      = self._worker_state[wid]
            active = len(w["tasks"])
            cpu, ram = self._worker_metrics(wid)

            obs.append(active / max(self.max_tasks_per_worker, 1))
            obs.append(cpu / 100.0)
            obs.append(self.WORKER_CPU_LIMITS[wid] / 2.0)
            obs.append(min(ram / max(self.WORKER_RAM_MAX[wid], 1), 10.0))

        # Queue
        qlen = len(self._task_queue)
        obs.append(min(qlen / 20.0, 10.0))
        for i in range(self.max_queue_obs):
            if i < qlen:
                t    = self._task_queue[i]
                tvec = [0.0] * len(self.TASK_TYPES)
                tvec[self.TASK_TYPE_IDX[t["type"]]] = 1.0
                snorm = self._normalize_size(t["type"], t["size"])
                obs.extend(tvec + [snorm])
            else:
                obs.extend([0.0] * (len(self.TASK_TYPES) + 1))

        return np.array(obs, dtype=np.float32)

    # ------------------------------------------------------------------ #
    # Time simulation                                                      #
    # ------------------------------------------------------------------ #
    def _advance_time(self) -> float:
        """
        Advance simulation clock by scheduler_interval_ms.
        Returns the reward contribution from task completions in this interval.
        """
        target = self._current_time + self.scheduler_interval_ms
        reward = 0.0

        # Process completions
        while self._event_heap and self._event_heap[0][0] <= target:
            _, task_id, wid = heapq.heappop(self._event_heap)
            w = self._worker_state[wid]
            w["tasks"] = [t for t in w["tasks"] if t["id"] != task_id]
            self._running_tasks.pop(task_id, None)
            self._completed_count += 1
            reward += 1.0

        # Poisson arrivals
        interval_s = self.scheduler_interval_ms / 1000.0
        n_arrivals = self.np_random.poisson(self.task_arrival_rate * interval_s)
        for _ in range(n_arrivals):
            if self._total_submitted < self.episode_tasks:
                task = self._sample_task()
                # Uniform arrival time within the interval
                offset = self.np_random.uniform(0.0, self.scheduler_interval_ms)
                task["arrival_time"] = self._current_time + offset
                self._task_queue.append(task)
                self._total_submitted += 1

        self._current_time = target
        return reward

    # ------------------------------------------------------------------ #
    # Gymnasium API                                                        #
    # ------------------------------------------------------------------ #
    def reset(self, *, seed: Optional[int] = None, options=None):
        super().reset(seed=seed)

        self._current_time    = 0.0
        self._task_counter    = 0
        self._total_submitted = 0
        self._completed_count = 0
        self._task_queue      = []
        self._event_heap      = []
        self._running_tasks   = {}

        self._worker_state = [{"tasks": []} for _ in range(self.N_WORKERS)]

        # Seed initial task batch
        n_init = self.np_random.poisson(self.task_arrival_rate * self.scheduler_interval_ms / 1000.0)
        for _ in range(min(n_init, self.episode_tasks)):
            self._task_queue.append(self._sample_task())
            self._total_submitted += 1

        obs  = self._get_obs()
        info = {"time_ms": 0.0, "completed": 0, "queue": len(self._task_queue)}
        return obs, info

    def step(self, action: int):
        invalid_action = False
        assigned       = False

        # Try to assign if action is a worker index and queue is non-empty
        if action < self.N_WORKERS and self._task_queue:
            wid = action
            w   = self._worker_state[wid]

            if len(w["tasks"]) < self.max_tasks_per_worker:
                task        = self._task_queue.pop(0)
                n_conc      = len(w["tasks"]) + 1
                duration    = self._task_duration(wid, task["type"], task["size"], n_conc)
                # Add ±5 % noise
                jitter   = float(self.np_random.normal(0.0, duration * 0.05))
                duration = max(duration + jitter, duration * 0.3)

                end_time = self._current_time + duration
                heapq.heappush(self._event_heap, (end_time, task["id"], wid))
                w["tasks"].append({
                    "id":   task["id"],
                    "type": task["type"],
                    "size": task["size"],
                })
                self._running_tasks[task["id"]] = task["arrival_time"]
                assigned = True
            else:
                # Agent tried to assign to a full worker
                invalid_action = True

        # Advance simulation clock
        completion_reward = self._advance_time()

        # Reward components
        reward  = completion_reward                           # +1 per completed task
        reward -= 0.02 * len(self._task_queue)               # latency penalty
        if invalid_action:
            reward -= 1.0                                    # penalty for bad action

        # Termination: all tasks submitted and completed, queues empty
        all_submitted  = self._total_submitted >= self.episode_tasks
        all_done       = all_submitted and self._completed_count >= self._total_submitted
        queue_empty    = len(self._task_queue) == 0
        workers_idle   = all(len(w["tasks"]) == 0 for w in self._worker_state)
        terminated     = all_done and queue_empty and workers_idle

        obs  = self._get_obs()
        info = {
            "time_ms":   self._current_time,
            "completed": self._completed_count,
            "submitted": self._total_submitted,
            "queue":     len(self._task_queue),
            "assigned":  assigned,
        }
        return obs, float(reward), terminated, False, info

    def render(self):
        if self.render_mode != "human":
            return
        print(
            f"t={self._current_time:8.0f} ms | "
            f"done={self._completed_count:3d}/{self.episode_tasks} | "
            f"queue={len(self._task_queue):2d}"
        )
        for wid in range(self.N_WORKERS):
            w      = self._worker_state[wid]
            cpu, _ = self._worker_metrics(wid)
            tasks  = ", ".join(
                f"{t['type'][:3]}({t['size']})" for t in w["tasks"]
            ) or "idle"
            print(
                f"  w{wid+1} [{self.WORKER_CPU_LIMITS[wid]:.2f}c] "
                f"cpu={cpu:5.1f}%  {tasks}"
            )
