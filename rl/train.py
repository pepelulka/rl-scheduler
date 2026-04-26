"""
PPO training script for ClusterSchedulerEnv.

Dependencies:
    pip install gymnasium stable-baselines3[extra] torch pandas numpy

Usage:
    # Train from scratch
    python train.py

    # Evaluate a saved model
    python train.py --eval --model-path models/best/best_model.zip
"""

from __future__ import annotations

import argparse
import os
from pathlib import Path
from typing import Callable

import numpy as np
from stable_baselines3 import PPO
from stable_baselines3.common.callbacks import (
    BaseCallback,
    CheckpointCallback,
    EvalCallback,
)
from stable_baselines3.common.env_util import make_vec_env
from stable_baselines3.common.monitor import Monitor
from stable_baselines3.common.vec_env import VecEnv

from cluster_env import ClusterSchedulerEnv

# ------------------------------------------------------------------ #
# Paths                                                               #
# ------------------------------------------------------------------ #
_HERE          = Path(__file__).parent
PROFILE_CSV    = str(_HERE / "../cluster/profile_results.csv")
PAIR_CSV       = str(_HERE / "../cluster/profile_results_pairs.csv")
MODELS_DIR     = str(_HERE / "models")


# ------------------------------------------------------------------ #
# Environment factory                                                 #
# ------------------------------------------------------------------ #
def make_env(
    episode_tasks: int    = 50,
    task_arrival_rate: float = 2.0,
    max_tasks_per_worker: int = 2,
    scheduler_interval_ms: float = 500.0,
    max_queue_obs: int = 5,
    render_mode: str | None = None,
) -> Callable[[], Monitor]:
    """Return a thunk that creates a monitored ClusterSchedulerEnv."""
    def _thunk() -> Monitor:
        env = ClusterSchedulerEnv(
            profile_csv           = PROFILE_CSV,
            profile_pairs_csv     = PAIR_CSV,
            max_tasks_per_worker  = max_tasks_per_worker,
            scheduler_interval_ms = scheduler_interval_ms,
            max_queue_obs         = max_queue_obs,
            episode_tasks         = episode_tasks,
            task_arrival_rate     = task_arrival_rate,
            render_mode           = render_mode,
        )
        return Monitor(env)
    return _thunk


# ------------------------------------------------------------------ #
# Custom callback                                                     #
# ------------------------------------------------------------------ #
class EpisodeStatsCallback(BaseCallback):
    """Logs mean episode reward and length to stdout every `log_freq` steps."""

    def __init__(self, log_freq: int = 10_000, verbose: int = 0):
        super().__init__(verbose)
        self.log_freq    = log_freq
        self._ep_rewards: list[float] = []
        self._ep_lengths: list[int]   = []

    def _on_step(self) -> bool:
        infos = self.locals.get("infos", [])
        for info in infos:
            if "episode" in info:
                ep = info["episode"]
                self._ep_rewards.append(ep["r"])
                self._ep_lengths.append(ep["l"])

        if self.n_calls % self.log_freq == 0 and self._ep_rewards:
            mean_r = np.mean(self._ep_rewards[-50:])
            mean_l = np.mean(self._ep_lengths[-50:])
            print(
                f"step={self.n_calls:>8d} | "
                f"ep_rew_mean={mean_r:7.2f} | "
                f"ep_len_mean={mean_l:6.1f}"
            )
        return True


# ------------------------------------------------------------------ #
# Training                                                            #
# ------------------------------------------------------------------ #
def train(
    total_timesteps: int   = 1_000_000,
    n_envs: int            = 8,
    episode_tasks: int     = 50,
    task_arrival_rate: float = 2.0,
    learning_rate: float   = 3e-4,
    n_steps: int           = 2048,
    batch_size: int        = 256,
    n_epochs: int          = 10,
    gamma: float           = 0.99,
    gae_lambda: float      = 0.95,
    clip_range: float      = 0.2,
    ent_coef: float        = 0.01,
    save_dir: str          = MODELS_DIR,
    eval_freq_steps: int   = 20_000,
    n_eval_episodes: int   = 10,
    checkpoint_freq: int   = 100_000,
) -> PPO:
    os.makedirs(save_dir, exist_ok=True)

    env_fn = make_env(episode_tasks=episode_tasks, task_arrival_rate=task_arrival_rate)

    train_env = make_vec_env(env_fn, n_envs=n_envs)
    eval_env  = make_vec_env(
        make_env(episode_tasks=episode_tasks, task_arrival_rate=task_arrival_rate),
        n_envs=1,
    )

    model = PPO(
        policy            = "MlpPolicy",
        env               = train_env,
        learning_rate     = learning_rate,
        n_steps           = n_steps,
        batch_size        = batch_size,
        n_epochs          = n_epochs,
        gamma             = gamma,
        gae_lambda        = gae_lambda,
        clip_range        = clip_range,
        ent_coef          = ent_coef,
        verbose           = 1,
        tensorboard_log   = os.path.join(save_dir, "tensorboard"),
        policy_kwargs     = dict(net_arch=[256, 256]),
    )

    callbacks = [
        EpisodeStatsCallback(log_freq=10_000),
        EvalCallback(
            eval_env,
            best_model_save_path = os.path.join(save_dir, "best"),
            log_path             = os.path.join(save_dir, "logs"),
            eval_freq            = max(eval_freq_steps // n_envs, 1),
            n_eval_episodes      = n_eval_episodes,
            deterministic        = True,
            render               = False,
        ),
        CheckpointCallback(
            save_freq  = max(checkpoint_freq // n_envs, 1),
            save_path  = os.path.join(save_dir, "checkpoints"),
            name_prefix = "ppo_scheduler",
        ),
    ]

    print(f"Training PPO for {total_timesteps:,} steps on {n_envs} envs …")
    model.learn(
        total_timesteps      = total_timesteps,
        callback             = callbacks,
        progress_bar         = True,
    )

    final_path = os.path.join(save_dir, "final_model")
    model.save(final_path)
    print(f"Model saved → {final_path}.zip")

    train_env.close()
    eval_env.close()
    return model


# ------------------------------------------------------------------ #
# Evaluation helper                                                   #
# ------------------------------------------------------------------ #
def evaluate(
    model_path: str,
    n_episodes: int  = 20,
    episode_tasks: int = 50,
    render: bool     = False,
) -> dict:
    model = PPO.load(model_path)
    env   = ClusterSchedulerEnv(
        profile_csv       = PROFILE_CSV,
        profile_pairs_csv = PAIR_CSV,
        episode_tasks     = episode_tasks,
        render_mode       = "human" if render else None,
    )

    rewards: list[float] = []
    completions: list[int] = []
    times: list[float] = []

    for ep in range(n_episodes):
        obs, _ = env.reset()
        ep_reward = 0.0
        done = False
        while not done:
            action, _ = model.predict(obs, deterministic=True)
            obs, reward, terminated, truncated, info = env.step(int(action))
            ep_reward += reward
            done = terminated or truncated
            if render:
                env.render()

        rewards.append(ep_reward)
        completions.append(info["completed"])
        times.append(info["time_ms"])

    results = {
        "mean_reward":     float(np.mean(rewards)),
        "std_reward":      float(np.std(rewards)),
        "mean_completed":  float(np.mean(completions)),
        "mean_time_ms":    float(np.mean(times)),
    }
    print("\n=== Evaluation results ===")
    for k, v in results.items():
        print(f"  {k}: {v:.2f}")
    return results


# ------------------------------------------------------------------ #
# Baseline: LeastLoaded scheduler (no RL)                            #
# ------------------------------------------------------------------ #
def run_least_loaded_baseline(
    n_episodes: int    = 20,
    episode_tasks: int = 50,
    task_arrival_rate: float = 2.0,
) -> dict:
    """Run the LeastLoadedScheduler heuristic for comparison."""
    env = ClusterSchedulerEnv(
        profile_csv       = PROFILE_CSV,
        profile_pairs_csv = PAIR_CSV,
        episode_tasks     = episode_tasks,
        task_arrival_rate = task_arrival_rate,
    )

    rewards: list[float] = []
    completions: list[int] = []
    times: list[float] = []

    for _ in range(n_episodes):
        obs, _ = env.reset()
        ep_reward = 0.0
        done = False
        while not done:
            # Pick worker with fewest active tasks; wait if all full
            worker_loads = [
                len(env._worker_state[wid]["tasks"])
                for wid in range(env.N_WORKERS)
            ]
            if env._task_queue:
                min_load = min(worker_loads)
                if min_load < env.max_tasks_per_worker:
                    action = int(np.argmin(worker_loads))
                else:
                    action = env.N_WORKERS  # wait
            else:
                action = env.N_WORKERS  # wait

            obs, reward, terminated, truncated, info = env.step(action)
            ep_reward += reward
            done = terminated or truncated

        rewards.append(ep_reward)
        completions.append(info["completed"])
        times.append(info["time_ms"])

    results = {
        "mean_reward":    float(np.mean(rewards)),
        "std_reward":     float(np.std(rewards)),
        "mean_completed": float(np.mean(completions)),
        "mean_time_ms":   float(np.mean(times)),
    }
    print("\n=== Baseline (LeastLoaded) results ===")
    for k, v in results.items():
        print(f"  {k}: {v:.2f}")
    return results


# ------------------------------------------------------------------ #
# CLI                                                                  #
# ------------------------------------------------------------------ #
def main() -> None:
    parser = argparse.ArgumentParser(description="PPO scheduler trainer / evaluator")
    parser.add_argument("--eval",       action="store_true", help="Evaluate a saved model")
    parser.add_argument("--baseline",   action="store_true", help="Run LeastLoaded baseline")
    parser.add_argument("--model-path", type=str, default=os.path.join(MODELS_DIR, "best/best_model.zip"))
    parser.add_argument("--timesteps",  type=int, default=1_000_000)
    parser.add_argument("--n-envs",     type=int, default=8)
    parser.add_argument("--episodes",   type=int, default=50,  help="Tasks per episode")
    parser.add_argument("--n-eval",     type=int, default=20,  help="Evaluation episodes")
    parser.add_argument("--render",     action="store_true")
    args = parser.parse_args()

    if args.baseline:
        run_least_loaded_baseline(n_episodes=args.n_eval, episode_tasks=args.episodes)
    elif args.eval:
        evaluate(
            model_path    = args.model_path,
            n_episodes    = args.n_eval,
            episode_tasks = args.episodes,
            render        = args.render,
        )
    else:
        train(
            total_timesteps = args.timesteps,
            n_envs          = args.n_envs,
            episode_tasks   = args.episodes,
        )


if __name__ == "__main__":
    main()
