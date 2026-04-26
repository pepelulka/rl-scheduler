#!/usr/bin/env bash
# Test requests for the RL Scheduler Inference service.
# Requires the service to be running on localhost:8000.

BASE="http://localhost:8000"

echo "=== 1. Health check ==="
curl -s "$BASE/health" | python3 -m json.tool

echo ""
echo "=== 2. Normal case: worker 0 is busy, rest are idle, queue has 3 tasks ==="
curl -s -X POST "$BASE/infer" \
  -H "Content-Type: application/json" \
  -d '{
    "max_tasks_per_worker": 2,
    "workers": [
      {"active_tasks": 1, "cpu_util_pct": 45.2, "cpu_limit_cores": 0.30, "ram_usage_kib": 51200,   "ram_max_kib": 307200},
      {"active_tasks": 0, "cpu_util_pct":  0.0, "cpu_limit_cores": 0.50, "ram_usage_kib":      0,  "ram_max_kib": 512000},
      {"active_tasks": 0, "cpu_util_pct":  0.0, "cpu_limit_cores": 0.80, "ram_usage_kib":      0,  "ram_max_kib": 819200},
      {"active_tasks": 0, "cpu_util_pct":  0.0, "cpu_limit_cores": 1.00, "ram_usage_kib":      0,  "ram_max_kib": 1048576},
      {"active_tasks": 0, "cpu_util_pct":  0.0, "cpu_limit_cores": 1.50, "ram_usage_kib":      0,  "ram_max_kib": 1572864},
      {"active_tasks": 0, "cpu_util_pct":  0.0, "cpu_limit_cores": 2.00, "ram_usage_kib":      0,  "ram_max_kib": 2097152}
    ],
    "queue_size": 3,
    "queue_tasks": [
      {"task_type": "monte_carlo", "size_index": 1},
      {"task_type": "mat_mul",     "size_index": 0}
    ]
  }' | python3 -m json.tool

echo ""
echo "=== 3. All workers full — expect action_type=wait ==="
curl -s -X POST "$BASE/infer" \
  -H "Content-Type: application/json" \
  -d '{
    "max_tasks_per_worker": 2,
    "workers": [
      {"active_tasks": 2, "cpu_util_pct": 90.0, "cpu_limit_cores": 0.30, "ram_usage_kib": 280000,  "ram_max_kib": 307200},
      {"active_tasks": 2, "cpu_util_pct": 88.0, "cpu_limit_cores": 0.50, "ram_usage_kib": 480000,  "ram_max_kib": 512000},
      {"active_tasks": 2, "cpu_util_pct": 85.0, "cpu_limit_cores": 0.80, "ram_usage_kib": 800000,  "ram_max_kib": 819200},
      {"active_tasks": 2, "cpu_util_pct": 82.0, "cpu_limit_cores": 1.00, "ram_usage_kib": 1000000, "ram_max_kib": 1048576},
      {"active_tasks": 2, "cpu_util_pct": 78.0, "cpu_limit_cores": 1.50, "ram_usage_kib": 1500000, "ram_max_kib": 1572864},
      {"active_tasks": 2, "cpu_util_pct": 75.0, "cpu_limit_cores": 2.00, "ram_usage_kib": 2000000, "ram_max_kib": 2097152}
    ],
    "queue_size": 5,
    "queue_tasks": [
      {"task_type": "prime_sieve", "size_index": 2}
    ]
  }' | python3 -m json.tool

echo ""
echo "=== 4. Empty queue — no tasks visible ==="
curl -s -X POST "$BASE/infer" \
  -H "Content-Type: application/json" \
  -d '{
    "max_tasks_per_worker": 2,
    "workers": [
      {"active_tasks": 0, "cpu_util_pct": 0.0, "cpu_limit_cores": 0.30, "ram_usage_kib": 0, "ram_max_kib": 307200},
      {"active_tasks": 0, "cpu_util_pct": 0.0, "cpu_limit_cores": 0.50, "ram_usage_kib": 0, "ram_max_kib": 512000},
      {"active_tasks": 0, "cpu_util_pct": 0.0, "cpu_limit_cores": 0.80, "ram_usage_kib": 0, "ram_max_kib": 819200},
      {"active_tasks": 0, "cpu_util_pct": 0.0, "cpu_limit_cores": 1.00, "ram_usage_kib": 0, "ram_max_kib": 1048576},
      {"active_tasks": 0, "cpu_util_pct": 0.0, "cpu_limit_cores": 1.50, "ram_usage_kib": 0, "ram_max_kib": 1572864},
      {"active_tasks": 0, "cpu_util_pct": 0.0, "cpu_limit_cores": 2.00, "ram_usage_kib": 0, "ram_max_kib": 2097152}
    ],
    "queue_size": 0,
    "queue_tasks": []
  }' | python3 -m json.tool

echo ""
echo "=== 5. Validation error: wrong number of workers — expect HTTP 422 ==="
curl -s -X POST "$BASE/infer" \
  -H "Content-Type: application/json" \
  -d '{
    "workers": [],
    "queue_size": 0
  }' | python3 -m json.tool

echo ""
echo "=== 6. Validation error: unknown task_type — expect HTTP 422 ==="
curl -s -X POST "$BASE/infer" \
  -H "Content-Type: application/json" \
  -d '{
    "max_tasks_per_worker": 2,
    "workers": [
      {"active_tasks": 0, "cpu_util_pct": 0.0, "cpu_limit_cores": 0.30, "ram_usage_kib": 0, "ram_max_kib": 307200},
      {"active_tasks": 0, "cpu_util_pct": 0.0, "cpu_limit_cores": 0.50, "ram_usage_kib": 0, "ram_max_kib": 512000},
      {"active_tasks": 0, "cpu_util_pct": 0.0, "cpu_limit_cores": 0.80, "ram_usage_kib": 0, "ram_max_kib": 819200},
      {"active_tasks": 0, "cpu_util_pct": 0.0, "cpu_limit_cores": 1.00, "ram_usage_kib": 0, "ram_max_kib": 1048576},
      {"active_tasks": 0, "cpu_util_pct": 0.0, "cpu_limit_cores": 1.50, "ram_usage_kib": 0, "ram_max_kib": 1572864},
      {"active_tasks": 0, "cpu_util_pct": 0.0, "cpu_limit_cores": 2.00, "ram_usage_kib": 0, "ram_max_kib": 2097152}
    ],
    "queue_size": 1,
    "queue_tasks": [
      {"task_type": "unknown_type", "size_index": 0}
    ]
  }' | python3 -m json.tool
