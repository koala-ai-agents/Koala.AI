"""Minimal benchmark harness for Kola-AI.

Measures executor throughput and latency for simple flows.
This is intentionally lightweight and avoids external deps.
"""

from __future__ import annotations

import time
from statistics import mean
from typing import List

from koala.flow import LocalExecutor, dag
from koala.tools import default_registry, tool


@tool("noop")
def noop(x: int) -> int:
    return x


def run_benchmark(iterations: int = 100) -> dict:
    ex = LocalExecutor()
    registry = {name: meta.func for name, meta in default_registry._tools.items()}
    latencies: List[float] = []

    flow = dag("bench-noop").step("a", "noop", x=1).build()

    for _ in range(iterations):
        t0 = time.perf_counter()
        res = ex.run_dagflow(flow, registry)
        t1 = time.perf_counter()
        assert res.get("a") == 1
        latencies.append(t1 - t0)

    return {
        "iterations": iterations,
        "avg_latency_ms": mean(latencies) * 1000.0,
        "min_latency_ms": min(latencies) * 1000.0,
        "max_latency_ms": max(latencies) * 1000.0,
        "throughput_ops_sec": iterations / sum(latencies),
    }


def main():
    stats = run_benchmark(iterations=200)
    print("Benchmark stats:", stats)


if __name__ == "__main__":
    main()
