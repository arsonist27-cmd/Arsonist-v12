from __future__ import annotations

import asyncio

from inference.batch_executor import BatchExecutor
from telemetry.inference_metrics import InferenceMetrics


async def main() -> None:
    metrics = InferenceMetrics()
    ex = BatchExecutor(max_concurrency=4)

    async def task(i: int):
        metrics.record_chat(model="m", latency_ms=float(i * 10), tokens_out=i * 5)
        return i

    out = await ex.run_many([lambda i=i: task(i) for i in range(12)])
    print({"results": out, "metrics": metrics.snapshot()})


if __name__ == "__main__":
    asyncio.run(main())
