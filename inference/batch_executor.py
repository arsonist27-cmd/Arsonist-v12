from __future__ import annotations

import asyncio
from typing import Any, Awaitable, Callable, Dict, List


class BatchExecutor:
    """Runs bounded concurrent inference tasks with backpressure."""

    def __init__(self, max_concurrency: int = 8) -> None:
        self.sem = asyncio.Semaphore(max_concurrency)

    async def run_many(self, tasks: List[Callable[[], Awaitable[Any]]]) -> List[Any]:
        async def _wrap(fn: Callable[[], Awaitable[Any]]) -> Any:
            async with self.sem:
                return await fn()

        return await asyncio.gather(*[_wrap(t) for t in tasks])
