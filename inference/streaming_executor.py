from __future__ import annotations

import asyncio
from typing import Any, AsyncIterator, Dict, List


class StreamingExecutor:
    """Streams tokens from a backend that supports stream=true (chunk parsing simplified)."""

    def __init__(self, chunk_delay_ms: float = 0.0) -> None:
        self.chunk_delay_ms = chunk_delay_ms

    async def stream_text(self, chunks: List[str]) -> AsyncIterator[str]:
        for c in chunks:
            if self.chunk_delay_ms:
                await asyncio.sleep(self.chunk_delay_ms / 1000.0)
            yield c
