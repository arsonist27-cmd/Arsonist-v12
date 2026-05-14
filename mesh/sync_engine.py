from __future__ import annotations

import asyncio

from mesh.anti_entropy import AntiEntropyEngine
from mesh.gossip import GossipService
from observability.metrics import MeshMetricsCollector


class SyncEngine:
    """Runs gossip loop concurrently with slower anti-entropy bookkeeping ticks."""

    def __init__(
        self,
        gossip: GossipService,
        anti_entropy: AntiEntropyEngine,
        metrics: MeshMetricsCollector,
        anti_entropy_interval_sec: float = 30.0,
    ) -> None:
        self.gossip = gossip
        self.anti_entropy = anti_entropy
        self.metrics = metrics
        self.anti_entropy_interval_sec = anti_entropy_interval_sec
        self._stop = asyncio.Event()
        self.gossip.bind_stop(self._stop)

    async def run(self) -> None:
        async def anti_entropy_loop() -> None:
            while not self._stop.is_set():
                try:
                    await asyncio.wait_for(self._stop.wait(), timeout=self.anti_entropy_interval_sec)
                except asyncio.TimeoutError:
                    _ = self.anti_entropy.local_summary()
                    self.metrics.anti_entropy_ticks += 1
                else:
                    break

        gossip_task = asyncio.create_task(self.gossip.run())
        ae_task = asyncio.create_task(anti_entropy_loop())
        try:
            await asyncio.gather(gossip_task, ae_task)
        finally:
            self._stop.set()
            self.gossip.stop()
            for t in (gossip_task, ae_task):
                t.cancel()
                try:
                    await t
                except asyncio.CancelledError:
                    pass

    def stop(self) -> None:
        self._stop.set()
        self.gossip.stop()
