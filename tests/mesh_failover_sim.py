from __future__ import annotations

import tempfile
from pathlib import Path

from mesh.mesh_failover import mesh_failover_snapshot, suggest_failover_target
from mesh.peer_registry import PeerRecord, PeerRegistry


class _Mem:
    def __init__(self) -> None:
        self.jobs = {}
        self.nodes = {}


def main() -> None:
    tmp = tempfile.mkdtemp()
    reg = PeerRegistry(db_path=str(Path(tmp) / "pf.db"))
    reg.merge_state(
        [
            PeerRecord(
                cluster_id="a",
                public_url="http://a:8000",
                region="us",
                gpu_capacity=2,
                load=0.2,
                health="healthy",
                latency_estimate_ms=20,
                last_seen=1_800_000_000,
                queue_depth=1,
                version=3,
                reliability_score=0.95,
                hop_distance=1,
            ),
            PeerRecord(
                cluster_id="b",
                public_url="http://b:8000",
                region="eu",
                gpu_capacity=0,
                load=0.9,
                health="degraded",
                latency_estimate_ms=200,
                last_seen=1_800_000_000,
                queue_depth=40,
                version=2,
                reliability_score=0.6,
                hop_distance=3,
            ),
        ]
    )
    mem = _Mem()
    print("failover_snapshot", mesh_failover_snapshot(mem, reg))
    print("suggest", suggest_failover_target(reg, prefer_region="us", require_gpu=True))


if __name__ == "__main__":
    main()
