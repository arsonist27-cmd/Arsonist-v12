from __future__ import annotations

import random
import tempfile
from pathlib import Path

from mesh.mesh_protocol import ClusterGossipState
from mesh.peer_registry import PeerRegistry


def main() -> None:
    tmp = tempfile.mkdtemp()
    reg = PeerRegistry(db_path=str(Path(tmp) / "p.db"))
    clusters = [f"c{i}" for i in range(12)]
    for _ in range(400):
        cid = random.choice(clusters)
        st = ClusterGossipState(
            cluster_id=cid,
            public_url=f"http://{cid}.local:8000",
            region=random.choice(["us", "eu", "ap"]),
            gpu_capacity=random.randint(0, 4),
            load=random.random(),
            health=random.choice(["healthy", "degraded"]),
            queue_depth=random.randint(0, 50),
            latency_ms=float(random.randint(5, 120)),
            heartbeat_ts=float(random.randint(1_700_000_000, 1_800_000_000)),
            version=random.randint(1, 10_000),
            reliability_score=random.random(),
            hop_distance=random.randint(1, 4),
        )
        reg.merge_state([st])
    peers = reg.list_peers()
    print(
        "stress_merge",
        {
            "unique_clusters": len(peers),
            "expected_cap": len(set(clusters)),
            "samples": [reg.score_peer(p) for p in peers[:5]],
        },
    )


if __name__ == "__main__":
    main()
