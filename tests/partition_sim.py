from __future__ import annotations

from mesh.partition_handler import PartitionHandler


def main() -> None:
    ph = PartitionHandler()
    ph.mark_unreachable(["east-1", "east-2"])
    snap1 = ph.snapshot()
    ph.mark_reachable("east-1")
    snap2 = ph.snapshot()
    ph.classify_event("PARTITION_DETECTED")
    print("partition_sim", {"phase1": snap1, "phase2": snap2, "handler": ph.snapshot()})


if __name__ == "__main__":
    main()
