from __future__ import annotations

from orchestrator.deployment_manager import DeploymentManager
from orchestrator.rollout_manager import RolloutManager
from orchestrator.runtime_manager import RuntimeManager


def main() -> None:
    rt = RuntimeManager()
    dm = DeploymentManager(rt)
    rm = RolloutManager(rt)
    dep = dm.create("demo", "python:3.11-slim", replicas=2)
    out = rm.start_rollout(dep, "python:3.12-slim", canary_fraction=0.2)
    print({"deployment_id": dep, "rollout": out, "deployments": dm.list()})


if __name__ == "__main__":
    main()
