from __future__ import annotations

import json
import os
import subprocess
import tempfile
from pathlib import Path
from typing import Any, Dict, List, Optional

from containers.sandbox_profiles import SandboxProfile


class ContainerRuntime:
    """
    Docker-backed container execution with structured metadata.
    containerd can be added later by swapping command generation.
    """

    def __init__(self, runtime: str = "docker") -> None:
        self.runtime = runtime

    def run_workload(
        self,
        *,
        image: str,
        command: List[str],
        env: Dict[str, str] | None = None,
        profile: SandboxProfile | None = None,
        labels: Dict[str, str] | None = None,
        timeout_sec: int = 300,
    ) -> Dict[str, Any]:
        env = env or {}
        profile = profile or SandboxProfile()
        labels = labels or {}
        with tempfile.TemporaryDirectory(prefix="arsonist-ctr-") as tmpdir:
            meta_path = Path(tmpdir) / "meta.json"
            meta_path.write_text(json.dumps({"labels": labels, "image": image}), encoding="utf-8")
            cmd: List[str] = [self.runtime, "run", "--rm"]
            for k, v in labels.items():
                cmd += ["--label", f"{k}={v}"]
            if profile.read_only_root:
                cmd += ["--read-only"]
            if profile.network == "none":
                cmd += ["--network", "none"]
            if profile.memory_mb:
                cmd += ["--memory", f"{profile.memory_mb}m"]
            if profile.cpu_quota_cpus:
                cmd += ["--cpus", str(profile.cpu_quota_cpus)]
            if profile.gpu_device_requests:
                for dev in profile.gpu_device_requests:
                    cmd += ["--device", dev]
            for key, val in env.items():
                cmd += ["-e", f"{key}={val}"]
            cmd += ["-v", f"{meta_path}:/meta.json:ro", image, *command]
            try:
                proc = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout_sec, check=False)
                return {
                    "ok": proc.returncode == 0,
                    "exit_code": proc.returncode,
                    "stdout": proc.stdout,
                    "stderr": proc.stderr,
                    "runtime": self.runtime,
                }
            except subprocess.TimeoutExpired:
                return {"ok": False, "exit_code": 124, "stdout": "", "stderr": "timeout", "runtime": self.runtime}

    def health_check(self, container_name: str) -> bool:
        try:
            proc = subprocess.run(
                [self.runtime, "inspect", "--format", "{{.State.Running}}", container_name],
                capture_output=True,
                text=True,
                timeout=5,
                check=False,
            )
            return proc.returncode == 0 and proc.stdout.strip() == "true"
        except Exception:
            return False
