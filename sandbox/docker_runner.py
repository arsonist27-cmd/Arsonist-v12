from __future__ import annotations

import subprocess
import tempfile
from pathlib import Path
from typing import Dict


def run_python_in_docker(code: str, timeout: int = 30) -> Dict[str, str | int | bool]:
    with tempfile.TemporaryDirectory(prefix="arsonist-job-") as tmpdir:
        script_path = Path(tmpdir) / "job.py"
        script_path.write_text(code, encoding="utf-8")
        cmd = [
            "docker",
            "run",
            "--rm",
            "-v",
            f"{script_path}:/work/job.py:ro",
            "python:3.11-slim",
            "python",
            "/work/job.py",
        ]
        try:
            proc = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout, check=False)
            return {
                "ok": proc.returncode == 0,
                "exit_code": proc.returncode,
                "stdout": proc.stdout,
                "stderr": proc.stderr,
            }
        except subprocess.TimeoutExpired:
            return {"ok": False, "exit_code": 124, "stdout": "", "stderr": "Execution timed out"}
