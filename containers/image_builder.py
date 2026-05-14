from __future__ import annotations

import os
import subprocess
import tempfile
from pathlib import Path
from typing import List


def build_image_from_dockerfile(dockerfile_content: str, tag: str) -> int:
    """Build a local image tag from Dockerfile string; returns exit code."""
    with tempfile.TemporaryDirectory(prefix="arsonist-img-") as tmp:
        df = Path(tmp) / "Dockerfile"
        df.write_text(dockerfile_content, encoding="utf-8")
        cmd = ["docker", "build", "-t", tag, tmp]
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=int(os.getenv("ARSONIST_IMAGE_BUILD_TIMEOUT_SEC", "600")), check=False)
        return proc.returncode
