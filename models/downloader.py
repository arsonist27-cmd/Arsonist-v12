from __future__ import annotations

import hashlib
import os
from pathlib import Path
from typing import Callable, Optional

import httpx


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


class ModelDownloader:
    """HTTP(S) downloads with checksum verification."""

    def __init__(self, cache_root: Path) -> None:
        self.cache_root = cache_root
        self.cache_root.mkdir(parents=True, exist_ok=True)

    def download(self, url: str, dest_name: str, expected_sha256: str | None = None, progress: Optional[Callable[[int], None]] = None) -> Path:
        dest = self.cache_root / dest_name
        with httpx.Client(timeout=httpx.Timeout(120.0, connect=10.0), follow_redirects=True) as client:
            with client.stream("GET", url) as resp:
                resp.raise_for_status()
                total = 0
                with dest.open("wb") as f:
                    for chunk in resp.iter_bytes():
                        f.write(chunk)
                        total += len(chunk)
                        if progress:
                            progress(total)
        if expected_sha256:
            digest = sha256_file(dest)
            if digest != expected_sha256.lower():
                raise ValueError("checksum mismatch")
        return dest
