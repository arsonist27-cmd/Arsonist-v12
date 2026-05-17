from __future__ import annotations

from enum import Enum


class Role(str, Enum):
    admin = "admin"
    dev = "dev"
    billing = "billing"
    viewer = "viewer"
