from __future__ import annotations

from typing import Tuple

from containers.sandbox_profiles import SandboxProfile


def merge_profiles(base: SandboxProfile, override: SandboxProfile) -> SandboxProfile:
    return SandboxProfile(
        read_only_root=override.read_only_root,
        network=override.network,
        memory_mb=override.memory_mb or base.memory_mb,
        cpu_quota_cpus=override.cpu_quota_cpus or base.cpu_quota_cpus,
        gpu_device_requests=list({*base.gpu_device_requests, *override.gpu_device_requests}),
    )
