from __future__ import annotations

import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from ipaddress import ip_network
from typing import Callable, List

import requests

from control_plane.memory import ClusterMemory
from control_plane.nodes import register_node
from shared.models import NodeRegistration, NodeType
from shared.utils import setup_logging

logger = setup_logging("control.discovery")


def _probe(host: str, port: int, timeout: float = 0.8) -> dict | None:
    try:
        resp = requests.get(f"http://{host}:{port}/ping", timeout=timeout)
        if resp.status_code == 200:
            return resp.json()
    except requests.RequestException:
        return None
    return None


def scan_and_register(
    memory: ClusterMemory,
    cidr: str = "127.0.0.0/30",
    port: int = 9001,
    retries: int = 2,
) -> List[str]:
    discovered: List[str] = []
    hosts = [str(ip) for ip in ip_network(cidr).hosts()]

    with ThreadPoolExecutor(max_workers=32) as pool:
        futs = []
        for host in hosts:
            for _ in range(retries):
                futs.append(pool.submit(_probe, host, port))
        for fut in as_completed(futs):
            data = fut.result()
            if not data:
                continue
            node_id = data["node_id"]
            if node_id in memory.nodes:
                continue
            reg = NodeRegistration(
                node_id=node_id,
                host=data.get("host", "127.0.0.1"),
                port=data.get("port", port),
                node_type=NodeType(data.get("node_type", "CPU")),
                has_gpu=bool(data.get("has_gpu", False)),
            )
            register_node(memory, reg)
            discovered.append(node_id)
    if discovered:
        memory.emit("info", "discovery_registered", {"nodes": discovered})
    return discovered


def start_discovery_task(
    memory: ClusterMemory,
    leader_ok: Callable[[], bool] | None,
    cidr: str | None = None,
    port: int | None = None,
) -> None:
    """
    Heartbeat/registry mode relies on explicit registration + DB restore.
    Scan/both optionally probes a CIDR once the process is leader-capable.
    """
    mode = os.getenv("ARSONIST_DISCOVERY_MODE", "heartbeat").lower()
    cidr = cidr or os.getenv("ARSONIST_DISCOVERY_CIDR", "127.0.0.0/30")
    port = int(port or os.getenv("ARSONIST_DISCOVERY_PORT", "9001"))

    def _run() -> None:
        time.sleep(2.0)
        if mode not in ("scan", "both"):
            logger.info("Discovery mode=%s (registry/heartbeat only)", mode)
            return
        if leader_ok is not None and not leader_ok():
            logger.info("Skipping LAN scan: not leader")
            return
        scan_and_register(memory, cidr=cidr, port=port)

    threading.Thread(target=_run, daemon=True, name="discovery-bootstrap").start()
