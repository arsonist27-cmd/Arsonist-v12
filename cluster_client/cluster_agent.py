from __future__ import annotations

import os
import threading
import time
from typing import Any, Optional

import requests

from control_plane.memory import ClusterMemory
from federation.federation_models import ClusterHealth, ClusterRegistration, FederationHeartbeatPayload
from federation.federation_security import build_headers
from shared.utils import setup_logging

logger = setup_logging("cluster.agent")


class ClusterAgent:
    """
    Registers this control plane as a federated cluster and sends periodic heartbeats + metrics.
    Additive: only runs when ARSONIST_CLUSTER_ID and ARSONIST_FEDERATION_URL are set.
    """

    def __init__(
        self,
        memory: ClusterMemory,
        cluster_id: str,
        region: str,
        federation_url: str,
        federation_token: str,
        control_plane_public_url: str,
        api_token: str,
        heartbeat_sec: float = 6.0,
        register_retry_sec: float = 5.0,
    ) -> None:
        self.memory = memory
        self.cluster_id = cluster_id
        self.region = region
        self.federation_url = federation_url.rstrip("/")
        self.federation_token = federation_token
        self.control_plane_public_url = control_plane_public_url.rstrip("/")
        self.api_token = api_token
        self.heartbeat_sec = heartbeat_sec
        self.register_retry_sec = register_retry_sec
        self._stop = threading.Event()

    def _headers(self, payload: dict[str, object]) -> dict[str, str]:
        h = dict(build_headers(payload))
        if self.federation_token:
            h["Authorization"] = f"Bearer {self.federation_token}"
        return h

    def _register_once(self) -> bool:
        nodes = list(self.memory.nodes.values())
        gpu_capacity = sum(1 for n in nodes if n.has_gpu)
        q = len(self.memory.queue_snapshot())
        metrics_load = 0.0
        try:
            # lightweight average from in-memory nodes
            if nodes:
                metrics_load = sum(n.current_load for n in nodes) / len(nodes)
        except Exception:
            pass
        payload = ClusterRegistration(
            cluster_id=self.cluster_id,
            region=self.region,
            control_plane_url=self.control_plane_public_url,
            api_token=self.api_token,
            node_count=len(nodes),
            gpu_capacity=gpu_capacity,
            current_load=metrics_load,
            queue_depth=q,
            avg_latency_ms=sum(n.avg_latency_ms for n in nodes) / len(nodes) if nodes else 10.0,
            health_state=ClusterHealth.healthy if nodes else ClusterHealth.degraded,
        )
        body = payload.model_dump(mode="json")
        try:
            r = requests.post(
                f"{self.federation_url}/register_cluster",
                json=body,
                headers=self._headers(body),
                timeout=5.0,
            )
            r.raise_for_status()
            logger.info("Registered cluster %s with federation", self.cluster_id)
            return True
        except requests.RequestException as exc:
            logger.warning("Federation register failed: %s", exc)
            return False

    def _heartbeat_once(self) -> None:
        nodes = list(self.memory.nodes.values())
        gpu_capacity = sum(1 for n in nodes if n.has_gpu)
        q = len(self.memory.queue_snapshot())
        load = sum(n.current_load for n in nodes) / len(nodes) if nodes else 0.0
        lat = sum(n.avg_latency_ms for n in nodes) / len(nodes) if nodes else 0.0
        hp = FederationHeartbeatPayload(
            cluster_id=self.cluster_id,
            node_count=len(nodes),
            gpu_capacity=gpu_capacity,
            current_load=load,
            queue_depth=q,
            avg_latency_ms=lat,
            health_state=ClusterHealth.healthy if nodes else ClusterHealth.degraded,
        )
        body = hp.model_dump(mode="json")
        try:
            requests.post(
                f"{self.federation_url}/heartbeat",
                json=body,
                headers=self._headers(body),
                timeout=5.0,
            )
        except requests.RequestException:
            pass

    def _run(self) -> None:
        while not self._stop.is_set():
            if self._register_once():
                break
            time.sleep(self.register_retry_sec)
        while not self._stop.is_set():
            self._heartbeat_once()
            time.sleep(self.heartbeat_sec)

    def start(self) -> None:
        threading.Thread(target=self._run, daemon=True, name="fed-agent").start()


_agent: Optional[ClusterAgent] = None


def maybe_start_cluster_agent(memory: ClusterMemory) -> None:
    global _agent
    cid = os.getenv("ARSONIST_CLUSTER_ID", "").strip()
    fed = os.getenv("ARSONIST_FEDERATION_URL", "").strip()
    if not cid or not fed:
        return
    if _agent is not None:
        return
    region = os.getenv("ARSONIST_CLUSTER_REGION", "default")
    pub = os.getenv("ARSONIST_CONTROL_PLANE_PUBLIC_URL", "http://127.0.0.1:8000").strip()
    api_tok = os.getenv("ARSONIST_API_TOKEN", "")
    fed_tok = os.getenv("ARSONIST_FEDERATION_TOKEN", "")
    _agent = ClusterAgent(
        memory=memory,
        cluster_id=cid,
        region=region,
        federation_url=fed,
        federation_token=fed_tok,
        control_plane_public_url=pub,
        api_token=api_tok,
        heartbeat_sec=float(os.getenv("ARSONIST_FEDERATION_HEARTBEAT_INTERVAL_SEC", "6")),
    )
    _agent.start()
    logger.info("Cluster federation agent started cluster_id=%s", cid)
