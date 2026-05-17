from __future__ import annotations

import hashlib
import hmac
import os
import threading
import time
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set

from shared.utils import now_ts, setup_logging

logger = setup_logging("networking.overlay")


class TransportProtocol(str, Enum):
    tcp = "tcp"
    quic = "quic"
    wireguard = "wireguard"


class PeerConnection:
    def __init__(
        self,
        peer_id: str,
        endpoint: str,
        protocol: TransportProtocol = TransportProtocol.tcp,
    ) -> None:
        self.peer_id = peer_id
        self.endpoint = endpoint
        self.protocol = protocol
        self.connected = False
        self.established_at: float = 0.0
        self.last_activity: float = 0.0
        self.bytes_sent: int = 0
        self.bytes_received: int = 0
        self.latency_ms: float = 0.0
        self.encrypted = True

    def to_dict(self) -> Dict[str, Any]:
        return {
            "peer_id": self.peer_id,
            "endpoint": self.endpoint,
            "protocol": self.protocol.value,
            "connected": self.connected,
            "established_at": self.established_at,
            "last_activity": self.last_activity,
            "bytes_sent": self.bytes_sent,
            "bytes_received": self.bytes_received,
            "latency_ms": self.latency_ms,
            "encrypted": self.encrypted,
        }


class OverlayNetwork:
    """Encrypted inter-region overlay network with service discovery and connection pooling."""

    def __init__(
        self,
        local_id: str,
        shared_secret: str | None = None,
        max_connections: int = 100,
        heartbeat_interval: float = 15.0,
    ) -> None:
        self.local_id = local_id
        self._secret = shared_secret or os.getenv("ARSONIST_OVERLAY_SECRET", "")
        self.max_connections = max_connections
        self.heartbeat_interval = heartbeat_interval
        self._lock = threading.Lock()
        self._peers: Dict[str, PeerConnection] = {}
        self._service_registry: Dict[str, Dict[str, str]] = {}
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._total_bytes_sent = 0
        self._total_bytes_received = 0
        self._connection_failures = 0

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._loop, daemon=True, name="overlay-net")
        self._thread.start()
        logger.info("Overlay network started for %s", self.local_id)

    def stop(self) -> None:
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=self.heartbeat_interval + 2)
        with self._lock:
            for peer in self._peers.values():
                peer.connected = False

    def _loop(self) -> None:
        while not self._stop.is_set():
            try:
                self._check_peers()
            except Exception:
                logger.exception("Overlay network check error")
            self._stop.wait(self.heartbeat_interval)

    def _check_peers(self) -> None:
        ts = now_ts()
        with self._lock:
            for peer in self._peers.values():
                if peer.connected and ts - peer.last_activity > self.heartbeat_interval * 3:
                    peer.connected = False
                    logger.warning("Peer %s connection timed out", peer.peer_id)

    def add_peer(self, peer_id: str, endpoint: str, protocol: TransportProtocol = TransportProtocol.tcp) -> PeerConnection:
        conn = PeerConnection(peer_id=peer_id, endpoint=endpoint, protocol=protocol)
        conn.connected = True
        conn.established_at = now_ts()
        conn.last_activity = now_ts()
        with self._lock:
            if len(self._peers) >= self.max_connections:
                self._evict_idle_peer()
            self._peers[peer_id] = conn
        logger.info("Peer added: %s (%s)", peer_id, endpoint)
        return conn

    def remove_peer(self, peer_id: str) -> None:
        with self._lock:
            self._peers.pop(peer_id, None)

    def get_peer(self, peer_id: str) -> Optional[PeerConnection]:
        with self._lock:
            return self._peers.get(peer_id)

    def connected_peers(self) -> List[PeerConnection]:
        with self._lock:
            return [p for p in self._peers.values() if p.connected]

    def record_traffic(self, peer_id: str, bytes_sent: int = 0, bytes_received: int = 0) -> None:
        with self._lock:
            peer = self._peers.get(peer_id)
            if peer:
                peer.bytes_sent += bytes_sent
                peer.bytes_received += bytes_received
                peer.last_activity = now_ts()
                self._total_bytes_sent += bytes_sent
                self._total_bytes_received += bytes_received

    def update_latency(self, peer_id: str, latency_ms: float) -> None:
        with self._lock:
            peer = self._peers.get(peer_id)
            if peer:
                peer.latency_ms = latency_ms

    def register_service(self, service_name: str, peer_id: str, endpoint: str) -> None:
        with self._lock:
            self._service_registry[service_name] = {
                "peer_id": peer_id,
                "endpoint": endpoint,
                "registered_at": str(now_ts()),
            }

    def discover_service(self, service_name: str) -> Optional[Dict[str, str]]:
        with self._lock:
            return self._service_registry.get(service_name)

    def list_services(self) -> Dict[str, Dict[str, str]]:
        with self._lock:
            return dict(self._service_registry)

    def sign_message(self, message: bytes) -> str:
        if not self._secret:
            return ""
        return hmac.new(self._secret.encode(), message, hashlib.sha256).hexdigest()

    def verify_message(self, message: bytes, signature: str) -> bool:
        if not self._secret:
            return True
        expected = hmac.new(self._secret.encode(), message, hashlib.sha256).hexdigest()
        return hmac.compare_digest(expected, signature)

    def _evict_idle_peer(self) -> None:
        if not self._peers:
            return
        idle_peer = min(self._peers, key=lambda k: self._peers[k].last_activity)
        self._peers.pop(idle_peer)
        logger.info("Evicted idle peer: %s", idle_peer)

    def metrics(self) -> Dict[str, Any]:
        with self._lock:
            connected = sum(1 for p in self._peers.values() if p.connected)
            latencies = [p.latency_ms for p in self._peers.values() if p.connected and p.latency_ms > 0]
            return {
                "ts": now_ts(),
                "local_id": self.local_id,
                "total_peers": len(self._peers),
                "connected_peers": connected,
                "total_bytes_sent": self._total_bytes_sent,
                "total_bytes_received": self._total_bytes_received,
                "connection_failures": self._connection_failures,
                "registered_services": len(self._service_registry),
                "avg_peer_latency_ms": round(sum(latencies) / len(latencies), 2) if latencies else 0.0,
            }
