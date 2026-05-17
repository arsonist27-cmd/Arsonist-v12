from __future__ import annotations

import json
import threading
import time
from typing import Any, Callable, Dict, List, Optional

from shared.utils import now_ts, setup_logging

logger = setup_logging("replication.state")


class StateChangeEntry:
    def __init__(
        self,
        namespace: str,
        key: str,
        value: Any,
        version: int,
        source_region: str,
    ) -> None:
        self.namespace = namespace
        self.key = key
        self.value = value
        self.version = version
        self.source_region = source_region
        self.ts = now_ts()

    def to_dict(self) -> Dict[str, Any]:
        return {
            "namespace": self.namespace,
            "key": self.key,
            "value": self.value,
            "version": self.version,
            "source_region": self.source_region,
            "ts": self.ts,
        }


class IncrementalStateReplicator:
    """Incremental state replication with conflict resolution and checkpointing."""

    def __init__(
        self,
        local_region: str,
        peer_regions: Optional[List[str]] = None,
        batch_size: int = 50,
        sync_interval: float = 5.0,
        send_fn: Optional[Callable[[str, List[Dict[str, Any]]], bool]] = None,
    ) -> None:
        self.local_region = local_region
        self.peer_regions = list(peer_regions or [])
        self.batch_size = batch_size
        self.sync_interval = sync_interval
        self._send_fn = send_fn
        self._lock = threading.Lock()
        self._outbound_queue: List[StateChangeEntry] = []
        self._checkpoints: Dict[str, int] = {}
        self._sync_count = 0
        self._conflict_count = 0
        self._entries_sent = 0
        self._entries_received = 0
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._loop, daemon=True, name="state-replication")
        self._thread.start()
        logger.info("Incremental state replicator started for %s", self.local_region)

    def stop(self) -> None:
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=self.sync_interval + 2)

    def _loop(self) -> None:
        while not self._stop.is_set():
            try:
                self._flush()
            except Exception:
                logger.exception("State replication flush error")
            self._stop.wait(self.sync_interval)

    def enqueue(self, namespace: str, key: str, value: Any, version: int) -> None:
        entry = StateChangeEntry(
            namespace=namespace,
            key=key,
            value=value,
            version=version,
            source_region=self.local_region,
        )
        with self._lock:
            self._outbound_queue.append(entry)

    def _flush(self) -> None:
        with self._lock:
            batch = self._outbound_queue[:self.batch_size]
            self._outbound_queue = self._outbound_queue[self.batch_size:]
        if not batch:
            return
        payload = [e.to_dict() for e in batch]
        for peer in self.peer_regions:
            success = self._send_to_peer(peer, payload)
            if success:
                self._entries_sent += len(batch)
                checkpoint_version = max(e.version for e in batch)
                self._checkpoints[peer] = max(
                    self._checkpoints.get(peer, 0), checkpoint_version
                )
        self._sync_count += 1

    def _send_to_peer(self, peer: str, payload: List[Dict[str, Any]]) -> bool:
        if self._send_fn:
            try:
                return self._send_fn(peer, payload)
            except Exception:
                logger.exception("Failed to send state to peer %s", peer)
                return False
        logger.debug("Would send %d entries to peer %s (no send_fn)", len(payload), peer)
        return True

    def receive_changes(self, changes: List[Dict[str, Any]], apply_fn: Optional[Callable[[str, str, Any, int], bool]] = None) -> Dict[str, int]:
        applied = 0
        skipped = 0
        conflicts = 0
        for change in changes:
            source = change.get("source_region", "")
            if source == self.local_region:
                skipped += 1
                continue
            ns = change.get("namespace", "")
            key = change.get("key", "")
            value = change.get("value")
            version = change.get("version", 0)
            if apply_fn:
                try:
                    ok = apply_fn(ns, key, value, version)
                    if ok:
                        applied += 1
                    else:
                        conflicts += 1
                        self._conflict_count += 1
                except Exception:
                    conflicts += 1
                    self._conflict_count += 1
            else:
                applied += 1
            self._entries_received += 1
        return {"applied": applied, "skipped": skipped, "conflicts": conflicts}

    def add_peer(self, region_id: str) -> None:
        if region_id not in self.peer_regions:
            self.peer_regions.append(region_id)

    def remove_peer(self, region_id: str) -> None:
        if region_id in self.peer_regions:
            self.peer_regions.remove(region_id)

    def get_checkpoint(self, peer: str) -> int:
        return self._checkpoints.get(peer, 0)

    def metrics(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "ts": now_ts(),
                "local_region": self.local_region,
                "peer_count": len(self.peer_regions),
                "outbound_queue": len(self._outbound_queue),
                "sync_count": self._sync_count,
                "entries_sent": self._entries_sent,
                "entries_received": self._entries_received,
                "conflict_count": self._conflict_count,
                "checkpoints": dict(self._checkpoints),
            }
