from __future__ import annotations

import json
import threading
import time
from enum import Enum
from typing import Any, Callable, Dict, List, Optional

from global_control.global_state import GlobalState
from shared.utils import now_ts, setup_logging

logger = setup_logging("global_control.replication")


class ReplicationMode(str, Enum):
    async_replication = "async"
    sync_replication = "sync"
    best_effort = "best_effort"


class ReplicationCheckpoint:
    def __init__(self, region_id: str, namespace: str, version: int, ts: float) -> None:
        self.region_id = region_id
        self.namespace = namespace
        self.version = version
        self.ts = ts

    def to_dict(self) -> Dict[str, Any]:
        return {
            "region_id": self.region_id,
            "namespace": self.namespace,
            "version": self.version,
            "ts": self.ts,
        }


class StateReplicator:
    """Replicates global state across regions with conflict resolution."""

    def __init__(
        self,
        local_region_id: str,
        state: GlobalState,
        mode: ReplicationMode = ReplicationMode.async_replication,
        sync_interval: float = 5.0,
        on_sync: Optional[Callable[[str, Dict[str, Any]], None]] = None,
    ) -> None:
        self.local_region_id = local_region_id
        self.state = state
        self.mode = mode
        self.sync_interval = sync_interval
        self._on_sync = on_sync
        self._lock = threading.Lock()
        self._checkpoints: Dict[str, ReplicationCheckpoint] = {}
        self._pending_changes: List[Dict[str, Any]] = []
        self._conflict_count = 0
        self._sync_count = 0
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._loop, daemon=True, name="state-replication")
        self._thread.start()
        logger.info("State replicator started (mode=%s, interval=%ss)", self.mode.value, self.sync_interval)

    def stop(self) -> None:
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=self.sync_interval + 2)

    def _loop(self) -> None:
        while not self._stop.is_set():
            try:
                self._sync_tick()
            except Exception:
                logger.exception("Replication sync error")
            self._stop.wait(self.sync_interval)

    def _sync_tick(self) -> None:
        with self._lock:
            batch = list(self._pending_changes)
            self._pending_changes.clear()
        if batch and self._on_sync:
            payload = {
                "source_region": self.local_region_id,
                "ts": now_ts(),
                "changes": batch,
            }
            try:
                self._on_sync(self.local_region_id, payload)
                self._sync_count += 1
            except Exception:
                logger.exception("Sync callback failed, re-queuing %d changes", len(batch))
                with self._lock:
                    self._pending_changes = batch + self._pending_changes

    def enqueue_change(self, namespace: str, key: str, value: Any, version: int) -> None:
        change = {
            "namespace": namespace,
            "key": key,
            "value": value,
            "version": version,
            "region": self.local_region_id,
            "ts": now_ts(),
        }
        with self._lock:
            self._pending_changes.append(change)

    def apply_remote_changes(self, changes: List[Dict[str, Any]]) -> Dict[str, int]:
        applied = 0
        conflicts = 0
        skipped = 0
        for change in changes:
            ns = change.get("namespace", "")
            key = change.get("key", "")
            value = change.get("value")
            remote_version = change.get("version", 0)
            local = self.state.get(ns, key)
            local_version = local["version"] if local else 0
            if remote_version <= local_version:
                skipped += 1
                continue
            if local and local_version > 0 and remote_version != local_version + 1:
                resolved = self._resolve_conflict(local, change)
                self.state.put(ns, key, resolved)
                conflicts += 1
                self._conflict_count += 1
            else:
                self.state.put(ns, key, value)
                applied += 1
        return {"applied": applied, "conflicts": conflicts, "skipped": skipped}

    def _resolve_conflict(self, local: Dict[str, Any], remote: Dict[str, Any]) -> Any:
        local_ts = local.get("updated_at", 0)
        remote_ts = remote.get("ts", 0)
        if remote_ts >= local_ts:
            return remote.get("value")
        return local.get("value")

    def update_checkpoint(self, region_id: str, namespace: str, version: int) -> None:
        with self._lock:
            self._checkpoints[f"{region_id}:{namespace}"] = ReplicationCheckpoint(
                region_id=region_id, namespace=namespace, version=version, ts=now_ts()
            )

    def get_checkpoint(self, region_id: str, namespace: str) -> Optional[ReplicationCheckpoint]:
        with self._lock:
            return self._checkpoints.get(f"{region_id}:{namespace}")

    def status(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "local_region": self.local_region_id,
                "mode": self.mode.value,
                "pending_changes": len(self._pending_changes),
                "sync_count": self._sync_count,
                "conflict_count": self._conflict_count,
                "checkpoints": {k: v.to_dict() for k, v in self._checkpoints.items()},
                "ts": now_ts(),
            }
