from __future__ import annotations

import threading
from typing import Any, Dict, List, Optional

from shared.utils import now_ts, setup_logging

logger = setup_logging("edge.scheduler")


class EdgeWorkload:
    def __init__(
        self,
        workload_id: str,
        model_id: str = "",
        priority: int = 0,
        max_latency_ms: float = 0.0,
        offline_capable: bool = False,
    ) -> None:
        self.workload_id = workload_id
        self.model_id = model_id
        self.priority = priority
        self.max_latency_ms = max_latency_ms
        self.offline_capable = offline_capable
        self.submitted_at = now_ts()
        self.started_at: float = 0.0
        self.completed_at: float = 0.0
        self.status = "queued"
        self.assigned_node: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return {
            "workload_id": self.workload_id,
            "model_id": self.model_id,
            "priority": self.priority,
            "max_latency_ms": self.max_latency_ms,
            "offline_capable": self.offline_capable,
            "submitted_at": self.submitted_at,
            "started_at": self.started_at,
            "completed_at": self.completed_at,
            "status": self.status,
            "assigned_node": self.assigned_node,
        }


class EdgeScheduler:
    """Schedules workloads across edge nodes with priority and offline awareness."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._queue: List[EdgeWorkload] = []
        self._running: Dict[str, EdgeWorkload] = {}
        self._completed: List[EdgeWorkload] = []
        self._node_capacity: Dict[str, int] = {}
        self._node_current_load: Dict[str, int] = {}

    def register_node(self, node_id: str, capacity: int = 4) -> None:
        with self._lock:
            self._node_capacity[node_id] = capacity
            self._node_current_load.setdefault(node_id, 0)

    def unregister_node(self, node_id: str) -> List[EdgeWorkload]:
        orphaned = []
        with self._lock:
            self._node_capacity.pop(node_id, None)
            self._node_current_load.pop(node_id, None)
            to_requeue = []
            for wid, wl in list(self._running.items()):
                if wl.assigned_node == node_id:
                    wl.status = "queued"
                    wl.assigned_node = ""
                    to_requeue.append(wl)
                    orphaned.append(wl)
            for wl in to_requeue:
                self._running.pop(wl.workload_id, None)
                self._queue.append(wl)
        return orphaned

    def submit(self, workload: EdgeWorkload) -> None:
        with self._lock:
            self._queue.append(workload)
            self._queue.sort(key=lambda w: w.priority, reverse=True)

    def schedule_next(self) -> Optional[EdgeWorkload]:
        with self._lock:
            if not self._queue:
                return None
            best_node = self._pick_node()
            if not best_node:
                return None
            workload = self._queue.pop(0)
            workload.assigned_node = best_node
            workload.started_at = now_ts()
            workload.status = "running"
            self._running[workload.workload_id] = workload
            self._node_current_load[best_node] = self._node_current_load.get(best_node, 0) + 1
            return workload

    def complete(self, workload_id: str, success: bool = True) -> Optional[EdgeWorkload]:
        with self._lock:
            wl = self._running.pop(workload_id, None)
            if not wl:
                return None
            wl.completed_at = now_ts()
            wl.status = "completed" if success else "failed"
            if wl.assigned_node in self._node_current_load:
                self._node_current_load[wl.assigned_node] = max(
                    0, self._node_current_load[wl.assigned_node] - 1
                )
            self._completed.append(wl)
            if len(self._completed) > 500:
                self._completed = self._completed[-500:]
            return wl

    def _pick_node(self) -> Optional[str]:
        best: Optional[str] = None
        best_headroom = -1
        for node_id, capacity in self._node_capacity.items():
            current = self._node_current_load.get(node_id, 0)
            headroom = capacity - current
            if headroom > 0 and headroom > best_headroom:
                best = node_id
                best_headroom = headroom
        return best

    def queue_depth(self) -> int:
        with self._lock:
            return len(self._queue)

    def running_count(self) -> int:
        with self._lock:
            return len(self._running)

    def metrics(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "ts": now_ts(),
                "queue_depth": len(self._queue),
                "running": len(self._running),
                "completed": len(self._completed),
                "registered_nodes": len(self._node_capacity),
                "node_loads": dict(self._node_current_load),
            }
