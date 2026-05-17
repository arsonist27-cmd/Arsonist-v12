from __future__ import annotations

import threading
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any, DefaultDict, Dict, List


class UsageTracker:
    """Aggregates usage per org (memory); pair with Redis in metering for scale."""

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._by_org: DefaultDict[str, List[Dict[str, Any]]] = defaultdict(list)

    def record(
        self,
        org_id: str,
        *,
        metric: str,
        amount: float,
        unit: str,
        meta: Dict[str, Any] | None = None,
    ) -> None:
        row = {
            "ts": datetime.now(timezone.utc).isoformat(),
            "metric": metric,
            "amount": amount,
            "unit": unit,
            "meta": meta or {},
        }
        with self._lock:
            self._by_org[org_id].append(row)
            # cap memory per org for dev safety
            cap = 50_000
            if len(self._by_org[org_id]) > cap:
                self._by_org[org_id] = self._by_org[org_id][-cap:]

    def recent(self, org_id: str, limit: int = 200) -> List[Dict[str, Any]]:
        with self._lock:
            return list(self._by_org.get(org_id, []))[-limit:]

    def totals(self, org_id: str) -> Dict[str, float]:
        with self._lock:
            rows = self._by_org.get(org_id, [])
        out: Dict[str, float] = defaultdict(float)
        for r in rows:
            out[r["metric"]] += float(r["amount"])
        return dict(out)


TRACKER = UsageTracker()
