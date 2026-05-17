from __future__ import annotations

import threading
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List

from billing.usage_tracking import TRACKER


class InvoiceEngine:
    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._invoices: Dict[str, Dict[str, Any]] = {}

    def generate(self, org_id: str, period: str, line_items: List[Dict[str, Any]]) -> Dict[str, Any]:
        inv_id = f"inv_{uuid.uuid4().hex[:12]}"
        total = sum(float(x.get("amount", 0)) * float(x.get("unit_price", 0)) for x in line_items)
        doc = {
            "invoice_id": inv_id,
            "org_id": org_id,
            "period": period,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "line_items": line_items,
            "total_usd": round(total, 4),
            "status": "draft",
        }
        with self._lock:
            self._invoices[inv_id] = doc
        return doc

    def finalize(self, invoice_id: str) -> Dict[str, Any] | None:
        with self._lock:
            inv = self._invoices.get(invoice_id)
            if not inv:
                return None
            inv["status"] = "final"
            return dict(inv)

    def from_usage_snapshot(self, org_id: str, period: str, prices: Dict[str, float]) -> Dict[str, Any]:
        totals = TRACKER.totals(org_id)
        lines = []
        for metric, qty in totals.items():
            price = float(prices.get(metric, 0.0))
            if qty and price:
                lines.append({"metric": metric, "amount": qty, "unit_price": price, "description": metric})
        if not lines:
            lines.append({"metric": "minimum", "amount": 1, "unit_price": 0.0, "description": "no billable usage"})
        return self.generate(org_id, period, lines)


INVOICES = InvoiceEngine()
