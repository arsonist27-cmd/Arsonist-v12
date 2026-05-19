"""v16 Link Health Monitor.

Monitors communication link health across the interplanetary
infrastructure with trend analysis, degradation detection, and
predictive link failure warnings.
"""
from __future__ import annotations

import threading
from enum import Enum
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field

from shared.utils import now_ts, setup_logging

logger = setup_logging("telemetry.link_health")


class LinkHealthState(str, Enum):
    healthy = "healthy"
    warning = "warning"
    degraded = "degraded"
    critical = "critical"
    failed = "failed"


class LinkSample(BaseModel):
    link_id: str
    ts: float = 0.0
    latency_ms: float = 0.0
    bandwidth_kbps: float = 0.0
    packet_loss_pct: float = 0.0
    jitter_ms: float = 0.0
    utilization_pct: float = 0.0


class LinkHealthReport(BaseModel):
    link_id: str
    state: LinkHealthState = LinkHealthState.healthy
    health_score: float = 1.0
    latency_trend: str = "stable"
    bandwidth_trend: str = "stable"
    loss_trend: str = "stable"
    avg_latency_ms: float = 0.0
    avg_bandwidth_kbps: float = 0.0
    avg_packet_loss_pct: float = 0.0
    avg_jitter_ms: float = 0.0
    samples: int = 0
    prediction: str = ""
    ts: float = 0.0


class LinkHealthMonitor:
    """Monitors link health with trend analysis and predictive
    degradation detection."""

    def __init__(self, max_samples_per_link: int = 200,
                 max_history: int = 500) -> None:
        self._lock = threading.RLock()
        self._max_samples = max_samples_per_link
        self._max_history = max_history
        self._samples: Dict[str, List[LinkSample]] = {}
        self._reports: List[LinkHealthReport] = []
        self._total_samples = 0
        self._total_reports = 0
        self._events: List[Dict[str, Any]] = []

    def record_sample(self, sample: LinkSample) -> None:
        with self._lock:
            sample.ts = now_ts()
            if sample.link_id not in self._samples:
                self._samples[sample.link_id] = []
            self._samples[sample.link_id].append(sample)
            if len(self._samples[sample.link_id]) > self._max_samples:
                self._samples[sample.link_id] = self._samples[sample.link_id][-self._max_samples:]
            self._total_samples += 1

    def analyze(self, link_id: str) -> Optional[LinkHealthReport]:
        with self._lock:
            samples = self._samples.get(link_id)
            if not samples or len(samples) < 2:
                return None

            recent = samples[-50:]
            avg_latency = sum(s.latency_ms for s in recent) / len(recent)
            avg_bw = sum(s.bandwidth_kbps for s in recent) / len(recent)
            avg_loss = sum(s.packet_loss_pct for s in recent) / len(recent)
            avg_jitter = sum(s.jitter_ms for s in recent) / len(recent)

            half = len(recent) // 2
            first_half = recent[:half]
            second_half = recent[half:]

            lat_first = sum(s.latency_ms for s in first_half) / len(first_half) if first_half else 0
            lat_second = sum(s.latency_ms for s in second_half) / len(second_half) if second_half else 0
            bw_first = sum(s.bandwidth_kbps for s in first_half) / len(first_half) if first_half else 0
            bw_second = sum(s.bandwidth_kbps for s in second_half) / len(second_half) if second_half else 0
            loss_first = sum(s.packet_loss_pct for s in first_half) / len(first_half) if first_half else 0
            loss_second = sum(s.packet_loss_pct for s in second_half) / len(second_half) if second_half else 0

            latency_trend = self._trend(lat_first, lat_second)
            bw_trend = self._trend(bw_second, bw_first)
            loss_trend = self._trend(loss_first, loss_second)

            health_score = 1.0
            if avg_loss > 0:
                health_score -= min(0.4, avg_loss / 25.0)
            if avg_latency > 1000:
                health_score -= min(0.3, (avg_latency - 1000) / 10000.0)
            if avg_jitter > 100:
                health_score -= min(0.2, (avg_jitter - 100) / 500.0)
            health_score = max(0.0, round(health_score, 3))

            if health_score >= 0.8:
                state = LinkHealthState.healthy
            elif health_score >= 0.6:
                state = LinkHealthState.warning
            elif health_score >= 0.3:
                state = LinkHealthState.degraded
            elif health_score > 0:
                state = LinkHealthState.critical
            else:
                state = LinkHealthState.failed

            prediction = "stable"
            if latency_trend == "increasing" and loss_trend == "increasing":
                prediction = "degradation_likely"
            elif latency_trend == "decreasing" and loss_trend == "decreasing":
                prediction = "improving"
            elif loss_trend == "increasing" and avg_loss > 5:
                prediction = "failure_risk"

            report = LinkHealthReport(
                link_id=link_id,
                state=state,
                health_score=health_score,
                latency_trend=latency_trend,
                bandwidth_trend=bw_trend,
                loss_trend=loss_trend,
                avg_latency_ms=round(avg_latency, 1),
                avg_bandwidth_kbps=round(avg_bw, 1),
                avg_packet_loss_pct=round(avg_loss, 2),
                avg_jitter_ms=round(avg_jitter, 1),
                samples=len(recent),
                prediction=prediction,
                ts=now_ts(),
            )

            self._reports.append(report)
            if len(self._reports) > self._max_history:
                self._reports = self._reports[-self._max_history:]
            self._total_reports += 1

            if state in (LinkHealthState.critical, LinkHealthState.failed):
                self._add_event("link_health_alert", link_id,
                                state=state.value, score=health_score,
                                prediction=prediction)

            return report

    def analyze_all(self) -> List[LinkHealthReport]:
        reports = []
        with self._lock:
            link_ids = list(self._samples.keys())
        for lid in link_ids:
            report = self.analyze(lid)
            if report:
                reports.append(report)
        return reports

    def _trend(self, first: float, second: float, threshold: float = 0.1) -> str:
        if first == 0:
            return "stable"
        change = (second - first) / max(first, 0.001)
        if change > threshold:
            return "increasing"
        elif change < -threshold:
            return "decreasing"
        return "stable"

    def link_report(self, link_id: str) -> Optional[Dict[str, Any]]:
        with self._lock:
            for report in reversed(self._reports):
                if report.link_id == link_id:
                    return report.model_dump(mode="json")
            return None

    def all_reports(self) -> List[Dict[str, Any]]:
        with self._lock:
            seen = set()
            latest = []
            for report in reversed(self._reports):
                if report.link_id not in seen:
                    seen.add(report.link_id)
                    latest.append(report.model_dump(mode="json"))
            return latest

    def _add_event(self, event_type: str, ref: str, **kwargs: Any) -> None:
        event: Dict[str, Any] = {"type": event_type, "ref": ref, "ts": now_ts()}
        event.update(kwargs)
        self._events.append(event)
        if len(self._events) > self._max_history:
            self._events = self._events[-self._max_history:]

    def recent_events(self, limit: int = 50) -> List[Dict[str, Any]]:
        with self._lock:
            return list(reversed(self._events))[:limit]

    def metrics(self) -> Dict[str, Any]:
        with self._lock:
            healthy = sum(1 for r in self._reports[-len(self._samples):]
                          if r.state == LinkHealthState.healthy)
            return {
                "ts": now_ts(),
                "monitored_links": len(self._samples),
                "total_samples": self._total_samples,
                "total_reports": self._total_reports,
                "healthy_links": healthy,
            }
