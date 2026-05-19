"""v16 Signal Latency Model.

Models and predicts signal propagation delays across orbital and
ground infrastructure including distance-based delay, atmospheric
effects, relay overhead, and contact window prediction.
"""
from __future__ import annotations

import math
import threading
from enum import Enum
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field

from shared.utils import now_ts, setup_logging

logger = setup_logging("communications.signal_latency_model")

SPEED_OF_LIGHT_KM_S = 299_792.458

LEO_ALTITUDE_KM = 550.0
MEO_ALTITUDE_KM = 20_200.0
GEO_ALTITUDE_KM = 35_786.0
LUNAR_DISTANCE_KM = 384_400.0
EARTH_RADIUS_KM = 6_371.0


class PropagationPath(str, Enum):
    ground_ground = "ground_ground"
    ground_leo = "ground_leo"
    ground_meo = "ground_meo"
    ground_geo = "ground_geo"
    ground_lunar = "ground_lunar"
    leo_leo = "leo_leo"
    leo_geo = "leo_geo"
    orbital_relay = "orbital_relay"


class LatencyEstimate(BaseModel):
    source: str = ""
    destination: str = ""
    path: PropagationPath = PropagationPath.ground_ground
    propagation_delay_ms: float = 0.0
    processing_delay_ms: float = 0.0
    relay_delay_ms: float = 0.0
    atmospheric_delay_ms: float = 0.0
    total_one_way_ms: float = 0.0
    total_rtt_ms: float = 0.0
    distance_km: float = 0.0
    hops: int = 1
    ts: float = 0.0


class SignalLatencyModel:
    """Models signal propagation delays across orbital infrastructure
    for scheduling and routing decisions."""

    def __init__(self, processing_overhead_ms: float = 5.0,
                 atmospheric_overhead_ms: float = 2.0,
                 relay_overhead_ms: float = 10.0,
                 max_history: int = 500) -> None:
        self._lock = threading.RLock()
        self._processing_ms = processing_overhead_ms
        self._atmospheric_ms = atmospheric_overhead_ms
        self._relay_ms = relay_overhead_ms
        self._max_history = max_history
        self._estimates: List[LatencyEstimate] = []
        self._custom_distances: Dict[str, float] = {}
        self._total_estimates = 0

    def estimate(self, source: str, destination: str,
                 path: PropagationPath,
                 custom_distance_km: Optional[float] = None,
                 relay_hops: int = 0) -> LatencyEstimate:
        if custom_distance_km is not None:
            distance_km = custom_distance_km
        else:
            distance_km = self._calculate_distance(path)

        propagation_ms = (distance_km / SPEED_OF_LIGHT_KM_S) * 1000
        processing_ms = self._processing_ms * (1 + relay_hops)
        relay_ms = self._relay_ms * relay_hops
        atmospheric_ms = self._atmospheric_ms if self._path_crosses_atmosphere(path) else 0.0

        total_one_way = propagation_ms + processing_ms + relay_ms + atmospheric_ms
        total_rtt = total_one_way * 2

        estimate = LatencyEstimate(
            source=source,
            destination=destination,
            path=path,
            propagation_delay_ms=round(propagation_ms, 3),
            processing_delay_ms=round(processing_ms, 3),
            relay_delay_ms=round(relay_ms, 3),
            atmospheric_delay_ms=round(atmospheric_ms, 3),
            total_one_way_ms=round(total_one_way, 3),
            total_rtt_ms=round(total_rtt, 3),
            distance_km=round(distance_km, 1),
            hops=1 + relay_hops,
            ts=now_ts(),
        )

        with self._lock:
            self._estimates.append(estimate)
            if len(self._estimates) > self._max_history:
                self._estimates = self._estimates[-self._max_history:]
            self._total_estimates += 1
            pair_key = f"{source}-{destination}"
            self._custom_distances[pair_key] = distance_km

        return estimate

    def _calculate_distance(self, path: PropagationPath) -> float:
        distances = {
            PropagationPath.ground_ground: 5000.0,
            PropagationPath.ground_leo: math.sqrt(
                (EARTH_RADIUS_KM + LEO_ALTITUDE_KM) ** 2 - EARTH_RADIUS_KM ** 2
            ),
            PropagationPath.ground_meo: math.sqrt(
                (EARTH_RADIUS_KM + MEO_ALTITUDE_KM) ** 2 - EARTH_RADIUS_KM ** 2
            ),
            PropagationPath.ground_geo: math.sqrt(
                (EARTH_RADIUS_KM + GEO_ALTITUDE_KM) ** 2 - EARTH_RADIUS_KM ** 2
            ),
            PropagationPath.ground_lunar: LUNAR_DISTANCE_KM,
            PropagationPath.leo_leo: 2 * LEO_ALTITUDE_KM + 1000,
            PropagationPath.leo_geo: GEO_ALTITUDE_KM - LEO_ALTITUDE_KM,
            PropagationPath.orbital_relay: GEO_ALTITUDE_KM * 2,
        }
        return distances.get(path, 5000.0)

    def _path_crosses_atmosphere(self, path: PropagationPath) -> bool:
        atmospheric_paths = {
            PropagationPath.ground_leo,
            PropagationPath.ground_meo,
            PropagationPath.ground_geo,
            PropagationPath.ground_lunar,
        }
        return path in atmospheric_paths

    def recent_estimates(self, limit: int = 50) -> List[Dict[str, Any]]:
        with self._lock:
            return [e.model_dump(mode="json") for e in reversed(self._estimates)][:limit]

    def reference_latencies(self) -> Dict[str, float]:
        result = {}
        for path in PropagationPath:
            est = self.estimate("ref", "ref", path)
            result[path.value] = est.total_rtt_ms
        return result

    def metrics(self) -> Dict[str, Any]:
        with self._lock:
            recent = self._estimates[-100:] if self._estimates else []
            avg_rtt = sum(e.total_rtt_ms for e in recent) / len(recent) if recent else 0.0
            return {
                "ts": now_ts(),
                "total_estimates": self._total_estimates,
                "avg_rtt_ms": round(avg_rtt, 3),
                "reference_latencies": self.reference_latencies(),
            }
