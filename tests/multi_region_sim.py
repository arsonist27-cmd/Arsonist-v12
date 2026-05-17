"""Multi-region simulation for Arsonist OS v13 Global AI Compute Fabric.

Simulates multiple regions, edge nodes, region outages, replication delays,
latency spikes, model migrations, routing failovers, and network partitions.

Usage:
    PYTHONPATH=$PWD python tests/multi_region_sim.py
"""

from __future__ import annotations

import asyncio
import json
import random
import sys
import time
from typing import Any, Dict, List

sys.path.insert(0, ".")

from regions.region_registry import GPUInventory, RegionRecord, RegionRegistry, RegionStatus, RegionType
from regions.region_health import RegionHealthMonitor
from regions.latency_map import LatencyMap
from regions.regional_capacity import RegionalCapacityTracker
from regions.geo_routing import GeoRouter
from routing.global_router import GlobalRouter, RoutingRequest, RoutingStrategy
from routing.smart_failover import SmartFailover
from replication.model_replication import ModelReplicationManager, ModelReplica, ReplicationState, ReplicationTier
from replication.cache_replication import DistributedCacheFabric, CacheEntryType
from fabric.placement_engine import PlacementEngine, PlacementRequest
from fabric.compute_fabric import ComputeFabric
from fabric.topology_manager import TopologyManager
from telemetry.global_metrics import GlobalMetricsCollector
from telemetry.routing_metrics import RoutingMetrics
from networking.overlay_network import OverlayNetwork
from shared.utils import setup_logging

logger = setup_logging("sim.multi_region")

REGIONS = [
    RegionRecord(
        region_id="us-east-1",
        display_name="US East",
        geographic_location="Virginia, USA",
        latitude=38.9,
        longitude=-77.0,
        region_type=RegionType.cloud,
        capacity=1.0,
        gpu_inventory=GPUInventory(total_gpus=64, available_gpus=40, gpu_types={"A100": 32, "H100": 32}, total_vram_gb=5120.0, available_vram_gb=3200.0),
        avg_latency_ms=15.0,
        workload_saturation=0.35,
        endpoint_url="http://us-east-1:8000",
    ),
    RegionRecord(
        region_id="eu-west-1",
        display_name="EU West",
        geographic_location="Ireland",
        latitude=53.3,
        longitude=-6.3,
        region_type=RegionType.cloud,
        capacity=0.9,
        gpu_inventory=GPUInventory(total_gpus=48, available_gpus=30, gpu_types={"A100": 24, "H100": 24}, total_vram_gb=3840.0, available_vram_gb=2400.0),
        avg_latency_ms=25.0,
        workload_saturation=0.45,
        endpoint_url="http://eu-west-1:8000",
    ),
    RegionRecord(
        region_id="ap-east-1",
        display_name="Asia Pacific",
        geographic_location="Tokyo, Japan",
        latitude=35.7,
        longitude=139.7,
        region_type=RegionType.cloud,
        capacity=0.8,
        gpu_inventory=GPUInventory(total_gpus=32, available_gpus=20, gpu_types={"A100": 16, "H100": 16}, total_vram_gb=2560.0, available_vram_gb=1600.0),
        avg_latency_ms=35.0,
        workload_saturation=0.55,
        endpoint_url="http://ap-east-1:8000",
    ),
    RegionRecord(
        region_id="edge-nyc",
        display_name="NYC Edge",
        geographic_location="New York, USA",
        latitude=40.7,
        longitude=-74.0,
        region_type=RegionType.edge,
        capacity=0.3,
        gpu_inventory=GPUInventory(total_gpus=4, available_gpus=3, gpu_types={"T4": 4}, total_vram_gb=64.0, available_vram_gb=48.0),
        avg_latency_ms=5.0,
        workload_saturation=0.20,
        endpoint_url="http://edge-nyc:8000",
    ),
    RegionRecord(
        region_id="edge-lon",
        display_name="London Edge",
        geographic_location="London, UK",
        latitude=51.5,
        longitude=-0.1,
        region_type=RegionType.edge,
        capacity=0.3,
        gpu_inventory=GPUInventory(total_gpus=4, available_gpus=2, gpu_types={"T4": 4}, total_vram_gb=64.0, available_vram_gb=32.0),
        avg_latency_ms=8.0,
        workload_saturation=0.30,
        endpoint_url="http://edge-lon:8000",
    ),
]

INTER_REGION_LATENCIES = {
    ("us-east-1", "eu-west-1"): 80.0,
    ("us-east-1", "ap-east-1"): 150.0,
    ("eu-west-1", "ap-east-1"): 120.0,
    ("us-east-1", "edge-nyc"): 10.0,
    ("eu-west-1", "edge-lon"): 12.0,
    ("edge-nyc", "edge-lon"): 85.0,
    ("ap-east-1", "edge-nyc"): 160.0,
    ("ap-east-1", "edge-lon"): 130.0,
}


def setup_fabric() -> Dict[str, Any]:
    registry = RegionRegistry(db_path=":memory:")
    latency_map = LatencyMap()
    capacity_tracker = RegionalCapacityTracker(registry)
    health_monitor = RegionHealthMonitor(registry, check_interval=999)
    geo_router = GeoRouter(registry)
    replication_mgr = ModelReplicationManager(db_path=":memory:")
    metrics_collector = GlobalMetricsCollector()
    routing_metrics = RoutingMetrics()
    overlay = OverlayNetwork(local_id="sim-controller")
    topology = TopologyManager(registry)
    compute_fabric = ComputeFabric(registry, capacity_tracker)

    for region in REGIONS:
        registry.register(region)
        overlay.add_peer(region.region_id, region.endpoint_url)
        logger.info("Registered region: %s (%s)", region.region_id, region.geographic_location)

    for (src, dst), lat in INTER_REGION_LATENCIES.items():
        latency_map.record_inter_region(src, dst, lat)
        latency_map.record_inter_region(dst, src, lat)
        topology.update_link(src, dst, latency_ms=lat, bandwidth_mbps=random.uniform(500, 10000))

    global_router = GlobalRouter(registry, latency_map, capacity_tracker)
    failover = SmartFailover(registry, capacity_tracker)

    return {
        "registry": registry,
        "latency_map": latency_map,
        "capacity_tracker": capacity_tracker,
        "health_monitor": health_monitor,
        "geo_router": geo_router,
        "global_router": global_router,
        "failover": failover,
        "replication_mgr": replication_mgr,
        "metrics": metrics_collector,
        "routing_metrics": routing_metrics,
        "overlay": overlay,
        "topology": topology,
        "compute_fabric": compute_fabric,
    }


def sim_routing(ctx: Dict[str, Any]) -> None:
    logger.info("\n=== ROUTING SIMULATION ===")
    router: GlobalRouter = ctx["global_router"]
    metrics: GlobalMetricsCollector = ctx["metrics"]
    routing_metrics: RoutingMetrics = ctx["routing_metrics"]

    requests = [
        RoutingRequest(request_id="req-1", client_region="us-east-1", model_id="llama-70b", require_gpu=True, strategy=RoutingStrategy.weighted),
        RoutingRequest(request_id="req-2", client_region="eu-west-1", model_id="gpt-neo", require_gpu=True, strategy=RoutingStrategy.nearest),
        RoutingRequest(request_id="req-3", client_region="ap-east-1", model_id="llama-70b", strategy=RoutingStrategy.least_loaded),
        RoutingRequest(request_id="req-4", client_region="us-east-1", preferred_region="edge-nyc", strategy=RoutingStrategy.weighted),
        RoutingRequest(request_id="req-5", client_region="eu-west-1", require_gpu=True, min_vram_gb=40.0, strategy=RoutingStrategy.gpu_affinity),
    ]

    for req in requests:
        decision = router.route(req)
        if decision:
            metrics.record_request_flow(req.request_id, req.client_region, decision.target_region, decision.decision_time_ms)
            routing_metrics.record_route(decision.target_region, decision.strategy_used.value, decision.decision_time_ms)
            logger.info(
                "  %s -> %s (score=%.3f, time=%.1fms, fallbacks=%s)",
                req.request_id, decision.target_region, decision.score,
                decision.decision_time_ms, decision.fallback_regions,
            )


def sim_replication(ctx: Dict[str, Any]) -> None:
    logger.info("\n=== MODEL REPLICATION SIMULATION ===")
    mgr: ModelReplicationManager = ctx["replication_mgr"]

    models = ["llama-70b", "gpt-neo", "whisper-large"]
    for model_id in models:
        for region in REGIONS[:3]:
            replica = ModelReplica(
                model_id=model_id,
                region_id=region.region_id,
                tier=ReplicationTier.hot if region.region_id == "us-east-1" else ReplicationTier.warm,
                state=ReplicationState.completed,
                size_gb=random.uniform(10, 150),
            )
            mgr.add_replica(replica)
        logger.info("  Replicated %s across %d regions", model_id, 3)

    for _ in range(20):
        model = random.choice(models)
        region = random.choice(REGIONS[:3])
        mgr.record_access(model, region.region_id)

    stats = mgr.metrics()
    logger.info("  Replication stats: %s", json.dumps(stats, indent=2))


def sim_placement(ctx: Dict[str, Any]) -> None:
    logger.info("\n=== WORKLOAD PLACEMENT SIMULATION ===")
    fabric: ComputeFabric = ctx["compute_fabric"]

    workloads = [
        PlacementRequest(workload_id="wl-1", require_gpu=True, gpu_type="A100", min_vram_gb=40.0),
        PlacementRequest(workload_id="wl-2", require_gpu=True, preferred_region="eu-west-1"),
        PlacementRequest(workload_id="wl-3", require_gpu=False, preferred_region="edge-nyc"),
        PlacementRequest(workload_id="wl-4", require_gpu=True, min_vram_gb=80.0, performance_weight=0.8),
        PlacementRequest(workload_id="wl-5", require_gpu=True, gpu_type="H100"),
    ]

    for wl in workloads:
        result = fabric.submit_workload(wl)
        if result:
            logger.info("  %s -> %s (score=%.3f)", wl.workload_id, result["region_id"], result["score"])


def sim_region_outage(ctx: Dict[str, Any]) -> None:
    logger.info("\n=== REGION OUTAGE SIMULATION ===")
    registry: RegionRegistry = ctx["registry"]
    failover: SmartFailover = ctx["failover"]
    metrics: GlobalMetricsCollector = ctx["metrics"]

    registry.update_status("ap-east-1", RegionStatus.offline)
    logger.info("  Region ap-east-1 marked OFFLINE")

    events = failover.check_all()
    for event in events:
        metrics.record_failover(event.source_region, event.target_region, event.trigger.value)
        logger.info(
            "  Failover: %s -> %s (trigger=%s)",
            event.source_region, event.target_region, event.trigger.value,
        )

    failover.recover_region("ap-east-1")
    logger.info("  Region ap-east-1 RECOVERED")


def sim_latency_spike(ctx: Dict[str, Any]) -> None:
    logger.info("\n=== LATENCY SPIKE SIMULATION ===")
    registry: RegionRegistry = ctx["registry"]
    latency_map: LatencyMap = ctx["latency_map"]
    failover: SmartFailover = ctx["failover"]

    region = registry.get("eu-west-1")
    if region:
        registry.heartbeat("eu-west-1", {"avg_latency_ms": 1500.0})
        logger.info("  eu-west-1 latency spiked to 1500ms")

    events = failover.check_all()
    for event in events:
        logger.info(
            "  Failover triggered: %s -> %s (%s)",
            event.source_region, event.target_region, event.trigger.value,
        )

    registry.heartbeat("eu-west-1", {"avg_latency_ms": 25.0})
    failover.recover_region("eu-west-1")
    logger.info("  eu-west-1 latency recovered")


def sim_cache_fabric(ctx: Dict[str, Any]) -> None:
    logger.info("\n=== DISTRIBUTED CACHE SIMULATION ===")
    cache_us = DistributedCacheFabric("us-east-1")
    cache_eu = DistributedCacheFabric("eu-west-1")

    for i in range(50):
        cache_us.put(f"model:llama-70b:chunk-{i}", CacheEntryType.model, size_bytes=1024 * 1024)
    for i in range(30):
        cache_eu.put(f"embedding:doc-{i}", CacheEntryType.embedding, size_bytes=4096)

    for i in range(100):
        key = f"model:llama-70b:chunk-{random.randint(0, 49)}"
        cache_us.get(key)

    logger.info("  US cache metrics: %s", json.dumps(cache_us.metrics(), indent=2))
    logger.info("  EU cache metrics: %s", json.dumps(cache_eu.metrics(), indent=2))

    warm = cache_us.get_warm_candidates()
    logger.info("  Warm candidates for replication: %d keys", len(warm))


def sim_network_partition(ctx: Dict[str, Any]) -> None:
    logger.info("\n=== NETWORK PARTITION SIMULATION ===")
    topology: TopologyManager = ctx["topology"]

    topology.update_link("us-east-1", "ap-east-1", healthy=False)
    topology.update_link("ap-east-1", "us-east-1", healthy=False)
    logger.info("  Partition: us-east-1 <-> ap-east-1 severed")

    path = topology.shortest_path("us-east-1", "ap-east-1")
    if path:
        logger.info("  Alternate path found: %s", " -> ".join(path))
    else:
        logger.info("  No path between us-east-1 and ap-east-1")

    topology.update_link("us-east-1", "ap-east-1", latency_ms=150.0, bandwidth_mbps=5000, healthy=True)
    topology.update_link("ap-east-1", "us-east-1", latency_ms=150.0, bandwidth_mbps=5000, healthy=True)
    logger.info("  Partition healed")


def print_final_report(ctx: Dict[str, Any]) -> None:
    logger.info("\n" + "=" * 60)
    logger.info("SIMULATION REPORT")
    logger.info("=" * 60)

    metrics: GlobalMetricsCollector = ctx["metrics"]
    routing_metrics: RoutingMetrics = ctx["routing_metrics"]
    overlay: OverlayNetwork = ctx["overlay"]
    topology: TopologyManager = ctx["topology"]
    latency_map: LatencyMap = ctx["latency_map"]
    compute_fabric: ComputeFabric = ctx["compute_fabric"]

    logger.info("\nGlobal Metrics:")
    gm = metrics.global_metrics()
    logger.info("  Counters: %s", json.dumps(gm["counters"], indent=4))
    logger.info("  Request Latency: %s", json.dumps(gm["request_latency"], indent=4))

    logger.info("\nRouting Metrics:")
    rm = routing_metrics.snapshot()
    logger.info("  Total routes: %d", rm["total_routes"])
    logger.info("  By region: %s", json.dumps(rm["by_region"], indent=4))
    logger.info("  By strategy: %s", json.dumps(rm["by_strategy"], indent=4))

    logger.info("\nTopology:")
    topo = topology.metrics()
    logger.info("  Links: %d total, %d healthy", topo["total_links"], topo["healthy_links"])
    logger.info("  Avg latency: %.1fms", topo["avg_latency_ms"])

    logger.info("\nLatency Map:")
    lm = latency_map.summary()
    logger.info("  Inter-region pairs: %d", lm["inter_region_pairs"])
    logger.info("  Avg inter-region: %.1fms", lm["avg_inter_region_ms"])

    logger.info("\nOverlay Network:")
    om = overlay.metrics()
    logger.info("  Connected peers: %d", om["connected_peers"])

    logger.info("\nCompute Fabric:")
    cf = compute_fabric.metrics()
    logger.info("  Active workloads: %d", cf["active_workloads"])
    logger.info("  By region: %s", json.dumps(cf["by_region"], indent=4))

    logger.info("\n" + "=" * 60)
    logger.info("SIMULATION COMPLETE")
    logger.info("=" * 60)


def main() -> None:
    logger.info("=" * 60)
    logger.info("Arsonist OS v13 — Multi-Region Simulation")
    logger.info("=" * 60)

    ctx = setup_fabric()

    sim_routing(ctx)
    sim_replication(ctx)
    sim_placement(ctx)
    sim_region_outage(ctx)
    sim_latency_spike(ctx)
    sim_cache_fabric(ctx)
    sim_network_partition(ctx)
    print_final_report(ctx)


if __name__ == "__main__":
    main()
