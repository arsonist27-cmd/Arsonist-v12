from __future__ import annotations

import os
from typing import Any, Callable

from fastapi import APIRouter, Depends, FastAPI

from gpu.gpu_discovery import GpuDiscovery
from gpu.gpu_metrics import GpuMetrics
from gpu.gpu_scheduler import GpuScheduler
from gpu.vram_manager import VramManager
from inference.inference_server import InferenceServer
from inference.openai_routes import router as openai_router
from models.model_router import ModelRouter
from models.registry import ModelRecord, ModelRegistry
from orchestrator.deployment_manager import DeploymentManager
from orchestrator.rollout_manager import RolloutManager
from orchestrator.runtime_manager import RuntimeManager
from scaling.inference_autoscaler import InferenceAutoscaler
from scaling.gpu_scaler import GpuScaler
from telemetry.workload_metrics import WorkloadMetrics


def ai_orchestration_enabled() -> bool:
    mode = os.getenv("ARSONIST_ORCHESTRATION_MODE", "").lower().strip()
    if mode in ("ai", "ai_native", "v11"):
        return True
    return os.getenv("ARSONIST_AI_ORCHESTRATION_ENABLED", "").lower() in ("1", "true", "yes")


def attach_v11(app: FastAPI, require_token: Callable[..., Any]) -> None:
    """Wire v11 routers and shared app.state; safe to call when disabled (no-op)."""
    discovery = GpuDiscovery()
    vram = VramManager()
    gpu_sched = GpuScheduler(discovery, vram)
    gpu_metrics = GpuMetrics(discovery)
    registry = ModelRegistry()
    model_router = ModelRouter(registry, gpu_sched)
    runtime_mgr = RuntimeManager()
    deployments = DeploymentManager(runtime_mgr)
    rollouts = RolloutManager(runtime_mgr)
    workload_metrics = WorkloadMetrics()
    inf_srv = InferenceServer()
    app.state.ollama = inf_srv.backend
    app.state.inference_metrics = inf_srv.metrics
    app.state.tokenizer_pool = inf_srv.tokenizer_pool
    app.state.v11_registry = registry
    app.state.v11_gpu_scheduler = gpu_sched
    app.state.v11_model_router = model_router
    app.state.v11_runtime = runtime_mgr
    app.state.v11_deployments = deployments
    app.state.v11_rollouts = rollouts
    app.state.v11_workload_metrics = workload_metrics
    app.state.v11_gpu_metrics = gpu_metrics

    metrics_router = APIRouter(tags=["v11-metrics"])

    @metrics_router.get("/inference_metrics")
    def inference_metrics(_: None = Depends(require_token)) -> Any:
        return inf_srv.metrics.snapshot()

    @metrics_router.get("/gpu_metrics")
    def gpu_metrics(_: None = Depends(require_token)) -> Any:
        return gpu_metrics.snapshot()

    @metrics_router.get("/deployment_metrics")
    def deployment_metrics(_: None = Depends(require_token)) -> Any:
        return {
            "deployments": runtime_mgr.list_deployments(),
            "workload": workload_metrics.snapshot(),
        }

    @metrics_router.post("/v11/models/register")
    def register_model(payload: dict, _: None = Depends(require_token)) -> Any:
        rec = ModelRecord(**payload)
        errs = registry.validate_record(rec)
        if errs:
            return {"errors": errs}
        registry.upsert(rec)
        return {"status": "ok", "model_id": rec.model_id}

    @metrics_router.get("/v11/models/search")
    def search_models(q: str, _: None = Depends(require_token)) -> Any:
        return {"models": [m.model_dump() for m in registry.search(q)]}

    @metrics_router.post("/v11/deployments")
    def create_dep(payload: dict, _: None = Depends(require_token)) -> Any:
        name = str(payload.get("name", "app"))
        image = str(payload.get("image", "python:3.11-slim"))
        replicas = int(payload.get("replicas", 1))
        dep_id = deployments.create(name, image, replicas)
        workload_metrics.inc_deployment()
        return {"deployment_id": dep_id}

    @metrics_router.post("/v11/rollouts")
    def start_rollout(payload: dict, _: None = Depends(require_token)) -> Any:
        dep_id = str(payload.get("deployment_id", ""))
        image = str(payload.get("image", ""))
        if not dep_id or not image:
            return {"error": "deployment_id and image required"}
        out = rollouts.start_rollout(dep_id, image, float(payload.get("canary", 0.1)))
        workload_metrics.inc_rollout()
        return out

    app.include_router(openai_router)
    app.include_router(metrics_router)

    if ai_orchestration_enabled():
        autoscaler = InferenceAutoscaler(inf_srv.metrics, scale_fn=lambda reason, n: None)
        autoscaler.start_background()
        GpuScaler(gpu_metrics, fn=None).start_background()
