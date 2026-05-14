from __future__ import annotations

import os
import time
import uuid
from typing import Any, Dict

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import JSONResponse

from inference.ollama_backend import OllamaBackend
from inference.tokenizer_pool import TokenizerPool
from security.inference_auth import require_inference_auth
from telemetry.inference_metrics import InferenceMetrics

router = APIRouter(prefix="/v1", tags=["openai-v1"])


def get_backend(request: Request) -> OllamaBackend:
    return request.app.state.ollama  # type: ignore[attr-defined]


def get_metrics(request: Request) -> InferenceMetrics:
    return request.app.state.inference_metrics  # type: ignore[attr-defined]


def get_tokenizer_pool(request: Request) -> TokenizerPool:
    return request.app.state.tokenizer_pool  # type: ignore[attr-defined]


@router.post("/chat/completions")
async def chat_completions(
    payload: Dict[str, Any],
    _: None = Depends(require_inference_auth),
    backend: OllamaBackend = Depends(get_backend),
    metrics: InferenceMetrics = Depends(get_metrics),
    pool: TokenizerPool = Depends(get_tokenizer_pool),
) -> JSONResponse:
    model = str(payload.get("model", os.getenv("ARSONIST_DEFAULT_CHAT_MODEL", "llama3.2")))
    messages = payload.get("messages") or []
    stream = bool(payload.get("stream", False))
    if stream:
        raise HTTPException(status_code=400, detail="streaming mode not implemented; set stream=false")
    if not backend.available():
        raise HTTPException(status_code=503, detail="inference backend unavailable (set OLLAMA_HOST)")
    t0 = time.perf_counter()
    th = pool.acquire()
    try:
        data = await backend.chat(model=model, messages=messages, stream=False)
    finally:
        if th:
            pool.release(th)
    dt_ms = (time.perf_counter() - t0) * 1000.0
    metrics.record_chat(model=model, latency_ms=dt_ms, tokens_out=len(str(data.get("message", {}).get("content", ""))) // 4)
    # OpenAI-ish shape
    content = str(data.get("message", {}).get("content", ""))
    return JSONResponse(
        {
            "id": f"chatcmpl-{uuid.uuid4()}",
            "object": "chat.completion",
            "created": int(time.time()),
            "model": model,
            "choices": [{"index": 0, "message": {"role": "assistant", "content": content}, "finish_reason": "stop"}],
        }
    )


@router.post("/embeddings")
async def embeddings(
    payload: Dict[str, Any],
    _: None = Depends(require_inference_auth),
    backend: OllamaBackend = Depends(get_backend),
    metrics: InferenceMetrics = Depends(get_metrics),
) -> JSONResponse:
    model = str(payload.get("model", os.getenv("ARSONIST_DEFAULT_EMBED_MODEL", "nomic-embed-text")))
    inp = payload.get("input")
    if isinstance(inp, list):
        text = inp[0] if inp else ""
    else:
        text = str(inp or "")
    if not backend.available():
        raise HTTPException(status_code=503, detail="inference backend unavailable")
    t0 = time.perf_counter()
    data = await backend.embeddings(model=model, prompt=text)
    dt_ms = (time.perf_counter() - t0) * 1000.0
    emb = data.get("embedding") or []
    metrics.record_embedding(model=model, latency_ms=dt_ms, dim=len(emb))
    return JSONResponse(
        {
            "object": "list",
            "data": [{"object": "embedding", "embedding": emb, "index": 0}],
            "model": model,
        }
    )


@router.post("/generate")
async def generate(
    payload: Dict[str, Any],
    _: None = Depends(require_inference_auth),
    backend: OllamaBackend = Depends(get_backend),
    metrics: InferenceMetrics = Depends(get_metrics),
) -> JSONResponse:
    model = str(payload.get("model", os.getenv("ARSONIST_DEFAULT_CHAT_MODEL", "llama3.2")))
    prompt = str(payload.get("prompt", ""))
    if not backend.available():
        raise HTTPException(status_code=503, detail="inference backend unavailable")
    t0 = time.perf_counter()
    data = await backend.generate(model=model, prompt=prompt)
    dt_ms = (time.perf_counter() - t0) * 1000.0
    text = str(data.get("response", ""))
    metrics.record_generate(model=model, latency_ms=dt_ms, tokens_out=len(text) // 4)
    return JSONResponse({"model": model, "response": text})
