from __future__ import annotations

import os

from inference.ollama_backend import OllamaBackend
from inference.tokenizer_pool import TokenizerPool
from telemetry.inference_metrics import InferenceMetrics


class InferenceServer:
    """Coordinates inference metrics, tokenizer pool, and backend selection."""

    def __init__(self) -> None:
        self.backend = OllamaBackend()
        self.metrics = InferenceMetrics()
        self.tokenizer_pool = TokenizerPool(int(os.getenv("ARSONIST_TOKENIZER_POOL", "4")))
