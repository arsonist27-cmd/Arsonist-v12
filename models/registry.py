from __future__ import annotations

import json
import os
import sqlite3
import threading
import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, ConfigDict, Field

from shared.utils import now_ts


class ModelRecord(BaseModel):
    model_config = ConfigDict(extra="ignore")

    model_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    model_name: str
    version: str = "1"
    parameter_count: int = 0
    quantization: str = "none"
    supported_gpu_types: List[str] = Field(default_factory=list)
    required_vram_mb: int = 8192
    tokenizer: str = "default"
    architecture: str = "unknown"
    source_uri: str = ""
    checksum_sha256: str = ""
    updated_at: float = 0.0


class ModelRegistry:
    """SQLite-backed model metadata registry with simple search."""

    def __init__(self, db_path: str | None = None) -> None:
        self._lock = threading.RLock()
        self.db_path = db_path or os.getenv(
            "ARSONIST_MODEL_REGISTRY_DB",
            os.path.join(os.path.dirname(os.getenv("ARSONIST_DB_PATH", "control_plane/arsonist.db")), "models_v11.db"),
        )
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    def _conn(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_db(self) -> None:
        conn = self._conn()
        with conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS models (
                    model_id TEXT PRIMARY KEY,
                    payload TEXT NOT NULL,
                    updated_at REAL NOT NULL
                )
                """
            )
        conn.close()

    def upsert(self, rec: ModelRecord) -> None:
        rec.updated_at = now_ts()
        blob = json.dumps(rec.model_dump())
        with self._lock:
            conn = self._conn()
            with conn:
                conn.execute(
                    "INSERT INTO models(model_id, payload, updated_at) VALUES(?,?,?) "
                    "ON CONFLICT(model_id) DO UPDATE SET payload=excluded.payload, updated_at=excluded.updated_at",
                    (rec.model_id, blob, rec.updated_at),
                )
            conn.close()

    def get(self, model_id: str) -> Optional[ModelRecord]:
        with self._lock:
            conn = self._conn()
            try:
                row = conn.execute("SELECT payload FROM models WHERE model_id = ?", (model_id,)).fetchone()
                return ModelRecord(**json.loads(row["payload"])) if row else None
            finally:
                conn.close()

    def search(self, query: str, limit: int = 50) -> List[ModelRecord]:
        q = query.lower()
        out: List[ModelRecord] = []
        with self._lock:
            conn = self._conn()
            try:
                rows = conn.execute("SELECT payload FROM models ORDER BY updated_at DESC").fetchall()
                for r in rows:
                    rec = ModelRecord(**json.loads(r["payload"]))
                    if q in rec.model_name.lower() or q in rec.architecture.lower():
                        out.append(rec)
                    if len(out) >= limit:
                        break
                return out
            finally:
                conn.close()

    def validate_record(self, rec: ModelRecord) -> List[str]:
        errors: List[str] = []
        if not rec.model_name:
            errors.append("model_name required")
        if rec.required_vram_mb <= 0:
            errors.append("required_vram_mb must be positive")
        return errors
