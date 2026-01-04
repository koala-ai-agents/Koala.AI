"""RunRepository: simple SQLite storage for orchestrator run metadata and results.

Provides methods to create runs, update status, store results, and fetch them.
This keeps run state durable across process restarts for demos and tests.
"""

from __future__ import annotations

import json
import os
import sqlite3
from typing import Any, Dict, Optional


class RunRepoError(Exception):
    pass


class RunRepository:
    def __init__(self, db_path: Optional[str] = None):
        # default to a local sqlite file in the cwd
        self.db_path = db_path or "kola_runs.db"
        os.makedirs(os.path.dirname(self.db_path) or ".", exist_ok=True)
        self.conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self._ensure_table()

    def _ensure_table(self) -> None:
        cur = self.conn.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS runs (
                id TEXT PRIMARY KEY,
                flow_id TEXT,
                status TEXT,
                payload TEXT,
                result TEXT
            )
            """
        )
        self.conn.commit()

    def create_run(self, run_id: str, flow_id: str, payload: Dict[str, Any]) -> None:
        cur = self.conn.cursor()
        cur.execute(
            "INSERT OR REPLACE INTO runs (id, flow_id, status, payload, result) VALUES (?, ?, ?, ?, ?)",
            (run_id, flow_id, "pending", json.dumps(payload), None),
        )
        self.conn.commit()

    def update_status(self, run_id: str, status: str) -> None:
        cur = self.conn.cursor()
        cur.execute("UPDATE runs SET status = ? WHERE id = ?", (status, run_id))
        self.conn.commit()

    def save_result(self, run_id: str, result: Any) -> None:
        cur = self.conn.cursor()
        cur.execute(
            "UPDATE runs SET result = ?, status = ? WHERE id = ?",
            (json.dumps(result), "done", run_id),
        )
        self.conn.commit()

    def fail_run(self, run_id: str, error: str) -> None:
        cur = self.conn.cursor()
        cur.execute(
            "UPDATE runs SET result = ?, status = ? WHERE id = ?",
            (json.dumps({"error": error}), "failed", run_id),
        )
        self.conn.commit()

    def get_status(self, run_id: str) -> Optional[str]:
        cur = self.conn.cursor()
        cur.execute("SELECT status FROM runs WHERE id = ?", (run_id,))
        row = cur.fetchone()
        return row[0] if row else None

    def get_result(self, run_id: str) -> Optional[Any]:
        cur = self.conn.cursor()
        cur.execute("SELECT result FROM runs WHERE id = ?", (run_id,))
        row = cur.fetchone()
        if not row or row[0] is None:
            return None
        try:
            return json.loads(row[0])
        except Exception:
            return row[0]
