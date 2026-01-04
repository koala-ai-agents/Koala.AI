"""State store abstraction and simple implementations.

Provides a minimal key/value state API and a SQLite-backed implementation for
durable state storage. This complements the RunRepository for run metadata and
is useful for storing agent state, checkpoints, or shared data between steps.
"""

from __future__ import annotations

import json
import os
import sqlite3
from threading import Lock
from typing import Any, Dict, List, Optional


class StateStoreError(Exception):
    pass


class StateStore:
    def set(self, key: str, value: Any) -> None:
        raise NotImplementedError()

    def get(self, key: str) -> Optional[Any]:
        raise NotImplementedError()

    def delete(self, key: str) -> None:
        raise NotImplementedError()

    def list_keys(self, prefix: Optional[str] = None) -> List[str]:
        raise NotImplementedError()


class InMemoryStateStore(StateStore):
    def __init__(self) -> None:
        self._store: Dict[str, Any] = {}
        self._lock = Lock()

    def set(self, key: str, value: Any) -> None:
        with self._lock:
            self._store[key] = value

    def get(self, key: str) -> Optional[Any]:
        with self._lock:
            return self._store.get(key)

    def delete(self, key: str) -> None:
        with self._lock:
            self._store.pop(key, None)

    def list_keys(self, prefix: Optional[str] = None) -> List[str]:
        with self._lock:
            if prefix is None:
                return list(self._store.keys())
            return [k for k in self._store.keys() if k.startswith(prefix)]


class SQLiteStateStore(StateStore):
    def __init__(self, path: Optional[str] = None):
        self.path = path or "kola_state.db"
        os.makedirs(os.path.dirname(self.path) or ".", exist_ok=True)
        self.conn = sqlite3.connect(self.path, check_same_thread=False)
        self._lock = Lock()
        self._ensure_table()

    def _ensure_table(self) -> None:
        cur = self.conn.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS state (
                key TEXT PRIMARY KEY,
                value TEXT
            )
            """
        )
        self.conn.commit()

    def set(self, key: str, value: Any) -> None:
        with self._lock:
            cur = self.conn.cursor()
            cur.execute(
                "REPLACE INTO state (key, value) VALUES (?, ?)",
                (key, json.dumps(value)),
            )
            self.conn.commit()

    def get(self, key: str) -> Optional[Any]:
        with self._lock:
            cur = self.conn.cursor()
            cur.execute("SELECT value FROM state WHERE key = ?", (key,))
            row = cur.fetchone()
            if not row:
                return None
            try:
                return json.loads(row[0])
            except Exception:
                return row[0]

    def delete(self, key: str) -> None:
        with self._lock:
            cur = self.conn.cursor()
            cur.execute("DELETE FROM state WHERE key = ?", (key,))
            self.conn.commit()

    def list_keys(self, prefix: Optional[str] = None) -> List[str]:
        with self._lock:
            cur = self.conn.cursor()
            if prefix is None:
                cur.execute("SELECT key FROM state")
                rows = cur.fetchall()
                return [r[0] for r in rows]
            like = f"{prefix}%"
            cur.execute("SELECT key FROM state WHERE key LIKE ?", (like,))
            rows = cur.fetchall()
            return [r[0] for r in rows]
