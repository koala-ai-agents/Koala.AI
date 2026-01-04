"""Simple Memory subsystem for Kola.

Provides an in-memory key-value store and a SQLite-backed persistent store.
The API is intentionally small: set, get, delete, list_keys.
Values are JSON-serialised for storage.
"""

from __future__ import annotations

import json
import os
import sqlite3
import time
from typing import Any, Dict, Iterable, Optional


class MemoryError(Exception):
    pass


class MemoryStore:
    def set(self, key: str, value: Any) -> None:
        raise NotImplementedError()

    def get(self, key: str) -> Optional[Any]:
        raise NotImplementedError()

    def delete(self, key: str) -> None:
        raise NotImplementedError()

    def list_keys(self, prefix: Optional[str] = None) -> Iterable[str]:
        raise NotImplementedError()


class InMemoryStore(MemoryStore):
    def __init__(self) -> None:
        self._store: Dict[str, Any] = {}

    def set(self, key: str, value: Any) -> None:
        self._store[key] = value

    def get(self, key: str) -> Optional[Any]:
        return self._store.get(key)

    def delete(self, key: str) -> None:
        self._store.pop(key, None)

    def list_keys(self, prefix: Optional[str] = None) -> Iterable[str]:
        if prefix is None:
            return list(self._store.keys())
        return [k for k in self._store.keys() if k.startswith(prefix)]


class SQLiteStore(MemoryStore):
    def __init__(self, db_path: str):
        os.makedirs(os.path.dirname(db_path) or ".", exist_ok=True)
        self.conn = sqlite3.connect(db_path)
        self._ensure_table()

    def _ensure_table(self) -> None:
        cur = self.conn.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS memory (
                key TEXT PRIMARY KEY,
                value TEXT,
                updated REAL
            )
            """
        )
        self.conn.commit()

    def set(self, key: str, value: Any) -> None:
        cur = self.conn.cursor()
        cur.execute(
            "REPLACE INTO memory (key, value, updated) VALUES (?, ?, ?)",
            (key, json.dumps(value), time.time()),
        )
        self.conn.commit()

    def get(self, key: str) -> Optional[Any]:
        cur = self.conn.cursor()
        cur.execute("SELECT value FROM memory WHERE key = ?", (key,))
        row = cur.fetchone()
        if not row:
            return None
        return json.loads(row[0])

    def delete(self, key: str) -> None:
        cur = self.conn.cursor()
        cur.execute("DELETE FROM memory WHERE key = ?", (key,))
        self.conn.commit()

    def list_keys(self, prefix: Optional[str] = None) -> Iterable[str]:
        cur = self.conn.cursor()
        if prefix is None:
            cur.execute("SELECT key FROM memory")
        else:
            cur.execute("SELECT key FROM memory WHERE key LIKE ?", (prefix + "%",))
        return [r[0] for r in cur.fetchall()]
