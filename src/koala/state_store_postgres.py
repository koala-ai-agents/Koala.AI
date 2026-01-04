"""Postgres-backed StateStore.

Optional dependency: `psycopg2-binary`. This adapter provides a minimal
transactional implementation of the `StateStore` interface using Postgres.
"""

from __future__ import annotations

from typing import Any, List, Optional

from .state_store import StateStore, StateStoreError

try:
    import psycopg2  # type: ignore[import-untyped]
    from psycopg2.extras import Json  # type: ignore[import-untyped]
except Exception:  # pragma: no cover - optional dep may be missing
    psycopg2 = None  # type: ignore
    Json = None  # type: ignore


class PostgresStateStore(StateStore):
    def __init__(self, dsn: str, table: str = "state"):
        if psycopg2 is None:
            raise StateStoreError(
                "psycopg2 not installed. Install with extras: pip install '.[postgres]' or add psycopg2-binary"
            )
        self.dsn = dsn
        self.table = table
        self._ensure_table()

    def _ensure_table(self) -> None:
        conn = psycopg2.connect(self.dsn)
        try:
            conn.autocommit = True
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    CREATE TABLE IF NOT EXISTS {self.table} (
                        key TEXT PRIMARY KEY,
                        value JSONB
                    )
                    """
                )
        finally:
            conn.close()

    def set(self, key: str, value: Any) -> None:
        conn = psycopg2.connect(self.dsn)
        try:
            conn.autocommit = False
            with conn.cursor() as cur:
                cur.execute(
                    f"INSERT INTO {self.table} (key, value) VALUES (%s, %s) ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value",
                    (key, Json(value)),
                )
            conn.commit()
        except Exception as e:
            conn.rollback()
            raise StateStoreError(str(e)) from e
        finally:
            conn.close()

    def get(self, key: str) -> Optional[Any]:
        conn = psycopg2.connect(self.dsn)
        try:
            with conn.cursor() as cur:
                cur.execute(f"SELECT value FROM {self.table} WHERE key = %s", (key,))
                row = cur.fetchone()
                if not row:
                    return None
                return row[0]
        except Exception as e:
            raise StateStoreError(str(e)) from e
        finally:
            conn.close()

    def delete(self, key: str) -> None:
        conn = psycopg2.connect(self.dsn)
        try:
            conn.autocommit = False
            with conn.cursor() as cur:
                cur.execute(f"DELETE FROM {self.table} WHERE key = %s", (key,))
            conn.commit()
        except Exception as e:
            conn.rollback()
            raise StateStoreError(str(e)) from e
        finally:
            conn.close()

    def list_keys(self, prefix: Optional[str] = None) -> List[str]:
        conn = psycopg2.connect(self.dsn)
        try:
            with conn.cursor() as cur:
                if prefix is None:
                    cur.execute(f"SELECT key FROM {self.table}")
                else:
                    like = f"{prefix}%"
                    cur.execute(
                        f"SELECT key FROM {self.table} WHERE key LIKE %s", (like,)
                    )
                rows = cur.fetchall()
                return [r[0] for r in rows]
        except Exception as e:
            raise StateStoreError(str(e)) from e
        finally:
            conn.close()
