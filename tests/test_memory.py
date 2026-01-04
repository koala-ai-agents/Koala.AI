from koala.memory import InMemoryStore, SQLiteStore


def test_inmemory_set_get_delete_list():
    m = InMemoryStore()
    m.set("k1", {"a": 1})
    assert m.get("k1") == {"a": 1}
    m.set("k2", 123)
    keys = set(m.list_keys())
    assert "k1" in keys and "k2" in keys
    m.delete("k1")
    assert m.get("k1") is None


def test_sqlite_store_persistence(tmp_path):
    db = tmp_path / "mem.db"
    s = SQLiteStore(str(db))
    s.set("x", [1, 2, 3])
    assert s.get("x") == [1, 2, 3]
    s.set("y", {"v": "ok"})
    keys = set(s.list_keys())
    assert "x" in keys and "y" in keys
    s.delete("x")
    assert s.get("x") is None
