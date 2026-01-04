from koala.output import send_to_file
from koala.queue import InMemoryQueue


def test_send_to_file(tmp_path):
    p = tmp_path / "out" / "data.ndjson"
    payload = {"a": 1}
    send_to_file(str(p), payload)
    content = p.read_text(encoding="utf-8").strip()
    assert content


def test_inmemory_queue_publish_subscribe():
    q = InMemoryQueue()
    seen = []

    def handler(msg):
        seen.append(msg)

    q.subscribe("t1", handler)
    q.publish("t1", {"x": 1})
    assert len(seen) == 1
    assert seen[0]["x"] == 1
