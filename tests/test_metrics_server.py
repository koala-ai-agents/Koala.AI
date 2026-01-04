import urllib.request as urllib_request

from koala.metrics_server import MetricsServer
from koala.observability import metrics


def test_metrics_endpoint():
    # reset some metrics
    metrics.inc("test_server_counter", 3)
    srv = MetricsServer(host="127.0.0.1", port=0)
    addr = srv.start()
    host, port = addr
    # request /metrics
    url = f"http://{host}:{port}/metrics"
    with urllib_request.urlopen(url, timeout=2.0) as resp:
        body = resp.read().decode("utf-8")
    assert "test_server_counter 3" in body
    srv.stop()
