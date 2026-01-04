"""Very small HTTP metrics endpoint exposing observability.metrics.export_prometheus.

This is intended for local testing only (no production hardened server).
"""

from __future__ import annotations

import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Tuple

from .observability import metrics


class _MetricsHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path != "/metrics":
            self.send_response(404)
            self.end_headers()
            return
        data = metrics.export_prometheus().encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "text/plain; version=0.0.4")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)


class MetricsServer:
    def __init__(self, host: str = "127.0.0.1", port: int = 8000):
        self._server = HTTPServer((host, port), _MetricsHandler)
        self._host = host
        self._port = port
        self._thread: threading.Thread | None = None

    def start(self) -> Tuple[str, int]:
        def run():
            self._server.serve_forever()

        self._thread = threading.Thread(target=run, daemon=True)
        self._thread.start()
        # Return the actual bound host/port assigned by OS (supports port=0)
        addr = self._server.server_address
        try:
            host, port = addr  # type: ignore[misc]
            return (str(host), int(port))
        except Exception:
            # Fallback to configured values
            return (self._host, self._port)

    def stop(self) -> None:
        self._server.shutdown()
        if self._thread:
            self._thread.join(timeout=1.0)
