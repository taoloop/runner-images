"""Reference miner server: the entry point of the approved miner-server image.

``/opt/taoloop/miner_server --candidate-dir=/candidate --port=N --health-path=/path``

The image, not the candidate, owns this entry point. It answers the health path itself and
hands every other request to the candidate's own handler, when it has one. A candidate offers
``taoloop_miner.py`` defining ``handle(method, path, headers, body) -> (status, headers,
body)``; without it only the health path answers. This is a protocol-neutral harness for the
hosting lifecycle (staging, rollout, rollback, health), not an implementation of any subnet's
validator protocol: a subnet adapter ships its own image on this contract.

The candidate directory is mounted read-only. The handler runs in this process, inside the
container the runner confines (read-only root, no capabilities, bounded memory and PIDs).

With ``--services=<json>`` (ADR 011) the runner has mounted its per-server proxy at
``/run/taoloop/services.sock``; a loopback bridge here forwards ``/svc/<id>/...`` to it and the
connection's SDK base URL variables (and ``TAOLOOP_SERVICE_<PROVIDER>_URL``) point at the
bridge before the candidate loads. The candidate never sees a key: the runner adds it.
"""

import argparse
import http.client
import importlib.util
import json
import os
import re
import socket
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

MAX_BODY = 1_000_000
HEALTH = re.compile(r"^/[A-Za-z0-9_./-]{0,99}$")


SERVICES_SOCKET = "/run/taoloop/services.sock"
SERVICE_ID = re.compile(r"^[0-9a-f-]{36}$")
SERVICE_PROVIDER = re.compile(r"^[a-z0-9][a-z0-9-]{1,39}$")
SERVICE_VARIABLE = re.compile(r"^[A-Z][A-Z0-9_]{1,63}$")


class SocketConnection(http.client.HTTPConnection):
    def __init__(self, path: str) -> None:
        super().__init__("local")
        self.path = path

    def connect(self) -> None:
        self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.sock.settimeout(120)
        self.sock.connect(self.path)


def service_bridge(allowed: set[str]):
    class Bridge(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, *args) -> None:
            return

        def forward(self) -> None:
            parts = self.path.split("/", 3)
            if len(parts) < 3 or parts[1] != "svc" or parts[2] not in allowed:
                self.send_error(404)
                return
            length = int(self.headers.get("Content-Length") or 0)
            if length > 16 * 1024 * 1024:
                self.send_error(413)
                return
            body = self.rfile.read(length) if length else None
            headers = {
                name: self.headers[name]
                for name in ("Content-Type", "Accept", "Range", "Authorization", "X-Amz-Date")
                if self.headers.get(name)
            }
            if body is not None:
                headers["Content-Length"] = str(len(body))
            connection = SocketConnection(SERVICES_SOCKET)
            try:
                connection.request(self.command, self.path, body, headers)
                response = connection.getresponse()
                data = response.read()
                self.send_response(response.status)
                self.send_header(
                    "Content-Type", response.getheader("Content-Type", "application/octet-stream")
                )
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                if self.command != "HEAD":
                    self.wfile.write(data)
            except OSError:
                self.send_error(502)
            finally:
                connection.close()

        do_GET = do_POST = do_PUT = do_HEAD = forward  # noqa: N815

    return Bridge


def start_services(encoded: str) -> dict[str, str]:
    """Start the loopback bridge and return the environment that points SDKs at it."""
    items = json.loads(encoded)
    if not isinstance(items, list) or len(items) > 4:
        raise SystemExit("Invalid service connections")
    allowed: dict[str, tuple[str, list[str]]] = {}
    for item in items:
        connection, provider = item.get("connection_id"), item.get("provider")
        variables = item.get("base_url_env") or []
        if (
            not isinstance(connection, str)
            or not SERVICE_ID.match(connection)
            or not isinstance(provider, str)
            or not SERVICE_PROVIDER.match(provider)
            or not isinstance(variables, list)
            or any(not isinstance(v, str) or not SERVICE_VARIABLE.match(v) for v in variables)
        ):
            raise SystemExit("Invalid service connection")
        allowed[connection] = (provider, variables)
    bridge = ThreadingHTTPServer(("127.0.0.1", 0), service_bridge(set(allowed)))
    bridge.daemon_threads = True
    threading.Thread(target=bridge.serve_forever, daemon=True).start()
    port = bridge.server_address[1]
    env: dict[str, str] = {}
    for connection, (provider, variables) in allowed.items():
        url = f"http://127.0.0.1:{port}/svc/{connection}"
        for variable in [
            *variables,
            "TAOLOOP_SERVICE_" + provider.upper().replace("-", "_") + "_URL",
        ]:
            env[variable] = url
    if allowed:
        env.setdefault("AWS_ACCESS_KEY_ID", "taoloop-placeholder")
        env.setdefault("AWS_SECRET_ACCESS_KEY", "taoloop-placeholder")
    return env


def load_handler(candidate: Path):
    path = candidate / "taoloop_miner.py"
    if not path.is_file() or path.is_symlink():
        return None
    spec = importlib.util.spec_from_file_location("taoloop_miner", path)
    if spec is None or spec.loader is None:
        return None
    module = importlib.util.module_from_spec(spec)
    sys.dont_write_bytecode = True
    spec.loader.exec_module(module)
    handler = getattr(module, "handle", None)
    return handler if callable(handler) else None


def make_handler(health_path: str, candidate: Path):
    handle = load_handler(candidate)

    class Handler(BaseHTTPRequestHandler):
        server_version = "taoloop-miner"
        protocol_version = "HTTP/1.1"

        def log_message(self, *args) -> None:  # no request logging: nothing to leak
            return

        def reply(self, status: int, body: bytes, headers: dict[str, str] | None = None) -> None:
            self.send_response(status)
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Content-Type", "application/json")
            for key, value in (headers or {}).items():
                if re.fullmatch(r"[A-Za-z0-9-]{1,64}", key) and "\n" not in value:
                    self.send_header(key, value[:256])
            self.end_headers()
            self.wfile.write(body)

        def serve(self) -> None:
            length = int(self.headers.get("Content-Length") or 0)
            if length > MAX_BODY:
                self.reply(413, b'{"error":"body too large"}')
                return
            body = self.rfile.read(length) if length else b""
            if self.command == "GET" and self.path.split("?", 1)[0] == health_path:
                self.reply(
                    200, json.dumps({"status": "ok", "handler": handle is not None}).encode()
                )
                return
            if handle is None:
                self.reply(404, b'{"error":"no handler"}')
                return
            try:
                status, headers, payload = handle(
                    self.command, self.path, dict(self.headers.items()), body
                )
            except Exception:  # noqa: BLE001 - a faulty candidate must not take the server down
                self.reply(500, b'{"error":"handler failed"}')
                return
            data = payload if isinstance(payload, bytes) else str(payload).encode()
            self.reply(int(status), data, headers if isinstance(headers, dict) else None)

        do_GET = do_POST = do_PUT = do_DELETE = serve  # noqa: N815

    return Handler


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--candidate-dir", type=Path, required=True)
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--health-path", required=True)
    parser.add_argument("--services", default="")
    args = parser.parse_args()
    if not HEALTH.fullmatch(args.health_path) or not 1024 <= args.port <= 65535:
        print("Invalid port or health path", file=sys.stderr)
        return 2
    if args.services:
        # Before the candidate loads, so its SDKs read the bridge's base URLs.
        os.environ.update(start_services(args.services))
    server = ThreadingHTTPServer(
        ("0.0.0.0", args.port), make_handler(args.health_path, args.candidate_dir)
    )  # noqa: S104
    server.daemon_threads = True
    server.serve_forever()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
