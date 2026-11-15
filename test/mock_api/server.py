#!/usr/bin/env python3
"""A dependency-free Dagster GraphQL stand-in that replays recorded responses.

The SQL tests run against this server so they stay deterministic and offline. Each
GraphQL request is keyed by its whitespace-normalized query and canonical variables;
the response recorded for that key (from a real open-source ``dagster-webserver``
seeded by ``scripts/dagster_fixture.sh``) is replayed with its original HTTP status.

Endpoints
---------
``POST /graphql``
    Replays the recording for the request, or answers a GraphQL error naming the
    request when there is none (so a missing recording fails a test loudly).
``GET /health``
    Returns ``{"status": "ok"}``.

Behaviour switches (environment)
--------------------------------
* ``MOCK_API_RECORD_UPSTREAM=<url>``: record mode. Forward every request to
  ``<url>/graphql`` and save the response under ``test/fixtures/graphql/``.
* ``MOCK_API_TOKEN``: act like Dagster+. Every request must send
  ``Dagster-Cloud-Api-Token: <token>`` or it gets a 401, and ``fullDeployments``
  is answered from a hand-written Dagster+ fixture.
* ``MOCK_API_FLAKY=1``: the first request for each key returns ``429`` with
  ``Retry-After: 0``, exercising the client's retry path.

Usage::

    python3 test/mock_api/server.py            # prints PORT:<n> once listening
"""

from __future__ import annotations

import argparse
import contextlib
import hashlib
import json
import os
import re
import sys
import threading
import urllib.error
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

RECORDINGS = Path(__file__).resolve().parent.parent / "fixtures" / "graphql"

# What Dagster+ returns for fullDeployments (open-source Dagster has no such field).
CLOUD_DEPLOYMENTS = {
    "data": {
        "fullDeployments": [
            {"deploymentName": "prod", "deploymentId": 1, "deploymentType": "PRODUCTION"},
            {"deploymentName": "staging", "deploymentId": 2, "deploymentType": "DEV"},
            {"deploymentName": "feature-branch-42", "deploymentId": 17, "deploymentType": "BRANCH"},
        ]
    }
}

_flaky_seen: set[str] = set()
_lock = threading.Lock()


def request_key(query: str, variables: Any) -> str:
    """Stable key for a GraphQL request."""
    normalized = re.sub(r"\s+", " ", query).strip()
    canonical = json.dumps(variables or {}, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(f"{normalized}\n{canonical}".encode()).hexdigest()[:24]


def _scrub(text: str) -> str:
    """Keep local paths (home directory, repository root) out of committed recordings."""
    root = str(Path(__file__).resolve().parent.parent.parent)
    return text.replace(root, "/fixture/vgi-dagster").replace(str(Path.home()), "/fixture/home")


def operation_name(query: str) -> str:
    """The ``query Name`` of a document, for readable recordings and errors."""
    match = re.search(r"\b(?:query|mutation)\s+(\w+)", query)
    return match.group(1) if match else "anonymous"


class Handler(BaseHTTPRequestHandler):
    """Serve ``/graphql`` from recordings (or record them) and ``/health``."""

    def log_message(self, format: str, *args: object) -> None:  # noqa: A002 - stdlib signature
        """Log to stderr only, so stdout carries nothing except the ``PORT:`` line."""
        sys.stderr.write("mock_api: " + (format % args) + "\n")

    def _send(self, status: int, body: object, headers: dict[str, str] | None = None) -> None:
        payload = json.dumps(body).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        for key, value in (headers or {}).items():
            self.send_header(key, value)
        self.end_headers()
        self.wfile.write(payload)

    def do_GET(self) -> None:
        """Health check."""
        if self.path.split("?")[0] == "/health":
            self._send(200, {"status": "ok"})
        else:
            self._send(404, {"error": "not found"})

    def do_POST(self) -> None:
        """Replay (or record) one GraphQL request."""
        if self.path.split("?")[0] != "/graphql":
            self._send(404, {"error": "not found"})
            return
        token = os.environ.get("MOCK_API_TOKEN")
        if token and self.headers.get("Dagster-Cloud-Api-Token") != token:
            self._send(401, {"error": "Dagster-Cloud-Api-Token missing or invalid"})
            return
        try:
            body = json.loads(self.rfile.read(int(self.headers.get("Content-Length") or 0)) or b"{}")
            query = str(body["query"])
        except (ValueError, KeyError):
            self._send(400, {"errors": [{"message": "body must be JSON with a 'query'"}]})
            return
        variables = body.get("variables") or {}
        key = request_key(query, variables)
        if os.environ.get("MOCK_API_FLAKY") == "1":
            with _lock:
                if key not in _flaky_seen:
                    _flaky_seen.add(key)
                    self._send(429, {"error": "slow down"}, {"Retry-After": "0"})
                    return
        if token and "fullDeployments" in query:
            self._send(200, CLOUD_DEPLOYMENTS)
            return
        upstream = os.environ.get("MOCK_API_RECORD_UPSTREAM")
        if upstream:
            status, response = self._forward(upstream, query, variables)
            record = {"operation": operation_name(query), "query": query, "variables": variables}
            record.update({"status": status, "response": response})
            RECORDINGS.mkdir(parents=True, exist_ok=True)
            path = RECORDINGS / f"{operation_name(query)}-{key}.json"
            tmp = path.with_suffix(f".{os.getpid()}.tmp")
            tmp.write_text(_scrub(json.dumps(record, indent=1, sort_keys=True)) + "\n")
            tmp.replace(path)
            self._send(status, response)
            return
        matches = list(RECORDINGS.glob(f"*-{key}.json"))
        if not matches:
            message = f"mock: no recording for {operation_name(query)} {json.dumps(variables, sort_keys=True)}"
            self._send(200, {"data": None, "errors": [{"message": message}]})
            return
        record = json.loads(matches[0].read_text())
        self._send(int(record["status"]), record["response"])

    @staticmethod
    def _forward(upstream: str, query: str, variables: Any) -> tuple[int, Any]:
        data = json.dumps({"query": query, "variables": variables}).encode()
        request = urllib.request.Request(
            upstream.rstrip("/") + "/graphql", data=data, headers={"Content-Type": "application/json"}, method="POST"
        )
        try:
            with urllib.request.urlopen(request, timeout=60) as response:
                return response.status, json.load(response)
        except urllib.error.HTTPError as err:
            return err.code, json.loads(err.read() or b"{}")


def main() -> None:
    """Start the server and print ``PORT:<n>`` once it is listening."""
    parser = argparse.ArgumentParser(description=(__doc__ or "").splitlines()[0])
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=0)
    args = parser.parse_args()
    server = ThreadingHTTPServer((args.host, args.port), Handler)
    print(f"PORT:{server.server_address[1]}", flush=True)
    with contextlib.suppress(KeyboardInterrupt):
        server.serve_forever()


if __name__ == "__main__":
    main()
