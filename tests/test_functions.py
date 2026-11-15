"""Unit tests. The SQL-level tests live in test/sql and run through DuckDB."""

from __future__ import annotations

import json
import threading
from datetime import UTC, datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from types import SimpleNamespace
from typing import Any

import pyarrow as pa
import pytest
from vgi.table_filter_pushdown import ComparisonOp, ConstantFilter, InFilter, PushdownFilters

from dagster_worker import assets, client, runs, status
from dagster_worker.client import DagsterError, endpoint_from_secret
from dagster_worker.worker import DagsterWorker

# ----------------------------------------------------------------------------- endpoint


def test_endpoint_base_url_variants() -> None:
    ep = endpoint_from_secret({"base_url": "http://localhost:3000/"})
    assert (ep.graphql_url, ep.ui_url, ep.token, ep.is_cloud) == (
        "http://localhost:3000/graphql",
        "http://localhost:3000",
        None,
        False,
    )
    assert endpoint_from_secret({"base_url": "http://h:1/graphql"}).graphql_url == "http://h:1/graphql"
    assert endpoint_from_secret({"base_url": pa.scalar("http://h:1/sub")}).run_url("r1") == "http://h:1/sub/runs/r1"


def test_endpoint_dagster_plus() -> None:
    ep = endpoint_from_secret({"organization": "acme", "api_token": "user:abc"})
    assert ep.graphql_url == "https://acme.dagster.cloud/prod/graphql"
    assert ep.token == "user:abc"
    assert ep.is_cloud
    ep = endpoint_from_secret({"organization": "acme", "deployment": "staging", "base_url": None})
    assert ep.graphql_url == "https://acme.dagster.cloud/staging/graphql"
    assert ep.asset_url(["a", "b"]) == "https://acme.dagster.cloud/staging/assets/a/b"


@pytest.mark.parametrize(
    ("secret", "message"),
    [
        (None, "no dagster secret"),
        ({}, "no dagster secret"),
        ({"api_token": "x"}, "BASE_URL"),
        ({"base_url": "localhost:3000"}, "http://"),
    ],
)
def test_endpoint_errors(secret: dict[str, Any] | None, message: str) -> None:
    with pytest.raises(DagsterError, match=message):
        endpoint_from_secret(secret)


def test_endpoint_rejects_two_secrets() -> None:
    two: list[dict[str, Any]] = [{"base_url": "http://a"}, {"base_url": "http://b"}]
    secrets = SimpleNamespace(of_type=lambda t: two)
    with pytest.raises(DagsterError, match="more than one"):
        client.endpoint(secrets)


# ----------------------------------------------------------------------------- HTTP client


class _Flaky(BaseHTTPRequestHandler):
    calls: list[dict[str, Any]] = []

    def log_message(self, format: str, *args: object) -> None:  # noqa: A002
        pass

    def do_POST(self) -> None:
        body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
        _Flaky.calls.append({"token": self.headers.get("Dagster-Cloud-Api-Token"), "body": body})
        if len(_Flaky.calls) == 1:
            self.send_response(429)
            self.send_header("Retry-After", "0")
            self.end_headers()
            return
        if body["query"] == "error":
            payload: dict[str, Any] = {"errors": [{"message": "boom"}], "data": None}
        else:
            payload = {"data": {"ok": body["variables"]}}
        data = json.dumps(payload).encode()
        self.send_response(200)
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)


@pytest.fixture()
def flaky_server() -> Any:
    _Flaky.calls = []
    server = ThreadingHTTPServer(("127.0.0.1", 0), _Flaky)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    yield f"http://127.0.0.1:{server.server_address[1]}"
    server.shutdown()


def test_graphql_retries_429_and_sends_token(flaky_server: str) -> None:
    ep = endpoint_from_secret({"base_url": flaky_server, "api_token": "tok"})
    assert client.graphql(ep, "query Q { ok }", {"a": 1}) == {"ok": {"a": 1}}
    assert len(_Flaky.calls) == 2
    assert all(c["token"] == "tok" for c in _Flaky.calls)


def test_graphql_errors_raise(flaky_server: str) -> None:
    ep = endpoint_from_secret({"base_url": flaky_server})
    with pytest.raises(DagsterError, match="boom"):
        client.graphql(ep, "error")
    assert _Flaky.calls[-1]["token"] is None


def test_graphql_unreachable() -> None:
    ep = endpoint_from_secret({"base_url": "http://127.0.0.1:9"})
    old = client.MAX_ATTEMPTS
    client.MAX_ATTEMPTS = 1
    try:
        with pytest.raises(DagsterError, match="cannot reach"):
            client.graphql(ep, "query Q { ok }")
    finally:
        client.MAX_ATTEMPTS = old


# ----------------------------------------------------------------------------- value helpers


def test_metadata_json_and_tags() -> None:
    entries: list[dict[str, Any]] = [
        {"__typename": "IntMetadataEntry", "label": "row_count", "intValue": 42, "intRepr": "42"},
        {"__typename": "FloatMetadataEntry", "label": "ratio", "floatValue": None, "floatRepr": "nan"},
        {"__typename": "JsonMetadataEntry", "label": "j", "jsonString": '{"x": [1]}'},
        {"__typename": "AssetMetadataEntry", "label": "upstream", "assetKey": {"path": ["a", "b"]}},
        {"__typename": "TextMetadataEntry", "label": "t", "text": "hi"},
        {"__typename": "NullMetadataEntry", "label": "n"},
    ]
    assert json.loads(client.metadata_json(entries) or "") == {
        "row_count": 42,
        "ratio": "nan",
        "j": {"x": [1]},
        "upstream": "a/b",
        "t": "hi",
        "n": None,
    }
    assert client.metadata_json([]) is None
    assert client.tag_map([{"key": "a", "value": "1"}, {"key": "a", "value": "2"}]) == [("a", "2")]
    assert client.asset_key_input("a/b") == {"path": ["a", "b"]}
    assert client.epoch_millis("1000") == datetime(1970, 1, 1, 0, 0, 1, tzinfo=UTC)


def test_every_run_status_is_mapped() -> None:
    dagster_statuses = {
        "QUEUED", "NOT_STARTED", "MANAGED", "STARTING", "STARTED", "SUSPENDED",
        "SUCCESS", "FAILURE", "CANCELING", "CANCELED",
    }  # fmt: skip
    assert set(runs.RUN_STATUS) == dagster_statuses
    assert set(runs.RUN_STATUS.values()) <= {"queued", "running", "success", "failed", "cancelled", "skipped"}


@pytest.mark.parametrize(
    ("tags", "expected"),
    [
        ({"dagster/schedule_name": "s"}, "schedule"),
        ({"dagster/sensor_name": "s"}, "sensor"),
        ({"dagster/auto_materialize": "true"}, "event"),
        ({"dagster/backfill": "b1"}, "other"),
        ({"dagster/user": "me@x"}, "manual"),
        ({}, None),
    ],
)
def test_trigger(tags: dict[str, str], expected: str | None) -> None:
    assert runs.trigger(tags) == expected


def test_root_cause() -> None:
    error = {"message": "wrapper", "cause": {"message": "inner", "className": "ValueError"}}
    assert runs.root_cause(error)["className"] == "ValueError"
    assert runs.root_cause(None) == {}


# ----------------------------------------------------------------------------- pushdown


def _filters(*items: Any) -> PushdownFilters:
    return PushdownFilters(filters=tuple(items))


def _eq(column: str, value: Any, op: ComparisonOp = ComparisonOp.EQ) -> ConstantFilter:
    return ConstantFilter(column_name=column, column_index=0, op=op, value=pa.scalar(value))


def test_runs_filter_translation() -> None:
    ts = datetime(2026, 1, 1, tzinfo=UTC)
    filters = _filters(
        InFilter(column_name="run_id", column_index=0, values=pa.array(["b", "a"])),
        _eq("status", "FAILURE"),
        _eq("job_name", "etl"),
        _eq("created_at", ts, ComparisonOp.GE),
        _eq("updated_at", ts, ComparisonOp.LT),
    )
    out = runs.runs_filter(filters, runs.RunsScan.FILTER_COLUMNS)
    assert out == {
        "runIds": ["a", "b"],
        "statuses": ["FAILURE"],
        "pipelineName": "etl",
        "createdAfter": ts.timestamp() - 1,
        "updatedBefore": ts.timestamp() + 1,
    }


def test_runs_filter_unified_status_and_impossible() -> None:
    cols = runs.UnifiedRunsScan.FILTER_COLUMNS
    out = runs.runs_filter(_filters(_eq("status", "running")), cols)
    assert out == {"statuses": ["CANCELING", "STARTED", "SUSPENDED"]}
    assert runs.runs_filter(_filters(_eq("status", "failed"), _eq("raw_status", "SUCCESS")), cols) is None
    assert runs.runs_filter(_filters(_eq("run_id", "a"), _eq("run_id", "b")), cols) is None
    # Several job names cannot be one pipelineName: leave it to the SDK's re-check.
    jobs = _filters(InFilter(column_name="pipeline", column_index=0, values=pa.array(["x", "y"])))
    assert runs.runs_filter(jobs, cols) == {}
    assert runs.runs_filter(None, cols) == {}


# ----------------------------------------------------------------------------- partitions


def test_partition_statuses_default_and_time() -> None:
    node = {
        "partitionKeys": ["a", "b", "c", "d"],
        "assetPartitionStatuses": {
            "__typename": "TimePartitionStatuses",
            "ranges": [
                {"startKey": "a", "endKey": "b", "status": "MATERIALIZED"},
                {"startKey": "c", "endKey": "c", "status": "FAILED"},
            ],
        },
    }
    assert assets.partition_statuses(node) == [
        ("a", "MATERIALIZED"),
        ("b", "MATERIALIZED"),
        ("c", "FAILED"),
        ("d", "MISSING"),
    ]
    node = {
        "partitionKeys": ["x", "y"],
        "assetPartitionStatuses": {
            "__typename": "DefaultPartitionStatuses",
            "materializedPartitions": [],
            "failedPartitions": ["y"],
            "materializingPartitions": [],
        },
    }
    assert assets.partition_statuses(node) == [("x", "MISSING"), ("y", "FAILED")]


def test_partition_statuses_multi() -> None:
    node = {
        "partitionKeys": ["2026-01-01|east", "2026-01-01|west", "2026-01-02|east", "2026-01-02|west"],
        "partitionKeysByDimension": [
            {"name": "date", "partitionKeys": ["2026-01-01", "2026-01-02"]},
            {"name": "region", "partitionKeys": ["east", "west"]},
        ],
        "assetPartitionStatuses": {
            "__typename": "MultiPartitionStatuses",
            "primaryDimensionName": "date",
            "ranges": [
                {
                    "primaryDimStartKey": "2026-01-01",
                    "primaryDimEndKey": "2026-01-01",
                    "secondaryDim": {
                        "__typename": "DefaultPartitionStatuses",
                        "materializedPartitions": ["east"],
                        "failedPartitions": ["west"],
                        "materializingPartitions": [],
                    },
                }
            ],
        },
    }
    assert assets.partition_statuses(node) == [
        ("2026-01-01|east", "MATERIALIZED"),
        ("2026-01-01|west", "FAILED"),
        ("2026-01-02|east", "MISSING"),
        ("2026-01-02|west", "MISSING"),
    ]


# ----------------------------------------------------------------------------- paging


class _Out:
    def __init__(self) -> None:
        self.batches: list[pa.RecordBatch] = []
        self.finished = False

    def emit(self, batch: pa.RecordBatch) -> None:
        self.batches.append(batch)

    def finish(self) -> None:
        self.finished = True


def _params(schema: pa.Schema, filters: Any = None) -> Any:
    return SimpleNamespace(
        secrets=SimpleNamespace(of_type=lambda t: [{"base_url": "http://dagster.test"}]),
        current_pushdown_filters=filters,
        output_schema=schema,
        args=None,
    )


def _drain(cls: Any, params: Any, limit: int = 50) -> tuple[list[dict[str, Any]], Any]:
    state = cls.initial_state(params)
    out = _Out()
    for _ in range(limit):
        cls.process(params, state, out)
        if out.finished:
            break
    rows = [row for b in out.batches for row in b.to_pylist()]
    return rows, state


def _run(i: int, status: str = "SUCCESS") -> dict[str, Any]:
    return {
        "runId": f"r{i}",
        "status": status,
        "jobName": "job",
        "creationTime": 1000.0 + i,
        "startTime": 1000.0 + i,
        "endTime": 1002.5 + i,
        "tags": [],
        "stepStats": [{"stepKey": "s", "status": None, "attempts": []}],
    }


def test_runs_pages_with_cursor(monkeypatch: pytest.MonkeyPatch) -> None:
    all_runs = [_run(i, "FAILURE" if i == 3 else "SUCCESS") for i in range(5, 0, -1)]
    seen: list[dict[str, Any]] = []

    def fake(ep: Any, query: str, variables: dict[str, Any]) -> dict[str, Any]:
        seen.append(variables)
        ids = [r["runId"] for r in all_runs]
        start = ids.index(variables["cursor"]) + 1 if variables["cursor"] else 0
        page = all_runs[start : start + variables["limit"]]
        return {"runsOrError": {"__typename": "Runs", "results": page}}

    monkeypatch.setattr(runs, "graphql", fake)
    monkeypatch.setattr(runs.RunsScan, "PAGE_SIZE", 2)
    rows, state = _drain(runs.RunsScan, _params(runs.RUNS_SCHEMA))
    assert [r["run_id"] for r in rows] == ["r5", "r4", "r3", "r2", "r1"]
    assert [v["cursor"] for v in seen] == [None, "r4", "r2"]
    assert rows[0]["duration_seconds"] == 2.5
    assert rows[0]["url"] == "http://dagster.test/runs/r5"
    assert state.done

    # The cursor state round-trips through Arrow, as it does over HTTP.
    restored = runs.RunsCursor.deserialize_from_bytes(state.serialize_to_bytes())
    assert restored.cursor == state.cursor


def test_task_runs_status_for_unstarted_steps(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        runs,
        "graphql",
        lambda ep, q, v: {"runsOrError": {"__typename": "Runs", "results": [_run(1, "FAILURE"), _run(2, "QUEUED")]}},
    )
    rows, _ = _drain(runs.UnifiedTaskRunsScan, _params(runs.UNIFIED_TASK_RUNS_SCHEMA))
    assert [(r["task_run_id"], r["status"], r["attempt"]) for r in rows] == [
        ("r1:s", "cancelled", 1),
        ("r2:s", "queued", 1),
    ]


def test_materializations_boundary_dedupe(monkeypatch: pytest.MonkeyPatch) -> None:
    # Five events, two sharing the millisecond at the page boundary.
    stamps = [5000, 4000, 3000, 3000, 2000]
    events: list[dict[str, Any]] = [
        {"runId": f"r{i}", "timestamp": str(ts), "partition": None, "stepKey": "s"} for i, ts in enumerate(stamps)
    ]
    calls: list[dict[str, Any]] = []

    def fake(ep: Any, query: str, variables: dict[str, Any]) -> dict[str, Any]:
        calls.append(variables)
        before = int(str(variables["before"])) if variables["before"] else 10**9
        page = [e for e in events if int(e["timestamp"]) < before][: variables["limit"]]
        return {"assetOrError": {"__typename": "Asset", "assetMaterializations": page}}

    monkeypatch.setattr(assets, "graphql", fake)
    monkeypatch.setattr(assets.AssetMaterializationsScan, "PAGE_SIZE", 3)
    filters = _filters(_eq("asset_key", "a/b"))
    rows, _ = _drain(assets.AssetMaterializationsScan, _params(assets.AssetMaterializationsScan.FIXED_SCHEMA, filters))
    assert [r["run_id"] for r in rows] == ["r0", "r1", "r2", "r3", "r4"]
    assert calls[0]["assetKey"] == {"path": ["a", "b"]}
    assert calls[1]["before"] == "3001"


def test_catalog_lists_tables() -> None:
    schemas = {tuple(s.path): s for s in DagsterWorker.catalog.schemas}
    main = {t.name for t in schemas[("main",)].tables}
    assert {"runs", "assets", "asset_materializations", "asset_checks", "asset_partitions", "jobs"} <= main
    assert {t.name for t in schemas[("unified",)].tables} == {"runs", "task_runs"}
    columns = [f.name for f in runs.UNIFIED_RUNS_SCHEMA]
    assert columns == [
        "orchestrator", "run_id", "pipeline", "status", "raw_status", "trigger",
        "queued_at", "started_at", "ended_at", "duration_seconds", "url",
    ]  # fmt: skip


def test_unified_run_status_scalar() -> None:
    out = status.UnifiedRunStatus.compute(pa.array(["FAILURE", "started", None, "nope"]))
    assert out.to_pylist() == ["failed", "running", None, None]
