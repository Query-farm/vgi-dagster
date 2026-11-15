"""Assemble the Dagster catalog and run the worker.

Run it:

    vgi-dagster-worker                      # stdio: what DuckDB launches by default
    vgi-dagster-worker --http --port 8000   # HTTP: for Docker / hosted deploys

Attach it from DuckDB:

    CREATE SECRET (TYPE dagster, BASE_URL 'http://localhost:3000');
    ATTACH 'dagster' (TYPE vgi, LOCATION '/abs/path/to/.venv/bin/vgi-dagster-worker');
    SELECT * FROM dagster.main.runs WHERE status = 'FAILURE';
"""

from __future__ import annotations

import json
from typing import Any

import pyarrow as pa
from vgi import Worker
from vgi.catalog import Catalog, Schema, Table

from .assets import (
    AssetCheckExecutionsScan,
    AssetChecksScan,
    AssetMaterializationsScan,
    AssetPartitionsScan,
    AssetsScan,
)
from .base import catalog_table
from .client import DAGSTER_SECRET_TYPE
from .definitions import (
    CodeLocationsScan,
    ConcurrencyKeysScan,
    DeploymentsScan,
    JobsScan,
    RepositoriesScan,
    ResourcesScan,
    SchedulesScan,
    SensorsScan,
)
from .meta import (
    CATALOG_COMMENT,
    CATALOG_TAGS,
    SCHEMA_CATEGORIES,
    SCHEMA_DOC_LLM,
    SCHEMA_DOC_MD,
    SOURCE_URL,
    UNIFIED_DOC_LLM,
    UNIFIED_DOC_MD,
    keywords_array,
)
from .runs import (
    RunEventsFunction,
    RunFunction,
    RunsScan,
    RunStepStatsFunction,
    UnifiedRunsScan,
    UnifiedTaskRunsScan,
)
from .status import UnifiedRunStatus

FUNCTIONS: list[type[Any]] = [RunFunction, RunStepStatsFunction, RunEventsFunction, UnifiedRunStatus]

# (table name, scan, primary key)
MAIN_TABLES: list[tuple[str, type[Any], list[str] | None]] = [
    ("deployments", DeploymentsScan, ["deployment_name"]),
    ("code_locations", CodeLocationsScan, ["location_name"]),
    ("repositories", RepositoriesScan, ["location_name", "repository_name"]),
    ("jobs", JobsScan, ["location_name", "repository_name", "job_name"]),
    ("schedules", SchedulesScan, ["location_name", "repository_name", "schedule_name"]),
    ("sensors", SensorsScan, ["location_name", "repository_name", "sensor_name"]),
    ("resources", ResourcesScan, ["location_name", "repository_name", "resource_name"]),
    ("concurrency_keys", ConcurrencyKeysScan, ["concurrency_key"]),
    ("runs", RunsScan, ["run_id"]),
    ("assets", AssetsScan, ["asset_key"]),
    ("asset_materializations", AssetMaterializationsScan, None),
    ("asset_checks", AssetChecksScan, ["asset_key", "check_name"]),
    ("asset_check_executions", AssetCheckExecutionsScan, ["asset_key", "check_name", "execution_id"]),
    ("asset_partitions", AssetPartitionsScan, ["asset_key", "partition_key"]),
]
UNIFIED_TABLES: list[tuple[str, type[Any], list[str] | None]] = [
    ("runs", UnifiedRunsScan, ["run_id"]),
    ("task_runs", UnifiedTaskRunsScan, ["task_run_id"]),
]


def _examples(cls: type[Any]) -> list[dict[str, str]]:
    return [{"description": ex.description, "sql": ex.sql} for ex in getattr(cls.Meta, "examples", [])]


def _tables(specs: list[tuple[str, type[Any], list[str] | None]], schema: str) -> list[Table]:
    tables = []
    for name, scan, pk in specs:
        tags = {**scan.Meta.tags, "vgi.example_queries": json.dumps(_examples(scan))}
        tables.append(
            catalog_table(
                name,
                scan,
                comment=scan.Meta.description,
                tags=tags,
                primary_key=pk,
                schema_path=[schema],
            )
        )
    return tables


_DUCKDB_TYPES = {
    "string": "VARCHAR",
    "int32": "INTEGER",
    "int64": "BIGINT",
    "double": "DOUBLE",
    "bool": "BOOLEAN",
    "timestamp[us, tz=UTC]": "TIMESTAMP WITH TIME ZONE",
}


def _duckdb_type(t: pa.DataType) -> str:
    if pa.types.is_map(t):
        return f"MAP({_duckdb_type(t.key_type)}, {_duckdb_type(t.item_type)})"
    if pa.types.is_list(t):
        return f"{_duckdb_type(t.value_type)}[]"
    if pa.types.is_struct(t):
        return "STRUCT(" + ", ".join(f"{f.name} {_duckdb_type(f.type)}" for f in t) + ")"
    return _DUCKDB_TYPES[str(t)]


def _result_columns(schema: Any) -> str:
    return json.dumps(
        [
            {
                "name": f.name,
                "type": _duckdb_type(f.type),
                "description": (f.metadata or {}).get(b"comment", b"").decode(),
            }
            for f in schema
        ]
    )


for _fn in FUNCTIONS:
    _fn.Meta.tags = {**_fn.Meta.tags, "vgi.example_queries": json.dumps(_examples(_fn))}
    if hasattr(_fn, "FIXED_SCHEMA"):
        _fn.Meta.tags["vgi.result_columns_schema"] = _result_columns(_fn.FIXED_SCHEMA)

MAIN_EXAMPLES = [ex for _, scan, _ in MAIN_TABLES for ex in _examples(scan)] + [
    ex for fn in FUNCTIONS for ex in _examples(fn)
]
UNIFIED_EXAMPLES = [ex for _, scan, _ in UNIFIED_TABLES for ex in _examples(scan)]
UNIFIED_CATEGORIES = [c for c in SCHEMA_CATEGORIES if c["name"] == "unified"]
MAIN_CATEGORIES = [c for c in SCHEMA_CATEGORIES if c["name"] != "unified"]


class DagsterWorker(Worker):
    """The worker process: the ``dagster`` catalog with ``main`` and ``unified`` schemas."""

    catalog = Catalog(
        name="dagster",
        comment=CATALOG_COMMENT,
        source_url=SOURCE_URL,
        tags=CATALOG_TAGS,
        schemas=[
            Schema(
                path=["main"],
                comment=SCHEMA_DOC_MD,
                functions=FUNCTIONS,
                tables=_tables(MAIN_TABLES, "main"),
                tags={
                    "vgi.title": "Dagster Native Tables",
                    "domain": "data orchestration",
                    "vgi.keywords": keywords_array(
                        "dagster, assets, runs, jobs, schedules, sensors, asset checks, partitions, lineage"
                    ),
                    "vgi.doc_llm": SCHEMA_DOC_LLM,
                    "vgi.doc_md": SCHEMA_DOC_MD,
                    "vgi.categories": json.dumps(MAIN_CATEGORIES),
                    "vgi.example_queries": json.dumps(MAIN_EXAMPLES),
                },
            ),
            Schema(
                path=["unified"],
                comment=UNIFIED_DOC_MD,
                tables=_tables(UNIFIED_TABLES, "unified"),
                tags={
                    "vgi.title": "Unified Orchestration",
                    "domain": "data orchestration",
                    "vgi.keywords": keywords_array("unified, cross orchestrator, runs, task runs, union all"),
                    "vgi.doc_llm": UNIFIED_DOC_LLM,
                    "vgi.doc_md": UNIFIED_DOC_MD,
                    "vgi.categories": json.dumps(UNIFIED_CATEGORIES),
                    "vgi.example_queries": json.dumps(UNIFIED_EXAMPLES),
                },
            ),
        ],
    )

    secret_types = [DAGSTER_SECRET_TYPE]


def main() -> None:
    """Console-script entry point: parse ``--http`` / ``--unix`` / logging flags and serve."""
    DagsterWorker.main()


if __name__ == "__main__":
    main()
