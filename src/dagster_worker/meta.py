"""Discovery metadata: the one file to edit when you rename or extend the worker.

DuckDB, coding agents and the ``vgi-lint`` metadata checker all read these
tags. Keep every object documented and ``make lint`` stays at 100.

* Catalog-level tags (title, license, author, links, long description) live in
  ``CATALOG_TAGS`` below.
* Every function and table carries ``object_tags(...)``: a title, a category
  from ``SCHEMA_CATEGORIES``, search keywords, and LLM- and human-facing docs.
"""

from __future__ import annotations

import json
from typing import Any

import pyarrow as pa

CATALOG_NAME = "dagster"

SOURCE_URL = "https://github.com/Query-farm/vgi-dagster"

# Categories group functions in docs and agent tools. Every object's
# ``vgi.category`` must name one of these.
SCHEMA_CATEGORIES = [
    {
        "name": "definitions",
        "title": "Definitions",
        "description": "Code locations, jobs, schedules, sensors and resources.",
    },
    {
        "name": "assets",
        "title": "Assets",
        "description": "Software-defined assets, materializations, checks and partitions.",
    },
    {"name": "runs", "title": "Runs", "description": "Runs, their steps and their event logs."},
    {"name": "instance", "title": "Instance", "description": "Deployment- and instance-level configuration."},
    {"name": "unified", "title": "Unified orchestration", "description": "Cross-orchestrator run and task-run tables."},
]


def keywords_array(keywords: str) -> str:
    """Turn ``"a, b, c"`` into the JSON array string ``vgi.keywords`` requires."""
    return json.dumps([k.strip() for k in keywords.split(",") if k.strip()])


def object_tags(*, title: str, category: str, keywords: str, doc_llm: str, doc_md: str) -> dict[str, str]:
    """Build the standard per-object discovery tags."""
    if category not in {c["name"] for c in SCHEMA_CATEGORIES}:
        raise ValueError(f"unknown category {category!r}; add it to SCHEMA_CATEGORIES")
    return {
        "vgi.title": title,
        "domain": "data orchestration",
        "vgi.category": category,
        "vgi.keywords": keywords_array(keywords),
        "vgi.doc_llm": doc_llm,
        "vgi.doc_md": doc_md,
    }


def column(name: str, type_: pa.DataType, comment: str, *, nullable: bool = True) -> pa.Field[Any]:
    """Build an Arrow field whose ``comment`` shows up in DuckDB's ``DESCRIBE`` and agent tools."""
    return pa.field(name, type_, nullable=nullable, metadata={b"comment": comment.encode()})


CATALOG_COMMENT = (
    "Dagster as SQL: assets, materializations, asset checks, partitions, runs, steps, jobs, schedules and "
    "sensors from Dagster+ or an open-source dagster-webserver, read through its GraphQL API."
)

CATALOG_DOC_LLM = (
    "Read-only SQL access to a Dagster instance through its GraphQL API: an open-source "
    "dagster-webserver (or `dagster dev`) or a Dagster+ deployment. Reach for it to audit pipeline "
    "health: which runs and steps failed and why, which assets are stale, which partitions are "
    "missing before a backfill, which asset checks are failing, and what schedules and sensors launch.\n\n"
    "Connection: after ATTACH, create one DuckDB secret of type `dagster`. Set `base_url` (for example "
    "http://localhost:3000) for open-source Dagster, or `organization`, `deployment` (default prod) and "
    "`api_token` for Dagster+. Without a secret every table raises an error that says how to create one.\n\n"
    "Conventions: asset keys are strings with path parts joined by '/'. Timestamps are `TIMESTAMPTZ` in "
    "UTC. Native run statuses use Dagster's RunStatus values (SUCCESS, FAILURE, CANCELED, STARTED, "
    "QUEUED, ...). Filters on run ids, statuses, job names, creation times, asset keys, materialization "
    "times and partitions are sent to Dagster, so filter on them instead of scanning all history. "
    "Resource configuration values are never returned. The `unified` schema repeats runs and steps in "
    "the column layout shared by every Query.Farm orchestrator worker for UNION ALL across orchestrators."
)

CATALOG_DOC_MD = (
    "# Dagster\n\n"
    "Query [Dagster](https://dagster.io) from DuckDB: the asset graph, materialization history, asset "
    "checks, partition status, runs and their steps, and the jobs, schedules and sensors that launch "
    "them. Works with open-source `dagster-webserver` and with Dagster+.\n\n"
    "Connect by creating a DuckDB secret of type `dagster` after attaching: `base_url` for open-source "
    "Dagster, or `organization`, `deployment` and `api_token` for Dagster+.\n\n"
    "The `unified` schema exposes runs and task runs in the shape shared by Query.Farm's other "
    "orchestrator workers, so pipelines from several orchestrators can be compared in one query."
)

SCHEMA_DOC_LLM = (
    "Dagster-native tables and per-run functions, grouped by category: definitions (what code defines: "
    "jobs, schedules, sensors, resources and the code locations serving them), assets (the asset graph "
    "with lineage, materialization history with metadata as JSON, data-quality checks and their "
    "executions, and partition-by-partition status), runs (run history with step counts and triggers, "
    "plus functions that take a run id for step timing and the event log with root-cause errors) and "
    "instance (concurrency pools; Dagster+ deployments).\n\n"
    "Start broad, then drill in: find failed runs, then read their steps and events by run id; find "
    "assets with failed partitions or failing checks, then read their history. Asset keys are 'a/b' "
    "strings. History tables page through the API, so filter on asset key, run id, status or a time "
    "range whenever you can: those filters are sent to Dagster."
)

SCHEMA_DOC_MD = (
    "## main\n\n"
    "Dagster-native tables: definitions (jobs, schedules, sensors, resources, code locations), assets "
    "(materializations, checks, partitions) and runs.\n\n"
    "Per-run table functions take a run id and return its steps or its event log. Filters on run ids, "
    "statuses, job names, asset keys, partitions and time ranges are pushed to Dagster's API."
)

UNIFIED_DOC_LLM = (
    "Cross-orchestrator tables with a fixed column layout shared by every Query.Farm orchestrator worker "
    "(Airflow, Prefect, Temporal, Databricks, dbt, GitHub Actions), so a UNION ALL of the same table "
    "across catalogs lines up. Status is normalized to queued, running, success, failed, cancelled or "
    "skipped, with the orchestrator's own value kept as raw_status. In Dagster a run is a job run and a "
    "task run is one step of it (task_run_id is '<run_id>:<step_key>'). Use the `main` schema for "
    "Dagster-specific columns such as tags, assets and partitions."
)

UNIFIED_DOC_MD = (
    "## unified\n\n"
    "Dagster runs and steps in the shared orchestrator shape, with statuses normalized to queued, "
    "running, success, failed, cancelled and skipped.\n\n"
    "`UNION ALL` these tables with the `unified` schema of other Query.Farm orchestrator workers to "
    "compare pipelines across orchestrators in one query."
)

AGENT_TEST_TASKS = [
    {"name": "failed_runs_by_job", "prompt": "Using the dagster catalog, how many failed runs does each job have?"},
    {"name": "failed_partitions", "prompt": "Which partitions of the daily_events asset failed to materialize?"},
    {"name": "failing_checks", "prompt": "Which asset checks failed on their asset's latest materialization?"},
    {"name": "schedule_targets", "prompt": "Which job does each Dagster schedule launch, and on what cron?"},
    {
        "name": "check_history",
        "prompt": "How many times has each asset check been executed, and how often did it fail?",
    },
    {"name": "raw_orders_row_counts", "prompt": "What row_count did each materialization of raw_orders record?"},
    {"name": "downstream_of_raw_orders", "prompt": "Which assets depend directly on raw_orders?"},
    {"name": "broken_code_locations", "prompt": "Are any Dagster code locations failing to load?"},
    {"name": "pool_backlog", "prompt": "Which concurrency pools have steps waiting for a slot?"},
    {"name": "plus_deployments", "prompt": "List the Dagster+ deployments by type."},
    {"name": "op_jobs_by_team", "prompt": "Which non-asset jobs exist, and which team tag does each carry?"},
    {"name": "repository_contents", "prompt": "How many jobs, schedules and sensors does each repository define?"},
    {"name": "resource_consumers", "prompt": "Which jobs use each Dagster resource?"},
    {"name": "sensor_targets", "prompt": "Which jobs can each sensor launch, and is it running?"},
    {
        "name": "latest_failure_reason",
        "prompt": "Run 00000000-0000-4000-8000-000000000003 failed. Which step failed, with what error, "
        "how long did each of its steps take, and what job was it?",
    },
    {"name": "cross_orchestrator_status", "prompt": "Count Dagster runs by the normalized cross-orchestrator status."},
    {"name": "flaky_steps", "prompt": "Across all Dagster runs, which steps have failed, and how many times?"},
    {
        "name": "normalize_statuses",
        "prompt": "Map the Dagster run statuses FAILURE, CANCELED and STARTING to the unified status vocabulary.",
    },
]

CATALOG_TAGS = {
    "vgi.title": "Dagster Orchestration",
    "vgi.subtitle": "Dagster assets, runs, checks, partitions and schedules as DuckDB tables (Dagster+ or OSS).",
    "vgi.doc_llm": CATALOG_DOC_LLM,
    "vgi.doc_md": CATALOG_DOC_MD,
    "vgi.source_url": SOURCE_URL,
    "vgi.icon_url": "https://query.farm/media-kit/products/vgi/vgi-logo.png",
    "vgi.license": "0BSD",
    "vgi.author": "Query.Farm",
    "vgi.copyright": "Copyright 2026 Query Farm LLC - https://query.farm",
    "vgi.keywords": keywords_array(
        "dagster, dagster plus, data orchestration, assets, asset checks, runs, schedules, sensors, graphql"
    ),
    "vgi.support_contact": f"{SOURCE_URL}/issues",
    "vgi.support_policy_url": f"{SOURCE_URL}#support",
    "vgi.agent_test_tasks": json.dumps(AGENT_TEST_TASKS),
}
