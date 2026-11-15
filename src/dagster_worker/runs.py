"""Runs: the ``runs`` table, ``unified.runs`` / ``unified.task_runs``, and per-run functions.

All run scans page through ``runsOrError`` (newest first) with the last run id as the
cursor, which lives in the table-function state so a scan resumes on any worker process.
Pushed-down filters become a ``RunsFilter``: run id, status, job name and created/updated
time ranges. The SDK re-applies every predicate to each batch, so the translation only
has to never drop a matching row.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from typing import Annotated, Any, ClassVar

import pyarrow as pa
from vgi.arguments import Arg
from vgi.metadata import FunctionExample
from vgi.table_function import (
    OutputCollector,
    ProcessParams,
    TableFunctionGenerator,
    bind_fixed_schema,
    init_single_worker,
)
from vgi_rpc import ArrowSerializableDataclass

from .base import NoArgs, dumps, eq_values, range_bounds, rows_to_batch, scan_meta_secrets
from .client import Endpoint, check_union, duration, endpoint, epoch_millis, epoch_seconds, graphql, tag_map
from .meta import column, object_tags

ORCHESTRATOR = "dagster"
TSTZ = pa.timestamp("us", tz="UTC")
STR_LIST = pa.list_(pa.string())
TAG_MAP = pa.map_(pa.string(), pa.string())

# Dagster RunStatus -> the shared orchestrator vocabulary.
RUN_STATUS = {
    "QUEUED": "queued",
    "NOT_STARTED": "queued",
    "MANAGED": "queued",
    "STARTING": "queued",
    "STARTED": "running",
    "SUSPENDED": "running",
    "CANCELING": "running",
    "SUCCESS": "success",
    "FAILURE": "failed",
    "CANCELED": "cancelled",
}
# Dagster StepEventStatus -> the shared vocabulary (a step with no status has not started).
STEP_STATUS = {"SUCCESS": "success", "FAILURE": "failed", "SKIPPED": "skipped", "IN_PROGRESS": "running"}

STEP_FIELDS = "stepStats { stepKey status startTime endTime attempts { startTime endTime } }"
RUN_FIELDS = """
  runId status jobName mode creationTime startTime endTime updateTime rootRunId parentRunId
  tags { key value }
  assetSelection { path }
  stats { __typename ... on RunStatsSnapshot { stepsSucceeded stepsFailed materializations enqueuedTime } }
"""


def runs_query(with_steps: bool) -> str:
    """Build the paginated ``runsOrError`` query, optionally with each run's step stats."""
    fields = RUN_FIELDS + (STEP_FIELDS if with_steps else "")
    return (
        "query Runs($filter: RunsFilter, $cursor: String, $limit: Int!) { "
        "runsOrError(filter: $filter, cursor: $cursor, limit: $limit) { __typename "
        "... on Runs { results { " + fields + " } } "
        "... on InvalidPipelineRunsFilterError { message } ... on PythonError { message } } }"
    )


def trigger(tags: dict[str, str]) -> str | None:
    """Classify what launched a run, from Dagster's system tags; None when no tag says."""
    if "dagster/schedule_name" in tags:
        return "schedule"
    if "dagster/sensor_name" in tags:
        return "sensor"
    if any(k.startswith("dagster/auto_materialize") or k == "dagster/from_automation_condition" for k in tags):
        return "event"
    if "dagster/backfill" in tags:
        return "other"
    if "dagster/user" in tags:
        return "manual"
    return None


def _seconds(value: Any) -> float | None:
    if isinstance(value, datetime):
        return value.timestamp()
    return None


def runs_filter(filters: Any, columns: dict[str, str]) -> dict[str, Any] | None:
    """Translate pushed-down filters into a ``RunsFilter``; None when nothing can match.

    ``columns`` maps a RunsFilter role (run_id, status, raw_status, job, created, updated)
    to the scan's column name.
    """
    out: dict[str, Any] = {}
    if "run_id" in columns and (ids := eq_values(filters, columns["run_id"])) is not None:
        if not ids:
            return None
        out["runIds"] = [str(i) for i in ids]
    statuses: set[str] | None = None
    if "raw_status" in columns and (raw := eq_values(filters, columns["raw_status"])) is not None:
        statuses = {str(s) for s in raw if str(s) in RUN_STATUS}
    if "status" in columns and (norm := eq_values(filters, columns["status"])) is not None:
        mapped = {raw for raw, n in RUN_STATUS.items() if n in {str(v) for v in norm}}
        statuses = mapped if statuses is None else statuses & mapped
    if statuses is not None:
        if not statuses:
            return None
        out["statuses"] = sorted(statuses)
    if "job" in columns and (jobs := eq_values(filters, columns["job"])) is not None:
        if not jobs:
            return None
        if len(jobs) == 1:
            out["pipelineName"] = str(jobs[0])
    for role, after, before in (
        ("created", "createdAfter", "createdBefore"),
        ("updated", "updatedAfter", "updatedBefore"),
    ):
        if role not in columns:
            continue
        lower, upper = range_bounds(filters, columns[role])
        if (lo := _seconds(lower)) is not None:
            out[after] = lo - 1.0
        if (hi := _seconds(upper)) is not None:
            out[before] = hi + 1.0
    return out


@dataclass(kw_only=True)
class RunsCursor(ArrowSerializableDataclass):
    """Resumable position in a runs scan: the RunsFilter and the last run id seen."""

    filter_json: str = "{}"
    cursor: str = ""
    done: bool = False


class RunsPager(TableFunctionGenerator[NoArgs, RunsCursor]):
    """Base for scans that page through runs. Subclasses map runs to rows."""

    FIXED_SCHEMA: ClassVar[pa.Schema]
    PAGE_SIZE: ClassVar[int] = 100
    WITH_STEPS: ClassVar[bool] = False
    FILTER_COLUMNS: ClassVar[dict[str, str]] = {}

    @classmethod
    def build_rows(cls, ep: Endpoint, runs: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Rows for one page of runs."""
        raise NotImplementedError

    @classmethod
    def initial_state(cls, params: ProcessParams[NoArgs]) -> RunsCursor:
        """Translate the pushed-down filters once; an impossible filter ends the scan."""
        run_filter = runs_filter(params.current_pushdown_filters, cls.FILTER_COLUMNS)
        if run_filter is None:
            return RunsCursor(done=True)
        return RunsCursor(filter_json=dumps(run_filter))

    @classmethod
    def process(cls, params: ProcessParams[NoArgs], state: RunsCursor, out: OutputCollector) -> None:
        """Fetch one page of runs and emit its rows."""
        if state.done:
            out.finish()
            return
        ep = endpoint(params.secrets)
        run_filter = json.loads(state.filter_json)
        variables = {"filter": run_filter or None, "cursor": state.cursor or None, "limit": cls.PAGE_SIZE}
        data = graphql(ep, runs_query(cls.WITH_STEPS), variables)
        root = check_union(data.get("runsOrError"), "Runs", "runsOrError")
        runs = list(root.get("results") or [])
        if len(runs) < cls.PAGE_SIZE or run_filter.get("runIds"):
            state.done = True
        else:
            state.cursor = str(runs[-1].get("runId"))
        out.emit(rows_to_batch(cls.build_rows(ep, runs), params.output_schema))


def _stats(run: dict[str, Any]) -> dict[str, Any]:
    stats = run.get("stats") or {}
    return stats if stats.get("__typename") == "RunStatsSnapshot" else {}


def native_run_row(ep: Endpoint, run: dict[str, Any]) -> dict[str, Any]:
    """One ``runs`` row from a GraphQL Run."""
    stats = _stats(run)
    tags = tag_map(run.get("tags"))
    return {
        "run_id": run.get("runId"),
        "job_name": run.get("jobName"),
        "status": run.get("status"),
        "created_at": epoch_seconds(run.get("creationTime")),
        "enqueued_at": epoch_seconds(stats.get("enqueuedTime")),
        "started_at": epoch_seconds(run.get("startTime")),
        "ended_at": epoch_seconds(run.get("endTime")),
        "updated_at": epoch_seconds(run.get("updateTime")),
        "duration_seconds": duration(run.get("startTime"), run.get("endTime")),
        "trigger": trigger(dict(tags)),
        "tags": tags,
        "asset_selection": ["/".join(a.get("path") or []) for a in run.get("assetSelection") or []] or None,
        "steps_succeeded": stats.get("stepsSucceeded"),
        "steps_failed": stats.get("stepsFailed"),
        "materializations": stats.get("materializations"),
        "root_run_id": run.get("rootRunId"),
        "parent_run_id": run.get("parentRunId"),
        "mode": run.get("mode"),
        "url": ep.run_url(str(run.get("runId"))),
    }


RUNS_SCHEMA = pa.schema(
    [
        column("run_id", pa.string(), "Run id (a UUID).", nullable=False),
        column(
            "job_name",
            pa.string(),
            "Job the run executed; __ASSET_JOB for ad-hoc asset materializations.",
            nullable=False,
        ),
        column(
            "status",
            pa.string(),
            "Dagster run status: QUEUED, NOT_STARTED, STARTING, STARTED, SUCCESS, FAILURE, CANCELING, CANCELED, ...",
            nullable=False,
        ),
        column("created_at", TSTZ, "When the run was created (submitted).", nullable=False),
        column("enqueued_at", TSTZ, "When the run entered the run queue; NULL when it was not queued."),
        column("started_at", TSTZ, "When execution started."),
        column("ended_at", TSTZ, "When execution finished."),
        column("updated_at", TSTZ, "Last status change."),
        column("duration_seconds", pa.float64(), "ended_at - started_at in seconds; NULL until the run ends."),
        column(
            "trigger", pa.string(), "schedule, sensor, event (automation), manual (UI user), other (backfill) or NULL."
        ),
        column("tags", TAG_MAP, "Run tags as a MAP, e.g. tags['dagster/schedule_name']."),
        column("asset_selection", STR_LIST, "Assets the run targeted, as 'a/b' keys; NULL for op jobs."),
        column("steps_succeeded", pa.int32(), "Number of steps that succeeded."),
        column("steps_failed", pa.int32(), "Number of steps that failed."),
        column("materializations", pa.int32(), "Number of asset materializations the run produced."),
        column("root_run_id", pa.string(), "First run of a re-execution chain; NULL for an original run."),
        column("parent_run_id", pa.string(), "Run this one re-executed; NULL for an original run."),
        column("mode", pa.string(), "Legacy execution mode (always 'default' on current Dagster)."),
        column("url", pa.string(), "Link to the run in the Dagster UI.", nullable=False),
    ]
)


@init_single_worker
@bind_fixed_schema
class RunsScan(RunsPager):
    """Rows of ``runs``."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = RUNS_SCHEMA
    FILTER_COLUMNS: ClassVar[dict[str, str]] = {
        "run_id": "run_id",
        "raw_status": "status",
        "job": "job_name",
        "created": "created_at",
        "updated": "updated_at",
    }

    class Meta:
        """Scan metadata."""

        name = "runs_scan"
        description = "Runs in the deployment, newest first, with filter pushdown"
        required_secrets = scan_meta_secrets()
        filter_pushdown = True
        auto_apply_filters = True
        projection_pushdown = True
        tags = object_tags(
            title="Dagster Run History",
            category="runs",
            keywords="run, job run, failure, success, duration, run history, schedule run, backfill",
            doc_llm=(
                "Every run in the deployment, newest first, paged 100 at a time. Columns: `run_id`, `job_name`, "
                "`status` (Dagster RunStatus: SUCCESS, FAILURE, CANCELED, STARTED, QUEUED, ...), `created_at`, "
                "`enqueued_at`, `started_at`, `ended_at`, `updated_at`, `duration_seconds`, `trigger`, `tags` "
                "(`MAP(VARCHAR, VARCHAR)`), `asset_selection`, step counts, `materializations`, re-execution ids and "
                "`url`. Filters on run_id (=, IN), status (=, IN), job_name (=), created_at and updated_at "
                "(ranges) are sent to Dagster, so filter on them rather than scanning all history. Per-run "
                "detail: `run_events(run_id)` and `run_step_stats(run_id)`."
            ),
            doc_md=(
                "Run history, newest first. `run_id`, `status`, `job_name`, `created_at` and `updated_at` "
                "filters are pushed to Dagster's `RunsFilter`."
            ),
        )
        examples = [
            FunctionExample(
                sql="SELECT job_name, count(*) FILTER (WHERE status = 'FAILURE') AS failures, count(*) AS runs "
                "FROM dagster.main.runs WHERE created_at >= TIMESTAMPTZ '2026-01-01' "
                "GROUP BY job_name ORDER BY failures DESC",
                description="Failure counts per job since the start of 2026",
            ),
            FunctionExample(
                sql="SELECT run_id, job_name, duration_seconds FROM dagster.main.runs "
                "WHERE status = 'SUCCESS' ORDER BY duration_seconds DESC LIMIT 10",
                description="The ten slowest successful runs",
            ),
        ]

    @classmethod
    def build_rows(cls, ep: Endpoint, runs: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """One row per run."""
        return [native_run_row(ep, run) for run in runs]


# ----------------------------------------------------------------------------- unified


UNIFIED_RUNS_SCHEMA = pa.schema(
    [
        column("orchestrator", pa.string(), "Always 'dagster'.", nullable=False),
        column("run_id", pa.string(), "Dagster run id.", nullable=False),
        column("pipeline", pa.string(), "Job name."),
        column("status", pa.string(), "queued, running, success, failed, cancelled or skipped.", nullable=False),
        column("raw_status", pa.string(), "Dagster RunStatus, e.g. FAILURE."),
        column("trigger", pa.string(), "schedule, manual, sensor, api, event, other or NULL."),
        column("queued_at", TSTZ, "When the run was created (submitted)."),
        column("started_at", TSTZ, "When execution started."),
        column("ended_at", TSTZ, "When execution finished."),
        column("duration_seconds", pa.float64(), "ended_at - started_at in seconds."),
        column("url", pa.string(), "Link to the run in the Dagster UI."),
    ]
)


@init_single_worker
@bind_fixed_schema
class UnifiedRunsScan(RunsPager):
    """Rows of ``unified.runs``."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = UNIFIED_RUNS_SCHEMA
    FILTER_COLUMNS: ClassVar[dict[str, str]] = {
        "run_id": "run_id",
        "raw_status": "raw_status",
        "status": "status",
        "job": "pipeline",
        "created": "queued_at",
    }

    class Meta:
        """Scan metadata."""

        name = "unified_runs_scan"
        description = "Dagster runs in the shared cross-orchestrator shape"
        required_secrets = scan_meta_secrets()
        filter_pushdown = True
        auto_apply_filters = True
        projection_pushdown = True
        tags = object_tags(
            title="Unified Runs",
            category="unified",
            keywords="unified, cross orchestrator, airflow, prefect, union all, run status, pipeline run",
            doc_llm=(
                "Dagster runs in the column layout shared by every Query.Farm orchestrator worker (Airflow, "
                "Prefect, Temporal, dbt, ...), so `UNION ALL` across them works: `orchestrator` ('dagster'), "
                "`run_id`, `pipeline` (job name), `status` (queued/running/success/failed/cancelled/skipped), "
                "`raw_status` (Dagster RunStatus), `trigger`, `queued_at`, `started_at`, `ended_at`, "
                "`duration_seconds`, `url`. Filters on run_id, status, raw_status, pipeline and queued_at push "
                "down. Use `dagster.main.runs` for Dagster-specific columns."
            ),
            doc_md="Runs in the shared orchestrator shape; `UNION ALL` it with other orchestrator workers.",
        )
        examples = [
            FunctionExample(
                sql="SELECT status, count(*) AS runs FROM dagster.unified.runs GROUP BY status ORDER BY status",
                description="Run counts by normalized status",
            ),
        ]

    @classmethod
    def build_rows(cls, ep: Endpoint, runs: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """One row per run."""
        rows = []
        for run in runs:
            raw = run.get("status")
            rows.append(
                {
                    "orchestrator": ORCHESTRATOR,
                    "run_id": run.get("runId"),
                    "pipeline": run.get("jobName"),
                    "status": RUN_STATUS.get(str(raw), "queued"),
                    "raw_status": raw,
                    "trigger": trigger(dict(tag_map(run.get("tags")))),
                    "queued_at": epoch_seconds(run.get("creationTime")),
                    "started_at": epoch_seconds(run.get("startTime")),
                    "ended_at": epoch_seconds(run.get("endTime")),
                    "duration_seconds": duration(run.get("startTime"), run.get("endTime")),
                    "url": ep.run_url(str(run.get("runId"))),
                }
            )
        return rows


UNIFIED_TASK_RUNS_SCHEMA = pa.schema(
    [
        column("orchestrator", pa.string(), "Always 'dagster'.", nullable=False),
        column("run_id", pa.string(), "Dagster run id.", nullable=False),
        column("task_run_id", pa.string(), "'<run_id>:<step_key>', unique per step of a run.", nullable=False),
        column("task_name", pa.string(), "Step key (op or asset step name)."),
        column("status", pa.string(), "queued, running, success, failed, cancelled or skipped.", nullable=False),
        column("raw_status", pa.string(), "Dagster StepEventStatus: SUCCESS, FAILURE, SKIPPED, IN_PROGRESS or NULL."),
        column("attempt", pa.int32(), "Number of attempts the step made (1 unless it retried)."),
        column("started_at", TSTZ, "When the step started."),
        column("ended_at", TSTZ, "When the step finished."),
        column("duration_seconds", pa.float64(), "ended_at - started_at in seconds."),
        column("url", pa.string(), "Link to the step's run in the Dagster UI."),
    ]
)


def step_rows(ep: Endpoint, run: dict[str, Any]) -> list[dict[str, Any]]:
    """``unified.task_runs`` rows for one run's step stats."""
    rows = []
    run_id = str(run.get("runId"))
    run_status = RUN_STATUS.get(str(run.get("status")), "queued")
    for step in run.get("stepStats") or []:
        raw = step.get("status")
        if raw:
            status = STEP_STATUS.get(str(raw), "queued")
        else:
            status = "cancelled" if run_status in ("cancelled", "failed") else "queued"
        rows.append(
            {
                "orchestrator": ORCHESTRATOR,
                "run_id": run_id,
                "task_run_id": f"{run_id}:{step.get('stepKey')}",
                "task_name": step.get("stepKey"),
                "status": status,
                "raw_status": raw,
                "attempt": max(1, len(step.get("attempts") or [])),
                "started_at": epoch_seconds(step.get("startTime")),
                "ended_at": epoch_seconds(step.get("endTime")),
                "duration_seconds": duration(step.get("startTime"), step.get("endTime")),
                "url": ep.run_url(run_id),
            }
        )
    return rows


@init_single_worker
@bind_fixed_schema
class UnifiedTaskRunsScan(RunsPager):
    """Rows of ``unified.task_runs`` (one per step of each run)."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = UNIFIED_TASK_RUNS_SCHEMA
    PAGE_SIZE: ClassVar[int] = 25
    WITH_STEPS: ClassVar[bool] = True
    FILTER_COLUMNS: ClassVar[dict[str, str]] = {"run_id": "run_id"}

    class Meta:
        """Scan metadata."""

        name = "unified_task_runs_scan"
        description = "Dagster run steps in the shared cross-orchestrator task-run shape"
        required_secrets = scan_meta_secrets()
        filter_pushdown = True
        auto_apply_filters = True
        projection_pushdown = True
        tags = object_tags(
            title="Unified Task Runs",
            category="unified",
            keywords="unified, task run, step, op, cross orchestrator, union all, task status",
            doc_llm=(
                "One row per step of every Dagster run, in the column layout shared by every Query.Farm "
                "orchestrator worker: `orchestrator`, `run_id`, `task_run_id` ('<run_id>:<step_key>'), "
                "`task_name` (step key), `status` (queued/running/success/failed/cancelled/skipped), "
                "`raw_status` (Dagster StepEventStatus), `attempt`, `started_at`, `ended_at`, "
                "`duration_seconds`, `url`. A run_id filter (=, IN) pushes down; without one every run is read, "
                "25 per request. Join to `dagster.unified.runs` on run_id."
            ),
            doc_md="Run steps in the shared orchestrator task-run shape.",
        )
        examples = [
            FunctionExample(
                sql="SELECT task_name, count(*) FILTER (WHERE status = 'failed') AS failures "
                "FROM dagster.unified.task_runs GROUP BY task_name ORDER BY failures DESC, task_name",
                description="Which steps fail most often",
            ),
        ]

    @classmethod
    def build_rows(cls, ep: Endpoint, runs: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """One row per step."""
        return [row for run in runs for row in step_rows(ep, run)]


# ----------------------------------------------------------------------------- run(run_id) & friends


@dataclass(slots=True, frozen=True, kw_only=True)
class RunIdArgs:
    """A single run id."""

    run_id: Annotated[str, Arg(0, doc="Run id as shown in the Dagster UI or in dagster.main.runs.run_id.")]


SINGLE_RUN_QUERY = (
    "query Run($runId: ID!) { runOrError(runId: $runId) { __typename "
    "... on Run { " + RUN_FIELDS + STEP_FIELDS + " } "
    "... on RunNotFoundError { message } ... on PythonError { message } } }"
)


def _single_run(ep: Endpoint, run_id: str) -> dict[str, Any] | None:
    data = graphql(ep, SINGLE_RUN_QUERY, {"runId": run_id})
    root = data.get("runOrError") or {}
    if root.get("__typename") == "RunNotFoundError":
        return None
    return check_union(root, "Run", "runOrError")


@init_single_worker
@bind_fixed_schema
class RunFunction(TableFunctionGenerator[RunIdArgs, None]):
    """``run(run_id)``: one run, or no rows."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = RUNS_SCHEMA

    class Meta:
        """Function metadata."""

        name = "run"
        description = "One run by id, with the same columns as dagster.main.runs (no rows if it does not exist)"
        required_secrets = scan_meta_secrets()
        tags = object_tags(
            title="Run By Id",
            category="runs",
            keywords="run, run id, lookup, run detail",
            doc_llm=(
                "`run(run_id)` returns the run with that id as one row with the columns of `dagster.main.runs`, "
                "or no rows when it does not exist. Filtering `dagster.main.runs` on `run_id` is equivalent "
                "(that filter is pushed down too)."
            ),
            doc_md="Look up one run by id.",
        )
        examples = [
            FunctionExample(
                sql="SELECT status, job_name, duration_seconds "
                "FROM dagster.main.run('00000000-0000-4000-8000-000000000003')",
                description="Status, job and duration of one run (paste a run id from dagster.main.runs)",
            )
        ]

    @classmethod
    def process(cls, params: ProcessParams[RunIdArgs], state: None, out: OutputCollector) -> None:
        """Fetch the run and emit it (or nothing)."""
        ep = endpoint(params.secrets)
        run = _single_run(ep, params.args.run_id)
        out.emit(rows_to_batch([native_run_row(ep, run)] if run else [], params.output_schema))
        out.finish()


@init_single_worker
@bind_fixed_schema
class RunStepStatsFunction(TableFunctionGenerator[RunIdArgs, None]):
    """``run_step_stats(run_id)``."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = pa.schema(
        [
            column("run_id", pa.string(), "Run id.", nullable=False),
            column("step_key", pa.string(), "Step key (op or asset step).", nullable=False),
            column("status", pa.string(), "SUCCESS, FAILURE, SKIPPED, IN_PROGRESS, or NULL if it never started."),
            column("started_at", TSTZ, "Step start."),
            column("ended_at", TSTZ, "Step end."),
            column("duration_seconds", pa.float64(), "ended_at - started_at in seconds."),
            column("attempt_count", pa.int32(), "Attempts made (more than 1 when the step retried).", nullable=False),
        ]
    )

    class Meta:
        """Function metadata."""

        name = "run_step_stats"
        description = "Per-step status and timing for one run"
        required_secrets = scan_meta_secrets()
        tags = object_tags(
            title="Run Step Stats",
            category="runs",
            keywords="step, op, timing, slow step, step failure, retry",
            doc_llm=(
                "`run_step_stats(run_id)`: one row per step of the run with `step_key`, `status`, `started_at`, "
                "`ended_at`, `duration_seconds` and `attempt_count`. No rows for an unknown run. For steps across "
                "many runs use `dagster.unified.task_runs`."
            ),
            doc_md="Step-level timing and status for one run.",
        )
        examples = [
            FunctionExample(
                sql="SELECT step_key, status, duration_seconds "
                "FROM dagster.main.run_step_stats('00000000-0000-4000-8000-000000000003') ORDER BY step_key",
                description="Status and duration of each step of one run",
            )
        ]

    @classmethod
    def process(cls, params: ProcessParams[RunIdArgs], state: None, out: OutputCollector) -> None:
        """Emit the run's step stats."""
        ep = endpoint(params.secrets)
        run = _single_run(ep, params.args.run_id)
        rows = []
        for s in (run or {}).get("stepStats") or []:
            rows.append(
                {
                    "run_id": params.args.run_id,
                    "step_key": s.get("stepKey"),
                    "status": s.get("status"),
                    "started_at": epoch_seconds(s.get("startTime")),
                    "ended_at": epoch_seconds(s.get("endTime")),
                    "duration_seconds": duration(s.get("startTime"), s.get("endTime")),
                    "attempt_count": len(s.get("attempts") or []),
                }
            )
        out.emit(rows_to_batch(rows, params.output_schema))
        out.finish()


EVENTS_QUERY = """
query RunEvents($runId: ID!, $afterCursor: String, $limit: Int) {
  logsForRun(runId: $runId, afterCursor: $afterCursor, limit: $limit) {
    __typename
    ... on EventConnection {
      cursor
      hasMore
      events {
        __typename
        ... on MessageEvent { runId timestamp level message stepKey eventType }
        ... on ExecutionStepFailureEvent { error { ...ErrorFields } }
        ... on RunFailureEvent { error { ...ErrorFields } }
      }
    }
    ... on RunNotFoundError { message }
    ... on PythonError { message }
  }
}
fragment ErrorFields on PythonError {
  message className cause { message className cause { message className cause { message className } } }
}
"""


def root_cause(error: dict[str, Any] | None) -> dict[str, Any]:
    """Return the innermost ``cause`` of a PythonError: the user's exception, not Dagster's wrapper."""
    error = error or {}
    while error.get("cause"):
        error = error["cause"]
    return error


@dataclass(kw_only=True)
class EventsCursor(ArrowSerializableDataclass):
    """Resumable position in a run's event log."""

    cursor: str = ""
    next_index: int = 0
    done: bool = False


@init_single_worker
@bind_fixed_schema
class RunEventsFunction(TableFunctionGenerator[RunIdArgs, EventsCursor]):
    """``run_events(run_id)``: the run's event log, oldest first."""

    PAGE_SIZE: ClassVar[int] = 500
    FIXED_SCHEMA: ClassVar[pa.Schema] = pa.schema(
        [
            column("run_id", pa.string(), "Run id.", nullable=False),
            column("event_index", pa.int64(), "0-based position in the run's event log.", nullable=False),
            column("timestamp", TSTZ, "When the event was logged."),
            column("level", pa.string(), "Log level: DEBUG, INFO, WARNING, ERROR, CRITICAL."),
            column("event_type", pa.string(), "DagsterEventType, e.g. STEP_FAILURE; NULL for plain log messages."),
            column(
                "event_typename", pa.string(), "GraphQL event class, e.g. ExecutionStepFailureEvent.", nullable=False
            ),
            column("step_key", pa.string(), "Step the event belongs to; NULL for run-level events."),
            column("message", pa.string(), "Event message."),
            column("error_message", pa.string(), "Root-cause error message (the user exception), for failure events."),
            column("error_class", pa.string(), "Root-cause Python exception class, for failure events."),
        ]
    )

    class Meta:
        """Function metadata."""

        name = "run_events"
        description = "The event log of one run, oldest first, paged"
        required_secrets = scan_meta_secrets()
        tags = object_tags(
            title="Run Events",
            category="runs",
            keywords="event log, logs, step failure, error, traceback, run events",
            doc_llm=(
                "`run_events(run_id)`: every event of the run in order (`event_index`), with `timestamp`, `level`, "
                "`event_type` (STEP_START, STEP_SUCCESS, STEP_FAILURE, ASSET_MATERIALIZATION, RUN_FAILURE, ...; "
                "NULL for user log lines), `event_typename`, `step_key`, `message`, and `error_message` / "
                "`error_class` for failures. Paged 500 events per request. No rows for an unknown run."
            ),
            doc_md="The full event log for one run, including failure messages.",
        )
        examples = [
            FunctionExample(
                sql="SELECT step_key, error_class, error_message "
                "FROM dagster.main.run_events('00000000-0000-4000-8000-000000000003') "
                "WHERE event_type = 'STEP_FAILURE' ORDER BY step_key",
                description="Which step of a failed run failed, and the root-cause error",
            )
        ]

    @classmethod
    def initial_state(cls, params: ProcessParams[RunIdArgs]) -> EventsCursor:
        """Start at the beginning of the log."""
        return EventsCursor()

    @classmethod
    def process(cls, params: ProcessParams[RunIdArgs], state: EventsCursor, out: OutputCollector) -> None:
        """Fetch one page of events."""
        if state.done:
            out.finish()
            return
        ep = endpoint(params.secrets)
        variables = {"runId": params.args.run_id, "afterCursor": state.cursor or None, "limit": cls.PAGE_SIZE}
        root = graphql(ep, EVENTS_QUERY, variables).get("logsForRun") or {}
        if root.get("__typename") == "RunNotFoundError":
            state.done = True
            out.emit(rows_to_batch([], params.output_schema))
            return
        root = check_union(root, "EventConnection", "logsForRun")
        start = state.next_index
        rows = []
        for i, e in enumerate(root.get("events") or []):
            error = root_cause(e.get("error"))
            rows.append(
                {
                    "run_id": e.get("runId") or params.args.run_id,
                    "event_index": start + i,
                    "timestamp": epoch_millis(e.get("timestamp")),
                    "level": e.get("level"),
                    "event_type": e.get("eventType"),
                    "event_typename": e.get("__typename") or "Event",
                    "step_key": e.get("stepKey"),
                    "message": e.get("message"),
                    "error_message": (error.get("message") or "").strip() or None,
                    "error_class": error.get("className"),
                }
            )
        next_cursor = str(root.get("cursor") or "")
        if not root.get("hasMore") or not rows or next_cursor == state.cursor:
            state.done = True
        state.cursor = next_cursor
        state.next_index = start + len(rows)
        out.emit(rows_to_batch(rows, params.output_schema))
