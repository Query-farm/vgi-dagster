"""Asset tables: assets, materializations, checks, check executions and partitions.

The per-asset tables (materializations, check executions, partitions) walk a list of
assets one GraphQL request at a time. The list and the position in it live in the
table-function state, so a long scan resumes on any worker process. An ``asset_key``
filter (``=`` / ``IN``) narrows that list on the server; materializations also push
their time range and partition filters down.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from typing import Any, ClassVar

import pyarrow as pa
from vgi.metadata import FunctionExample
from vgi.table_function import (
    OutputCollector,
    ProcessParams,
    TableFunctionGenerator,
    bind_fixed_schema,
    init_single_worker,
)
from vgi_rpc import ArrowSerializableDataclass

from .base import NoArgs, SnapshotScan, eq_values, range_bounds, rows_to_batch, scan_meta_secrets
from .client import (
    METADATA_FRAGMENT,
    Endpoint,
    asset_key_input,
    asset_key_str,
    check_union,
    endpoint,
    epoch_millis,
    epoch_seconds,
    graphql,
    metadata_json,
    tag_map,
)
from .meta import column, object_tags

TSTZ = pa.timestamp("us", tz="UTC")
STR_LIST = pa.list_(pa.string())
TAG_MAP = pa.map_(pa.string(), pa.string())


def _key_filter(params: ProcessParams[Any]) -> list[dict[str, list[str]]] | None:
    """``assetKeys`` input from an ``asset_key`` = / IN filter, or None for all assets."""
    keys = eq_values(params.current_pushdown_filters, "asset_key")
    return None if keys is None else [asset_key_input(str(k)) for k in keys]


def _owner(owner: dict[str, Any]) -> str | None:
    if owner.get("email"):
        return str(owner["email"])
    if owner.get("team"):
        return f"team:{owner['team']}"
    return None


# ----------------------------------------------------------------------------- assets


@init_single_worker
@bind_fixed_schema
class AssetsScan(SnapshotScan):
    """Rows of ``assets``."""

    QUERY = """
query Assets($assetKeys: [AssetKeyInput!]) {
  assetNodes(assetKeys: $assetKeys, loadMaterializations: true) {
    assetKey { path }
    groupName description computeKind kinds
    isPartitioned isMaterializable isObservable hasAssetChecks
    jobNames
    dependencyKeys { path }
    dependedByKeys { path }
    owners { __typename ... on UserAssetOwner { email } ... on TeamAssetOwner { team } }
    tags { key value }
    repository { name location { name } }
    partitionStats { numMaterialized numPartitions numFailed numMaterializing }
    assetMaterializations(limit: 1) { timestamp runId }
  }
}
"""
    FIXED_SCHEMA: ClassVar[pa.Schema] = pa.schema(
        [
            column(
                "asset_key", pa.string(), "Asset key with path parts joined by '/', e.g. 'raw/orders'.", nullable=False
            ),
            column("asset_key_path", STR_LIST, "Asset key path parts.", nullable=False),
            column("group_name", pa.string(), "Asset group.", nullable=False),
            column("description", pa.string(), "Asset description from code."),
            column("compute_kind", pa.string(), "Legacy compute kind tag, e.g. 'python' or 'dbt'."),
            column("kinds", STR_LIST, "Kinds (technology tags) declared on the asset."),
            column("is_partitioned", pa.bool_(), "True when the asset has a partitions definition.", nullable=False),
            column("is_materializable", pa.bool_(), "False for external/observable-only assets.", nullable=False),
            column("is_observable", pa.bool_(), "True for observable source assets.", nullable=False),
            column("has_asset_checks", pa.bool_(), "True when checks target the asset.", nullable=False),
            column("job_names", STR_LIST, "Jobs that can materialize the asset."),
            column("dependency_keys", STR_LIST, "Upstream asset keys."),
            column("depended_by_keys", STR_LIST, "Downstream asset keys."),
            column("owners", STR_LIST, "Owners: user emails, or 'team:<name>'."),
            column("tags", TAG_MAP, "Definition tags as a MAP."),
            column("repository_name", pa.string(), "Repository defining the asset.", nullable=False),
            column("location_name", pa.string(), "Code location defining the asset.", nullable=False),
            column("partition_count", pa.int32(), "Number of partitions in the definition; NULL when unpartitioned."),
            column("materialized_partition_count", pa.int32(), "Number of partitions materialized at least once."),
            column(
                "failed_partition_count",
                pa.int32(),
                "Number of partitions whose latest materialization attempt failed.",
            ),
            column("materializing_partition_count", pa.int32(), "Number of partitions being materialized right now."),
            column("last_materialized_at", TSTZ, "Latest materialization time; NULL if never materialized."),
            column("last_materialization_run_id", pa.string(), "Run of the latest materialization."),
            column("url", pa.string(), "Link to the asset in the Dagster UI.", nullable=False),
        ]
    )

    class Meta:
        """Scan metadata."""

        name = "assets_scan"
        description = "Software-defined assets with lineage, owners, partition counts and latest materialization"
        required_secrets = scan_meta_secrets()
        filter_pushdown = True
        auto_apply_filters = True
        tags = object_tags(
            title="Dagster Software-Defined Assets",
            category="assets",
            keywords="asset, software-defined asset, lineage, upstream, downstream, owner, group, freshness",
            doc_llm=(
                "One row per software-defined asset: `asset_key` ('a/b'), `asset_key_path`, `group_name`, "
                "`description`, `kinds`, flags (`is_partitioned`, `is_materializable`, `is_observable`, "
                "`has_asset_checks`), lineage (`dependency_keys`, `depended_by_keys`), `owners`, "
                "`tags` (`MAP(VARCHAR, VARCHAR)`), repository/location, partition counts (`partition_count`, "
                "`materialized_partition_count`, `failed_partition_count`) and `last_materialized_at` / "
                "`last_materialization_run_id`. An `asset_key` = or IN filter is sent to Dagster."
            ),
            doc_md="Software-defined assets with lineage, ownership, partition health and latest materialization.",
        )
        examples = [
            FunctionExample(
                sql="SELECT asset_key, group_name, last_materialized_at FROM dagster.main.assets "
                "ORDER BY last_materialized_at NULLS FIRST, asset_key",
                description="Assets by staleness: never-materialized ones first",
            ),
            FunctionExample(
                sql="SELECT asset_key, failed_partition_count, partition_count FROM dagster.main.assets "
                "WHERE is_partitioned AND failed_partition_count > 0 ORDER BY asset_key",
                description="Partitioned assets with failed partitions",
            ),
        ]

    @classmethod
    def variables(cls, params: ProcessParams[NoArgs]) -> dict[str, Any]:
        """Push an asset_key filter into ``assetNodes(assetKeys:)``."""
        return {"assetKeys": _key_filter(params)}

    @classmethod
    def build_rows(cls, ep: Endpoint, data: dict[str, Any]) -> list[dict[str, Any]]:
        """One row per asset node."""
        rows = []
        for node in data.get("assetNodes") or []:
            path = (node.get("assetKey") or {}).get("path") or []
            stats = node.get("partitionStats") or {}
            latest = (node.get("assetMaterializations") or [None])[0] or {}
            repo = node.get("repository") or {}
            rows.append(
                {
                    "asset_key": "/".join(path),
                    "asset_key_path": path,
                    "group_name": node.get("groupName"),
                    "description": node.get("description"),
                    "compute_kind": node.get("computeKind"),
                    "kinds": node.get("kinds") or [],
                    "is_partitioned": bool(node.get("isPartitioned")),
                    "is_materializable": bool(node.get("isMaterializable")),
                    "is_observable": bool(node.get("isObservable")),
                    "has_asset_checks": bool(node.get("hasAssetChecks")),
                    "job_names": node.get("jobNames") or [],
                    "dependency_keys": [asset_key_str(k) for k in node.get("dependencyKeys") or []],
                    "depended_by_keys": [asset_key_str(k) for k in node.get("dependedByKeys") or []],
                    "owners": [o for o in (_owner(x) for x in node.get("owners") or []) if o],
                    "tags": tag_map(node.get("tags")),
                    "repository_name": repo.get("name"),
                    "location_name": (repo.get("location") or {}).get("name"),
                    "partition_count": stats.get("numPartitions"),
                    "materialized_partition_count": stats.get("numMaterialized"),
                    "failed_partition_count": stats.get("numFailed"),
                    "materializing_partition_count": stats.get("numMaterializing"),
                    "last_materialized_at": epoch_millis(latest.get("timestamp")),
                    "last_materialization_run_id": latest.get("runId"),
                    "url": ep.asset_url(path),
                }
            )
        return rows


# ----------------------------------------------------------------------------- asset_checks

CHECKS_QUERY = """
query AssetChecks($assetKeys: [AssetKeyInput!]) {
  assetNodes(assetKeys: $assetKeys) {
    assetKey { path }
    hasAssetChecks
    assetChecksOrError {
      __typename
      ... on AssetChecks {
        checks {
          name description blocking jobNames canExecuteIndividually
          executionForLatestMaterialization {
            id runId status timestamp partition
            evaluation { severity success description }
          }
        }
      }
      ... on AssetCheckNeedsMigrationError { message }
      ... on AssetCheckNeedsUserCodeUpgrade { message }
      ... on AssetCheckNeedsAgentUpgradeError { message }
    }
  }
}
"""


def _checks(data: dict[str, Any]) -> list[tuple[str, dict[str, Any]]]:
    """``(asset_key, check)`` pairs from a CHECKS_QUERY response."""
    pairs = []
    for node in data.get("assetNodes") or []:
        if not node.get("hasAssetChecks"):
            continue
        root = check_union(node.get("assetChecksOrError"), "AssetChecks", "assetChecksOrError")
        for check in root.get("checks") or []:
            pairs.append((asset_key_str(node.get("assetKey")), check))
    return pairs


@init_single_worker
@bind_fixed_schema
class AssetChecksScan(SnapshotScan):
    """Rows of ``asset_checks``."""

    QUERY = CHECKS_QUERY
    FIXED_SCHEMA: ClassVar[pa.Schema] = pa.schema(
        [
            column("asset_key", pa.string(), "Asset the check targets ('a/b').", nullable=False),
            column("check_name", pa.string(), "Check name, unique per asset.", nullable=False),
            column("description", pa.string(), "Check description from code."),
            column("blocking", pa.bool_(), "True when a failure blocks downstream materializations.", nullable=False),
            column("job_names", STR_LIST, "Jobs that execute the check."),
            column("can_execute_individually", pa.string(), "CAN_EXECUTE, REQUIRES_MATERIALIZATION or NONE."),
            column("latest_execution_id", pa.string(), "Execution for the latest materialization; NULL if none."),
            column("latest_status", pa.string(), "SUCCEEDED, FAILED, EXECUTION_FAILED, IN_PROGRESS or SKIPPED."),
            column("latest_run_id", pa.string(), "Run that executed the latest check."),
            column("latest_executed_at", TSTZ, "When the latest execution happened."),
            column("latest_severity", pa.string(), "WARN or ERROR, from the latest evaluation."),
            column("latest_passed", pa.bool_(), "Whether the latest evaluation passed."),
        ]
    )

    class Meta:
        """Scan metadata."""

        name = "asset_checks_scan"
        description = "Asset checks and the result of each one's latest execution"
        required_secrets = scan_meta_secrets()
        filter_pushdown = True
        auto_apply_filters = True
        tags = object_tags(
            title="Dagster Asset Checks",
            category="assets",
            keywords="asset check, data quality, test, blocking check, failed check",
            doc_llm=(
                "One row per asset check: `asset_key`, `check_name`, `description`, `blocking`, `job_names`, and "
                "the execution for the asset's latest materialization (`latest_status` SUCCEEDED/FAILED/..., "
                "`latest_passed`, `latest_severity`, `latest_run_id`, `latest_executed_at`). History: "
                "`dagster.main.asset_check_executions`. An asset_key = / IN filter is pushed down."
            ),
            doc_md="Data-quality checks on assets and their latest result.",
        )
        examples = [
            FunctionExample(
                sql="SELECT asset_key, check_name, latest_severity FROM dagster.main.asset_checks "
                "WHERE latest_status = 'FAILED' ORDER BY asset_key, check_name",
                description="Checks failing on the latest materialization",
            )
        ]

    @classmethod
    def variables(cls, params: ProcessParams[NoArgs]) -> dict[str, Any]:
        """Push an asset_key filter into ``assetNodes(assetKeys:)``."""
        return {"assetKeys": _key_filter(params)}

    @classmethod
    def build_rows(cls, ep: Endpoint, data: dict[str, Any]) -> list[dict[str, Any]]:
        """One row per check."""
        rows = []
        for asset_key, check in _checks(data):
            latest = check.get("executionForLatestMaterialization") or {}
            evaluation = latest.get("evaluation") or {}
            rows.append(
                {
                    "asset_key": asset_key,
                    "check_name": check.get("name"),
                    "description": check.get("description"),
                    "blocking": bool(check.get("blocking")),
                    "job_names": check.get("jobNames") or [],
                    "can_execute_individually": check.get("canExecuteIndividually"),
                    "latest_execution_id": latest.get("id"),
                    "latest_status": latest.get("status"),
                    "latest_run_id": latest.get("runId"),
                    "latest_executed_at": epoch_seconds(latest.get("timestamp")),
                    "latest_severity": evaluation.get("severity"),
                    "latest_passed": evaluation.get("success"),
                }
            )
        return rows


# ----------------------------------------------------------------------------- per-asset pager


@dataclass(kw_only=True)
class AssetWalk(ArrowSerializableDataclass):
    """Resumable walk over a list of assets (or checks), one page per request.

    ``items_json`` is loaded on the first call; ``cursor`` is the per-item page cursor and
    ``seen_json`` the rows already emitted at the current page boundary (materializations).
    """

    loaded: bool = False
    items_json: str = "[]"
    index: int = 0
    cursor: str = ""
    seen_json: str = "[]"
    done: bool = False


class AssetWalker(TableFunctionGenerator[NoArgs, AssetWalk]):
    """Base: list the items once, then fetch pages item by item."""

    FIXED_SCHEMA: ClassVar[pa.Schema]

    @classmethod
    def list_items(cls, ep: Endpoint, params: ProcessParams[NoArgs]) -> list[Any]:
        """Return the items to walk (JSON-serializable)."""
        raise NotImplementedError

    @classmethod
    def fetch(cls, ep: Endpoint, params: ProcessParams[NoArgs], item: Any, state: AssetWalk) -> list[dict[str, Any]]:
        """Rows for the next page of ``item``; set ``state.cursor`` to "" when the item is exhausted."""
        raise NotImplementedError

    @classmethod
    def initial_state(cls, params: ProcessParams[NoArgs]) -> AssetWalk:
        """Nothing loaded yet."""
        return AssetWalk()

    @classmethod
    def process(cls, params: ProcessParams[NoArgs], state: AssetWalk, out: OutputCollector) -> None:
        """Emit the next page; move to the next item when the current one is exhausted."""
        ep = endpoint(params.secrets)
        if not state.loaded:
            state.items_json = json.dumps(cls.list_items(ep, params))
            state.loaded = True
        items = json.loads(state.items_json)
        if state.index >= len(items):
            out.finish()
            return
        rows = cls.fetch(ep, params, items[state.index], state)
        if not state.cursor:
            state.index += 1
            state.seen_json = "[]"
        out.emit(rows_to_batch(rows, params.output_schema))


# ----------------------------------------------------------------------------- asset_materializations

ASSET_KEYS_QUERY = """
query AssetKeys($assetKeys: [AssetKeyInput!], $cursor: String, $limit: Int) {
  assetsOrError(assetKeys: $assetKeys, cursor: $cursor, limit: $limit) {
    __typename
    ... on AssetConnection { cursor nodes { key { path } } }
    ... on PythonError { message }
  }
}
"""

MATERIALIZATIONS_QUERY = (
    """
query Materializations(
  $assetKey: AssetKeyInput!, $before: String, $after: String, $partitions: [String!], $limit: Int
) {
  assetOrError(assetKey: $assetKey) {
    __typename
    ... on Asset {
      assetMaterializations(
        beforeTimestampMillis: $before, afterTimestampMillis: $after, partitions: $partitions, limit: $limit
      ) {
        runId timestamp partition stepKey label description
        tags { key value }
        metadataEntries { ...MetadataFields }
      }
    }
    ... on AssetNotFoundError { message }
  }
}
"""
    + METADATA_FRAGMENT
)


def _millis(value: Any) -> int | None:
    if isinstance(value, datetime):
        return int(value.timestamp() * 1000)
    return None


@init_single_worker
@bind_fixed_schema
class AssetMaterializationsScan(AssetWalker):
    """Rows of ``asset_materializations``."""

    PAGE_SIZE: ClassVar[int] = 200
    FIXED_SCHEMA: ClassVar[pa.Schema] = pa.schema(
        [
            column("asset_key", pa.string(), "Materialized asset ('a/b').", nullable=False),
            column("materialized_at", TSTZ, "When the materialization event was logged.", nullable=False),
            column("run_id", pa.string(), "Run that materialized the asset.", nullable=False),
            column("partition", pa.string(), "Partition key; NULL for unpartitioned assets."),
            column("step_key", pa.string(), "Step that produced the event."),
            column("label", pa.string(), "Event label."),
            column("description", pa.string(), "Event description."),
            column("tags", TAG_MAP, "Event tags (data version, code version, ...) as a MAP."),
            column("metadata", pa.string(), "Materialization metadata as a JSON object {label: value}; NULL if none."),
        ]
    )

    class Meta:
        """Scan metadata."""

        name = "asset_materializations_scan"
        description = "Materialization history of every asset, newest first per asset"
        required_secrets = scan_meta_secrets()
        filter_pushdown = True
        auto_apply_filters = True
        projection_pushdown = True
        tags = object_tags(
            title="Dagster Asset Materializations",
            category="assets",
            keywords="materialization, asset history, metadata, row count, partition, data version",
            doc_llm=(
                "Every materialization event: `asset_key`, `materialized_at`, `run_id`, `partition`, `step_key`, "
                "`label`, `description`, `tags` (`MAP(VARCHAR, VARCHAR)`) and `metadata` (a JSON object of the "
                "event's metadata, e.g. "
                "`metadata->>'row_count'`). Assets are read one at a time, newest first, 200 events per request. "
                "Pushed down: asset_key (=, IN), materialized_at (ranges) and partition (=, IN). Always filter "
                "on asset_key and/or a time range on large deployments."
            ),
            doc_md=(
                "Materialization history with metadata. `asset_key`, `materialized_at` and `partition` filters "
                "are sent to Dagster."
            ),
        )
        examples = [
            FunctionExample(
                sql="SELECT materialized_at, run_id, CAST(metadata->>'row_count' AS BIGINT) AS row_count "
                "FROM dagster.main.asset_materializations WHERE asset_key = 'raw_orders' "
                "AND materialized_at >= TIMESTAMPTZ '2026-01-01' ORDER BY materialized_at DESC",
                description="Row counts recorded by each raw_orders materialization since 2026",
            ),
            FunctionExample(
                sql="SELECT asset_key, count(*) AS materializations, max(materialized_at) AS latest "
                "FROM dagster.main.asset_materializations GROUP BY asset_key ORDER BY asset_key",
                description="How often each asset has been materialized",
            ),
        ]

    @classmethod
    def list_items(cls, ep: Endpoint, params: ProcessParams[NoArgs]) -> list[Any]:
        """Assets with an event history (pushed-down keys, or every asset)."""
        keys = eq_values(params.current_pushdown_filters, "asset_key")
        if keys is not None:
            return [str(k) for k in keys]
        found: list[str] = []
        cursor: str | None = None
        while True:
            data = graphql(ep, ASSET_KEYS_QUERY, {"assetKeys": None, "cursor": cursor, "limit": 1000})
            root = check_union(data.get("assetsOrError"), "AssetConnection", "assetsOrError")
            nodes = root.get("nodes") or []
            found.extend(asset_key_str(n.get("key")) for n in nodes)
            cursor = root.get("cursor")
            if len(nodes) < 1000 or not cursor:
                return found

    @classmethod
    def fetch(cls, ep: Endpoint, params: ProcessParams[NoArgs], item: Any, state: AssetWalk) -> list[dict[str, Any]]:
        """One page of one asset's materializations, newest first."""
        filters = params.current_pushdown_filters
        lower, upper = range_bounds(filters, "materialized_at")
        lower_ms, upper_ms = _millis(lower), _millis(upper)
        before = int(state.cursor) if state.cursor else (upper_ms + 1 if upper_ms is not None else None)
        partitions = eq_values(filters, "partition")
        if partitions is not None and not partitions:
            state.cursor = ""
            return []
        variables = {
            "assetKey": asset_key_input(str(item)),
            "before": None if before is None else str(before),
            "after": None if lower_ms is None else str(lower_ms - 1),
            "partitions": None if partitions is None else [str(p) for p in partitions],
            "limit": cls.PAGE_SIZE,
        }
        root = graphql(ep, MATERIALIZATIONS_QUERY, variables).get("assetOrError") or {}
        if root.get("__typename") == "AssetNotFoundError":
            state.cursor = ""
            return []
        events = check_union(root, "Asset", "assetOrError").get("assetMaterializations") or []
        seen = set(json.loads(state.seen_json))
        rows = []
        for e in events:
            ident = f"{e.get('runId')}|{e.get('timestamp')}|{e.get('partition')}|{e.get('stepKey')}"
            if ident in seen:
                continue
            rows.append(
                {
                    "asset_key": str(item),
                    "materialized_at": epoch_millis(e.get("timestamp")),
                    "run_id": e.get("runId"),
                    "partition": e.get("partition"),
                    "step_key": e.get("stepKey"),
                    "label": e.get("label"),
                    "description": e.get("description"),
                    "tags": tag_map(e.get("tags")),
                    "metadata": metadata_json(e.get("metadataEntries")),
                }
            )
        if len(events) < cls.PAGE_SIZE:
            state.cursor = ""
            return rows
        # Next page: events strictly older than the oldest one here, plus any that share its
        # millisecond (re-requested with +1 and de-duplicated against `seen`).
        oldest = min(int(e["timestamp"]) for e in events)
        boundary = [
            f"{e.get('runId')}|{e.get('timestamp')}|{e.get('partition')}|{e.get('stepKey')}"
            for e in events
            if int(e["timestamp"]) == oldest
        ]
        state.cursor = str(oldest + 1)
        state.seen_json = json.dumps(boundary)
        return rows


# ----------------------------------------------------------------------------- asset_check_executions

CHECK_EXECUTIONS_QUERY = (
    """
query CheckExecutions($assetKey: AssetKeyInput!, $checkName: String!, $limit: Int!, $cursor: String) {
  assetCheckExecutions(assetKey: $assetKey, checkName: $checkName, limit: $limit, cursor: $cursor) {
    id runId status timestamp partition stepKey
    evaluation { severity success description metadataEntries { ...MetadataFields } }
  }
}
"""
    + METADATA_FRAGMENT
)


@init_single_worker
@bind_fixed_schema
class AssetCheckExecutionsScan(AssetWalker):
    """Rows of ``asset_check_executions``."""

    PAGE_SIZE: ClassVar[int] = 200
    FIXED_SCHEMA: ClassVar[pa.Schema] = pa.schema(
        [
            column("asset_key", pa.string(), "Asset the check targets ('a/b').", nullable=False),
            column("check_name", pa.string(), "Check the execution belongs to (unique per asset).", nullable=False),
            column("execution_id", pa.string(), "Execution id (event storage id).", nullable=False),
            column("run_id", pa.string(), "Run that executed the check.", nullable=False),
            column(
                "status", pa.string(), "SUCCEEDED, FAILED, EXECUTION_FAILED, IN_PROGRESS or SKIPPED.", nullable=False
            ),
            column("executed_at", TSTZ, "When the execution was recorded.", nullable=False),
            column("partition", pa.string(), "Partition the check ran for; NULL when unpartitioned."),
            column("step_key", pa.string(), "Step that ran the check."),
            column("passed", pa.bool_(), "Evaluation result; NULL when the check did not evaluate."),
            column("severity", pa.string(), "WARN or ERROR."),
            column("description", pa.string(), "Evaluation description."),
            column("metadata", pa.string(), "Evaluation metadata as a JSON object {label: value}; NULL if none."),
        ]
    )

    class Meta:
        """Scan metadata."""

        name = "asset_check_executions_scan"
        description = "Execution history of every asset check, newest first"
        required_secrets = scan_meta_secrets()
        filter_pushdown = True
        auto_apply_filters = True
        projection_pushdown = True
        tags = object_tags(
            title="Dagster Asset Check Executions",
            category="assets",
            keywords="asset check, check history, data quality trend, evaluation, severity",
            doc_llm=(
                "Every execution of every asset check: `asset_key`, `check_name`, `execution_id`, `run_id`, "
                "`status` (SUCCEEDED/FAILED/EXECUTION_FAILED/IN_PROGRESS/SKIPPED), `executed_at`, `partition`, "
                "`passed`, `severity`, `description` and `metadata` JSON. Checks are read one at a time, newest "
                "first. asset_key (=, IN) and check_name (=, IN) filters narrow which checks are read."
            ),
            doc_md="History of asset check executions with their evaluation results.",
        )
        examples = [
            FunctionExample(
                sql="SELECT asset_key, check_name, count(*) FILTER (WHERE NOT passed) AS failures, count(*) AS runs "
                "FROM dagster.main.asset_check_executions GROUP BY ALL ORDER BY failures DESC, asset_key",
                description="Failure rate of each asset check",
            )
        ]

    @classmethod
    def list_items(cls, ep: Endpoint, params: ProcessParams[NoArgs]) -> list[Any]:
        """``[asset_key, check_name]`` pairs, narrowed by pushed-down filters."""
        data = graphql(ep, CHECKS_QUERY, {"assetKeys": _key_filter(params)})
        names = eq_values(params.current_pushdown_filters, "check_name")
        wanted = None if names is None else {str(n) for n in names}
        return [
            [key, check.get("name")] for key, check in _checks(data) if wanted is None or check.get("name") in wanted
        ]

    @classmethod
    def fetch(cls, ep: Endpoint, params: ProcessParams[NoArgs], item: Any, state: AssetWalk) -> list[dict[str, Any]]:
        """One page of one check's executions."""
        asset_key, check_name = item
        variables = {
            "assetKey": asset_key_input(asset_key),
            "checkName": check_name,
            "limit": cls.PAGE_SIZE,
            "cursor": state.cursor or None,
        }
        executions = graphql(ep, CHECK_EXECUTIONS_QUERY, variables).get("assetCheckExecutions") or []
        rows = []
        for x in executions:
            evaluation = x.get("evaluation") or {}
            rows.append(
                {
                    "asset_key": asset_key,
                    "check_name": check_name,
                    "execution_id": x.get("id"),
                    "run_id": x.get("runId"),
                    "status": x.get("status"),
                    "executed_at": epoch_seconds(x.get("timestamp")),
                    "partition": x.get("partition"),
                    "step_key": x.get("stepKey"),
                    "passed": evaluation.get("success"),
                    "severity": evaluation.get("severity"),
                    "description": evaluation.get("description"),
                    "metadata": metadata_json(evaluation.get("metadataEntries")),
                }
            )
        state.cursor = str(executions[-1].get("id")) if len(executions) == cls.PAGE_SIZE else ""
        return rows


# ----------------------------------------------------------------------------- asset_partitions

PARTITIONED_QUERY = """
query Partitioned($assetKeys: [AssetKeyInput!]) {
  assetNodes(assetKeys: $assetKeys) { assetKey { path } isPartitioned }
}
"""

STATUS_1D = """
  __typename
  ... on DefaultPartitionStatuses { materializedPartitions failedPartitions materializingPartitions }
  ... on TimePartitionStatuses { ranges { startKey endKey status } }
"""

PARTITIONS_QUERY = (
    "query Partitions($assetKey: AssetKeyInput!) { assetNodeOrError(assetKey: $assetKey) { __typename "
    "... on AssetNode { partitionKeys partitionKeysByDimension { name partitionKeys } "
    "assetPartitionStatuses { " + STATUS_1D + " ... on MultiPartitionStatuses { primaryDimensionName "
    "ranges { primaryDimStartKey primaryDimEndKey secondaryDim { " + STATUS_1D + " } } } } } "
    "... on AssetNotFoundError { message } } }"
)


def statuses_1d(keys: list[str], statuses: dict[str, Any]) -> dict[str, str]:
    """Partition key -> status for a single-dimension status object."""
    result: dict[str, str] = {}
    if statuses.get("__typename") == "DefaultPartitionStatuses":
        for field, status in (
            ("materializedPartitions", "MATERIALIZED"),
            ("failedPartitions", "FAILED"),
            ("materializingPartitions", "MATERIALIZING"),
        ):
            for key in statuses.get(field) or []:
                result[key] = status
    elif statuses.get("__typename") == "TimePartitionStatuses":
        index = {k: i for i, k in enumerate(keys)}
        for r in statuses.get("ranges") or []:
            start, end = index.get(r.get("startKey")), index.get(r.get("endKey"))
            if start is None or end is None:
                continue
            for key in keys[start : end + 1]:
                result[key] = str(r.get("status"))
    return result


def partition_statuses(node: dict[str, Any]) -> list[tuple[str, str]]:
    """``(partition_key, status)`` for every partition of an asset node, in Dagster's order."""
    keys: list[str] = node.get("partitionKeys") or []
    statuses = node.get("assetPartitionStatuses") or {}
    if statuses.get("__typename") != "MultiPartitionStatuses":
        known = statuses_1d(keys, statuses)
        return [(k, known.get(k, "MISSING")) for k in keys]
    # Multi-partitioned: keys are dimension values ordered by dimension name, joined by '|'.
    dims = {d.get("name"): d.get("partitionKeys") or [] for d in node.get("partitionKeysByDimension") or []}
    primary_name = statuses.get("primaryDimensionName")
    secondary_name = next((n for n in dims if n != primary_name), None)
    primary_keys = dims.get(primary_name, [])
    secondary_keys = dims.get(secondary_name, []) if secondary_name else []
    primary_index = {k: i for i, k in enumerate(primary_keys)}
    by_key: dict[str, str] = {}
    for r in statuses.get("ranges") or []:
        start, end = primary_index.get(r.get("primaryDimStartKey")), primary_index.get(r.get("primaryDimEndKey"))
        if start is None or end is None:
            continue
        secondary = statuses_1d(secondary_keys, r.get("secondaryDim") or {})
        for p in primary_keys[start : end + 1]:
            for s, status in secondary.items():
                parts = {primary_name: p, secondary_name: s}
                by_key["|".join(parts[name] for name in sorted(parts, key=str))] = status
    return [(k, by_key.get(k, "MISSING")) for k in keys]


@init_single_worker
@bind_fixed_schema
class AssetPartitionsScan(AssetWalker):
    """Rows of ``asset_partitions``."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = pa.schema(
        [
            column("asset_key", pa.string(), "Partitioned asset ('a/b').", nullable=False),
            column(
                "partition_key",
                pa.string(),
                "Partition key; multi-partition keys join dimensions with '|'.",
                nullable=False,
            ),
            column("partition_index", pa.int32(), "Position in the partitions definition (0-based).", nullable=False),
            column("status", pa.string(), "MATERIALIZED, FAILED, MATERIALIZING or MISSING.", nullable=False),
        ]
    )

    class Meta:
        """Scan metadata."""

        name = "asset_partitions_scan"
        description = "Every partition of every partitioned asset, with its materialization status"
        required_secrets = scan_meta_secrets()
        filter_pushdown = True
        auto_apply_filters = True
        tags = object_tags(
            title="Dagster Asset Partitions",
            category="assets",
            keywords="partition, partition status, backfill, missing partition, failed partition, daily partition",
            doc_llm=(
                "One row per partition of each partitioned asset: `asset_key`, `partition_key`, `partition_index` "
                "(order in the definition) and `status` (MATERIALIZED, FAILED, MATERIALIZING or MISSING = never "
                "materialized). Use it to find gaps before a backfill. One request per asset; an asset_key = / IN "
                "filter is pushed down."
            ),
            doc_md="Partition-by-partition materialization status of partitioned assets.",
        )
        examples = [
            FunctionExample(
                sql="SELECT asset_key, status, count(*) AS partitions FROM dagster.main.asset_partitions "
                "GROUP BY ALL ORDER BY asset_key, status",
                description="Partition health per asset",
            ),
            FunctionExample(
                sql="SELECT partition_key FROM dagster.main.asset_partitions "
                "WHERE asset_key = 'daily_events' AND status <> 'MATERIALIZED' ORDER BY partition_index",
                description="Partitions of daily_events still to backfill",
            ),
        ]

    @classmethod
    def list_items(cls, ep: Endpoint, params: ProcessParams[NoArgs]) -> list[Any]:
        """Partitioned assets, narrowed by a pushed-down asset_key filter."""
        data = graphql(ep, PARTITIONED_QUERY, {"assetKeys": _key_filter(params)})
        return [asset_key_str(n.get("assetKey")) for n in data.get("assetNodes") or [] if n.get("isPartitioned")]

    @classmethod
    def fetch(cls, ep: Endpoint, params: ProcessParams[NoArgs], item: Any, state: AssetWalk) -> list[dict[str, Any]]:
        """Every partition of one asset (one request)."""
        state.cursor = ""
        root = graphql(ep, PARTITIONS_QUERY, {"assetKey": asset_key_input(str(item))}).get("assetNodeOrError") or {}
        if root.get("__typename") == "AssetNotFoundError":
            return []
        node = check_union(root, "AssetNode", "assetNodeOrError")
        return [
            {"asset_key": str(item), "partition_key": key, "partition_index": i, "status": status}
            for i, (key, status) in enumerate(partition_statuses(node))
        ]
