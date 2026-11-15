"""Definition tables: code locations, repositories, jobs, schedules, sensors, resources.

Each is one GraphQL query against the deployment's workspace, read in a single batch.
"""

from __future__ import annotations

from typing import Any, ClassVar

import pyarrow as pa
from vgi.metadata import FunctionExample
from vgi.table_function import bind_fixed_schema, init_single_worker

from .base import SnapshotScan, scan_meta_secrets
from .client import DagsterError, Endpoint, check_union, epoch_seconds, tag_map
from .meta import column, object_tags

STR_LIST = pa.list_(pa.string())
TAG_MAP = pa.map_(pa.string(), pa.string())
LIBRARY_VERSION = pa.struct([pa.field("name", pa.string()), pa.field("version", pa.string())])


def _repo_nodes(data: dict[str, Any]) -> list[dict[str, Any]]:
    root = check_union(data.get("repositoriesOrError"), "RepositoryConnection", "repositoriesOrError")
    return list(root.get("nodes") or [])


def _repo_cols(repo: dict[str, Any]) -> dict[str, Any]:
    return {"repository_name": repo.get("name"), "location_name": (repo.get("location") or {}).get("name")}


def _repo_fields() -> list[pa.Field[Any]]:
    return [
        column("repository_name", pa.string(), "Repository (Definitions object) the row belongs to.", nullable=False),
        column("location_name", pa.string(), "Code location that serves the repository.", nullable=False),
    ]


def _repositories_query(selection: str) -> str:
    return (
        "query Repos { repositoriesOrError { __typename "
        "... on RepositoryConnection { nodes { name location { name } " + selection + " } } "
        "... on PythonError { message } } }"
    )


# ----------------------------------------------------------------------------- code_locations


@init_single_worker
@bind_fixed_schema
class CodeLocationsScan(SnapshotScan):
    """Rows of ``code_locations``."""

    QUERY = """
query CodeLocations {
  workspaceOrError {
    __typename
    ... on Workspace {
      locationEntries {
        name
        loadStatus
        updatedTimestamp
        locationOrLoadError {
          __typename
          ... on RepositoryLocation { isReloadSupported dagsterLibraryVersions { name version } repositories { name } }
          ... on PythonError { message }
        }
      }
    }
    ... on PythonError { message }
  }
}
"""
    FIXED_SCHEMA: ClassVar[pa.Schema] = pa.schema(
        [
            column("location_name", pa.string(), "Code location name.", nullable=False),
            column("load_status", pa.string(), "LOADING or LOADED.", nullable=False),
            column("updated_at", pa.timestamp("us", tz="UTC"), "When the location was last (re)loaded."),
            column("is_reload_supported", pa.bool_(), "Whether the location can be reloaded from the UI."),
            column("repository_names", STR_LIST, "Repositories the location serves; empty when it failed to load."),
            column("library_versions", pa.list_(LIBRARY_VERSION), "Dagster library versions the location runs."),
            column("error_message", pa.string(), "Load error, when the location failed to load; else NULL."),
        ]
    )

    class Meta:
        """Scan metadata."""

        name = "code_locations_scan"
        description = "Code locations in the Dagster workspace, with load status and errors"
        required_secrets = scan_meta_secrets()
        tags = object_tags(
            title="Dagster Code Locations",
            category="definitions",
            keywords="code location, workspace, load error, dagster version, deployment health",
            doc_llm=(
                "One row per code location in the workspace (`location_name`, `load_status`, `updated_at`, "
                "`repository_names`, `library_versions` (`LIST(STRUCT(name, version))`), `error_message`). A location "
                "that failed to import has `error_message` set and no repositories: filter "
                "`error_message IS NOT NULL` to find broken deployments."
            ),
            doc_md="Code locations in the workspace, with the Dagster versions they run and any load error.",
        )
        examples = [
            FunctionExample(
                sql="SELECT location_name, load_status, error_message FROM dagster.main.code_locations "
                "ORDER BY location_name",
                description="Every code location and whether it loaded",
            )
        ]

    @classmethod
    def build_rows(cls, ep: Endpoint, data: dict[str, Any]) -> list[dict[str, Any]]:
        """One row per workspace location entry."""
        ws = check_union(data.get("workspaceOrError"), "Workspace", "workspaceOrError")
        rows = []
        for entry in ws.get("locationEntries") or []:
            inner = entry.get("locationOrLoadError") or {}
            loaded = inner.get("__typename") == "RepositoryLocation"
            rows.append(
                {
                    "location_name": entry.get("name"),
                    "load_status": entry.get("loadStatus"),
                    "updated_at": epoch_seconds(entry.get("updatedTimestamp")),
                    "is_reload_supported": inner.get("isReloadSupported") if loaded else None,
                    "repository_names": [r.get("name") for r in inner.get("repositories") or []] if loaded else [],
                    "library_versions": inner.get("dagsterLibraryVersions") if loaded else None,
                    "error_message": None if loaded else inner.get("message"),
                }
            )
        return rows


# ----------------------------------------------------------------------------- repositories


@init_single_worker
@bind_fixed_schema
class RepositoriesScan(SnapshotScan):
    """Rows of ``repositories``."""

    QUERY = _repositories_query("id assetGroups { groupName } pipelines { name } schedules { name } sensors { name }")
    FIXED_SCHEMA: ClassVar[pa.Schema] = pa.schema(
        [
            *_repo_fields(),
            column("repository_id", pa.string(), "Dagster's opaque repository id.", nullable=False),
            column("job_count", pa.int32(), "Number of jobs, including implicit asset jobs.", nullable=False),
            column("schedule_count", pa.int32(), "Number of schedules.", nullable=False),
            column("sensor_count", pa.int32(), "Number of sensors.", nullable=False),
            column("asset_groups", STR_LIST, "Asset group names defined in the repository."),
        ]
    )

    class Meta:
        """Scan metadata."""

        name = "repositories_scan"
        description = "Repositories (Definitions objects) across all code locations"
        required_secrets = scan_meta_secrets()
        tags = object_tags(
            title="Dagster Repositories",
            category="definitions",
            keywords="repository, definitions, code location, asset group",
            doc_llm=(
                "One row per repository (a Definitions object) with its code location, job/schedule/sensor counts "
                "and asset group names. Most projects have one repository per location named `__repository__`."
            ),
            doc_md="Repositories across code locations, with counts of what each defines.",
        )
        examples = [
            FunctionExample(
                sql="SELECT location_name, repository_name, job_count, schedule_count "
                "FROM dagster.main.repositories ORDER BY location_name, repository_name",
                description="What each repository defines",
            )
        ]

    @classmethod
    def build_rows(cls, ep: Endpoint, data: dict[str, Any]) -> list[dict[str, Any]]:
        """One row per repository."""
        return [
            {
                **_repo_cols(repo),
                "repository_id": repo.get("id"),
                "job_count": len(repo.get("pipelines") or []),
                "schedule_count": len(repo.get("schedules") or []),
                "sensor_count": len(repo.get("sensors") or []),
                "asset_groups": [g.get("groupName") for g in repo.get("assetGroups") or []],
            }
            for repo in _repo_nodes(data)
        ]


# ----------------------------------------------------------------------------- jobs


@init_single_worker
@bind_fixed_schema
class JobsScan(SnapshotScan):
    """Rows of ``jobs``."""

    QUERY = _repositories_query(
        "pipelines { id name description isAssetJob tags { key value } schedules { name } sensors { name } }"
    )
    FIXED_SCHEMA: ClassVar[pa.Schema] = pa.schema(
        [
            column(
                "job_name",
                pa.string(),
                "Job name; asset jobs created implicitly are named __ASSET_JOB.",
                nullable=False,
            ),
            *_repo_fields(),
            column("job_id", pa.string(), "Dagster's opaque job id.", nullable=False),
            column("description", pa.string(), "Job description from code."),
            column("is_asset_job", pa.bool_(), "True for jobs that materialize assets.", nullable=False),
            column("tags", TAG_MAP, "Job tags as a MAP; read one with tags['team']."),
            column("schedule_names", STR_LIST, "Schedules that target the job."),
            column("sensor_names", STR_LIST, "Sensors that target the job."),
        ]
    )

    class Meta:
        """Scan metadata."""

        name = "jobs_scan"
        description = "Jobs defined across all repositories, with their schedules and sensors"
        required_secrets = scan_meta_secrets()
        tags = object_tags(
            title="Dagster Jobs",
            category="definitions",
            keywords="job, pipeline, asset job, op graph, schedule, sensor",
            doc_llm=(
                "One row per job: `job_name`, repository/location, `description`, `is_asset_job`, `tags` "
                "(`MAP(VARCHAR, VARCHAR)`) and the `schedule_names`/`sensor_names` that target it. Join to "
                "`dagster.main.runs` on `job_name` for run history."
            ),
            doc_md="Jobs in every repository, with tags and the schedules and sensors that launch them.",
        )
        examples = [
            FunctionExample(
                sql="SELECT job_name, len(schedule_names) AS schedules, tags['team'] AS team "
                "FROM dagster.main.jobs WHERE NOT is_asset_job ORDER BY job_name",
                description="Op jobs, how many schedules launch each, and the owning team tag",
            )
        ]

    @classmethod
    def build_rows(cls, ep: Endpoint, data: dict[str, Any]) -> list[dict[str, Any]]:
        """One row per pipeline/job."""
        rows = []
        for repo in _repo_nodes(data):
            for job in repo.get("pipelines") or []:
                rows.append(
                    {
                        "job_name": job.get("name"),
                        **_repo_cols(repo),
                        "job_id": job.get("id"),
                        "description": job.get("description"),
                        "is_asset_job": bool(job.get("isAssetJob")),
                        "tags": tag_map(job.get("tags")),
                        "schedule_names": [s.get("name") for s in job.get("schedules") or []],
                        "sensor_names": [s.get("name") for s in job.get("sensors") or []],
                    }
                )
        return rows


# ----------------------------------------------------------------------------- schedules


@init_single_worker
@bind_fixed_schema
class SchedulesScan(SnapshotScan):
    """Rows of ``schedules``."""

    QUERY = _repositories_query(
        "schedules { id name cronSchedule executionTimezone pipelineName description defaultStatus "
        "scheduleState { status nextTick { timestamp } } }"
    )
    FIXED_SCHEMA: ClassVar[pa.Schema] = pa.schema(
        [
            column("schedule_name", pa.string(), "Name the schedule is declared with in code.", nullable=False),
            *_repo_fields(),
            column("job_name", pa.string(), "Job the schedule launches.", nullable=False),
            column("cron_schedule", pa.string(), "Cron expression, e.g. '0 6 * * *'.", nullable=False),
            column("execution_timezone", pa.string(), "Timezone the cron expression is evaluated in."),
            column("status", pa.string(), "Current state: RUNNING or STOPPED.", nullable=False),
            column("default_status", pa.string(), "Status declared in code: RUNNING or STOPPED.", nullable=False),
            column("next_tick_at", pa.timestamp("us", tz="UTC"), "Next evaluation time, when the schedule is running."),
            column("description", pa.string(), "Schedule description from code."),
            column("schedule_id", pa.string(), "Dagster's opaque schedule id.", nullable=False),
        ]
    )

    class Meta:
        """Scan metadata."""

        name = "schedules_scan"
        description = "Schedules across all repositories, with cron, state and next tick"
        required_secrets = scan_meta_secrets()
        tags = object_tags(
            title="Dagster Schedules",
            category="definitions",
            keywords="schedule, cron, next tick, running, stopped, timezone",
            doc_llm=(
                "One row per schedule: `schedule_name`, target `job_name`, `cron_schedule`, `execution_timezone`, "
                "`status` (RUNNING/STOPPED, the live state) vs `default_status` (from code), and `next_tick_at` "
                "(NULL when stopped)."
            ),
            doc_md="Schedules with their cron expression, live state and next tick.",
        )
        examples = [
            FunctionExample(
                sql="SELECT schedule_name, job_name, cron_schedule, status FROM dagster.main.schedules "
                "ORDER BY schedule_name",
                description="Every schedule and whether it is running",
            )
        ]

    @classmethod
    def build_rows(cls, ep: Endpoint, data: dict[str, Any]) -> list[dict[str, Any]]:
        """One row per schedule."""
        rows = []
        for repo in _repo_nodes(data):
            for s in repo.get("schedules") or []:
                state = s.get("scheduleState") or {}
                rows.append(
                    {
                        "schedule_name": s.get("name"),
                        **_repo_cols(repo),
                        "job_name": s.get("pipelineName"),
                        "cron_schedule": s.get("cronSchedule"),
                        "execution_timezone": s.get("executionTimezone"),
                        "status": state.get("status") or s.get("defaultStatus"),
                        "default_status": s.get("defaultStatus"),
                        "next_tick_at": epoch_seconds((state.get("nextTick") or {}).get("timestamp")),
                        "description": s.get("description"),
                        "schedule_id": s.get("id"),
                    }
                )
        return rows


# ----------------------------------------------------------------------------- sensors


@init_single_worker
@bind_fixed_schema
class SensorsScan(SnapshotScan):
    """Rows of ``sensors``."""

    QUERY = _repositories_query(
        "sensors { id name description sensorType defaultStatus minIntervalSeconds targets { pipelineName } "
        "sensorState { status } }"
    )
    FIXED_SCHEMA: ClassVar[pa.Schema] = pa.schema(
        [
            column("sensor_name", pa.string(), "Name the sensor is declared with in code.", nullable=False),
            *_repo_fields(),
            column(
                "sensor_type", pa.string(), "STANDARD, ASSET, MULTI_ASSET, RUN_STATUS, AUTOMATION, ...", nullable=False
            ),
            column("status", pa.string(), "Current state: RUNNING or STOPPED.", nullable=False),
            column("default_status", pa.string(), "Status declared in code.", nullable=False),
            column("min_interval_seconds", pa.int32(), "Minimum seconds between evaluations.", nullable=False),
            column("target_jobs", STR_LIST, "Jobs the sensor can launch."),
            column("description", pa.string(), "Sensor description from code."),
            column("sensor_id", pa.string(), "Dagster's opaque sensor id.", nullable=False),
        ]
    )

    class Meta:
        """Scan metadata."""

        name = "sensors_scan"
        description = "Sensors across all repositories, with type, state and targets"
        required_secrets = scan_meta_secrets()
        tags = object_tags(
            title="Dagster Sensors",
            category="definitions",
            keywords="sensor, event trigger, automation, run status sensor, asset sensor",
            doc_llm=(
                "One row per sensor: `sensor_name`, `sensor_type`, live `status` vs `default_status`, "
                "`min_interval_seconds` and `target_jobs` (`VARCHAR[]`)."
            ),
            doc_md="Sensors with their type, live state and the jobs they launch.",
        )
        examples = [
            FunctionExample(
                sql="SELECT sensor_name, sensor_type, status, target_jobs FROM dagster.main.sensors "
                "ORDER BY sensor_name",
                description="Every sensor, its type and what it launches",
            )
        ]

    @classmethod
    def build_rows(cls, ep: Endpoint, data: dict[str, Any]) -> list[dict[str, Any]]:
        """One row per sensor."""
        rows = []
        for repo in _repo_nodes(data):
            for s in repo.get("sensors") or []:
                state = s.get("sensorState") or {}
                rows.append(
                    {
                        "sensor_name": s.get("name"),
                        **_repo_cols(repo),
                        "sensor_type": s.get("sensorType"),
                        "status": state.get("status") or s.get("defaultStatus"),
                        "default_status": s.get("defaultStatus"),
                        "min_interval_seconds": s.get("minIntervalSeconds"),
                        "target_jobs": [t["pipelineName"] for t in s.get("targets") or [] if t.get("pipelineName")],
                        "description": s.get("description"),
                        "sensor_id": s.get("id"),
                    }
                )
        return rows


# ----------------------------------------------------------------------------- resources


@init_single_worker
@bind_fixed_schema
class ResourcesScan(SnapshotScan):
    """Rows of ``resources``. Configured values are never returned: only keys and env-var names."""

    QUERY = _repositories_query(
        "allTopLevelResourceDetails { name resourceType description configuredValues { key type value } "
        "assetKeysUsing { path } jobsOpsUsing { jobName } schedulesUsing sensorsUsing }"
    )
    FIXED_SCHEMA: ClassVar[pa.Schema] = pa.schema(
        [
            column(
                "resource_name", pa.string(), "Resource key, as bound in Definitions(resources=...).", nullable=False
            ),
            *_repo_fields(),
            column("resource_type", pa.string(), "Python class of the resource.", nullable=False),
            column("description", pa.string(), "Resource description from code."),
            column("config_keys", STR_LIST, "Configured field names. Values are never returned (they can be secrets)."),
            column("env_vars", STR_LIST, "Environment variables the resource's config reads (names only)."),
            column("asset_keys_using", STR_LIST, "Assets that require the resource, as 'a/b' keys."),
            column("jobs_using", STR_LIST, "Jobs whose ops require the resource."),
            column("schedules_using", STR_LIST, "Schedules that require the resource."),
            column("sensors_using", STR_LIST, "Sensors that require the resource."),
        ]
    )

    class Meta:
        """Scan metadata."""

        name = "resources_scan"
        description = "Top-level resources and who uses them; configured values are masked"
        required_secrets = scan_meta_secrets()
        tags = object_tags(
            title="Dagster Resources",
            category="definitions",
            keywords="resource, configurable resource, environment variable, dependency, config",
            doc_llm=(
                "One row per top-level resource: `resource_name`, `resource_type`, `config_keys` (field names "
                "only: configured values are never returned because they can be credentials), `env_vars` (names of "
                "environment variables the config reads) and the assets, jobs, schedules and sensors that use it."
            ),
            doc_md="Top-level resources and their consumers. Config values are masked: only keys and env-var names.",
        )
        examples = [
            FunctionExample(
                sql="SELECT resource_name, resource_type, len(jobs_using) AS jobs FROM dagster.main.resources "
                "ORDER BY resource_name",
                description="Resources and how many jobs depend on each",
            )
        ]

    @classmethod
    def build_rows(cls, ep: Endpoint, data: dict[str, Any]) -> list[dict[str, Any]]:
        """One row per resource; configured values are dropped, env-var names kept."""
        rows = []
        for repo in _repo_nodes(data):
            for r in repo.get("allTopLevelResourceDetails") or []:
                configured = r.get("configuredValues") or []
                rows.append(
                    {
                        "resource_name": r.get("name"),
                        **_repo_cols(repo),
                        "resource_type": r.get("resourceType"),
                        "description": r.get("description"),
                        "config_keys": [c.get("key") for c in configured],
                        "env_vars": [c.get("value") for c in configured if c.get("type") == "ENV_VAR"],
                        "asset_keys_using": ["/".join(a.get("path") or []) for a in r.get("assetKeysUsing") or []],
                        "jobs_using": sorted(
                            {j.get("jobName") for j in r.get("jobsOpsUsing") or [] if j.get("jobName")}
                        ),
                        "schedules_using": list(r.get("schedulesUsing") or []),
                        "sensors_using": list(r.get("sensorsUsing") or []),
                    }
                )
        return rows


# ----------------------------------------------------------------------------- concurrency_keys


@init_single_worker
@bind_fixed_schema
class ConcurrencyKeysScan(SnapshotScan):
    """Rows of ``concurrency_keys``."""

    QUERY = """
query Concurrency {
  instance {
    concurrencyLimits {
      concurrencyKey slotCount limit usingDefaultLimit activeSlotCount pendingStepCount assignedStepCount activeRunIds
    }
  }
}
"""
    FIXED_SCHEMA: ClassVar[pa.Schema] = pa.schema(
        [
            column("concurrency_key", pa.string(), "Concurrency key (pool) name.", nullable=False),
            column("slot_count", pa.int32(), "Number of slots configured for the key.", nullable=False),
            column("concurrency_limit", pa.int32(), "Configured maximum number of concurrent steps; NULL when unset."),
            column("using_default_limit", pa.bool_(), "True when the instance default applies."),
            column(
                "active_slot_count", pa.int32(), "Number of slots currently claimed by running steps.", nullable=False
            ),
            column("pending_step_count", pa.int32(), "Number of steps waiting for a slot.", nullable=False),
            column("assigned_step_count", pa.int32(), "Number of steps assigned a slot.", nullable=False),
            column("active_run_ids", STR_LIST, "Runs holding a slot."),
        ]
    )

    class Meta:
        """Scan metadata."""

        name = "concurrency_keys_scan"
        description = "Concurrency keys (pools) on the instance, with slot usage"
        required_secrets = scan_meta_secrets()
        tags = object_tags(
            title="Dagster Concurrency Keys",
            category="instance",
            keywords="concurrency, pool, slot, limit, queue, op concurrency",
            doc_llm=(
                "One row per op-concurrency key (pool): `slot_count`, `concurrency_limit`, how many slots are "
                "active, and how many steps are pending or assigned. Empty when no pools are configured."
            ),
            doc_md="Op-concurrency keys (pools) and live slot usage.",
        )
        examples = [
            FunctionExample(
                sql="SELECT concurrency_key, slot_count, pending_step_count FROM dagster.main.concurrency_keys "
                "WHERE pending_step_count > 0 ORDER BY pending_step_count DESC",
                description="Pools with steps waiting for a slot",
            )
        ]

    @classmethod
    def build_rows(cls, ep: Endpoint, data: dict[str, Any]) -> list[dict[str, Any]]:
        """One row per concurrency key."""
        return [
            {
                "concurrency_key": c.get("concurrencyKey"),
                "slot_count": c.get("slotCount"),
                "concurrency_limit": c.get("limit"),
                "using_default_limit": c.get("usingDefaultLimit"),
                "active_slot_count": c.get("activeSlotCount"),
                "pending_step_count": c.get("pendingStepCount"),
                "assigned_step_count": c.get("assignedStepCount"),
                "active_run_ids": c.get("activeRunIds") or [],
            }
            for c in (data.get("instance") or {}).get("concurrencyLimits") or []
        ]


# ----------------------------------------------------------------------------- deployments (Dagster+)


@init_single_worker
@bind_fixed_schema
class DeploymentsScan(SnapshotScan):
    """Rows of ``deployments`` (Dagster+ only)."""

    QUERY = "query Deployments { fullDeployments { deploymentName deploymentId deploymentType } }"
    FIXED_SCHEMA: ClassVar[pa.Schema] = pa.schema(
        [
            column(
                "deployment_name", pa.string(), "Deployment name, e.g. 'prod' or a branch deployment.", nullable=False
            ),
            column("deployment_id", pa.int64(), "Dagster+ numeric deployment identifier (not a count)."),
            column("deployment_type", pa.string(), "PRODUCTION, DEV or BRANCH."),
        ]
    )

    class Meta:
        """Scan metadata."""

        name = "deployments_scan"
        description = "Deployments in the Dagster+ organization (Dagster+ only)"
        required_secrets = scan_meta_secrets()
        tags = object_tags(
            title="Dagster+ Deployments",
            category="instance",
            keywords="dagster plus, dagster cloud, deployment, branch deployment, organization",
            doc_llm=(
                "Dagster+ only: one row per deployment visible to the token (`deployment_name`, `deployment_id`, "
                "`deployment_type` PRODUCTION/DEV/BRANCH). Open-source dagster-webserver has no deployments and "
                "the query fails with an explanatory error. Point the dagster secret's DEPLOYMENT at a name from "
                "this table to query that deployment."
            ),
            doc_md="Dagster+ deployments (production, dev and branch). Not available on open-source Dagster.",
        )
        examples = [
            FunctionExample(
                sql="SELECT deployment_type, count(*) AS deployments FROM dagster.main.deployments "
                "GROUP BY deployment_type ORDER BY deployment_type",
                description="How many deployments of each type the organization has",
            )
        ]

    @classmethod
    def build_rows(cls, ep: Endpoint, data: dict[str, Any]) -> list[dict[str, Any]]:
        """One row per deployment."""
        return [
            {
                "deployment_name": d.get("deploymentName"),
                "deployment_id": d.get("deploymentId"),
                "deployment_type": d.get("deploymentType"),
            }
            for d in data.get("fullDeployments") or []
        ]

    @classmethod
    def process(cls, params: Any, state: None, out: Any) -> None:
        """Explain the Dagster+-only failure instead of surfacing a schema error."""
        try:
            super().process(params, state, out)
        except DagsterError as err:
            if "fullDeployments" in str(err):
                raise DagsterError(
                    "dagster.main.deployments needs Dagster+: this server has no deployments "
                    "(open-source dagster-webserver)"
                ) from err
            raise
