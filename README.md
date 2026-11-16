<p align="center">
  <img src="https://query.farm/media-kit/products/vgi/vgi-logo.png" alt="Vector Gateway Interface (VGI)" width="320">
</p>

# Query Dagster Assets, Runs And Checks In DuckDB

> **vgi-dagster** · a [Query.Farm](https://query.farm) VGI worker

[![vgi worker: query.farm](https://query.farm/media-kit/shields/vgi-worker.svg)](https://query.farm)

[Dagster](https://dagster.io) as SQL. This worker reads the Dagster GraphQL API of an
open-source `dagster-webserver` (or `dagster dev`) or a Dagster+ deployment and exposes it
as DuckDB tables: the asset graph, materialization history with metadata, asset checks,
partition status, runs and their steps, and the jobs, schedules, sensors and resources
behind them. It is read-only. It never launches, cancels or edits anything.

```sql
SELECT job_name, count(*) FILTER (WHERE status = 'FAILURE') AS failures
FROM dagster.main.runs
WHERE created_at >= TIMESTAMPTZ '2026-01-01'
GROUP BY job_name ORDER BY failures DESC;
```

## Install

Python 3.14+ and [uv](https://docs.astral.sh/uv/). The worker is not on PyPI; run it from a
release tag:

```bash
uvx --from git+https://github.com/Query-farm/vgi-dagster@v0.1.2 vgi-dagster-worker --help
```

Or from a checkout: `make build`, then use `/abs/path/to/vgi-dagster/.venv/bin/vgi-dagster-worker`.

**DuckDB 2.0:**

```sql
SET allow_extension_repositories = 'allowed';
CREATE EXTENSION REPOSITORY IF NOT EXISTS query_farm WITH PREFIX 'https://duckdb-extensions.query.farm';
INSTALL vgi FROM query_farm;
LOAD vgi FROM query_farm;
ATTACH 'dagster' (TYPE vgi, LOCATION 'uvx --from git+https://github.com/Query-farm/vgi-dagster@v0.1.2 vgi-dagster-worker');
```

**Haybarn 2.0** (`uvx haybarn-cli`): `INSTALL vgi FROM community; LOAD vgi;` then the same `ATTACH`.

## Connect to Dagster

`ATTACH` registers a `dagster` secret type. Create one secret after attaching:

```sql
-- open-source dagster-webserver / dagster dev (usually no auth)
CREATE SECRET dagster_local (TYPE dagster, BASE_URL 'http://localhost:3000');

-- Dagster+ : https://<organization>.dagster.cloud/<deployment>/graphql
CREATE SECRET dagster_plus (
    TYPE dagster, ORGANIZATION 'acme', DEPLOYMENT 'prod', API_TOKEN 'user:...'
);
```

| Field | Meaning |
|---|---|
| `base_url` | Root URL of the web server. GraphQL is at `<base_url>/graphql`. Wins over `organization`. Use it for self-hosted Dagster, a proxy, or Dagster+ under a custom URL. |
| `organization` | Dagster+ organization slug. |
| `deployment` | Dagster+ deployment (default `prod`; branch deployments work). |
| `api_token` | Dagster+ user or agent token, sent as `Dagster-Cloud-Api-Token`. Redacted in `duckdb_secrets()`. |

Keep one `dagster` secret per session; switch instances with `DROP SECRET` / `CREATE SECRET`.
The token is read from the secret on every request and is never written to logs or to the
table-function state that can travel over HTTP.

## Tables and functions

Schema `main` (Dagster-native):

| Object | What it is | Pushed down to Dagster |
|---|---|---|
| `assets` | Every software-defined asset: group, kinds, lineage, owners, tags, partition counts, latest materialization | `asset_key` =, IN |
| `asset_materializations` | Materialization history with `metadata` JSON and tags | `asset_key` =, IN; `materialized_at` ranges; `partition` =, IN |
| `asset_checks` | Asset checks and the result of each one's latest execution | `asset_key` =, IN |
| `asset_check_executions` | Every check execution with severity, pass/fail and metadata | `asset_key`, `check_name` =, IN |
| `asset_partitions` | One row per partition: MATERIALIZED, FAILED, MATERIALIZING or MISSING | `asset_key` =, IN |
| `runs` | Run history, newest first, with tags MAP, step counts, trigger and UI link | `run_id` =, IN; `status` =, IN; `job_name` =; `created_at`, `updated_at` ranges |
| `run(run_id)` | One run by id | the id |
| `run_step_stats(run_id)` | Per-step status, timing and attempts for one run | the id |
| `run_events(run_id)` | A run's event log, with root-cause error class and message for failures | the id |
| `jobs` | Jobs with tags and the schedules and sensors that launch them | |
| `schedules` | Cron, timezone, live status, next tick | |
| `sensors` | Type, live status, interval, target jobs | |
| `resources` | Top-level resources and their consumers. **Config values are never returned**, only keys and env-var names | |
| `code_locations` | Load status, Dagster versions and load errors | |
| `repositories` | Definitions objects and what they contain | |
| `concurrency_keys` | Op-concurrency pools and slot usage | |
| `deployments` | Dagster+ deployments (Dagster+ only; errors clearly on open source) | |

Schema `unified` (shared by every Query.Farm orchestrator worker, so `UNION ALL` works across
Airflow, Prefect, Temporal, ...):

| Table | Columns |
|---|---|
| `unified.runs` | orchestrator, run_id, pipeline, status, raw_status, trigger, queued_at, started_at, ended_at, duration_seconds, url |
| `unified.task_runs` | orchestrator, run_id, task_run_id, task_name, status, raw_status, attempt, started_at, ended_at, duration_seconds, url |

`status` is one of queued, running, success, failed, cancelled, skipped; `raw_status` is Dagster's
own value. A Dagster task run is a step (`task_run_id` = `<run_id>:<step_key>`). `trigger` comes
from Dagster's system tags: schedule, sensor, event (automation conditions), manual (a UI user
on Dagster+), other (backfills), or NULL when no tag says. On `unified.runs`, filters on
`run_id`, `status`, `raw_status`, `pipeline` and `queued_at` push down; on `unified.task_runs`,
`run_id` does.

Asset keys are strings with path parts joined by `/` (`analytics/orders`); `assets.asset_key_path`
has the parts. Timestamps are `TIMESTAMPTZ`. Filters the API cannot express are applied by the
worker after fetching, so every predicate is always exact.

### Examples

```sql
-- Partitions still to backfill
SELECT partition_key FROM dagster.main.asset_partitions
WHERE asset_key = 'daily_events' AND status <> 'MATERIALIZED' ORDER BY partition_index;

-- Row counts recorded by an asset's materializations
SELECT materialized_at, CAST(metadata->>'row_count' AS BIGINT) AS row_count
FROM dagster.main.asset_materializations
WHERE asset_key = 'raw_orders' AND materialized_at >= TIMESTAMPTZ '2026-01-01'
ORDER BY materialized_at;

-- Checks failing right now
SELECT asset_key, check_name, latest_severity FROM dagster.main.asset_checks
WHERE latest_status = 'FAILED';

-- Why a run failed
SELECT step_key, error_class, error_message FROM dagster.main.run_events('<run id>')
WHERE event_type = 'STEP_FAILURE';

-- Steps that fail most
SELECT task_name, count(*) FILTER (WHERE status = 'failed') AS failures
FROM dagster.unified.task_runs GROUP BY task_name ORDER BY failures DESC;
```

## How it reads Dagster

* One GraphQL request per definition table. Run scans page 100 runs per request
  (`unified.task_runs`: 25, with step stats inline); event logs 500 events; materializations
  and check executions 200 per asset/check.
* Every cursor (last run id, event-log cursor, per-asset position) lives in the
  table-function state, so a long scan resumes on any worker process over HTTP.
* 429 and 5xx responses are retried with exponential backoff that honours `Retry-After`;
  every request has a 60 s timeout. GraphQL errors and `...OrError` failures are raised with
  Dagster's message.

## Development

```bash
make check          # ruff, mypy, pydoclint, unit tests, SQL tests over stdio/launch/http, vgi-lint
make test-unit      # pytest
make test-sql       # SQL tests through haybarn-unittest against recorded GraphQL
make test-live      # the same SQL tests against a live, freshly seeded dagster-webserver
make fixture-record # re-record test/fixtures/graphql from a freshly seeded dagster-webserver
```

**Fixtures.** `scripts/dagster_fixture.sh up` builds a pinned venv (`dagster==1.13.30`,
`dagster-webserver`) under `.dagster-fixture/`, creates a throwaway `DAGSTER_HOME`, executes the
runs in `test/dagster_project/seed.py` against `test/dagster_project/definitions.py` (plain
assets with a dependency, a daily-partitioned asset with a failed partition, a failing and a
passing asset check, an op job with a failing run, a schedule, a sensor, a resource; fixed run
ids), and starts `dagster-webserver` on a free port. No Docker.

`test/mock_api/server.py` replays the recordings in `test/fixtures/graphql/` (keyed by
normalized query and variables), so `make test-sql` and CI run offline. Its auth mode
(`MOCK_API_TOKEN`) stands in for Dagster+: it requires the token header and answers
`fullDeployments`. `test/sql/dagster_live.test` runs only when `DAGSTER_LIVE_URL` is set
(`make test-live`); everywhere else it is skipped with the expected reason
`require-env DAGSTER_LIVE_URL`.

The metadata check runs with `--no-execute` locally and `execute: false` in CI: every example
queries a Dagster instance through a `dagster` secret, which CI does not have.

## Support

Open an issue at <https://github.com/Query-farm/vgi-dagster/issues>.

## Authorship & License

Written by [Query.Farm](https://query.farm). Released under the
[BSD Zero Clause License](LICENSE) (0BSD).

Copyright 2026 Query Farm LLC - https://query.farm
