"""A tiny Dagster code location used as the live fixture for vgi-dagster's SQL tests.

It has a little of everything the worker exposes: plain assets with a dependency,
a daily-partitioned asset, an asset check that fails, a job with an op that can
be told to fail, a schedule, a sensor, a resource and an asset job.
``scripts/dagster_fixture.sh`` materializes runs against it with a throwaway
``DAGSTER_HOME`` and then serves it with ``dagster-webserver``.
"""

import dagster as dg


class GreetingResource(dg.ConfigurableResource):  # type: ignore[misc]
    """A trivial configurable resource, so the resources table has a row."""

    greeting: str = "hello"


@dg.asset(group_name="raw", description="Raw orders pulled from the source system.", kinds={"python"})
def raw_orders(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    """Pretend to land 42 orders."""
    return dg.MaterializeResult(metadata={"row_count": 42, "source": "fixture"})


@dg.asset(group_name="analytics", deps=[raw_orders], description="Orders aggregated per customer.")
def customer_totals(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    """Aggregate the raw orders."""
    return dg.MaterializeResult(metadata={"row_count": 7})


@dg.asset_check(asset=customer_totals, description="Row count must exceed 100 (it never does).")
def customer_totals_has_rows(context: dg.AssetCheckExecutionContext) -> dg.AssetCheckResult:
    """A check that always fails, so check executions include a failure."""
    return dg.AssetCheckResult(passed=False, metadata={"row_count": 7}, severity=dg.AssetCheckSeverity.WARN)


@dg.asset_check(asset=raw_orders, description="Raw orders are never empty.")
def raw_orders_not_empty(context: dg.AssetCheckExecutionContext) -> dg.AssetCheckResult:
    """A check that always passes."""
    return dg.AssetCheckResult(passed=True)


daily = dg.DailyPartitionsDefinition(start_date="2026-01-01", end_date="2026-01-08")


@dg.asset(group_name="raw", partitions_def=daily, description="One file of events per day.")
def daily_events(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    """Fail on 2026-01-03 so partition status includes a failed partition."""
    if context.partition_key == "2026-01-03":
        raise RuntimeError("upstream file for 2026-01-03 is missing")
    return dg.MaterializeResult(metadata={"partition": context.partition_key})


@dg.op(config_schema={"fail": dg.Field(bool, default_value=False)})
def extract(context: dg.OpExecutionContext) -> int:
    """Return a number, or fail when configured to."""
    if context.op_config["fail"]:
        raise RuntimeError("extract was told to fail")
    return 3


@dg.op
def load(context: dg.OpExecutionContext, value: int, greeter: GreetingResource) -> None:
    """Log the value."""
    context.log.info(f"{greeter.greeting}: loaded {value}")


@dg.job(description="A two-step ETL job; config can make the first step fail.", tags={"team": "data"})
def etl_job() -> None:
    """Extract then load."""
    load(extract())


raw_assets_job = dg.define_asset_job(
    "raw_assets_job", selection=dg.AssetSelection.groups("raw") - dg.AssetSelection.assets(daily_events)
)

etl_schedule = dg.ScheduleDefinition(
    job=etl_job, cron_schedule="0 6 * * *", execution_timezone="UTC", description="Daily ETL at 06:00 UTC."
)


@dg.sensor(job=etl_job, description="Never fires; here so the sensors table has a row.")
def noop_sensor(context: dg.SensorEvaluationContext) -> dg.SkipReason:
    """Always skip."""
    return dg.SkipReason("nothing to do")


defs = dg.Definitions(
    assets=[raw_orders, customer_totals, daily_events],
    asset_checks=[customer_totals_has_rows, raw_orders_not_empty],
    jobs=[etl_job, raw_assets_job],
    schedules=[etl_schedule],
    sensors=[noop_sensor],
    resources={"greeter": GreetingResource()},
)
