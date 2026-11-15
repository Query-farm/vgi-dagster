"""Seed a throwaway DAGSTER_HOME with runs that have fixed run ids.

Run by ``scripts/dagster_fixture.sh`` with the fixture venv's Python. Fixed ids let
the SQL tests (and the recorded GraphQL fixtures) name runs; timestamps still vary
from seed to seed, so the tests never assert on them.

    DAGSTER_HOME=/tmp/home python seed.py definitions.py
"""

import sys

import dagster as dg
from dagster._core.definitions.reconstruct import ReconstructableRepository
from dagster._core.execution.api import execute_run

ASSET_JOB = "__ASSET_JOB"

# (run id, job, asset selection, run config, tags) in creation order; the last is newest.
RUNS = [
    ("00000000-0000-4000-8000-000000000001", ASSET_JOB, ["raw_orders", "customer_totals"], {}, {}),
    ("00000000-0000-4000-8000-000000000002", "etl_job", None, {}, {"team": "data"}),
    ("00000000-0000-4000-8000-000000000003", "etl_job", None, {"ops": {"extract": {"config": {"fail": True}}}}, {}),
    ("00000000-0000-4000-8000-000000000004", ASSET_JOB, ["daily_events"], {}, {"dagster/partition": "2026-01-01"}),
    ("00000000-0000-4000-8000-000000000005", ASSET_JOB, ["daily_events"], {}, {"dagster/partition": "2026-01-02"}),
    ("00000000-0000-4000-8000-000000000006", ASSET_JOB, ["daily_events"], {}, {"dagster/partition": "2026-01-03"}),
    (
        "00000000-0000-4000-8000-000000000007",
        "raw_assets_job",
        None,
        {},
        {"dagster/schedule_name": "etl_job_schedule"},
    ),
]


def main(path: str) -> None:
    """Execute every seed run in order against the instance in DAGSTER_HOME."""
    instance = dg.DagsterInstance.get()
    repo = ReconstructableRepository.for_file(path, "defs")
    for run_id, job_name, assets, config, tags in RUNS:
        recon_job = repo.get_reconstructable_job(job_name)
        selection = frozenset(dg.AssetKey(a) for a in assets) if assets else None
        if selection:
            recon_job = recon_job.get_subset(asset_selection=selection)
        job_def = recon_job.get_definition()
        run = instance.create_run_for_job(
            job_def=job_def, run_id=run_id, run_config=config, tags=tags, asset_selection=selection
        )
        result = execute_run(recon_job, run, instance)
        print(f"{run_id} {job_name} {'SUCCESS' if result.success else 'FAILURE'}", file=sys.stderr)


if __name__ == "__main__":
    main(sys.argv[1])
