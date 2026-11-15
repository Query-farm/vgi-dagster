"""Scalar ``unified_run_status(status)``: Dagster RunStatus to the shared orchestrator vocabulary.

Network-free, so it carries the worker's executable examples, and useful on its own
when comparing raw Dagster statuses (from ``dagster.main.runs`` or an export) with
other orchestrators.
"""

from __future__ import annotations

import json
from typing import Annotated

import pyarrow as pa
from vgi.arguments import Param, Returns
from vgi.metadata import FunctionExample
from vgi.scalar_function import ScalarFunction

from .meta import object_tags
from .runs import RUN_STATUS


class UnifiedRunStatus(ScalarFunction):
    """``unified_run_status(status)``."""

    class Meta:
        """Function metadata."""

        name = "unified_run_status"
        description = "Map a Dagster run status (e.g. 'FAILURE') to queued/running/success/failed/cancelled"
        tags = {
            **object_tags(
                title="Normalize Dagster Run Status",
                category="runs",
                keywords="run status, normalize, unified status, failure, canceled, cross orchestrator",
                doc_llm=(
                    "Map a Dagster `RunStatus` string to the vocabulary of the `unified` schema: QUEUED, "
                    "NOT_STARTED, MANAGED and STARTING become 'queued'; STARTED, SUSPENDED and CANCELING "
                    "become 'running'; SUCCESS is 'success', FAILURE is 'failed', CANCELED is 'cancelled'. "
                    "Case-insensitive. Unknown values and NULL give NULL. This is the mapping "
                    "`dagster.unified.runs.status` uses; it needs no Dagster connection."
                ),
                doc_md=(
                    "## unified_run_status(status)\n\n"
                    "Dagster `RunStatus` to queued / running / success / failed / cancelled.\n\n"
                    "Unknown values and NULL give NULL."
                ),
            ),
            "vgi.executable_examples": json.dumps(
                [
                    {
                        "description": "Normalize a few Dagster statuses",
                        "sql": "SELECT s, dagster.main.unified_run_status(s) AS status "
                        "FROM (VALUES ('FAILURE'), ('CANCELED'), ('STARTING'), ('success')) t(s) ORDER BY s",
                        "expected_result": [
                            ["CANCELED", "cancelled"],
                            ["FAILURE", "failed"],
                            ["STARTING", "queued"],
                            ["success", "success"],
                        ],
                    }
                ]
            ),
        }
        examples = [
            FunctionExample(
                sql="SELECT job_name, dagster.main.unified_run_status(status) AS status, count(*) AS runs "
                "FROM dagster.main.runs GROUP BY ALL ORDER BY job_name, status",
                description="Run counts per job in the cross-orchestrator status vocabulary",
            )
        ]

    @classmethod
    def compute(
        cls,
        status: Annotated[pa.StringArray, Param(doc="Dagster run status such as 'FAILURE' or 'STARTED'.")],
    ) -> Annotated[pa.StringArray, Returns()]:
        """Map each status; NULL and unknown values give NULL."""
        values = [None if s is None else RUN_STATUS.get(str(s).upper()) for s in status.to_pylist()]
        return pa.array(values, type=pa.string())
