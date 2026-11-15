"""Shared building blocks: the GraphQL-backed scan base class and the catalog-table builder."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, ClassVar

import pyarrow as pa
from vgi.catalog import Table
from vgi.catalog.constraints import constraint_record
from vgi.table_filter_pushdown import ComparisonOp, ConstantFilter, InFilter
from vgi.table_function import (
    OutputCollector,
    ProcessParams,
    TableFunctionGenerator,
)

from .client import SECRET_REQUIREMENT, Endpoint, endpoint, graphql


@dataclass(slots=True, frozen=True, kw_only=True)
class NoArgs:
    """Catalog tables take no arguments."""


def rows_to_batch(rows: list[dict[str, Any]], schema: pa.Schema) -> pa.RecordBatch:
    """Build a batch of ``schema`` (the projected output schema) from row dicts."""
    columns = {f.name: [row.get(f.name) for row in rows] for f in schema}
    return pa.RecordBatch.from_pydict(columns, schema=schema)


class SnapshotScan(TableFunctionGenerator[NoArgs, None]):
    """A table read with one GraphQL query: emit every row in one batch, then finish.

    Subclasses set ``QUERY`` and ``FIXED_SCHEMA`` and implement ``build_rows``;
    ``variables`` may translate pushed-down filters into query arguments.
    """

    QUERY: ClassVar[str]
    FIXED_SCHEMA: ClassVar[pa.Schema]

    @classmethod
    def variables(cls, params: ProcessParams[NoArgs]) -> dict[str, Any]:
        """GraphQL variables for this scan (default: none)."""
        return {}

    @classmethod
    def build_rows(cls, ep: Endpoint, data: dict[str, Any]) -> list[dict[str, Any]]:
        """Turn the GraphQL ``data`` into row dicts keyed by column name."""
        raise NotImplementedError

    @classmethod
    def process(cls, params: ProcessParams[NoArgs], state: None, out: OutputCollector) -> None:
        """Run the query once and emit every row."""
        ep = endpoint(params.secrets)
        data = graphql(ep, cls.QUERY, cls.variables(params))
        out.emit(rows_to_batch(cls.build_rows(ep, data), params.output_schema))
        out.finish()


def scan_meta_secrets() -> list[Any]:
    """Return the ``Meta.required_secrets`` list every scan declares."""
    return list(SECRET_REQUIREMENT)


def catalog_table(
    name: str,
    function: type[TableFunctionGenerator[Any, Any]],
    *,
    comment: str,
    tags: dict[str, str],
    primary_key: list[str] | None = None,
    schema_path: list[str] | None = None,
) -> Table:
    """Build a catalog table over ``function`` with NOT NULL records for its non-nullable columns.

    The constraints are ENFORCED/ALL_ROWS: every scan fills those columns from
    GraphQL fields that the Dagster schema declares non-null.
    """
    path = schema_path or ["main"]
    fields: pa.Schema = function.FIXED_SCHEMA  # type: ignore[attr-defined]
    constraints = [
        constraint_record(
            path, name, f"{f.name}_not_null", "NOT_NULL", [[f.name]], enforcement="ENFORCED", validation="ALL_ROWS"
        )
        for f in fields
        if not f.nullable
    ]
    if primary_key:
        constraints.append(
            constraint_record(
                path,
                name,
                "pk",
                "PRIMARY_KEY",
                [[c] for c in primary_key],
                enforcement="ENFORCED",
                validation="ALL_ROWS",
            )
        )
    comments = {
        f.name: (f.metadata or {}).get(b"comment", b"").decode() for f in fields if (f.metadata or {}).get(b"comment")
    }
    return Table(
        name=name,
        function=function,
        comment=comment,
        tags=tags,
        constraints=tuple(constraints),
        column_comments=comments,
    )


# ----------------------------------------------------------------------------- pushdown helpers


def _scalar(value: Any) -> Any:
    return value.as_py() if hasattr(value, "as_py") else value


def eq_values(filters: Any, column: str) -> list[Any] | None:
    """Values a column is constrained to by top-level ``=`` / ``IN`` filters, else None.

    Several such filters on one column intersect; an empty result means no row matches.
    """
    found: set[Any] | None = None
    for f in filters.filters if filters else ():
        if f.column_name != column:
            continue
        if isinstance(f, ConstantFilter) and f.op is ComparisonOp.EQ:
            values = {_scalar(f.value)}
        elif isinstance(f, InFilter):
            values = {v for v in f.values.to_pylist() if v is not None}
        else:
            continue
        found = values if found is None else found & values
    return None if found is None else sorted(found, key=str)


def range_bounds(filters: Any, column: str) -> tuple[Any, Any]:
    """Loosest-safe ``(lower, upper)`` bounds from top-level comparisons on ``column``.

    Both bounds are inclusive-or-looser; the SDK re-applies the exact predicates.
    """
    lower: Any = None
    upper: Any = None
    for f in filters.filters if filters else ():
        if f.column_name != column or not isinstance(f, ConstantFilter):
            continue
        value = _scalar(f.value)
        if value is None:
            continue
        if f.op in (ComparisonOp.GT, ComparisonOp.GE, ComparisonOp.EQ):
            lower = value if lower is None else max(lower, value)
        if f.op in (ComparisonOp.LT, ComparisonOp.LE, ComparisonOp.EQ):
            upper = value if upper is None else min(upper, value)
    return lower, upper


def dumps(value: Any) -> str:
    """Stable JSON for cursor state."""
    return json.dumps(value, sort_keys=True, default=str)
