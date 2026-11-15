"""Dagster GraphQL client: endpoint resolution from a DuckDB secret, retries, errors.

Every table and function in this worker reads one ``dagster`` DuckDB secret::

    -- open-source dagster-webserver / `dagster dev` (usually no auth)
    CREATE SECRET dagster_local (TYPE dagster, BASE_URL 'http://localhost:3000');

    -- Dagster+ (the token is sent as the Dagster-Cloud-Api-Token header)
    CREATE SECRET dagster_plus (TYPE dagster, ORGANIZATION 'acme', DEPLOYMENT 'prod', API_TOKEN 'user:...');

``BASE_URL`` wins when set: GraphQL is served at ``<base_url>/graphql``. Without it,
``ORGANIZATION`` selects Dagster+ at ``https://<organization>.dagster.cloud/<deployment>``
(``DEPLOYMENT`` defaults to ``prod``). The token is read on every call and never kept in
table-function state, because state can travel back to DuckDB over HTTP.
"""

from __future__ import annotations

import json
import logging
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

import pyarrow as pa
from vgi.arguments import SecretLookupEntry
from vgi.catalog import SecretTypeSpec

log = logging.getLogger(__name__)

SECRET_TYPE = "dagster"
TIMEOUT_SECONDS = 60.0
MAX_ATTEMPTS = 5
MAX_BACKOFF_SECONDS = 60.0
USER_AGENT = "vgi-dagster-worker/0.1"
DEFAULT_DEPLOYMENT = "prod"
CLOUD_HOST = "dagster.cloud"
DOC_URL = "https://github.com/Query-farm/vgi-dagster#connect-to-dagster"


def _doc_field(name: str, doc: str, *, redact: bool = False) -> pa.Field[Any]:
    metadata = {"vgi_doc": doc}
    if redact:
        metadata["redact"] = "true"
    return pa.field(name, pa.string(), metadata=metadata)


DAGSTER_SECRET_TYPE = SecretTypeSpec(
    name=SECRET_TYPE,
    description="Connection to a Dagster instance: open-source dagster-webserver (BASE_URL) or Dagster+.",
    schema=pa.schema(
        [
            _doc_field(
                "api_token",
                "Dagster+ user or agent token, sent as the Dagster-Cloud-Api-Token header. "
                "Leave unset for an open-source dagster-webserver without auth.",
                redact=True,
            ),
            _doc_field("organization", "Dagster+ organization slug: the <org> in https://<org>.dagster.cloud."),
            _doc_field("deployment", "Dagster+ deployment name, e.g. 'prod' (the default) or a branch deployment."),
            _doc_field(
                "base_url",
                "Root URL of the Dagster web server, e.g. 'http://localhost:3000' for dagster dev. "
                "GraphQL is served at <base_url>/graphql. Overrides organization/deployment.",
            ),
        ],
        metadata={"vgi_doc_url": DOC_URL},
    ),
)

# Every scan asks DuckDB for the (optional) dagster secret at bind time.
SECRET_REQUIREMENT = [SecretLookupEntry(secret_type=SECRET_TYPE)]


class DagsterError(RuntimeError):
    """Dagster returned an error, or the worker is not configured to reach it."""


@dataclass(frozen=True, slots=True)
class Endpoint:
    """Where to send GraphQL, how to link to the UI, and the token (never logged)."""

    graphql_url: str
    ui_url: str
    token: str | None
    is_cloud: bool

    def run_url(self, run_id: str) -> str:
        """Link to a run in the Dagster UI."""
        return f"{self.ui_url}/runs/{run_id}"

    def asset_url(self, path: list[str]) -> str:
        """Link to an asset in the Dagster UI."""
        return f"{self.ui_url}/assets/{'/'.join(path)}"


def _field(secret: dict[str, Any], name: str) -> str | None:
    value = secret.get(name)
    if value is None:
        return None
    py = value.as_py() if hasattr(value, "as_py") else value
    text = str(py).strip() if py is not None else ""
    return text or None


def endpoint_from_secret(secret: dict[str, Any] | None) -> Endpoint:
    """Resolve the GraphQL endpoint from a ``dagster`` secret's fields.

    Args:
        secret: The resolved secret's fields (Arrow scalars or plain values), or None.

    Returns:
        The endpoint to query.

    Raises:
        DagsterError: No secret, or one with neither ``base_url`` nor ``organization``.

    """
    if not secret:
        raise DagsterError(
            "no dagster secret found. Create one first: CREATE SECRET (TYPE dagster, BASE_URL "
            "'http://localhost:3000') for open-source Dagster, or CREATE SECRET (TYPE dagster, "
            "ORGANIZATION 'my-org', DEPLOYMENT 'prod', API_TOKEN '...') for Dagster+"
        )
    token = _field(secret, "api_token")
    base_url = _field(secret, "base_url")
    organization = _field(secret, "organization")
    if base_url:
        if not base_url.startswith(("http://", "https://")):
            raise DagsterError(f"dagster secret base_url must start with http:// or https://, got {base_url!r}")
        root = base_url.rstrip("/")
        root = root.removesuffix("/graphql")
        return Endpoint(graphql_url=f"{root}/graphql", ui_url=root, token=token, is_cloud=CLOUD_HOST in root)
    if organization:
        deployment = _field(secret, "deployment") or DEFAULT_DEPLOYMENT
        root = f"https://{organization}.{CLOUD_HOST}/{deployment}"
        return Endpoint(graphql_url=f"{root}/graphql", ui_url=root, token=token, is_cloud=True)
    raise DagsterError("the dagster secret needs BASE_URL (open-source Dagster) or ORGANIZATION (Dagster+)")


def endpoint(secrets: Any) -> Endpoint:
    """Resolve the endpoint from a call's resolved secrets (``params.secrets``).

    Args:
        secrets: The call's ``ResolvedSecrets``.

    Returns:
        The endpoint to query.

    Raises:
        DagsterError: More than one ``dagster`` secret applies, or none is usable.

    """
    found = list(secrets.of_type(SECRET_TYPE)) if secrets is not None else []
    if len(found) > 1:
        raise DagsterError(
            "more than one dagster secret is defined; keep one per DuckDB session (DROP SECRET the others)"
        )
    return endpoint_from_secret(found[0] if found else None)


def _retry_delay(err: urllib.error.HTTPError, attempt: int) -> float:
    retry_after = str(err.headers.get("Retry-After") or "") if err.headers else ""
    if retry_after:
        try:
            delay = min(max(float(retry_after), 0.0), MAX_BACKOFF_SECONDS)
            return delay
        except ValueError:
            pass
    return min(0.5 * float(2**attempt), MAX_BACKOFF_SECONDS)


def graphql(ep: Endpoint, query: str, variables: dict[str, Any] | None = None) -> dict[str, Any]:
    """POST one GraphQL query and return its ``data``.

    Rate limits (429) and server errors (5xx) are retried with exponential backoff that
    honours ``Retry-After``. GraphQL ``errors`` are raised with their messages.

    Args:
        ep: The endpoint.
        query: The GraphQL document.
        variables: Query variables.

    Returns:
        The response's ``data`` object.

    Raises:
        DagsterError: HTTP failure after retries, or a GraphQL error response.

    """
    body = json.dumps({"query": query, "variables": variables or {}}).encode()
    headers = {"Content-Type": "application/json", "Accept": "application/json", "User-Agent": USER_AGENT}
    if ep.token:
        headers["Dagster-Cloud-Api-Token"] = ep.token
    for attempt in range(1, MAX_ATTEMPTS + 1):
        request = urllib.request.Request(ep.graphql_url, data=body, headers=headers, method="POST")
        try:
            with urllib.request.urlopen(request, timeout=TIMEOUT_SECONDS) as response:
                payload = json.load(response)
            break
        except urllib.error.HTTPError as err:
            retryable = err.code == 429 or err.code >= 500
            if not retryable or attempt == MAX_ATTEMPTS:
                detail = err.read(300).decode(errors="replace").strip()
                hint = " (check the dagster secret's API_TOKEN)" if err.code in (401, 403) else ""
                raise DagsterError(
                    f"Dagster GraphQL {ep.graphql_url} returned HTTP {err.code}{hint}: {detail}"
                ) from err
            delay = _retry_delay(err, attempt)
            log.info("dagster: HTTP %s, retrying in %.1fs", err.code, delay)
            time.sleep(delay)
        except (urllib.error.URLError, TimeoutError, ConnectionError) as err:
            if attempt == MAX_ATTEMPTS:
                reason = getattr(err, "reason", err)
                raise DagsterError(f"cannot reach Dagster GraphQL at {ep.graphql_url}: {reason}") from err
            time.sleep(min(0.5 * 2**attempt, MAX_BACKOFF_SECONDS))
    else:  # pragma: no cover - the loop always breaks or raises
        raise DagsterError("unreachable: retry loop ended without a response")
    errors = payload.get("errors") if isinstance(payload, dict) else None
    if errors:
        messages = "; ".join(str(e.get("message", e)) if isinstance(e, dict) else str(e) for e in errors)
        raise DagsterError(f"Dagster GraphQL error: {messages}")
    data = payload.get("data") if isinstance(payload, dict) else None
    return data or {}


def check_union(node: dict[str, Any] | None, expected: str | tuple[str, ...], context: str) -> dict[str, Any]:
    """Return a GraphQL ``...OrError`` union member, raising on its error variants.

    Args:
        node: The union value.
        expected: The success ``__typename`` (or several).
        context: Field name for the error message.

    Returns:
        The node, when it is the expected type.

    Raises:
        DagsterError: The union resolved to an error type.

    """
    node = node or {}
    names = (expected,) if isinstance(expected, str) else expected
    typename = node.get("__typename")
    if typename in names:
        return node
    raise DagsterError(f"Dagster {context} returned {typename}: {node.get('message', 'no message')}")


def epoch_seconds(value: Any) -> datetime | None:
    """Dagster float epoch seconds to an aware datetime; None stays None."""
    if value is None or value == "":
        return None
    return datetime.fromtimestamp(float(value), tz=UTC)


def epoch_millis(value: Any) -> datetime | None:
    """Dagster event timestamps (string epoch milliseconds) to an aware datetime."""
    if value is None or value == "":
        return None
    return datetime.fromtimestamp(float(value) / 1000.0, tz=UTC)


def duration(start: Any, end: Any) -> float | None:
    """Seconds between two epoch-second values, or None when either is missing."""
    if start is None or end is None:
        return None
    return max(0.0, float(end) - float(start))


def tag_map(tags: list[dict[str, Any]] | None) -> list[tuple[str, str]]:
    """``[{key, value}]`` GraphQL tags to MAP entries (last key wins)."""
    merged: dict[str, str] = {}
    for tag in tags or []:
        key = tag.get("key")
        if key is not None:
            merged[str(key)] = str(tag.get("value") or "")
    return list(merged.items())


def asset_key_str(key: dict[str, Any] | None) -> str:
    """``{path: [a, b]}`` to Dagster's display form ``a/b``."""
    return "/".join((key or {}).get("path") or [])


def asset_key_input(key: str) -> dict[str, list[str]]:
    """``a/b`` to the ``AssetKeyInput`` ``{path: [a, b]}``."""
    return {"path": [part for part in key.split("/") if part]}


# One fragment for every MetadataEntry subtype a value can be read from.
METADATA_FRAGMENT = """
fragment MetadataFields on MetadataEntry {
  __typename
  label
  ... on TextMetadataEntry { text }
  ... on IntMetadataEntry { intValue intRepr }
  ... on FloatMetadataEntry { floatValue floatRepr }
  ... on BoolMetadataEntry { boolValue }
  ... on JsonMetadataEntry { jsonString }
  ... on MarkdownMetadataEntry { mdStr }
  ... on PathMetadataEntry { path }
  ... on NotebookMetadataEntry { path }
  ... on UrlMetadataEntry { url }
  ... on TimestampMetadataEntry { timestamp }
  ... on PipelineRunMetadataEntry { runId }
  ... on AssetMetadataEntry { assetKey { path } }
  ... on JobMetadataEntry { jobName }
  ... on PoolMetadataEntry { pool }
}
"""


def _metadata_value(entry: dict[str, Any]) -> Any:
    kind = entry.get("__typename")
    if kind == "IntMetadataEntry":
        return entry.get("intValue") if entry.get("intValue") is not None else entry.get("intRepr")
    if kind == "FloatMetadataEntry":
        return entry.get("floatValue") if entry.get("floatValue") is not None else entry.get("floatRepr")
    if kind == "JsonMetadataEntry":
        try:
            return json.loads(entry.get("jsonString") or "null")
        except ValueError:
            return entry.get("jsonString")
    if kind == "AssetMetadataEntry":
        return asset_key_str(entry.get("assetKey"))
    for name in ("text", "boolValue", "mdStr", "path", "url", "timestamp", "runId", "jobName", "pool"):
        if name in entry:
            return entry[name]
    return None


def metadata_json(entries: list[dict[str, Any]] | None) -> str | None:
    """Metadata entries as one JSON object ``{label: value}``; None when there are none."""
    if not entries:
        return None
    return json.dumps({str(e.get("label")): _metadata_value(e) for e in entries}, sort_keys=True, default=str)
