# Common tasks. `make help` lists them. Requires uv (https://docs.astral.sh/uv/).
# managed-by: vgi-starters (language section below is python-specific)

HAYBARN_VERSION ?= 2.0.0rc11
# vgi-lint is deliberately not pinned: the gate tracks the current quality bar.
VGI_LINT        := uvx --prerelease=allow --from 'vgi-lint-check>=2,<3' vgi-lint lint
# Examples query a live Dagster through a `dagster` secret, so they are not executed.
LINT_ARGS       ?= --no-execute
WORKER          := $(CURDIR)/.venv/bin/vgi-dagster-worker
TOOLS           := $(CURDIR)/.tools
HAYBARN_UNITTEST := $(TOOLS)/bin/haybarn-unittest
TRANSPORT       ?= stdio
# The live-Dagster SQL file runs only under `make test-live`; everywhere else it skips.
EXPECTED_SKIP_REASONS ?= require-env DAGSTER_LIVE_URL
FIXTURE         := $(CURDIR)/scripts/dagster_fixture.sh

.DEFAULT_GOAL := help
.PHONY: help build test test-unit test-sql test-sql-all lint lint-explain lint-ai fmt check run run-http debug tools template-diff clean fixture-up fixture-down fixture-record test-live

help: ## List the targets
	@awk 'BEGIN{FS=":.*## "} /^[a-zA-Z_-]+:.*## /{printf "  %-14s %s\n",$$1,$$2}' $(MAKEFILE_LIST)


build: ## Install the worker and dev tools into .venv
	uv sync --locked

tools: ## Install the pinned SQL test runner into .tools
	@test -x $(HAYBARN_UNITTEST) || { uv venv -q $(TOOLS) && uv pip install -q -p $(TOOLS) haybarn-unittest==$(HAYBARN_VERSION); }

fmt: ## Format the code
	uv run ruff format src tests test examples examples
	uv run ruff check --fix src tests test examples examples

lint: build ## Static checks: ruff, mypy, pydoclint
	uv run ruff format --check src tests test examples examples
	uv run ruff check src tests test examples examples
	uv run mypy
	uvx pydoclint --config=pyproject.toml src

test-unit: build ## Unit tests (pytest)
	uv run pytest -q

test-sql: build tools ## SQL tests through DuckDB: make test-sql TRANSPORT=stdio|launch|http
	WORKER=$(WORKER) RECIPES_WORKER="$(CURDIR)/.venv/bin/python $(CURDIR)/examples/run.py" HAYBARN_UNITTEST=$(HAYBARN_UNITTEST) TRANSPORT=$(TRANSPORT) EXPECTED_SKIP_REASONS="$(EXPECTED_SKIP_REASONS)" PYTHON="uv run --no-project python" ci/run-integration.sh

test-sql-all: ## SQL tests over every transport
	$(MAKE) test-sql TRANSPORT=stdio
	$(MAKE) test-sql TRANSPORT=launch
	$(MAKE) test-sql TRANSPORT=http

test: test-unit test-sql ## Unit + SQL tests

lint-explain: build ## Metadata quality report (vgi-lint, settings in vgi-lint.toml). LINT_ARGS="--format agent" for agents
	$(VGI_LINT) "$(WORKER)" $(LINT_ARGS)

lint-ai: ## Opt-in L3 check: an agent runs vgi-agent-tests.yaml and docs are reviewed (needs the claude CLI)
	@$(MAKE) --no-print-directory lint-explain LINT_ARGS="--ai $(LINT_ARGS)"

check: lint test-unit test-sql-all lint-explain ## Everything CI runs. Done = this passes.

run: build ## Serve over stdio (what DuckDB launches)
	$(WORKER)

run-http: build ## Serve over HTTP on port 8000
	$(WORKER) --http --host 127.0.0.1 --port 8000

debug: build tools ## Run one query against the worker with debug logging on stderr
	@printf "LOAD vgi;\nATTACH 'dagster' (TYPE vgi, LOCATION '$(WORKER) --debug');\nSELECT schema_name, table_name FROM duckdb_tables() WHERE database_name = 'dagster' ORDER BY ALL;\n" \
	  | uvx --from haybarn-cli==$(HAYBARN_VERSION) haybarn -no-agent

fixture-up: ## Start a seeded open-source dagster-webserver (no Docker) and print its URL
	$(FIXTURE) up

fixture-down: ## Stop the dagster-webserver fixture
	$(FIXTURE) down

test-live: build tools ## SQL tests against a live seeded dagster-webserver (starts and stops it)
	@url="$$($(FIXTURE) up)" && trap '$(FIXTURE) down' EXIT && \
	  DAGSTER_LIVE_URL="$$url" $(MAKE) --no-print-directory test-sql TRANSPORT=stdio EXPECTED_SKIP_REASONS=

fixture-record: build tools ## Re-record test/fixtures/graphql from a freshly seeded dagster-webserver
	@url="$$($(FIXTURE) up)" && trap '$(FIXTURE) down' EXIT && \
	  rm -f test/fixtures/graphql/*.json && \
	  MOCK_API_RECORD_UPSTREAM="$$url" $(MAKE) --no-print-directory test-sql TRANSPORT=stdio

template-diff: ## Show upstream template changes since this project was created
	uv run --no-project python scripts/vgi_template.py template-diff --stat

clean: ## Remove build outputs and tool caches
	rm -rf .venv .tools dist build .mypy_cache .ruff_cache .pytest_cache .dagster-fixture
