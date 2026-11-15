#!/usr/bin/env bash
# Run a real open-source Dagster (dagster-webserver) for the live SQL tests and for
# re-recording test/fixtures/graphql. No Docker: a pinned uv venv under .dagster-fixture/.
#
#   scripts/dagster_fixture.sh up      # seed a fresh DAGSTER_HOME, start the webserver, print its URL
#   scripts/dagster_fixture.sh down    # stop it
#   scripts/dagster_fixture.sh url     # print the URL of the running webserver
#
# `up` recreates DAGSTER_HOME every time and executes the runs in
# test/dagster_project/seed.py (fixed run ids, one failing op job, one failing
# partition, a failing asset check), then serves test/dagster_project/definitions.py.
set -euo pipefail

DAGSTER_VERSION="${DAGSTER_VERSION:-1.13.30}"
REPO="$(cd "$(dirname "$0")/.." && pwd)"
DIR="$REPO/.dagster-fixture"
VENV="$DIR/venv"
HOME_DIR="$DIR/home"
DEFS="$REPO/test/dagster_project/definitions.py"

down() {
  if [ -f "$DIR/pid" ]; then
    kill "$(cat "$DIR/pid")" 2>/dev/null || true
    rm -f "$DIR/pid" "$DIR/url"
  fi
}

case "${1:-}" in
  up)
    down
    mkdir -p "$DIR"
    if [ ! -x "$VENV/bin/dagster-webserver" ] || ! "$VENV/bin/dagster" --version | grep -q "$DAGSTER_VERSION"; then
      uv venv -q -p 3.13 "$VENV"
      uv pip install -q -p "$VENV" "dagster==$DAGSTER_VERSION" "dagster-webserver==$DAGSTER_VERSION"
    fi
    rm -rf "$HOME_DIR"; mkdir -p "$HOME_DIR"
    printf 'telemetry:\n  enabled: false\n' > "$HOME_DIR/dagster.yaml"
    export DAGSTER_HOME="$HOME_DIR"
    echo "seeding runs into $HOME_DIR" >&2
    "$VENV/bin/python" "$REPO/test/dagster_project/seed.py" "$DEFS" > "$DIR/seed.log" 2>&1 \
      || { cat "$DIR/seed.log" >&2; exit 1; }
    grep -E '^0{8}-' "$DIR/seed.log" >&2
    port="${DAGSTER_FIXTURE_PORT:-$("$VENV/bin/python" -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1])')}"
    nohup "$VENV/bin/dagster-webserver" -f "$DEFS" -h 127.0.0.1 -p "$port" > "$DIR/webserver.log" 2>&1 &
    echo $! > "$DIR/pid"
    url="http://127.0.0.1:$port"
    for _ in $(seq 1 120); do
      if curl -fsS "$url/server_info" >/dev/null 2>&1; then
        echo "$url" > "$DIR/url"
        echo "$url"
        exit 0
      fi
      kill -0 "$(cat "$DIR/pid")" 2>/dev/null || { cat "$DIR/webserver.log" >&2; exit 1; }
      sleep 1
    done
    echo "dagster-webserver did not come up; see $DIR/webserver.log" >&2
    exit 1
    ;;
  down) down ;;
  url) cat "$DIR/url" ;;
  *) echo "usage: $0 up|down|url" >&2; exit 2 ;;
esac
