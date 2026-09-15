#!/usr/bin/env bash
# Smoke test for portable binaries of `qlever-index` and `qlever-server`
# (see `build-portable-binaries.sh` and `build-portable-binaries-macos.sh`).
#
# Usage: smoke-test-portable-binaries.sh <dist-dir> <work-dir>
# (`dist-dir` contains the binaries, `work-dir` is created for the index)
set -euo pipefail
DIST_DIR="$(cd "$1" && pwd)"
SMOKE_DIR="$2"

# Smoke test: build a tiny index and answer a query whose result order
# checks that the statically linked ICU collation data works (`Apfel` must
# sort before `äpfel` before `zebra`; byte-wise ordering would sort `äpfel`
# last).
SMOKE_PORT="${SMOKE_PORT:-7777}"
rm -rf "$SMOKE_DIR" && mkdir -p "$SMOKE_DIR" && cd "$SMOKE_DIR"
printf '<a> <p> "\xc3\xa4pfel" .\n<b> <p> "zebra" .\n<c> <p> "Apfel" .\n' > smoke.nt
"$DIST_DIR/qlever-index" -i smoke -f smoke.nt -F nt
"$DIST_DIR/qlever-server" -i smoke -p "$SMOKE_PORT" &
SERVER_PID=$!
trap 'kill $SERVER_PID 2> /dev/null || true' EXIT
RESULT=$(curl -sf --retry 30 --retry-connrefused --retry-delay 1 \
    "http://localhost:$SMOKE_PORT/" \
    --data-urlencode 'query=SELECT ?s ?o WHERE { ?s <p> ?o } ORDER BY ?o' \
    -H 'Accept: text/csv' | tr -d '\r')
EXPECTED='s,o
c,Apfel
a,äpfel
b,zebra'
if [ "$RESULT" != "$EXPECTED" ]; then
    echo "ERROR: smoke test failed, got result:"
    echo "$RESULT"
    exit 1
fi

