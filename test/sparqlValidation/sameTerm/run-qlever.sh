#!/usr/bin/env bash
# Build a QLever index for `data.ttl`, start a QLever server on it, and run the
# `sameTerm` validation suite against that server (see `README.md`).
#
# Usage:
#   ./run-qlever.sh [<directory with `qlever-index` and `qlever-server`>]
#
# The default directory is the `build` directory of this repository.
# Environment variables:
#   PORT        The port of the server (default: 7099).
#   OUTPUT_DIR  Passed on to `run.sh`.
set -euo pipefail

cd "$(dirname "$0")"
binDir="$(realpath "${1:-../../../build}")"
port="${PORT:-7099}"
indexDir="$(mktemp -d)"
trap 'kill "${serverPid:-}" 2>/dev/null || true; rm -rf "$indexDir"' EXIT

# `http://example.org/enc/1` (the object of `:encodedIri`) is encoded in its
# `Id`, so that the suite also covers encoded IRIs.
"$binDir/qlever-index" -i "$indexDir/sameterm" -f data.ttl -F ttl \
  --encode-as-id "http://example.org/enc/" > "$indexDir/index.log" 2>&1 ||
  { cat "$indexDir/index.log"; exit 1; }
"$binDir/qlever-server" -i "$indexDir/sameterm" -p "$port" -n \
  > "$indexDir/server.log" 2>&1 &
serverPid=$!

url="http://localhost:$port"
for _ in $(seq 1 100); do
  curl -sf "$url" --data-urlencode "query=ASK {}" > /dev/null && break
  kill -0 "$serverPid" 2>/dev/null || { cat "$indexDir/server.log"; exit 1; }
  sleep 0.1
done
./run.sh endpoint "$url"
