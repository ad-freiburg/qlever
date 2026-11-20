#!/usr/bin/env bash
# Run the queries of the `sameTerm` validation suite (see `README.md`) with
# Apache Jena or against a SPARQL endpoint (for example QLever), and compare
# the results with the expected results in `expected/`.
#
# Usage:
#   ./run.sh sparql [<path to Jena's `sparql` or other compatible tool>]
#   ./run.sh endpoint <URL of the SPARQL endpoint>
#
# Environment variables:
#   OUTPUT_DIR  Where the actual results are written (default: a temporary
#               directory).
#   UPDATE=1    Write the actual results to `expected/` instead of comparing.
set -euo pipefail

cd "$(dirname "$0")"
engine="${1:-}"
case "$engine" in
  sparql)
    sparql="${2:-sparql}"
    runQuery() {
      "$sparql" --data data.ttl --query "$1" --results=TSV 2>/dev/null
    }
    ;;
  endpoint)
    url="${2:?Usage: $0 endpoint <URL>}"
    runQuery() {
      # No `-f`, so that the error message of a failing query ends up in the
      # actual result (and hence in the diff).
      curl -s "$url" -H "Accept: text/tab-separated-values" \
        --data-urlencode "query@$1"
    }
    ;;
  *)
    sed -n '5,14p' "$0"
    exit 2
    ;;
esac

outputDir="${OUTPUT_DIR:-$(mktemp -d)}"
mkdir -p "$outputDir"
numFailed=0
for query in queries/*.rq; do
  name="$(basename "$query" .rq)"
  actual="$outputDir/$name.tsv"
  # Remove carriage returns and a trailing empty line, which some engines add.
  # A failing query is reported as a difference to the expected result.
  { runQuery "$query" || true; } | tr -d '\r' | sed -e '${/^$/d;}' > "$actual"
  if [[ "${UPDATE:-0}" == 1 ]]; then
    cp "$actual" "expected/$name.tsv"
    echo "UPDATED $name"
  elif diff -u "expected/$name.tsv" "$actual" > "$outputDir/$name.diff"; then
    echo "PASS    $name"
  else
    echo "FAIL    $name"
    sed 's/^/        /' "$outputDir/$name.diff"
    numFailed=$((numFailed + 1))
  fi
done
echo "Actual results are in $outputDir"
exit $((numFailed > 0))
