#!/usr/bin/env bash
function bail {
	echo "$*"
	exit 1
}

function cleanup_server {
	echo "The Server Log:"
	cat "$BINARY_DIR/server_log.txt"
	echo "The Query Log:"
	cat "$BINARY_DIR/query_log.txt"
	if [ -f "$BINARY_DIR/server_with_proxy_log.txt" ]; then
		echo "The Log of the server that uses the proxy:"
		cat "$BINARY_DIR/server_with_proxy_log.txt"
	fi
	if [ -f "$BINARY_DIR/proxy_log.txt" ]; then
		echo "The Proxy Log:"
		cat "$BINARY_DIR/proxy_log.txt"
	fi
	# Killing 0 sends the signal to all processes in the current
	# process group
	kill "$SERVER_PID"
	[ -n "${SERVER_WITH_PROXY_PID:-}" ] && kill "$SERVER_WITH_PROXY_PID" || true
	[ -n "${PROXY_PID:-}" ] && kill "$PROXY_PID" || true
}

function print_usage {
  echo "Usage: $0 [options]"
  echo "Runs QLevers end to end tests."
  echo ""
  echo "Options:"
  echo "  -i  Use index from the given directory (which must be the root directory of the working copy of QLever, not the e2e_data subdirectory)"
  echo "  -d  Directory of the QLever binaries (relative to the main directory), default: 'build'"
  echo "  -t  Build the text index with a separate explicit call to `qlever-index`"
}

REBUILD_THE_INDEX="YES"
INDEX_DIRECTORY="." #if not set, we will build the index ourselves.
BINARY_DIRECTORY="build"
BUILD_TEXT_INDEX_SEPARATELY="NO"

while getopts ":i:d:t" arg; do
  case ${arg} in
    i)
      echo "The index will not be rebuilt"
      REBUILD_THE_INDEX="NO"
      INDEX_DIRECTORY="${OPTARG}"
    ;;
    d)
      BINARY_DIRECTORY="${OPTARG}"
    ;;
    t)
      BUILD_TEXT_INDEX_SEPARATELY="YES"
    ;;
  \?) echo "Invalid option: -$OPTARG exiting" >&2
      print_usage
      exit
  ;;
  :) echo "Option -$OPTARG requires an argument" >&2
     print_usage
     exit
  ;;
  esac
done

# Fail on unset variables and any non zero return-codes
set -Eeuo pipefail

PROJECT_DIR="$(readlink -f -- "$(dirname "${BASH_SOURCE[0]}")/..")"


# Change to the project directory so we can use simple relative paths
echo "Changing to project directory: $PROJECT_DIR"
pushd "$PROJECT_DIR"
echo "relative binary dir is $BINARY_DIRECTORY"
BINARY_DIR=$(readlink -f -- $BINARY_DIRECTORY)
if [ ! -e "$BINARY_DIR" ]; then
	BINARY_DIR="$(readlink -f -- .)"
fi
echo "Binary dir is $BINARY_DIR"

# Travis CI is super cool but also uses ancient OS images and so to get
# a python that supports typing we need to install from the deadsnakes
# repository which does not override the system python
if [ -f "/usr/bin/python3.6" ]; then
	export PYTHON_BINARY="/usr/bin/python3.6"
else
	export PYTHON_BINARY=`which python3`
fi
export PYTHON_BINARY=`which python3`

INDEX_DIR="$PROJECT_DIR/$INDEX_DIRECTORY/e2e_data"
INPUT_DIR="$PROJECT_DIR/e2e_data/scientist-collection"
ZIPPED_INPUT="$PROJECT_DIR/e2e/scientist-collection.zip"
INPUT_PREFIX="scientists"
INPUT="$INPUT_DIR/$INPUT_PREFIX"

mkdir -p "$INDEX_DIR"
INDEX_PREFIX="scientists-index"
INDEX="$INDEX_DIR/$INDEX_PREFIX"


# Delete and rebuild the index if necessary
if [ ${REBUILD_THE_INDEX} == "YES" ] || ! [ -f "${INDEX}.index.pso" ]; then
  # Can't check for the scientist-collection directory because
  # Travis' caching creates it
  if [ ! -e "$INPUT.nt" ]; then
  	unzip -j "$ZIPPED_INPUT" -d "$INPUT_DIR/"
  fi;


	rm -f "$INDEX.*"
	pushd "$BINARY_DIR"

  if [ ${BUILD_TEXT_INDEX_SEPARATELY} == "NO" ]; then
    echo "Building index $INDEX"
    ./qlever-index -i "$INDEX" \
        -F ttl \
        -f "$INPUT.nt" \
        -s "$PROJECT_DIR/e2e/e2e-build-settings.json" \
        -w "$INPUT.wordsfile.tsv" \
              -W \
        -d "$INPUT.docsfile.tsv" || bail "Building Index failed"
	else
    echo "Building index $INDEX without text index"
    ./qlever-index -i "$INDEX" \
        -F ttl \
        -f "$INPUT.nt" \
        -s "$PROJECT_DIR/e2e/e2e-build-settings.json" \
        || bail "Building Index failed"
    echo "Adding text index"
    ./qlever-index -A -i "$INDEX" \
        -s "$PROJECT_DIR/e2e/e2e-build-settings.json" \
        -w "$INPUT.wordsfile.tsv" \
              -W \
        -d "$INPUT.docsfile.tsv" || bail "Building Index failed"

	fi
	popd
fi

# Launch the Server using the freshly baked index. Can't simply use a subshell
# here because then we can't easily get the SERVER_PID out of that subshell
pushd "$BINARY_DIR"
echo "Launching server from path $(pwd)"
./qlever-server -i "$INDEX" -p 9099 -m 1GB -t --default-query-timeout 30s &> server_log.txt &
SERVER_PID=$!
popd

# Setup the kill switch so it gets called whatever way we exit
trap cleanup_server EXIT
echo "Waiting for qlever-server to launch and open port"
i=0
until [ $i -eq 60 ] || curl --max-time 1 --output /dev/null --silent http://localhost:9099/; do
	sleep 1;
  i=$((i+1));
done

if [ $i -ge 60 ]; then
  echo "qlever-server could not be reached after waiting for 60 seconds, exiting";
  exit 1
fi

echo "qlever-server was successfully started, running queries ..."
$PYTHON_BINARY "$PROJECT_DIR/e2e/queryit.py" "$PROJECT_DIR/e2e/scientists_queries.yaml" "http://localhost:9099" | tee "$BINARY_DIR/query_log.txt" || bail "Querying Server failed"

# Test that federated queries (`SERVICE`) can be routed through an HTTP proxy,
# configured via the `http_proxy` environment variable. We start a logging
# proxy and a second server that uses it, and send the second server a
# federated query whose `SERVICE` part must be answered by the first server.
# The proxy log then proves that the request actually went through the proxy.
echo "Testing federated queries through an HTTP proxy ..."
PROXY_PORT=9097
PROXY_LOG="$BINARY_DIR/proxy_log.txt"
rm -f "$PROXY_LOG"
$PYTHON_BINARY "$PROJECT_DIR/e2e/proxy.py" "$PROXY_PORT" "$PROXY_LOG" > /dev/null &
PROXY_PID=$!

pushd "$BINARY_DIR"
env http_proxy="http://localhost:$PROXY_PORT" ./qlever-server -i "$INDEX" -p 9098 -m 1GB --default-query-timeout 30s -a e2e-access-token &> server_with_proxy_log.txt &
SERVER_WITH_PROXY_PID=$!
popd

echo "Waiting for the server that uses the proxy to launch and open its port"
i=0
until [ $i -eq 60 ] || curl --max-time 1 --output /dev/null --silent http://localhost:9098/; do
	sleep 1;
  i=$((i+1));
done
[ $i -lt 60 ] || bail "The server that uses the proxy could not be reached"

grep -q "Proxy for outgoing HTTP requests: localhost:$PROXY_PORT" "$BINARY_DIR/server_with_proxy_log.txt" \
  || bail "The server did not log the configured proxy"

# The result of the federated query (which goes through the proxy) must match
# the result of the direct query, and the proxy must have seen the `SERVICE`
# request (a `POST` with an absolute-form target, as required for a relayed
# plain HTTP request).
DIRECT_COUNT=$(curl --silent http://localhost:9099/ -H "Accept: text/csv" \
  --data-urlencode "query=SELECT (COUNT(?s) AS ?count) WHERE { ?s <is-a> <Scientist> }" | tail -1 | tr -d '\r')
FEDERATED_COUNT=$(curl --silent http://localhost:9098/ -H "Accept: text/csv" \
  --data-urlencode "query=SELECT (COUNT(?s) AS ?count) WHERE { SERVICE <http://localhost:9099> { ?s <is-a> <Scientist> } }" | tail -1 | tr -d '\r')
echo "Number of scientists, direct query: ${DIRECT_COUNT}, federated query via the proxy: ${FEDERATED_COUNT}"
[[ "$DIRECT_COUNT" =~ ^[1-9][0-9]*$ ]] || bail "The direct query failed"
[ "$DIRECT_COUNT" == "$FEDERATED_COUNT" ] || bail "The federated query through the proxy returned a wrong result"
grep -q "REQUEST: POST http://localhost:9099/" "$PROXY_LOG" \
  || bail "The proxy did not see the federated request"

# A malformed proxy URL must fail the server startup with a readable message.
pushd "$BINARY_DIR"
if env http_proxy="socks5://proxy:1080" ./qlever-server -i "$INDEX" -p 9096 -m 1GB &> bad_proxy_log.txt; then
  bail "The server started despite a malformed http_proxy"
fi
popd
grep -q "only supports plain HTTP proxies" "$BINARY_DIR/bad_proxy_log.txt" \
  || bail "The error message for a malformed http_proxy is missing"
echo "The HTTP proxy tests passed"

# Test QLever's binary format for the results of federated queries
# (`SERVICE`), see `src/engine/BinaryExport.h`. The second server (which uses
# the proxy) sends queries with `SERVICE` parts to the first server. The result
# must be the same as the one of the direct query, both with the binary format
# (the default) and with JSON (when the runtime parameter
# `binary-service-enabled` is false), and the binary format must be used only
# in the first case. The queries cover all the datatypes that
# the binary format has to handle, except for the ones that are tested in the
# unit tests only (blank nodes, encoded IRIs, and the secondary vocabulary).
echo "Testing the binary format for federated queries ..."
function sorted_tsv_result {
  curl --silent --fail "http://localhost:$1/" -H "Accept: text/tab-separated-values" \
    --data-urlencode "query=$2" | sort
}
function num_binary_service_results {
  grep -c "transferred in QLever's binary format" "$BINARY_DIR/server_with_proxy_log.txt" || true
}
GEO_PREFIX="PREFIX geo: <http://www.opengis.net/ont/geosparql#>"
# Vocab entries, dates, undefined values, local vocab entries, booleans,
# integers, decimals, and geo points.
VARIABLES_1="?s ?name ?birth ?str ?bool ?len ?num ?geo"
BODY_1='?s <is-a> <Scientist> . ?s <label> ?name .
  OPTIONAL { ?s <Date_of_birth> ?birth }
  BIND(CONCAT(STR(?name), "!") AS ?str) BIND(STRLEN(?name) > 10 AS ?bool)
  BIND(STRLEN(?name) AS ?len) BIND(STRLEN(?name) / 4 AS ?num)
  BIND("POINT(7.8 47.9)"^^geo:wktLiteral AS ?geo)'
# Text records and words of the text index.
VARIABLES_2="?s ?t ?ql_matchingword_t_relati"
BODY_2='?s <is-a> <Scientist> . ?t ql:contains-entity ?s . ?t ql:contains-word "relati*"'
# The text records and words are printed without quotes by the direct query,
# but are ordinary literals after the transfer (with JSON as well as with the
# binary format), so the quotes are ignored for that query.
NORMALIZE_1="cat"
NORMALIZE_2="tr -d \""
function federated_result {
  sorted_tsv_result 9098 "$GEO_PREFIX SELECT ${!1} WHERE { SERVICE <http://localhost:9099> { SELECT ${!1} WHERE { ${!2} } } }"
}
NUM_BEFORE=$(num_binary_service_results)
FEDERATED_BINARY_1=$(federated_result VARIABLES_1 BODY_1)
FEDERATED_BINARY_2=$(federated_result VARIABLES_2 BODY_2)
[ "$(num_binary_service_results)" -eq $((NUM_BEFORE + 2)) ] \
  || bail "The federated queries did not use the binary format"
curl --silent --fail "http://localhost:9098/?binary-service-enabled=false&access-token=e2e-access-token" > /dev/null \
  || bail "Disabling the binary format failed"
# NOTE: The text query is not checked with JSON, because the import of JSON
# results currently mangles literals that contain quotes.
FEDERATED_JSON_1=$(federated_result VARIABLES_1 BODY_1)
[ "$(num_binary_service_results)" -eq $((NUM_BEFORE + 2)) ] \
  || bail "The federated queries used the binary format, although it was disabled"
for i in 1 2; do
  VARIABLES="VARIABLES_$i"
  BODY="BODY_$i"
  NORMALIZE="NORMALIZE_$i"
  BINARY="FEDERATED_BINARY_$i"
  DIRECT=$(sorted_tsv_result 9099 "$GEO_PREFIX SELECT ${!VARIABLES} WHERE { ${!BODY} }")
  [ "$(echo "$DIRECT" | wc -l)" -gt 100 ] || bail "The direct query $i returned too few rows"
  [ "$(echo "$DIRECT" | ${!NORMALIZE})" == "$(echo "${!BINARY}" | ${!NORMALIZE})" ] \
    || bail "The federated query $i returned a different result than the direct query"
  if [ $i -eq 1 ]; then
    [ "$DIRECT" == "$FEDERATED_JSON_1" ] \
      || bail "The federated query $i returned a different result with JSON than the direct query"
  fi
done
echo "The tests of the binary format for federated queries passed"
popd
