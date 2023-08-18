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
	# Killing 0 sends the signal to all processes in the current
	# process group
	kill "$SERVER_PID"
}

function print_usage {
  echo "Usage: $0 [options]"
  echo "Runs QLevers end to end tests."
  echo ""
  echo "Options:"
  echo "  -i  Use index from the given directory (which must be the root directory of the working copy of QLever, not the e2e_data subdirectory)"
  echo "  -d  Directory of the QLever binaries (relative to the main directory), default: 'build'"
}

REBUILD_THE_INDEX="YES"
INDEX_DIRECTORY="." #if not set, we will build the index ourselves.
BINARY_DIRECTORY="build"

while getopts ":i:d:" arg; do
  case ${arg} in
    i)
      echo "The index will not be rebuilt"
      REBUILD_THE_INDEX="NO"
      INDEX_DIRECTORY="${OPTARG}"
    ;;
    d)
      BINARY_DIRECTORY="${OPTARG}"
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
	echo "Building index $INDEX"
	./IndexBuilderMain -i "$INDEX" \
	    -F ttl \
	    -f "$INPUT.nt" \
	    -s "$PROJECT_DIR/e2e/e2e-build-settings.json" \
	    -w "$INPUT.wordsfile.tsv" \
            -W \
	    -d "$INPUT.docsfile.tsv" || bail "Building Index failed"
	popd
fi

# Launch the Server using the freshly baked index. Can't simply use a subshell
# here because then we can't easily get the SERVER_PID out of that subshell
pushd "$BINARY_DIR"
echo "Launching server from path $(pwd)"
./ServerMain -i "$INDEX" -p 9099 -m 1 -t &> server_log.txt &
SERVER_PID=$!
popd

# Setup the kill switch so it gets called whatever way we exit
trap cleanup_server EXIT
echo "Waiting for ServerMain to launch and open port"
i=0
until [ $i -eq 60 ] || curl --max-time 1 --output /dev/null --silent http://localhost:9099/; do
	sleep 1;
  i=$((i+1));
done

if [ $i -ge 60 ]; then
  echo "ServerMain could not be reached after waiting for 60 seconds, exiting";
  exit 1
fi

echo "ServerMain was succesfully started, running queries ..."
$PYTHON_BINARY "$PROJECT_DIR/e2e/queryit.py" "$PROJECT_DIR/e2e/scientists_queries.yaml" "http://localhost:9099" | tee "$BINARY_DIR/query_log.txt" || bail "Querying Server failed"
popd
