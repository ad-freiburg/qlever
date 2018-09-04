#!/usr/bin/env bash
set -e
PROJECT_DIR=$(realpath "$(dirname ${BASH_SOURCE[0]})/..")
# Change to the project directory so we can use simple relative paths
pushd $PROJECT_DIR
BINARY_DIR=$(realpath ./build)
if [ ! -e $BINARY_DIR ]; then
	BINARY_DIR=$(realpath .)
fi
function bail {
	echo "$*"
	exit 1
}

function cleanup_server {
	echo "The Server Log follows:"
	cat "$BINARY_DIR/server_log.txt"
	# Killing 0 sends the signal to all processes in the current
	# process group
	kill $SERVER_PID
}

# Travis CI is super cool but also uses ancient OS images and so to get
# a python that supports typing we need to install from the deadsnakes
# repository which does not override the system python
if [ -f "/usr/bin/python3.6" ]; then
	export PYTHON_BINARY="/usr/bin/python3.6"
else
	export PYTHON_BINARY=`which python3`
fi

INDEX_DIR="$PROJECT_DIR/e2e_data"
INPUT_DIR="$INDEX_DIR/scientist-collection"
INPUT_PREFIX="scientists"
INPUT="$INPUT_DIR/$INPUT_PREFIX"
mkdir -p "$INDEX_DIR"
# Can't check for the scientist-collection directory because
# Travis' caching creates it
if [ ! -e "$INPUT.nt" ]; then
	# Why the hell is this a ZIP that can't easily be decompressed from stdin?!?
	wget -O "$INDEX_DIR/scientist-collection.zip" \
		"http://filicudi.informatik.uni-freiburg.de/bjoern-data/scientist-collection.zip"
	unzip -j "$INDEX_DIR/scientist-collection.zip" -d "$INPUT_DIR/"
	rm "$INDEX_DIR/scientist-collection.zip"
fi;


INDEX_PREFIX="scientists-index"
INDEX="$INDEX_DIR/$INDEX_PREFIX"

# Delete and rebuild the index
if [ "$1" != "no-index" ]; then
	rm -f "$INDEX.*"
	pushd "$BINARY_DIR"
	echo "Building index $INDEX"
	./IndexBuilderMain -a -l -i "$INDEX" \
		-n "$INPUT.nt" \
		-w "$INPUT.wordsfile.tsv" \
		-d "$INPUT.docsfile.tsv" \
		--patterns || bail "Building Index failed"
	popd
fi

# Launch the Server using the freshly baked index. Can't simply use a subshell here because
# then we can't easily get the SERVER_PID out of that subshell
pushd "$BINARY_DIR"
echo "Launching server from path $(pwd)"
./ServerMain -i "$INDEX" -p 9099 -t -a --patterns &> server_log.txt &
SERVER_PID=$!
popd

# Setup the kill switch so it gets called whatever way we exit
trap cleanup_server EXIT
echo "Waiting for ServerMain to launch and open port"
while ! curl --max-time 1 --output /dev/null --silent http://localhost:9099/; do
	sleep 1
done
$PYTHON_BINARY "$PROJECT_DIR/e2e/queryit.py" "$PROJECT_DIR/e2e/scientists_queries.yaml" "http://localhost:9099" || bail "Querying Server failed"
popd
