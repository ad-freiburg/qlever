#!/usr/bin/env bash
set -e
function bail {
	echo "$*"
	exit 1
}

function cleanup_server {
	echo "The Server Log follows:"
	cat "build/server_log.txt"
	# Killing 0 sends the signal to all processes in the current
	# process group
	kill $SERVER_PID
}

# Travis CI is super cool but also uses ancient OS images
if [ -f "/usr/bin/python3.6" ]; then
	export PYTHON_BINARY="/usr/bin/python3.6"
else
	export PYTHON_BINARY=`which python3`
fi


mkdir -p "e2e_data"
if [ ! -d "e2e_data/scientist-collection/" ]; then
	# Why the hell is this a ZIP that can't easily be decompressed from stdin?!?
	wget -O "e2e_data/scientist-collection.zip" \
		"http://filicudi.informatik.uni-freiburg.de/bjoern-data/scientist-collection.zip"
	unzip "e2e_data/scientist-collection.zip" -d "e2e_data"
fi;

INDEX="e2e_data/scientists-index"

# Delete and rebuild the index
if [ "$1" != "no-index" ]; then
	rm -f "$INDEX.*"
	(
	cd build && ./IndexBuilderMain -a -l -i "../$INDEX" \
		-n "../e2e_data/scientist-collection/scientists.nt" \
		-w "../e2e_data/scientist-collection/scientists.wordsfile.tsv" \
		-d "../e2e_data/scientist-collection/scientists.docsfile.tsv"
	) || bail "Building Index failed"
fi

# Launch the Server using the freshly baked index
(
cd build && ./ServerMain -i "../$INDEX" -p 9099 -t -a -l &> server_log.txt
) &
SERVER_PID=$!

# Setup the kill switch so it gets called whatever way we exit
trap cleanup_server EXIT
echo "Waiting for ServerMain to launch and open port"
while ! curl --max-time 1 --output /dev/null --silent http://localhost:9099/; do
	sleep 1
done
$PYTHON_BINARY e2e/queryit.py "e2e/scientists_queries.yaml" "http://localhost:9099" || bail "Querying Server failed"
