#!/usr/bin/env bash
set -e
function bail {
	echo "$*"
	exit 1
}

function cleanup {
	echo "The Server Log follows:"
	cat "build/server_log.txt"
	# Killing 0 sends the signal to all processes in the current
	# process group
	kill $(jobs -p)
}

# Travis CI is super cool but also uses ancient OS images
if [ -f "/usr/bin/python3.6" ]; then
	export PYTHON_BINARY="/usr/bin/python3.6"
else
	export PYTHON_BINARY=`which python3`
fi


mkdir -p "e2e_data"
if [ ! -e "e2e_data/scientists-collection.zip" ]; then
	# Why the hell is this a ZIP that can't easily be decompressed from stdin?!?
	wget -O "e2e_data/scientists-collection.zip" \
		"http://filicudi.informatik.uni-freiburg.de/bjoern-data/scientist-collection.zip"
	unzip "e2e_data/scientists-collection.zip" -d "e2e_data"
fi;

INDEX="e2e_data/scientists-index"

# Delete and rebuild the index
rm -f "$INDEX.*"
(
cd build && ./IndexBuilderMain -a -l -i "../$INDEX" \
	-n "../e2e_data/scientist-collection/scientists.nt" \
	-w "../e2e_data/scientist-collection/scientists.wordsfile.tsv" \
	-d "../e2e_data/scientist-collection/scientists.docsfile.tsv"
) || bail "Building Index failed"

# Launch the Server using the freshly baked index
(
cd build && ./ServerMain -i "../$INDEX" -p 9099 -t -a -l &> server_log.txt
) &

# Setup the kill switch so it gets called whatever way we exit
trap cleanup EXIT
echo "Waiting for ServerMain to launch and open port"
while ! curl --max-time 1 --output /dev/null --silent http://localhost:9099/; do
	sleep 1
done
$PYTHON_BINARY e2e/queryit.py "e2e/scientists_queries.yaml" "http://localhost:9099" || bail "Querying Server failed"
