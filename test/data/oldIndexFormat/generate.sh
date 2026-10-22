#!/usr/bin/env bash
# Copyright 2026 The QLever Authors, in particular:
#
# 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
#
# UFR = University of Freiburg, Chair of Algorithms and Data Structures
#
# You may not use this file except in compliance with the Apache 2.0 License,
# which can be found in the `LICENSE` file at the root of the QLever project.

# Recreate the index in this directory, see `README.md`. This only works with a
# `qlever-index` binary that still writes the index format of that index, so
# pass the path to such a binary as the first argument.

set -euo pipefail

if [ "$#" -ne 1 ]; then
  echo "Usage: $0 <path to an old qlever-index binary>" >&2
  exit 1
fi
QLEVER_INDEX="$(realpath "$1")"
cd "$(dirname "$0")"

"$QLEVER_INDEX" \
  --index-basename oldFormat \
  --kg-input-file input.ttl \
  --file-format ttl \
  --settings-file settings.json \
  --text-words-input-file words.tsv \
  --text-docs-input-file docs.tsv \
  --encode-as-id "https://example.org/id/" \
  --materialized-views '{"testview": "SELECT ?s ?o WHERE { ?s <http://example.org/label> ?o }"}' \
  --no-resource-usage-log

# QLever itself does not write the settings file next to the index, this is done
# by `qlever-control`. Do the same here, so that the conversion of that file is
# covered by the test as well.
cp settings.json oldFormat.settings.json
