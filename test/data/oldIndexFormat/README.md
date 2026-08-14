# An index in the previous index format

This directory contains a (very small) QLever index in the index format that
directly precedes the current one (see `qlever::previousIndexFormatVersion` in
`src/index/IndexFormatVersion.h`). It is used by
`test/index/IndexFormatConverterTest.cpp` to test the conversion of an index to
the current format (see `src/index/IndexFormatConverter.h`).

The index has to be checked in, because the current code can no longer create
an index in that format. It was created by running `generate.sh` with the
`qlever-index` binary of the commit that directly precedes the change of the
index format (`e92edd7fe`, the parent of the commit that added
`IndexFormatConverter.h`).

The index deliberately covers everything that the conversion has to handle:

- Objects of all datatypes that occur in a permutation, that is IRIs and
  literals from the vocabulary, blank nodes, encoded IRIs (see
  `--encode-as-id`), integers, doubles, booleans, dates, date-times, and
  geometries.
- All six permutations, the two internal permutations, and the patterns.
- A materialized view (`testview`).
- A text index with both a words file and a docs file.
- A settings file (`languages-internal`).

The input files (`input.ttl`, `words.tsv`, `docs.tsv`, and `settings.json`) are
checked in next to the index itself, so that the index can be recreated with an
older QLever binary if that ever becomes necessary. Everything else in this
directory belongs to the index; the test copies exactly the files whose name
starts with `oldFormat.` into a temporary directory of its own.
