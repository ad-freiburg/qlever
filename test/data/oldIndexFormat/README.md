# An index in the previous index format

This directory contains a (very small) QLever index in the index format
`{PR = 3159, Date = 2026-09-01}`, which is the format that directly precedes the
current one `{PR = 3412, Date = 2026-09-19}` (see
`qlever::previousIndexFormatVersion` resp. `qlever::indexFormatVersion` in
`src/index/IndexFormatVersion.h`, and `sourceVersion` resp. `targetVersion` in
`src/index/IndexFormatConverter.h`). The index is used by
`test/index/IndexFormatConverterTest.cpp` to test the conversion of an index to
the current format (see `src/index/IndexFormatConverter.h`).

The index has to be checked in, because the current code can no longer create
an index in that format. It was created by running `generate.sh` with the
`qlever-index` binary of the commit `fead2f767` (the `master` of 2026-09-13,
which precedes the change of the index format).

The two formats differ only in the encoding of geo points (WKT `POINT`s), so
the index deliberately has encoded points in all the places that the conversion
has to handle, in addition to objects of all other datatypes (IRIs and literals
from the vocabulary, blank nodes, encoded IRIs, integers, doubles, booleans,
dates, and date-times), all six permutations, the two internal permutations,
the patterns, a text index with both a words file and a docs file, a settings
file (`languages-internal`), and a materialized view (`testview`):

- Points whose order differs between the two formats (the old format orders
  them by latitude, then longitude, the new one interleaves the bits of the
  two coordinates), so that the permutations have to be re-sorted.
- A subject with two points for the same predicate (`ex:s4` and `ex:s6`),
  which is a run of two rows that has to be re-sorted in the `SPO` and `PSO`
  permutations.
- Two predicates with points (`ex:geometry` and `ex:centroid`), which are two
  runs in the `POS` permutation.
- The same point for two subjects (`ex:s1` and `ex:s5`), which is a run of two
  rows in the `OSP` and `OPS` permutations.
- A geometry vocabulary (`--vocabulary-type on-disk-compressed-geo-split`) with
  a `LINESTRING` (`ex:s7`), a `POLYGON` (`ex:s8`), and an invalid WKT literal
  (`ex:s9`). The precomputed geometry information of the two valid ones (in
  the file `oldFormat.vocabulary.geometry.geoinfo`) holds encoded points (the
  corners of the bounding box and the centroid), which the conversion has to
  rewrite; the invalid one is an all-zero record there.

The input files (`input.ttl`, `words.tsv`, `docs.tsv`, and `settings.json`) are
checked in next to the index itself, so that the index can be recreated with an
older QLever binary if that ever becomes necessary. Everything else in this
directory belongs to the index; the test copies exactly the files whose name
starts with `oldFormat.` into a temporary directory of its own.
