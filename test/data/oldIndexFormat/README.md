# An index in the previous index format

This directory contains a (very small) QLever index in the index format
`{PR = 3159, Date = 2026-09-01}`, which is the format that directly precedes
the current one (see `qlever::previousIndexFormatVersion` resp.
`qlever::indexFormatVersion` in `src/index/IndexFormatVersion.h`, and
`sourceVersion` resp. `targetVersion` in `src/index/IndexFormatConverter.h`).
Its materialized view is in the corresponding format version of the
materialized views (see `MATERIALIZED_VIEWS_VERSION`; unlike the previous
change of the index format, this one does not change that format at all, see
`materializedViewsVersionOfSourceFormat`). The index is used by
`test/index/IndexFormatConverterTest.cpp` to test the conversion of an index to
the current format (see `src/index/IndexFormatConverter.h`).

The index has to be checked in, because the current code can no longer create
an index in that format (the source format's `Id` uses a packed 4-bit-datatype
+ 60-bit-payload single word, see `LegacyId` in `IndexFormatConverter.cpp`,
whereas the current code always writes the datatype-byte + 64-bit-payload
representation). It was created by running `generate.sh` with a `qlever-index`
binary built from `master` at the commit that directly precedes this change of
the index format (`47259112`, "Avoid a string allocation per triple component
in the partial vocabulary creation").

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
