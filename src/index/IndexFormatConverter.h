// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_INDEX_INDEXFORMATCONVERTER_H
#define QLEVER_SRC_INDEX_INDEXFORMATCONVERTER_H

#include <string>

#include "global/Id.h"
#include "index/ConstantsIndexBuilding.h"
#include "index/IndexFormatVersion.h"
#include "util/MemorySize/MemorySize.h"

// The conversion of an index from the previous on-disk format to the current
// one, see `convertIndexToCurrentFormat` below. This is what the standalone
// `qlever-upgrade-index` executable (see `src/IndexUpgraderMain.cpp`) does;
// nothing else in QLever uses it.
namespace qlever::indexFormatConverter {

// The index format that this converter converts from, and the index format that
// it converts to. They are deliberately hardcoded here, because the conversion
// is specific to exactly this pair of formats: the *only* difference between
// them is that the datatype `Datatype::SecondaryVocabIndex` was inserted into
// the `Datatype` enum (directly after `Datatype::LocalVocabIndex`, see the note
// there), which renumbered the datatypes after it. The `Id`s of an index in the
// source format are therefore converted by rewriting their datatype bits, and
// nothing else in the index changes.
//
// `convertIndexToCurrentFormat` checks that these two formats still are the
// previous and the current index format (`qlever::previousIndexFormatVersion`
// resp. `qlever::indexFormatVersion`), so that this converter cannot silently
// be applied to a different change of the index format.
inline const IndexFormatVersion sourceVersion{
    1572, DateYearOrDuration{Date{2024, 10, 22}}};
inline const IndexFormatVersion targetVersion{
    3159, DateYearOrDuration{Date{2026, 8, 14}}};

// Return a human-readable description of the two index formats above and of
// their difference. This is the overview message of `qlever-upgrade-index`.
std::string conversionDescription();

// Convert a single `Id` from the source format to the target format. As only
// the numbering of the `Datatype` enum differs between the two (see above),
// this only rewrites the datatype bits and leaves the value bits untouched. In
// particular, the conversion preserves the order of any two `Id`s, which is why
// a permutation can be converted by rewriting its `Id`s one by one, without
// having to sort it again (there is a `static_assert` for this in the
// implementation).
//
// Throw if `id` is not a valid `Id` of the source format, and also if it is of
// type `LocalVocabIndex`, which must never be stored on disk (such an `Id`
// holds a pointer into the memory of the process that created it).
Id convertId(Id id);

// The block size (per column) with which the permutations of the converted
// index are written. It is the default block size of the index builder, so that
// the converted permutations have exactly the blocks that a freshly built index
// would have. It is not `const`, so that a unit test can set it to a much
// smaller value; with the default, a relation only gets a
// `CompressedRelationMetadata` of its own if it has more than 25000 rows, which
// no unit test can afford to build (see `writePermutation` in the
// implementation).
inline ad_utility::MemorySize& blocksizeOfConvertedPermutations() {
  static ad_utility::MemorySize blocksize =
      UNCOMPRESSED_BLOCKSIZE_COMPRESSED_METADATA_PER_COLUMN;
  return blocksize;
}

// Convert the index with the base name `oldBasename` from the source format to
// the target format and write the result to the base name `newBasename`. The
// index at `oldBasename` is left unchanged, and the two base names must be
// different.
//
// All files that contain `Id`s are rewritten (the permutations, the patterns,
// and the materialized views), all other files are copied unchanged (the
// vocabulary, the text index, and the settings). Note that the text index needs
// no conversion, because it stores plain integers and reconstructs the `Id`s
// when it is read (see `index/TextIndexReadWrite.cpp`), and that the same holds
// for the vocabulary, which stores no `Id`s at all.
//
// Throw if the index at `oldBasename` does not exist, if it is not in the
// source format (in particular if it already is in the target format), if its
// materialized views are not in the corresponding format either, if it has
// persisted updates (see `UPDATE_TRIPLES_SUFFIX`; those have to be materialized
// into the index or deleted before the conversion), or if any of the files at
// `newBasename` already exist.
void convertIndexToCurrentFormat(const std::string& oldBasename,
                                 const std::string& newBasename);

}  // namespace qlever::indexFormatConverter

#endif  // QLEVER_SRC_INDEX_INDEXFORMATCONVERTER_H
