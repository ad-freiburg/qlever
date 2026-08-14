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

// The conversion of an index from the previous on-disk format
// (`qlever::previousIndexFormatVersion`) to the current one
// (`qlever::indexFormatVersion`), see `convertIndexToCurrentFormat` below. This
// is what the standalone `qlever-convert-index` executable (see
// `src/IndexConverterMain.cpp`) does; nothing else in QLever uses it.
namespace qlever::indexFormatConverter {

// Convert a single `Id` from the previous format to the current one. The only
// difference between the two formats is the numbering of the `Datatype` enum,
// so this only rewrites the datatype bits and leaves the value bits untouched.
// In particular, the conversion preserves the order of any two `Id`s, which is
// why a permutation can be converted by rewriting its `Id`s one by one, without
// having to sort it again (there is a `static_assert` for this in the
// implementation).
//
// Throw if `id` is not a valid `Id` of the previous format, and also if it is
// of type `LocalVocabIndex`, which must never be stored on disk (such an `Id`
// holds a pointer into the memory of the process that created it).
Id convertId(Id id);

// Convert the index with the base name `oldBasename` to the current index
// format and write the result to the base name `newBasename`. The index at
// `oldBasename` is left unchanged, and the two base names must be different.
//
// All files that contain `Id`s are rewritten (the permutations, the patterns,
// and the materialized views), all other files are copied unchanged (the
// vocabulary, the text index, and the settings). Note that the text index needs
// no conversion, because it stores plain integers and reconstructs the `Id`s
// when it is read (see `index/TextIndexReadWrite.cpp`), and that the same holds
// for the vocabulary, which stores no `Id`s at all.
//
// Throw if the index at `oldBasename` does not exist, if it is not in the
// previous index format (in particular if it already is in the current one), if
// it has persisted updates (see `UPDATE_TRIPLES_SUFFIX`; those have to be
// materialized into the index or deleted before the conversion), or if any of
// the files at `newBasename` already exist.
void convertIndexToCurrentFormat(const std::string& oldBasename,
                                 const std::string& newBasename);

}  // namespace qlever::indexFormatConverter

#endif  // QLEVER_SRC_INDEX_INDEXFORMATCONVERTER_H
