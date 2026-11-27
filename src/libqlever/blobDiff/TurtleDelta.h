// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_LIBQLEVER_BLOBDIFF_TURTLEDELTA_H
#define QLEVER_SRC_LIBQLEVER_BLOBDIFF_TURTLEDELTA_H

#include <memory>
#include <vector>

#include "backports/filesystem.h"
#include "global/IdTriple.h"
#include "index/Index.h"
#include "index/LocalVocab.h"

// Forward declarations to not include the heavy `parser/RdfParser.h`.
struct TurtleTriple;
struct RdfParserSettings;

namespace qlever::blobDiff {

// The difference between two versions of the data, as the triples to insert
// into and to delete from an index (in the default graph). Both are sorted,
// free of duplicates, and disjoint.
struct TripleDelta {
  // The index for which the delta was computed. Its `Id`s are only valid for
  // that index, which `Qlever::applyTripleDelta` checks. The `weak_ptr` does
  // not keep the index alive, but (unlike a raw pointer) it also cannot refer
  // to a different index that was later created at the same address.
  std::weak_ptr<const Index> index_;
  // The words of the triples that are not in the vocabulary of the index.
  LocalVocab localVocab_;
  std::vector<IdTriple<0>> toInsert_;
  std::vector<IdTriple<0>> toDelete_;
};

// Return the difference between the Turtle files `oldTurtleFile` and
// `newTurtleFile`, both parsed with the `EncodedIriManager` of the `index` and
// the `settings`. The `settings` have to be the ones with which the `index` was
// built (they are not stored in the index): otherwise, the same triple can get
// different `Id`s in the index and in the parsed files (for example, an integer
// that was converted to a double during the index build), and the delta would
// fail to delete it and insert it a second time.
// TODO<joka921> Store the parser settings in the index (they are then also
// available to all other users of the RDF parser, like `INSERT DATA` or
// `LOAD`), and remove the `settings` argument again.
//
// The difference is computed on the level of the `Id`s, which are exact and
// normalized: a change of a literal that only affects its lexical form (for
// example `"1"^^xsd:int` to `"01"^^xsd:int`) gives the same `Id` and is thus no
// change, whereas a change of the type (for example from `1` to `1.0`) is. A
// comparison of the string representations of the parsed triples would get both
// cases wrong. Words that are not in the vocabulary of the `index` become
// `LocalVocabIndex` `Id`s, which are kept alive by the `localVocab_` of the
// result.
//
// NOTE: Both files are read and parsed completely in RAM, and their triples are
// kept in RAM at the same time, so the files have to be small enough for that.
//
// Throw if one of the files contains a blank node. Blank nodes cannot be
// diffed: the same label gets a different `Id` in each parsed file, and the
// `Id`s that the blank nodes of the old file got when the `index` was built are
// not stored anywhere. A delta with blank nodes would thus silently fail to
// delete their triples and insert them a second time.
//
// To apply the result, use `Qlever::applyTripleDelta`.
TripleDelta computeTripleDelta(const ql::filesystem::path& oldTurtleFile,
                               const ql::filesystem::path& newTurtleFile,
                               std::shared_ptr<const Index> index,
                               const RdfParserSettings& settings);

namespace detail {
// Parse the Turtle file `path` (completely in RAM) using the
// `EncodedIriManager` of the `index` and the `settings`. Throw if one of the
// triples contains a blank node (see `computeTripleDelta` for the reason).
std::vector<TurtleTriple> parseTurtleFile(const ql::filesystem::path& path,
                                          const Index& index,
                                          const RdfParserSettings& settings);

// Convert the `triples` into sorted and deduplicated `IdTriple`s in the default
// graph. Words that are not in the vocabulary of the `index` become
// `LocalVocabIndex` `Id`s that are kept alive by the `localVocab`. The
// `triples` must not contain blank nodes, which is checked.
std::vector<IdTriple<0>> toIdTriples(std::vector<TurtleTriple> triples,
                                     const Index& index,
                                     LocalVocab& localVocab);
}  // namespace detail

}  // namespace qlever::blobDiff

#endif  // QLEVER_SRC_LIBQLEVER_BLOBDIFF_TURTLEDELTA_H
