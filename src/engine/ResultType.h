// Copyright 2021 - 2022, University of Freiburg
// Chair of Algorithms and Data Structures
// Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#pragma once

namespace qlever {

// Enumerate the types of entries we can have in a `ResultTable`.
enum class ResultType {
  // An entry that is contained in the vocabulary of the indexed data.
  KB,
  // An unsigned integer. TODO: Briefly describe why this is still needed. It is
  // used in `GroupBy`, `CountAvailablePredicates`, `TextOperationWithFilter`,
  // `TextOperationWithoutFilter`.
  VERBATIM,
  // An entry from the text index (a byte offset in the text index).
  TEXT,
  // A floating point number. TODO: Is this still used somwehere? It doesn't
  // look like it is, now that we have values "folded into" IDs.
  FLOAT,
  // An entry in the `LocalVocab` of a `ResultTable`. TODO: Is this still used
  // somewhere? It doesn't look like it is.
  LOCAL_VOCAB
};
}  // namespace qlever
