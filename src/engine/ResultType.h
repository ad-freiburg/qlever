// Copyright 2021 - 2022, University of Freiburg
// Chair of Algorithms and Data Structures
// Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#pragma once

namespace qlever {

// Enumerate the types of entries we can have in a `ResultTable`.
//
// NOTE: This was used in an old version of the QLever code, but no longer is
// (because reality is more complicated than "one type per column"). The class
// is still needed for the correctness of the code, see `ResultTable.h`.
//
// TODO: Properly keep track of result types again. In particular, efficiency
// should benefit in the common use case where all entries in a column have a
// certain type or types.
enum class ResultType {
  // An entry that is contained in the vocabulary of the indexed data.
  KB,
  // An unsigned integer.
  //
  // NOTE: This is currently used in `GroupBy`, `CountAvailablePredicates`,
  // `TextOperationWithFilter`, `TextOperationWithoutFilter`.
  VERBATIM,
  // An entry from the text index (a byte offset in the text index).
  TEXT,
  // A floating point number. NOTE: This isn't used anywhere anymore.
  FLOAT,
  // An entry in the `LocalVocab`. NOTE: This isn't used anywhere anymore.
  LOCAL_VOCAB
};
}  // namespace qlever
