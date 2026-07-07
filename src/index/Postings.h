// Copyright 2024, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Felix Meisen (fesemeisen@outlook.de)

#ifndef QLEVER_SRC_INDEX_POSTINGS_H
#define QLEVER_SRC_INDEX_POSTINGS_H

#include "global/Id.h"
#include "global/IndexTypes.h"

namespace qlever {

using Posting = std::tuple<TextRecordIndex, WordIndex, Score>;

}  // namespace qlever

#endif  // QLEVER_SRC_INDEX_POSTINGS_H
