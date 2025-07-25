// Copyright 2024, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Felix Meisen (fesemeisen@outlook.de)

#ifndef QLEVER_SRC_INDEX_POSTINGS_H
#define QLEVER_SRC_INDEX_POSTINGS_H

#include "global/IndexTypes.h"

using WordPosting = std::tuple<TextRecordIndex, WordVocabIndex, Score>;
using EntityPosting = std::tuple<TextRecordIndex, VocabIndex, Score>;

#endif  // QLEVER_SRC_INDEX_POSTINGS_H
