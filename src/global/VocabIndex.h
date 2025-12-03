//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#ifndef QLEVER_SRC_GLOBAL_VOCABINDEX_H
#define QLEVER_SRC_GLOBAL_VOCABINDEX_H

#include <cstdint>

#include "global/TypedIndex.h"

// TODO<joka921> Rename `VocabIndex` to `RdfVocabIndex` at a point in time where
// this (very intrusive) renaming doesn't interfere with too many open pull
// requests.
inline constexpr ad_utility::IndexTag vocabIndexTag = "VocabIndex";
using VocabIndex = ad_utility::TypedIndex<uint64_t, vocabIndexTag>;

#endif  // QLEVER_SRC_GLOBAL_VOCABINDEX_H
