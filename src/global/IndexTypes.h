//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#ifndef QLEVER_INDEXTYPES_H
#define QLEVER_INDEXTYPES_H

#include "./TypedIndex.h"

// Typedefs for several kinds of typed indices that are used across QLever.
using VocabIndex = ad_utility::TypedIndex<uint64_t, "VocabIndex">;
using LocalVocabIndex = ad_utility::TypedIndex<uint64_t, "LocalVocabIndex">;
using TextRecordIndex = ad_utility::TypedIndex<uint64_t, "TextRecordIndex">;

#endif  // QLEVER_INDEXTYPES_H
