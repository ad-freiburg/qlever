//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#ifndef QLEVER_INDEXTYPES_H
#define QLEVER_INDEXTYPES_H

#include "./StrongIndex.h"

// Typedefs for several strong index types that are used across QLever
using VocabIndex = ad_utility::StrongIndex<uint64_t, "VocabIndex">;
using LocalVocabIndex = ad_utility::StrongIndex<uint64_t, "LocalVocabIndex">;
using TextRecordIndex = ad_utility::StrongIndex<uint64_t, "TextRecordIndex">;

#endif  // QLEVER_INDEXTYPES_H
