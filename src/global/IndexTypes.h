//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#ifndef QLEVER_SRC_GLOBAL_INDEXTYPES_H
#define QLEVER_SRC_GLOBAL_INDEXTYPES_H

#include "global/TypedIndex.h"
#include "global/VocabIndex.h"
#include "index/LocalVocabEntry.h"

// Typedefs for several kinds of typed indices that are used across QLever.

// Note the `VocabIndex` is declared in a separate header `VocabIndex` to break
// a cyclic dependency (it is needed by `LocalVocabEntry.h`).
using LocalVocabIndex = const LocalVocabEntry*;
inline constexpr ad_utility::IndexTag textRecordIndexTag = "TextRecordIndex";
using TextRecordIndex = ad_utility::TypedIndex<uint64_t, textRecordIndexTag>;
inline constexpr ad_utility::IndexTag wordVocabIndexTag = "WordVocabIndex";
using WordVocabIndex = ad_utility::TypedIndex<uint64_t, wordVocabIndexTag>;
inline constexpr ad_utility::IndexTag blankNodeIndexTag = "BlankNodeIndex";
using BlankNodeIndex = ad_utility::TypedIndex<uint64_t, blankNodeIndexTag>;
inline constexpr ad_utility::IndexTag documentIndexTag = "DocumentIndex";
using DocumentIndex = ad_utility::TypedIndex<uint64_t, documentIndexTag>;

#endif  // QLEVER_SRC_GLOBAL_INDEXTYPES_H
