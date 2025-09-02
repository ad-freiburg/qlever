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

namespace detail::indexTypes {
constexpr inline std::string_view textRecordTag = "TextRecordIndex";
constexpr inline std::string_view wordVocabIndexTag = "WordVocabIndex";
constexpr inline std::string_view blankNodeIndexTag = "BlankNodeIndex";
constexpr inline std::string_view documentIndexTag = "DocumentIndex";
}  // namespace detail::indexTypes
using LocalVocabIndex = const LocalVocabEntry*;
using TextRecordIndex =
    ad_utility::TypedIndex<uint64_t, detail::indexTypes::textRecordTag>;
using WordVocabIndex =
    ad_utility::TypedIndex<uint64_t, detail::indexTypes::wordVocabIndexTag>;
using BlankNodeIndex =
    ad_utility::TypedIndex<uint64_t, detail::indexTypes::blankNodeIndexTag>;
using DocumentIndex =
    ad_utility::TypedIndex<uint64_t, detail::indexTypes::documentIndexTag>;

#endif  // QLEVER_SRC_GLOBAL_INDEXTYPES_H
