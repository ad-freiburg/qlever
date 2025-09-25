// Copyright 2025, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Felix Meisen (fesemeisen@outlook.de)

#ifndef QLEVER_SRC_INDEX_TEXTINDEXBUILDERTYPES_H
#define QLEVER_SRC_INDEX_TEXTINDEXBUILDERTYPES_H

#include "engine/idTable/CompressedExternalIdTable.h"
#include "index/ExternalSortFunctors.h"

// WordVocabIndex, TextRecordIndex, Score
using WordTextVec = ad_utility::CompressedExternalIdTableSorter<SortText, 3>;
// WordVocabIndex, TextRecordIndex, VocabIndex (of the entity that occurs in the
// `text`), Score
using EntityTextVec = ad_utility::CompressedExternalIdTableSorter<SortText, 4>;

using WordTextVecView = decltype(std::declval<WordTextVec&>().sortedView());
using EntityTextVecView = decltype(std::declval<EntityTextVec&>().sortedView());

using WordPosting = std::tuple<TextRecordIndex, WordVocabIndex, Score>;
using EntityPosting = std::tuple<TextRecordIndex, VocabIndex, Score>;

#endif  // QLEVER_SRC_INDEX_TEXTINDEXBUILDERTYPES_H
