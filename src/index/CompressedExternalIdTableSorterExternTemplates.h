// Copyright 2025, University of Freiburg
// Chair of Algorithms and Data Structures
// Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

// Extern-template declarations for the CompressedExternalIdTableSorter
// specialisations that are used during index building. Without these
// declarations every translation unit that includes IndexImpl.h or
// PatternCreator.h would instantiate all of the member functions of each
// specialisation, which is expensive (~100 s cumulative, per ftime-trace).
// The corresponding explicit instantiation definitions live in
// CompressedExternalIdTableSorterInstantiations.cpp (part of the index
// library).

#ifndef QLEVER_SRC_INDEX_COMPRESSEDEXTERNALIDTABLESORTEREXTERNTEMPLATES_H
#define QLEVER_SRC_INDEX_COMPRESSEDEXTERNALIDTABLESORTEREXTERNTEMPLATES_H

#include "engine/idTable/CompressedExternalIdTable.h"
#include "index/ConstantsIndexBuilding.h"
#include "index/ExternalSortFunctors.h"

namespace ad_utility {

extern template class CompressedExternalIdTableSorter<SortByPSONoGraphColumn,
                                                      3>;
extern template class CompressedExternalIdTableSorter<
    SortByOSP, NumColumnsIndexBuilding + 1>;
extern template class CompressedExternalIdTableSorter<SortBySPO,
                                                      NumColumnsIndexBuilding>;
extern template class CompressedExternalIdTableSorter<SortByOSP,
                                                      NumColumnsIndexBuilding>;
extern template class CompressedExternalIdTableSorter<SortByPSO,
                                                      NumColumnsIndexBuilding>;
extern template class CompressedExternalIdTableSorter<
    SortByPSO, NumColumnsIndexBuilding + 2>;
extern template class CompressedExternalIdTableSorter<SortText, 5>;

}  // namespace ad_utility

#endif  // QLEVER_SRC_INDEX_COMPRESSEDEXTERNALIDTABLESORTEREXTERNTEMPLATES_H
