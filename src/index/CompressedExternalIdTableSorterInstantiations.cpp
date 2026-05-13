// Copyright 2025, University of Freiburg
// Chair of Algorithms and Data Structures
// Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

// Explicit instantiation definitions for the CompressedExternalIdTableSorter
// specialisations declared in
// CompressedExternalIdTableSorterExternTemplates.h. Each specialisation is
// instantiated exactly once here (in the index library), rather than once per
// translation unit that uses it.

#include "engine/idTable/CompressedExternalIdTable.h"
#include "index/ConstantsIndexBuilding.h"
#include "index/ExternalSortFunctors.h"

namespace ad_utility {

template class CompressedExternalIdTableSorter<SortByPSONoGraphColumn, 3>;
template class CompressedExternalIdTableSorter<SortByOSP,
                                               NumColumnsIndexBuilding + 1>;
template class CompressedExternalIdTableSorter<SortBySPO,
                                               NumColumnsIndexBuilding>;
template class CompressedExternalIdTableSorter<SortByOSP,
                                               NumColumnsIndexBuilding>;
template class CompressedExternalIdTableSorter<SortByPSO,
                                               NumColumnsIndexBuilding>;
template class CompressedExternalIdTableSorter<SortByPSO,
                                               NumColumnsIndexBuilding + 2>;
template class CompressedExternalIdTableSorter<SortText, 5>;

}  // namespace ad_utility
