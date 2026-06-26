// Copyright 2025, University of Freiburg
// Chair of Algorithms and Data Structures
// Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

// Explicit instantiation definitions for the `CompressedExternalIdTableSorter`
// specializations declared in
// `ExternalSortFunctors.h`. Each specialization is
// instantiated exactly once here (in the index library), rather than once per
// translation unit that uses it.

#include "engine/idTable/CompressedExternalIdTable.h"
#include "index/ConstantsIndexBuilding.h"
#include "index/ExternalSortFunctors.h"
#include "util/CompilerWarnings.h"

namespace ad_utility {

// GCC 13 produces false-positive `-Warray-bounds` warnings when the comparators
// from `ExternalSortFunctors.h` are inlined into `std::sort` during these
// instantiations (see the macro definition for details).
DISABLE_ARRAY_BOUNDS_WARNINGS

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

GCC_REENABLE_WARNINGS

}  // namespace ad_utility
