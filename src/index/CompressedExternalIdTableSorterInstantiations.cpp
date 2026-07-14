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
#include "util/CompilerWarnings.h"

// GCC 13 produces a false-positive `-Warray-bounds` warning for the
// `SortTriple::operator()` comparator in `ExternalSortFunctors.h`: when it is
// instantiated for both `Row<ValueId, 4>` and `Row<ValueId, 5>` in this
// translation unit, GCC folds the two instantiations and then believes the
// `Row<5>` graph-column access happens on a `Row<4>`. `-Warray-bounds` is a
// middle-end warning, but GCC walks the inlining chain when deciding whether it
// is suppressed.
DISABLE_ARRAY_BOUNDS_WARNINGS
#include "index/ExternalSortFunctors.h"
GCC_REENABLE_WARNINGS

namespace qlever {

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

}  // namespace qlever
