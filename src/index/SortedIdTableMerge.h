// Copyright 2025, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Felix Meisen (fesemeisen@outlook.de)

#ifndef SORTEDIDTABLEMERGE_H
#define SORTEDIDTABLEMERGE_H

#include <vector>

#include "engine/idTable/IdTable.h"

namespace SortedIdTableMerge {

// Takes in multiple sorted IdTables, an allocator for the output IdTable, a
// comparator that evaluates to true iff the first row is smaller or equal to
// the second row and a MemorySize for the merge. The IdTables should all be
// sorted by comparator and should all have the same nof columns.
IdTable mergeIdTables(
    std::vector<IdTable> tablesToMerge,
    const ad_utility::AllocatorWithLimit<Id>& allocator,
    std::function<bool(const columnBasedIdTable::Row<ValueId>&,
                       const columnBasedIdTable::Row<ValueId>&)>
        comparator,
    const ad_utility::MemorySize& memory);

}  // namespace SortedIdTableMerge

#endif  // SORTEDIDTABLEMERGE_H
