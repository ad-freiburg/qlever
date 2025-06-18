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
// sorted by comparator and should all have the same nof columns. If no
// comparator is given, the row is sorted left to right.
IdTable mergeIdTables(
    std::vector<IdTable> tablesToMerge,
    const ad_utility::AllocatorWithLimit<Id>& allocator,
    const ad_utility::MemorySize& memory,
    const std::function<bool(const columnBasedIdTable::Row<ValueId>&,
                             const columnBasedIdTable::Row<ValueId>&)>&
        comparator = [](const columnBasedIdTable::Row<ValueId>& a,
                        const columnBasedIdTable::Row<ValueId>& b) {
          if (a.size() != b.size()) {
            throw std::runtime_error{"Row sizes differ"};
          }
          for (auto it1 = a.begin(), it2 = b.begin(); it1 != a.end();
               ++it1, ++it2) {
            if (it1->getBits() < it2->getBits()) {
              return true;
            } else if (it1->getBits() > it2->getBits()) {
              return false;
            }
          }
          return true;
        });

}  // namespace SortedIdTableMerge

#endif  // SORTEDIDTABLEMERGE_H
