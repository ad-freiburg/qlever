// Copyright 2025, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Felix Meisen (fesemeisen@outlook.de)
#ifndef QLEVER_SRC_ENGINE_SORTEDIDTABLEMERGER_H
#define QLEVER_SRC_ENGINE_SORTEDIDTABLEMERGER_H

#include "engine/idTable/IdTable.h"

class SortedIdTableMerger {
  using ColumnIterator = decltype(std::declval<const IdTable&>()
                                      .getColumn(std::declval<size_t>())
                                      .begin());
  using ColumnSentinel = decltype(std::declval<const IdTable&>()
                                      .getColumn(std::declval<size_t>())
                                      .end());

 public:
  static IdTable mergeIdTables(
      std::vector<IdTable> idTablesToMerge,
      const ad_utility::AllocatorWithLimit<Id>& allocator);
};

#endif  // QLEVER_SRC_ENGINE_SORTEDIDTABLEMERGER_H
