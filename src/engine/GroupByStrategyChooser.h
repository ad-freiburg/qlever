// Copyright 2025, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Benke Hargitai <hargitab@cs.uni-freiburg.de>

#ifndef QLEVER_SRC_ENGINE_GROUPBYSTRATEGYCHOOSER_H
#define QLEVER_SRC_ENGINE_GROUPBYSTRATEGYCHOOSER_H

#include <gtest/gtest_prod.h>
// Standard library
#include <algorithm>
#include <cmath>
#include <random>
#include <vector>

// QLever core types
#include "engine/idTable/IdTable.h"  // for IdTable
#include "global/Id.h"               // for ColumnIndex
#include "global/RuntimeParameters.h"

// Abseil
#include "absl/strings/str_format.h"

class GroupByImpl;

class GroupByStrategyChooser {
 public:
  // Check via sampling if the hash-map path should be skipped due to too many
  // groups. Friend of GroupByImpl so it can inspect private members.
  static bool shouldSkipHashMapGrouping(const GroupByImpl& gb,
                                        const IdTable& table,
                                        const bool log = false);

  // Data structure to be able to use IdTable rows as keys in a hash map.
  struct RowKey {
    const IdTable* table;
    size_t rowIndex;
    const std::vector<ColumnIndex>* groupByCols;

    // We store RowKeys in absl::flat_hash_set. Therefore, we need to
    // implement a Hash method and an equality operator.

    template <typename H>
    friend H AbslHashValue(H h, const RowKey& key) {
      for (const auto& colIdx : *key.groupByCols) {
        h = H::combine(std::move(h), (*key.table)(key.rowIndex, colIdx));
      }
      return h;
    }

    bool operator==(const RowKey& other) const {
      for (const auto& colIdx : *groupByCols) {
        if ((*table)(rowIndex, colIdx) !=
            (*other.table)(other.rowIndex, colIdx)) {
          return false;
        }
      }
      return true;
    }
  };
};  // class GroupByStrategyChooser

#endif  // QLEVER_SRC_ENGINE_GROUPBYSTRATEGYCHOOSER_H
