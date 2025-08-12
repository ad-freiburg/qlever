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
#include "util/Log.h"

class GroupByImpl;

class GroupByStrategyChooser {
 public:
  // Forward declaration for use in static methods
  struct RowKey;
  // Check via sampling if the hash-map path should be skipped due to too many
  // groups. Friend of GroupByImpl so it can inspect private members.
  static bool shouldSkipHashMapGrouping(const GroupByImpl& gb,
                                        const IdTable& table,
                                        LogLevel logLevel = LogLevel::INFO);

  // Estimate total distinct groups via Chao1 estimator for any map-like
  // container
  template <typename Map>
  static size_t estimateNumberOfTotalGroups(
      const Map& groupCounts, LogLevel logLevel = LogLevel::INFO) {
    // Chao1 estimator = d_obs + (f1 * f1) / (2 * f2)
    // d_obs: number of distinct groups observed
    // f1: number of groups with exactly one occurrence
    // f2: number of groups with exactly two occurrences
    size_t dObs = groupCounts.size(), f1 = 0, f2 = 0;
    for (auto& [key, cnt] : groupCounts) {
      if (cnt == 1)
        ++f1;
      else if (cnt == 2)
        ++f2;
    }
    double chaoCorrection = double(f1 * f1) / (f2 > 0 ? 2.0 * f2 : 1.0);
    if (logLevel == LogLevel::DEBUG || logLevel == LogLevel::TRACE) {
      AD_LOG_DEBUG << "dObs=" << dObs << ", f1=" << f1 << ", f2=" << f2
                   << std::endl;
    }
    return dObs + static_cast<size_t>(chaoCorrection);
  }

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
