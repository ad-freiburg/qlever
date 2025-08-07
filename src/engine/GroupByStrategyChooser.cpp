// Copyright 2025, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Benke Hargitai <hargitab@cs.uni-freiburg.de>

#include "engine/GroupByStrategyChooser.h"

#include <algorithm>
#include <cmath>
#include <random>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/strings/str_format.h"
#include "engine/GroupByImpl.h"
#include "global/RuntimeParameters.h"

// _____________________________________________________________________________
bool GroupByStrategyChooser::shouldSkipHashMapGrouping(const GroupByImpl& gb,
                                                       const IdTable& table,
                                                       const bool log) {
  // Fetch runtime parameters
  if (!RuntimeParameters().get<"group-by-sample-enabled">()) {
    return false;
  }
  size_t totalSize = table.size();
  size_t minimumTableSize =
      RuntimeParameters().get<"group-by-sample-min-table-size">();
  if (totalSize < minimumTableSize) {
    if (log) {
      AD_LOG_DEBUG << "Choosing hash-map grouping due to small table size: "
                   << totalSize << " (threshold: " << minimumTableSize << ")"
                   << std::endl;
    }
    return false;
  }
  double distinctRatio =
      RuntimeParameters().get<"group-by-sample-distinct-ratio">();
  size_t k = RuntimeParameters().get<"group-by-sample-constant">();

  size_t sampleSize = k * static_cast<size_t>(std::sqrt(double(totalSize)));
  if (sampleSize == 0) {
    return false;
  }
  // Reservoir-sample indices only
  std::vector<size_t> indices(sampleSize);
  for (size_t i = 0; i < sampleSize; ++i) {
    indices[i] = i;
  }
  std::mt19937_64 gen{42};
  for (size_t i = sampleSize; i < totalSize; ++i) {
    std::uniform_int_distribution<size_t> dist(0, i);
    size_t randIdx = dist(gen);
    if (randIdx < sampleSize) {
      indices[randIdx] = i;
    }
  }
  // Use absl::flat_hash_map to count frequencies for Chao1 estimator
  absl::flat_hash_map<RowKey, size_t> groupCounts;
  // Extract grouping columns from the GroupByImpl instance
  const auto& varCols = gb._subtree->getVariableColumns();
  std::vector<ColumnIndex> groupByCols;
  groupByCols.reserve(gb._groupByVariables.size());
  for (const auto& var : gb._groupByVariables) {
    groupByCols.push_back(varCols.at(var).columnIndex_);
  }
  for (size_t idx : indices) {
    RowKey key{&table, idx, &groupByCols};
    ++groupCounts[key];
  }
  // Chao1 estimator: D = d_obs + (f1 * f1) / (2 * f2)
  size_t dObs = groupCounts.size();
  size_t f1 = 0, f2 = 0;
  for (auto& [key, cnt] : groupCounts) {
    if (cnt == 1)
      ++f1;
    else if (cnt == 2)
      ++f2;
  }
  double chaoCorrection = double(f1 * f1) / (f2 > 0 ? 2.0 * f2 : 1.0);
  size_t estGroups = dObs + static_cast<size_t>(chaoCorrection);
  if (log) {
    AD_LOG_DEBUG << absl::StrFormat(
                        "size=%zu, total=%zu, est=%zu, dObs=%zu, f1=%zu, "
                        "f2=%zu, thr=%.1f",
                        sampleSize, totalSize, estGroups, dObs, f1, f2,
                        totalSize * distinctRatio)
                 << std::endl;
  }
  return estGroups > totalSize * distinctRatio;
}
