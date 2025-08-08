// Copyright 2025, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Benke Hargitai <hargitab@cs.uni-freiburg.de>

#include "engine/GroupByStrategyChooser.h"

#include <algorithm>
#include <cmath>
#include <random>
#include <vector>

#include "absl/strings/str_format.h"
#include "engine/GroupByImpl.h"
#include "global/RuntimeParameters.h"
#include "util/HashMap.h"
#include "util/Random.h"  // for ad_utility::randomShuffle and RandomSeed

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
  // Reservoir-sample indices only
  std::vector<size_t> indices(totalSize);
  for (size_t i = 0; i < totalSize; ++i) indices[i] = i;
  // Shuffle and keep only the first `keep` indices.
  // Use fixed seed for deterministic shuffle
  ad_utility::randomShuffle(indices.begin(), indices.end(),
                            ad_utility::RandomSeed::make(42));
  indices.resize(sampleSize);
  // Extract grouping columns from the GroupByImpl instance
  const auto& varCols = gb._subtree->getVariableColumns();
  std::vector<ColumnIndex> groupByCols;
  groupByCols.reserve(gb._groupByVariables.size());
  for (const auto& var : gb._groupByVariables) {
    groupByCols.push_back(varCols.at(var).columnIndex_);
  }
  // Use a HashMap to count frequencies for Chao1 estimator
  ad_utility::HashMap<RowKey, size_t> groupCounts;
  for (size_t idx : indices) {
    RowKey key{&table, idx, &groupByCols};
    ++groupCounts[key];
  }
  // Chao1 estimator: D = d_obs + (f1 * f1) / (2 * f2)
  // d_obs: number of distinct groups observed
  // f1: number of groups with exactly one occurrence
  // f2: number of groups with exactly two occurrences
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
