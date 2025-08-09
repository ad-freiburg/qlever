// Copyright 2025, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Benke Hargitai <hargitab@cs.uni-freiburg.de>

#include "engine/GroupByStrategyChooser.h"

#include <algorithm>
#include <chrono>
#include <cmath>
#include <numeric>
#include <random>
#include <unordered_set>
#include <vector>

#include "absl/strings/str_format.h"
#include "engine/GroupByImpl.h"
#include "global/RuntimeParameters.h"
#include "util/HashMap.h"
#include "util/Log.h"     // for AD_LOG_DEBUG, AD_LOG_TIMING
#include "util/Random.h"  // for ad_utility::randomShuffle and RandomSeed

// _____________________________________________________________________________
bool GroupByStrategyChooser::shouldSkipHashMapGrouping(const GroupByImpl& gb,
                                                       const IdTable& table,
                                                       LogLevel logLevel) {
  // Fetch runtime parameters
  if (!RuntimeParameters().get<"group-by-sample-enabled">()) {
    return false;
  }
  size_t totalSize = table.size();
  size_t minimumTableSize =
      RuntimeParameters().get<"group-by-sample-min-table-size">();
  if (totalSize < minimumTableSize) {
    if (logLevel == LogLevel::DEBUG || logLevel == LogLevel::TRACE) {
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
  // Timing instrumentation
  auto t0 = std::chrono::steady_clock::now();
  // Extract grouping columns from the GroupByImpl instance
  const auto& varCols = gb._subtree->getVariableColumns();
  std::vector<ColumnIndex> groupByCols;
  groupByCols.reserve(gb._groupByVariables.size());
  for (const auto& var : gb._groupByVariables) {
    groupByCols.push_back(varCols.at(var).columnIndex_);
  }
  // Direct sampling without full-pass shuffle: pick unique random indices

  // Direct uniform sampling via SlowRandomIntGenerator (unique draws)
  std::unordered_set<size_t> seen;
  ad_utility::SlowRandomIntGenerator<size_t> sampler(
      0, totalSize - 1, ad_utility::RandomSeed::make(42));
  auto t1 = std::chrono::steady_clock::now();
  ad_utility::HashMap<RowKey, size_t> groupCounts;
  while (seen.size() < sampleSize) {
    size_t idx = sampler();
    if (seen.insert(idx).second) {
      ++groupCounts[RowKey{&table, idx, &groupByCols}];
    }
  }
  auto t2 = std::chrono::steady_clock::now();
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
  auto t3 = std::chrono::steady_clock::now();
  // Detailed statistics if requested
  if (logLevel == LogLevel::DEBUG || logLevel == LogLevel::TRACE) {
    AD_LOG_DEBUG << absl::StrFormat(
                        "size=%zu, total=%zu, est=%zu, dObs=%zu, f1=%zu, "
                        "f2=%zu, thr=%.1f",
                        sampleSize, totalSize, estGroups, dObs, f1, f2,
                        totalSize * distinctRatio)
                 << std::endl;
  }
  // Detailed timing breakdown if requested
  if (logLevel == LogLevel::TIMING || logLevel == LogLevel::TRACE) {
    // Timing breakdown (microseconds)
    auto to_us = [](auto d) {
      return std::chrono::duration_cast<std::chrono::microseconds>(d).count();
    };
    AD_LOG_DEBUG << "Timing (us): sampling=" << to_us(t1 - t0)
                 << ", counting=" << to_us(t2 - t1)
                 << ", estimating=" << to_us(t3 - t2)
                 << ", total=" << to_us(t3 - t0) << std::endl;
  }
  return estGroups > totalSize * distinctRatio;
}
