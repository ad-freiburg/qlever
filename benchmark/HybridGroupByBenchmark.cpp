// Copyright 2025, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Benke Hargitai <hargitab@cs.uni-freiburg.de>

#include <algorithm>
#include <cmath>
#include <functional>
#include <iostream>
#include <limits>
#include <random>
#include <string>
#include <unordered_map>
#include <vector>

#include "../benchmark/infrastructure/Benchmark.h"
#include "../test/engine/GroupByStrategyHelpers.h"
#include "../test/engine/ValuesForTesting.h"
#include "../test/util/IdTableHelpers.h"
#include "../test/util/IndexTestHelpers.h"
#include "absl/strings/str_cat.h"
#include "engine/GroupBy.h"
#include "engine/Sort.h"
#include "engine/Values.h"
#include "engine/sparqlExpressions/AggregateExpression.h"
#include "global/RuntimeParameters.h"
#include "util/Log.h"
#include "util/Random.h"

namespace ad_benchmark {
using namespace sparqlExpression;

static SparqlExpression::Ptr makeVariableExpression(const Variable& var) {
  return std::make_unique<VariableExpression>(var);
}

// Run a simple GROUP BY ?a with COUNT(?b) as ?x on the provided subtree.
// Returns a map of detailed timing information from the operation's runtime
// info.
static std::unordered_map<std::string, std::string> runGroupByCount(
    QueryExecutionContext* qec,
    const std::shared_ptr<QueryExecutionTree>& subtree) {
  auto expr = std::make_unique<CountExpression>(
      /*distinct=*/false, makeVariableExpression(Variable{"?b"}));
  Alias alias{SparqlExpressionPimpl{std::move(expr), "COUNT(?b)"},
              Variable{"?x"}};
  GroupBy groupBy{qec, {Variable{"?a"}}, {std::move(alias)}, subtree};
  auto result = groupBy.getResult();
  (void)result->idTable();

  // Extract detailed timing information
  auto& runtimeInfo = groupBy.runtimeInfo();
  std::unordered_map<std::string, std::string> timings;
  for (const auto& [key, value] : runtimeInfo.details_.items()) {
    timings[key] = value.dump();
  }

  qec->clearCacheUnpinnedOnly();
  return timings;
}

// Generic group mapping: given a row index `i`, a requested (uniform) group
// count `numGroups` (ignored by skewed distributions), and the total number of
// rows `numRows`, return the group id.
using GroupFunc =
    std::function<int64_t(size_t i, size_t numGroups, size_t numRows)>;

struct ParameterSample {
  size_t value;
  bool end;
};

using ParameterFunc = std::function<ParameterSample(size_t index)>;

using NumGroupsFunc = ParameterFunc;
using NumRowsFunc = ParameterFunc;
using ThresholdFunc = ParameterFunc;

static GroupFunc makeModuloGrouping() {
  return [](size_t i, size_t numGroups, size_t /*numRows*/) {
    // Standard uniform grouping by modulo.
    if (numGroups == 0) return int64_t{0};
    return static_cast<int64_t>(i % numGroups);
  };
}

static GroupFunc makeBestCaseGrouping() {
  return [](size_t i, size_t /*numGroups*/, size_t numRows) {
    size_t half = numRows / 2;
    if (i < half) {
      return static_cast<int64_t>(i);
    } else {
      return static_cast<int64_t>(i % 5);
    }
  };
}

static GroupFunc makeWorstCaseGrouping() {
  // first half of the rows are unique groups, the other half all map to 5
  // groups all different from the first half
  return [](size_t i, size_t /*numGroups*/, size_t numRows) {
    size_t half = numRows / 2;
    if (i < half) {
      return static_cast<int64_t>(i);
    } else {
      return static_cast<int64_t>(half + (i % 5));
    }
  };
}

static NumGroupsFunc makeLinearNumGroupsFunc(size_t maxValue,
                                             size_t steps = 30) {
  return [maxValue, steps](size_t i) {
    size_t clampedMax = std::max<size_t>(1, maxValue);
    size_t totalSteps = std::max<size_t>(1, steps);
    size_t stepSize = std::max<size_t>(1, clampedMax / totalSteps);
    size_t value = std::min<size_t>(clampedMax, (i + 1) * stepSize);
    bool end = (i + 1 >= totalSteps) || value >= clampedMax;
    return ParameterSample{value, end};
  };
}

static NumGroupsFunc makeExponentialNumGroupsFunc(size_t maxValue,
                                                  size_t steps = 30,
                                                  double base = 1.5) {
  return [maxValue, steps, base](size_t i) {
    size_t clampedMax = std::max<size_t>(1, maxValue);
    size_t totalSteps = std::max<size_t>(1, steps);
    size_t value = 1;
    for (size_t step = 0; step < i && value < clampedMax; ++step) {
      value = std::ceil(value * base);
    }
    bool end = (i + 1 >= totalSteps) || value >= clampedMax;
    return ParameterSample{value, end};
  };
}

static ParameterFunc makeConstantParameterFunc(size_t value) {
  return [value](size_t) {
    size_t capped = std::max<size_t>(1, value);
    return ParameterSample{capped, true};
  };
}

std::vector<IdTable> makeBlocks(size_t numRows, size_t blockSize,
                                size_t numGroups, QueryExecutionContext* qec,
                                const GroupFunc& groupFunc) {
  std::vector<IdTable> blocks;
  blocks.reserve((numRows + blockSize - 1) / blockSize);
  ad_utility::SlowRandomIntGenerator<int64_t> gen{0, 1000};
  size_t produced = 0;
  while (produced < numRows) {
    size_t nThis = std::min(blockSize, numRows - produced);
    IdTable t{qec->getAllocator()};
    t.setNumColumns(2);
    t.resize(nThis);
    auto c0 = t.getColumn(0);
    auto c1 = t.getColumn(1);
    for (size_t i = 0; i < nThis; ++i) {
      size_t gIdx = produced + i;
      c0[i] = ValueId::makeFromInt(groupFunc(gIdx, numGroups, numRows));
      c1[i] = ValueId::makeFromInt(gen());
    }
    blocks.push_back(std::move(t));
    produced += nThis;
  }
  return blocks;
}

std::shared_ptr<QueryExecutionTree> buildSortedSubtree(
    bool useBlocks, size_t numRows, size_t blockSize, size_t numGroups,
    QueryExecutionContext* qec, const GroupFunc& groupFunc) {
  std::vector<std::optional<Variable>> vars = {Variable{"?a"}, Variable{"?b"}};
  std::shared_ptr<QueryExecutionTree> valuesTree;
  if (useBlocks) {
    auto blocks = makeBlocks(numRows, blockSize, numGroups, qec, groupFunc);
    // DON'T advertise sortedColumns - let the explicit Sort do the work
    valuesTree = ad_utility::makeExecutionTree<ValuesForTesting>(
        qec, std::move(blocks), vars, /*mayHaveUnbound=*/false);
  } else {
    IdTable table{qec->getAllocator()};
    table.setNumColumns(2);
    table.resize(numRows);
    auto col1 = table.getColumn(0);
    for (size_t i = 0; i < col1.size(); ++i) {
      col1[i] = ValueId::makeFromInt(groupFunc(i, numGroups, numRows));
    }
    auto col2 = table.getColumn(1);
    ad_utility::SlowRandomIntGenerator<int64_t> gen{0, 1000};
    for (size_t i = 0; i < col2.size(); ++i) {
      col2[i] = ValueId::makeFromInt(gen());
    }
    // DON'T advertise sortedColumns - let the explicit Sort do the work
    valuesTree = ad_utility::makeExecutionTree<ValuesForTesting>(
        qec, std::move(table), vars, /*mayHaveUnbound=*/false);
  }

  // Force an explicit Sort operation to ensure "Child is a Sort"
  std::vector<ColumnIndex> sortColumns = {ColumnIndex{0}};
  auto sortTree =
      ad_utility::makeExecutionTree<Sort>(qec, valuesTree, sortColumns);
  return sortTree;
}

// _____________________________________________________________________________

struct Strategy {
  std::string name_;
  bool useHashMap_;
  std::string note_;
  bool useBlocks_;
  bool hybrid_ = false;
};

struct Scenario {
  std::string name_;
  NumGroupsFunc numGroupsFunc_;
  NumRowsFunc numRowsFunc_;
  ThresholdFunc thresholdFunc_;
  GroupFunc groupFunc_;
  float blockSizeFactor_ = 0.08F;
  size_t numMeasurements_ = 30;
};

struct BenchmarkConfig {
  size_t numGroups;
  size_t numRows;
  size_t threshold;
  size_t blockSize;
};

static std::vector<size_t> sampleParameterValues(const ParameterFunc& func) {
  std::vector<size_t> values;
  size_t idx = 0;
  while (true) {
    ParameterSample sample = func(idx);
    values.push_back(sample.value);
    if (sample.end) break;
    ++idx;
  }
  return values;
}

static std::vector<size_t> sampleNumRows(const Scenario& scenario) {
  return sampleParameterValues(scenario.numRowsFunc_);
}

static std::vector<size_t> sampleNumGroups(const Scenario& scenario) {
  return sampleParameterValues(scenario.numGroupsFunc_);
}

static std::vector<size_t> computeThresholds(const Scenario& scenario,
                                             const Strategy& strategy) {
  if (!strategy.hybrid_) {
    return {std::numeric_limits<size_t>::max()};
  }
  return sampleParameterValues(scenario.thresholdFunc_);
  std::vector<size_t> thresholds;
}

static size_t computeBlockSize(const Scenario& scenario, size_t numRows) {
  size_t blockSize = static_cast<size_t>(numRows * scenario.blockSizeFactor_);
  return std::max<size_t>(1, blockSize);
}

static std::vector<BenchmarkConfig> buildBenchmarkPlan(
    const Scenario& scenario, const Strategy& strategy) {
  std::vector<BenchmarkConfig> configs;
  const auto numRowsSamples = sampleNumRows(scenario);
  const auto numGroupSamples = sampleNumGroups(scenario);
  for (size_t numGroups : numGroupSamples) {
    for (size_t numRows : numRowsSamples) {
      const auto thresholds = computeThresholds(scenario, strategy);
      const size_t blockSize = computeBlockSize(scenario, numRows);
      for (size_t threshold : thresholds) {
        configs.push_back(
            BenchmarkConfig{numGroups, numRows, threshold, blockSize});
      }
    }
  }
  return configs;
}

// _____________________________________________________________________________

class HybridGroupByBenchmark : public BenchmarkInterface {
  [[nodiscard]] std::string name() const final {
    return "Hybrid fallback for GROUP BY";
  }

  BenchmarkResults runAllBenchmarks() final {
    BenchmarkResults results{};

    auto* qec = ad_utility::testing::getQec();

    const std::vector<Strategy> strategies = {
        {
            "sort-only",
            /*useHashMap*/ false,
            "Optimization disabled (sorting path)",
            /*useBlocks*/ false,
        },
        {
            "hash-only",
            /*useHashMap*/ true,
            "Hash map enabled, fallback effectively disabled",
            /*useBlocks*/ false,
        },
        {
            "hybrid-approach",
            /*useHashMap*/ true,
            "Hash map with early fallback",
            /*useBlocks*/ false,
            /*hybrid*/ true,
        },
        {
            "sort-only-blocks",
            /*useHashMap*/ false,
            "Optimization disabled (sorting path)",
            /*useBlocks*/ true,
        },
        {
            "hash-only-blocks",
            /*useHashMap*/ true,
            "Hash map enabled, fallback effectively disabled",
            /*useBlocks*/ true,
        },
        {
            "hybrid-approach-blocks",
            /*useHashMap*/ true,
            "Hash map with early fallback",
            /*useBlocks*/ true,
            /*hybrid*/ true,
        },
    };

    const std::vector<Scenario> scenarios = {
        {
            "uniform-1.2-million",
            makeLinearNumGroupsFunc(1'200'000),
            /*numRows*/ makeConstantParameterFunc(1'200'000),
            /*threshold*/ makeConstantParameterFunc(350'000),
            makeModuloGrouping(),
        },
        {
            "uniform-12-million",
            makeLinearNumGroupsFunc(12'000'000),
            /*numRows*/ makeConstantParameterFunc(12'000'000),
            /*threshold*/ makeConstantParameterFunc(350'000),
            makeModuloGrouping(),
        },
        {
            "logscale-1.2-million",
            makeExponentialNumGroupsFunc(1'200'000),
            /*numRows*/ makeConstantParameterFunc(1'200'000),
            /*threshold*/ makeConstantParameterFunc(350'000),
            makeModuloGrouping(),
        },
        {
            "logscale-12-million",
            makeExponentialNumGroupsFunc(12'000'000),
            /*numRows*/ makeConstantParameterFunc(12'000'000),
            /*threshold*/ makeConstantParameterFunc(350'000),
            makeModuloGrouping(),
        },
        {
            "best-case-grouping",
            /*numGroups*/ makeConstantParameterFunc(1),
            /*numRows*/ makeConstantParameterFunc(12'000'000),
            /*threshold*/ makeConstantParameterFunc(350'000),
            makeBestCaseGrouping(),
        },
        {
            "worst-case-grouping",
            /*numGroups*/ makeConstantParameterFunc(1),
            /*numRows*/ makeConstantParameterFunc(12'000'000),
            /*threshold*/ makeConstantParameterFunc(350'000),
            makeWorstCaseGrouping(),
        },
    };

    Scenario scenario = scenarios[2];

    for (const auto& strategy : strategies) {
      RuntimeParameters().set<"group-by-hash-map-enabled">(
          strategy.useHashMap_);

      const auto plan = buildBenchmarkPlan(scenario, strategy);
      for (const auto& config : plan) {
        RuntimeParameters().set<"group-by-hash-map-group-threshold">(
            config.threshold);
        const std::string subName = absl::StrCat(
            strategy.name_, "|rows=", config.numRows,
            "|block=", config.blockSize, "|thresh=", config.threshold,
            "|groups=", config.numGroups);
        auto& group = results.addGroup(subName);
        group.metadata().addKeyValuePair("ParentGroup", strategy.name_);
        group.metadata().addKeyValuePair("Scenario", scenario.name_);
        group.metadata().addKeyValuePair("Sorted", false);
        group.metadata().addKeyValuePair("HashMapEnabled",
                                         strategy.useHashMap_);
        group.metadata().addKeyValuePair("Threshold", config.threshold);
        group.metadata().addKeyValuePair("Note", strategy.note_);
        group.metadata().addKeyValuePair("Rows", config.numRows);
        group.metadata().addKeyValuePair("BlockSize", config.blockSize);
        group.metadata().addKeyValuePair("Groups", config.numGroups);

        for (size_t i = 0; i < scenario.numMeasurements_; ++i) {
          std::unordered_map<std::string, std::string> timingsForMeasurement;

          auto& measurement = group.addMeasurement(std::to_string(i), [&]() {
            auto sortedTree = buildSortedSubtree(
                strategy.useBlocks_, config.numRows, config.blockSize,
                config.numGroups, qec, scenario.groupFunc_);
            timingsForMeasurement = runGroupByCount(qec, sortedTree);
          });

          for (const auto& [key, value] : timingsForMeasurement) {
            measurement.metadata().addKeyValuePair(key, value);
          }
        }
      }
    }

    return results;
  }
};

AD_REGISTER_BENCHMARK(HybridGroupByBenchmark);
}  // namespace ad_benchmark
