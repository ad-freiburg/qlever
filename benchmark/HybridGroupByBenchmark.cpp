// Copyright 2025, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Benke Hargitai <hargitab@cs.uni-freiburg.de>

#include <iostream>
#include <limits>
#include <random>
#include <unordered_map>

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

std::vector<IdTable> makeBlocks(size_t numRows, size_t blockSize,
                                size_t numGroups, QueryExecutionContext* qec) {
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

      c0[i] = ValueId::makeFromInt(static_cast<int64_t>(gIdx % numGroups));
      c1[i] = ValueId::makeFromInt(gen());
    }
    blocks.push_back(std::move(t));
    produced += nThis;
  }
  return blocks;
}

std::shared_ptr<QueryExecutionTree> buildSortedSubtree(
    bool useBlocks, size_t numRows, size_t blockSize, size_t numGroups,
    QueryExecutionContext* qec) {
  std::vector<std::optional<Variable>> vars = {Variable{"?a"}, Variable{"?b"}};
  std::shared_ptr<QueryExecutionTree> valuesTree;
  if (useBlocks) {
    auto blocks = makeBlocks(numRows, blockSize, numGroups, qec);
    // DON'T advertise sortedColumns - let the explicit Sort do the work
    valuesTree = ad_utility::makeExecutionTree<ValuesForTesting>(
        qec, std::move(blocks), vars, /*mayHaveUnbound=*/false);
  } else {
    IdTable table{qec->getAllocator()};
    table.setNumColumns(2);
    table.resize(numRows);
    auto col1 = table.getColumn(0);
    for (size_t i = 0; i < col1.size(); ++i) {
      col1[i] = ValueId::makeFromInt(static_cast<int64_t>(i % numGroups));
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

class HybridGroupByBenchmark : public BenchmarkInterface {
  [[nodiscard]] std::string name() const final {
    return "Hybrid fallback for GROUP BY";
  }

  BenchmarkResults runAllBenchmarks() final {
    BenchmarkResults results{};

    auto* qec = ad_utility::testing::getQec();

    struct Scenario {
      std::string name_;
      bool useHashMap_;
      std::string note_;
      bool useBlocks_;
      bool hybrid_ = false;
    };

    constexpr size_t hugeThreshold = std::numeric_limits<size_t>::max() / 4;

    std::vector<Scenario> scenarios = {
        {"sort-only", /*useHashMap*/ false,
         "Optimization disabled (sorting path)", /*useBlocks*/ false},
        {"hash-only", /*useHashMap*/ true,
         "Hash map enabled, fallback effectively disabled",
         /*useBlocks*/ false},
        {"hybrid-approach", /*useHashMap*/ true, "Hash map with early fallback",
         /*useBlocks*/ false,
         /*hybrid*/ true},
        {"sort-only-blocks", /*useHashMap*/ false,
         "Optimization disabled (sorting path)",
         /*useBlocks*/ true},
        {"hash-only-blocks", /*useHashMap*/ true,
         "Hash map enabled, fallback effectively disabled", /*useBlocks*/ true},
        {"hybrid-approach-blocks", /*useHashMap*/ true,
         "Hash map with early fallback",
         /*useBlocks*/ true,
         /*hybrid*/ true},
    };

    size_t numMeasurements = 1;
    size_t startNumRows = 1'000'000;
    size_t endNumRows = 1'000'000;
    float blockSizeFactor = 0.08;
    size_t startThreshold = 100'000;
    size_t endThreshold = 100'000;
    size_t totalNumGroups = 1'000'000;
    size_t groupSegments = 20;

    for (const auto& s : scenarios) {
      RuntimeParameters().set<"group-by-hash-map-enabled">(s.useHashMap_);

      size_t numGroups = totalNumGroups / groupSegments;
      while (numGroups <= totalNumGroups) {
        size_t numRows = startNumRows;
        while (numRows <= endNumRows) {
          size_t blockSize = size_t(numRows * blockSizeFactor);

          // For hybrid scenarios, vary the threshold; for others, run once with
          // a huge threshold to effectively disable fallback.
          std::vector<size_t> thresholds;
          if (s.hybrid_) {
            for (size_t t = startThreshold; t <= endThreshold && t < numRows;
                 t *= 10) {
              thresholds.push_back(t);
            }
          } else {
            thresholds.push_back(hugeThreshold);
          }

          for (size_t threshold : thresholds) {
            RuntimeParameters().set<"group-by-hash-map-group-threshold">(
                threshold);
            // Create a group per parameter setting (Rows x BlockSize).
            std::string subName =
                absl::StrCat(s.name_, "|rows=", numRows, "|block=", blockSize,
                             "|thresh=", threshold, "|groups=", numGroups);
            auto& group = results.addGroup(subName);
            // Inherit scenario metadata and add per-config values.
            group.metadata().addKeyValuePair("ParentGroup", s.name_);
            group.metadata().addKeyValuePair("Sorted", false);
            group.metadata().addKeyValuePair("HashMapEnabled", s.useHashMap_);
            group.metadata().addKeyValuePair("Threshold", threshold);
            group.metadata().addKeyValuePair("Note", s.note_);
            group.metadata().addKeyValuePair("Rows", numRows);
            group.metadata().addKeyValuePair("BlockSize", blockSize);

            for (size_t i = 0; i < numMeasurements; ++i) {
              std::unordered_map<std::string, std::string>
                  timingsForMeasurement;

              auto& measurement =
                  group.addMeasurement(std::to_string(i), [&]() {
                    auto sortedTree = buildSortedSubtree(
                        s.useBlocks_, numRows, blockSize, numGroups, qec);
                    timingsForMeasurement = runGroupByCount(qec, sortedTree);
                  });

              // After the measured function has run, attach all timing
              // key/values to the metadata of this measurement.
              for (const auto& [key, value] : timingsForMeasurement) {
                measurement.metadata().addKeyValuePair(key, value);
              }
            }
          }
          numRows *= 10;
        }
        numGroups += totalNumGroups / groupSegments;
      }
    }

    return results;
  }
};

AD_REGISTER_BENCHMARK(HybridGroupByBenchmark);
}  // namespace ad_benchmark
