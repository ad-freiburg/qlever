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

class HybridGroupByBenchmark : public BenchmarkInterface {
  [[nodiscard]] std::string name() const final {
    return "Hybrid fallback for GROUP BY";
  }

  BenchmarkResults runAllBenchmarks() final {
    BenchmarkResults results{};

    auto* qec = ad_utility::testing::getQec();

    static constexpr size_t kNumRows = 11'000'000;
    static constexpr size_t kChunkSize = 980'450;

    auto makeChunks = [&]() {
      std::vector<IdTable> chunks;
      chunks.reserve((kNumRows + kChunkSize - 1) / kChunkSize);
      ad_utility::SlowRandomIntGenerator<int64_t> gen{0, 1000};
      size_t produced = 0;
      while (produced < kNumRows) {
        size_t nThis = std::min(kChunkSize, kNumRows - produced);
        IdTable t{qec->getAllocator()};
        t.setNumColumns(2);
        t.resize(nThis);
        auto c0 = t.getColumn(0);
        auto c1 = t.getColumn(1);
        for (size_t i = 0; i < nThis; ++i) {
          size_t gIdx = produced + i;

          c0[i] = ValueId::makeFromInt(static_cast<int64_t>(gIdx / 1.2));
          c1[i] = ValueId::makeFromInt(gen());
        }
        chunks.push_back(std::move(t));
        produced += nThis;
      }
      return chunks;
    };

    auto buildSortedSubtree =
        [&](bool chunked) -> std::shared_ptr<QueryExecutionTree> {
      std::vector<std::optional<Variable>> vars = {Variable{"?a"},
                                                   Variable{"?b"}};
      std::shared_ptr<QueryExecutionTree> valuesTree;
      if (chunked) {
        auto chunks = makeChunks();
        // DON'T advertise sortedColumns - let the explicit Sort do the work
        valuesTree = ad_utility::makeExecutionTree<ValuesForTesting>(
            qec, std::move(chunks), vars, /*mayHaveUnbound=*/false);
      } else {
        IdTable table{qec->getAllocator()};
        table.setNumColumns(2);
        table.resize(kNumRows);
        auto col1 = table.getColumn(0);
        for (size_t i = 0; i < col1.size(); ++i) {
          col1[i] = ValueId::makeFromInt(static_cast<int64_t>(i / 1.2));
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
    };

    struct Scenario {
      std::string name_;
      bool useHashMap_;
      size_t threshold_;
      std::string note_;
      bool chunked_;
      std::string mergeMethod_ = "merge";  // "merge" or "sort"
    };

    constexpr size_t kHugeThreshold = std::numeric_limits<size_t>::max() / 4;
    constexpr size_t kTinyThreshold = 150'000;

    std::vector<Scenario> scenarios = {
        {"sort-only", false, kHugeThreshold,
         "Optimization disabled (sorting path)", false},
        {"hash-only", true, kHugeThreshold,
         "Hash map enabled, fallback effectively disabled", false},
        {"hybrid-sort", true, kTinyThreshold, "Hash map with early fallback",
         false, "sort"},
        {"hybrid-merge", true, kTinyThreshold, "Hash map with early fallback",
         false, "merge"},
        {"sort-only-chunked", false, kHugeThreshold,
         "Optimization disabled (sorting path)", true},
        {"hash-only-chunked", true, kHugeThreshold,
         "Hash map enabled, fallback effectively disabled", true},
        {"hybrid-chunked-sort", true, kTinyThreshold,
         "Hash map with early fallback", true, "sort"},
        {"hybrid-chunked-merge", true, kTinyThreshold,
         "Hash map with early fallback", true, "merge"},
    };

    size_t numMeasurements = 3;
    for (const auto& s : scenarios) {
      RuntimeParameters().set<"group-by-hash-map-enabled">(s.useHashMap_);
      RuntimeParameters().set<"group-by-hash-map-group-threshold">(
          s.threshold_);
      RuntimeParameters().set<"group-by-hybrid-merge-strategy">(s.mergeMethod_);

      auto& group = results.addGroup(s.name_);
      group.metadata().addKeyValuePair("Rows", kNumRows);
      group.metadata().addKeyValuePair("Sorted", false);
      group.metadata().addKeyValuePair("HashMapEnabled", s.useHashMap_);
      group.metadata().addKeyValuePair("Threshold", s.threshold_);
      group.metadata().addKeyValuePair("Note", s.note_);
      group.metadata().addKeyValuePair("MergeStrategy", s.mergeMethod_);

      for (size_t i = 0; i < numMeasurements; ++i) {
        std::unordered_map<std::string, std::string> timingsForMeasurement;

        auto& measurement = group.addMeasurement(std::to_string(i), [&]() {
          // Build fresh tree for each measurement to ensure clean state
          auto sortedTree = buildSortedSubtree(s.chunked_);
          timingsForMeasurement = runGroupByCount(qec, sortedTree);
        });

        // After the measured function has run, attach all timing key/values
        // to the metadata of this measurement.
        for (const auto& [key, value] : timingsForMeasurement) {
          measurement.metadata().addKeyValuePair(key, value);
        }
      }
    }

    return results;
  }
};

AD_REGISTER_BENCHMARK(HybridGroupByBenchmark);
}  // namespace ad_benchmark
