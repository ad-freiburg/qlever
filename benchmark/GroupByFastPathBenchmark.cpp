// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Pascal Keßler
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <memory>
#include <string>
#include <vector>

#include "../benchmark/infrastructure/Benchmark.h"
#include "../test/engine/ValuesForTesting.h"
#include "../test/util/IdTableHelpers.h"
#include "../test/util/IndexTestHelpers.h"
#include "engine/GroupBy.h"
#include "engine/sparqlExpressions/AggregateExpression.h"
#include "engine/sparqlExpressions/LiteralExpression.h"
#include "global/ValueId.h"

namespace ad_benchmark {
using namespace sparqlExpression;

// Measures `GROUP BY` on a single, already-sorted group column without
// `LocalVocabIndex` values -- the case `GroupByImpl::searchBlockBoundaries`'s
// datatype-run fast path applies to. Built on the public `GroupBy` operation
// (`searchBlockBoundaries` itself is private), so this file compiles
// unchanged whether or not that fast path exists, and its measurements can
// be compared directly across commits/branches.
class GroupByFastPathBenchmark : public BenchmarkInterface {
  static constexpr size_t numRows_ = 3'000'000;
  static constexpr size_t numGroups_ = 100'000;

  static IdTable makeSortedGroupTable() {
    IdTable table{2, ad_utility::testing::makeAllocator()};
    table.resize(numRows_);
    size_t rowsPerGroup = (numRows_ + numGroups_ - 1) / numGroups_;
    auto groupCol = table.getColumn(0);
    auto valueCol = table.getColumn(1);
    for (size_t i = 0; i < numRows_; ++i) {
      groupCol[i] =
          ValueId::makeFromInt(static_cast<int64_t>(i / rowsPerGroup));
      valueCol[i] = ValueId::makeFromInt(static_cast<int64_t>(i));
    }
    return table;
  }

 public:
  std::string name() const final {
    return "GROUP BY on a single sorted column (datatype-run fast path)";
  }

  BenchmarkResults runAllBenchmarks() final {
    BenchmarkResults results{};

    auto* qec = ad_utility::testing::getQec();
    IdTable table = makeSortedGroupTable();
    std::vector<std::optional<Variable>> variables{Variable{"?a"},
                                                    Variable{"?b"}};
    auto subtree = ad_utility::makeExecutionTree<ValuesForTesting>(
        qec, std::move(table), variables, false, std::vector<ColumnIndex>{0});

    results.addMeasurement(
        "GROUP BY ?a (AVG(?b)) over 3M rows / 100k groups, sorted, no "
        "LocalVocabIndex",
        [&]() {
          auto expr = std::make_unique<AvgExpression>(
              false, std::make_unique<VariableExpression>(Variable{"?b"}));
          auto alias =
              Alias{SparqlExpressionPimpl{std::move(expr), "AVG(?b)"},
                    Variable{"?x"}};
          GroupBy groupBy{qec, {Variable{"?a"}}, {std::move(alias)}, subtree};
          auto result = groupBy.getResult();
          (void)result->idTableView();
          qec->clearCacheUnpinnedOnly();
        });

    return results;
  }
};

AD_REGISTER_BENCHMARK(GroupByFastPathBenchmark);
}  // namespace ad_benchmark
