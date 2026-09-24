// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Pascal Keßler
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <string>
#include <vector>

#include "../benchmark/infrastructure/Benchmark.h"
#include "../test/engine/ValuesForTesting.h"
#include "../test/util/IdTableHelpers.h"
#include "../test/util/IndexTestHelpers.h"
#include "engine/JoinImpl.h"
#include "global/ValueId.h"

namespace ad_benchmark {

// Measures a merge join of two large, already-sorted tables on a single
// `IntId` join column without UNDEF values -- the case the datatype-aware
// zipper-join kernel (`ZipperJoiner`) applies to. Built on the public
// `JoinImpl` operation, so this file compiles unchanged whether or not that
// kernel exists, and its measurements can be compared directly across
// commits/branches.
class JoinFastPathBenchmark : public BenchmarkInterface {
  static constexpr size_t numRowsLeft_ = 1'000'000;
  static constexpr size_t numRowsRight_ = 1'000'000;

  // A two-column, sorted table with join-column values `offset + i * stride`
  // and an unrelated payload column.
  static IdTable makeSortedJoinTable(size_t numRows, int64_t stride,
                                     int64_t offset) {
    IdTable table{2, ad_utility::testing::makeAllocator()};
    table.resize(numRows);
    auto joinCol = table.getColumn(0);
    auto valueCol = table.getColumn(1);
    for (size_t i = 0; i < numRows; ++i) {
      joinCol[i] =
          ValueId::makeFromInt(offset + static_cast<int64_t>(i) * stride);
      valueCol[i] = ValueId::makeFromInt(static_cast<int64_t>(i));
    }
    return table;
  }

 public:
  std::string name() const final {
    return "Join on a single sorted column without UNDEF (datatype-aware "
           "zipper-join kernel)";
  }

  BenchmarkResults runAllBenchmarks() final {
    BenchmarkResults results{};

    auto* qec = ad_utility::testing::getQec();
    // Left: 0, 1, 2, ...; right: 0, 2, 4, ... -- every right row matches a
    // left row, every other left row is unmatched (~50% match rate).
    IdTable left = makeSortedJoinTable(numRowsLeft_, 1, 0);
    IdTable right = makeSortedJoinTable(numRowsRight_, 2, 0);

    // The join column must share the same variable name on both sides (see
    // `JoinImpl`'s constructor); the other column may differ.
    std::vector<std::optional<Variable>> leftVars{Variable{"?a"},
                                                   Variable{"?x"}};
    std::vector<std::optional<Variable>> rightVars{Variable{"?a"},
                                                    Variable{"?y"}};
    auto leftTree = ad_utility::makeExecutionTree<ValuesForTesting>(
        qec, std::move(left), leftVars, false, std::vector<ColumnIndex>{0});
    auto rightTree = ad_utility::makeExecutionTree<ValuesForTesting>(
        qec, std::move(right), rightVars, false, std::vector<ColumnIndex>{0});

    results.addMeasurement(
        "Join 1M x 1M rows on a single IntId column, ~50% match rate, no "
        "UNDEF",
        [&]() {
          JoinImpl join{qec, leftTree, rightTree, 0, 0, true, false};
          auto result = join.getResult();
          (void)result->idTableView();
          qec->clearCacheUnpinnedOnly();
        });

    return results;
  }
};

AD_REGISTER_BENCHMARK(JoinFastPathBenchmark);
}  // namespace ad_benchmark
