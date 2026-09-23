// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Pascal Keßler
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <algorithm>
#include <cstddef>
#include <numeric>
#include <random>
#include <vector>

#include "../benchmark/infrastructure/Benchmark.h"
#include "../benchmark/infrastructure/BenchmarkMeasurementContainer.h"
#include "../test/util/IdTableHelpers.h"
#include "engine/idTable/IdTable.h"
#include "global/ValueId.h"
#include "index/IdTableUtils.h"

namespace ad_benchmark {

// Measures operations on `IdTable`/`IdColumn` whose cost plausibly changes
// between a contiguous `Id[]` column layout and split-column storage
// (separate payload/datatype arrays, see `IdColumn.h`): sequential scans,
// random-order access, full-row materialization, and sorting. This file is
// written to compile unchanged against both the pre- and post-split-column
// `IdTable` (only its public interface is used: `getColumn`, `operator()`,
// `numRows`/`numColumns`, `ql::ranges::sort`), so the exact same source can
// be built on both branches and its measurements compared directly.
//
// Every measurement has a genuine memory-writing side effect (copying into
// an output buffer, or reordering the table itself), so the compiler cannot
// optimize the work away without a `DoNotOptimize`-equivalent, which this
// benchmark infrastructure does not provide (see the note at the top of
// `BenchmarkExamples.cpp`).
class IdTableColumnBenchmark : public BenchmarkInterface {
  static constexpr size_t numRows_ = 3'000'000;
  static constexpr size_t numColumns_ = 4;

 public:
  std::string name() const final {
    return "IdTable/IdColumn operations (split-column storage impact)";
  }

  BenchmarkResults runAllBenchmarks() final {
    BenchmarkResults results{};

    // A fixed random row order, used by the random-access measurement below,
    // so every run (and both branches) exercise the exact same access
    // pattern.
    std::vector<size_t> shuffledRowIndices(numRows_);
    std::iota(shuffledRowIndices.begin(), shuffledRowIndices.end(), 0);
    std::mt19937 rng{42};
    std::shuffle(shuffledRowIndices.begin(), shuffledRowIndices.end(), rng);

    {
      IdTable table = createRandomlyFilledIdTable(numRows_, numColumns_);
      std::vector<Id> output;
      output.reserve(numRows_);
      results.addMeasurement(
          "Sequential column scan (copy column 0 into a vector)",
          [&table, &output]() {
            output.clear();
            for (const Id& id : table.getColumn(0)) {
              output.push_back(id);
            }
          });
    }

    {
      IdTable table = createRandomlyFilledIdTable(numRows_, numColumns_);
      std::vector<Id> output(numRows_);
      results.addMeasurement(
          "Random-order access (copy column 0 in shuffled row order)",
          [&table, &output, &shuffledRowIndices]() {
            const auto& column = table.getColumn(0);
            for (size_t i = 0; i < numRows_; ++i) {
              output[i] = column[shuffledRowIndices[i]];
            }
          });
    }

    {
      IdTable table = createRandomlyFilledIdTable(numRows_, numColumns_);
      results.addMeasurement(
          "Full-row materialization (copy every cell into a flat vector, "
          "row-major)",
          [&table]() {
            std::vector<Id> flatRows;
            flatRows.reserve(numRows_ * numColumns_);
            for (size_t row = 0; row < numRows_; ++row) {
              for (size_t col = 0; col < numColumns_; ++col) {
                flatRows.push_back(table(row, col));
              }
            }
          });
    }

    {
      // A fresh, unsorted table: sorting is destructive, so this can't
      // share a table with the measurements above. Goes through
      // `IdTableUtils::sort`, the same entry point the real `Sort` operator
      // uses (see `Sort.cpp`), rather than a raw `ql::ranges::sort`, so this
      // measurement actually reflects any single-column sort fast path
      // `IdTableUtils::sort` may dispatch to.
      IdTable table = createRandomlyFilledIdTable(numRows_, numColumns_);
      results.addMeasurement(
          "Sort the whole table by its first column",
          [&table]() { IdTableUtils::sort(table, {ColumnIndex{0}}); });
    }

    return results;
  }
};

AD_REGISTER_BENCHMARK(IdTableColumnBenchmark);
}  // namespace ad_benchmark
