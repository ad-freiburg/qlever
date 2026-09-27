// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of this project.

#include <absl/strings/str_cat.h>

#include <array>
#include <string>
#include <utility>
#include <vector>

#include "../benchmark/infrastructure/Benchmark.h"
#include "../benchmark/infrastructure/BenchmarkMetadata.h"
#include "backports/span.h"
#include "engine/Result.h"
#include "engine/export_v2/VectorStreamSource.h"
#include "engine/idTable/IdTable.h"
#include "global/Id.h"
#include "util/MemorySize/MemorySize.h"

using namespace ql::engine::export_v2;

namespace ad_benchmark {

namespace {

// Create `numBlocks` blocks of `rowsPerBlock` rows and `numCols` columns,
// simulating the lazy output of an upstream operator. Column `c` of row `r`
// holds vocab index `r % (10 * 5^c)`, so column 1 has 50 distinct values.
std::vector<Result::IdTableVocabPair> createSyntheticBlocks(size_t numBlocks,
                                                            size_t rowsPerBlock,
                                                            size_t numCols) {
  ad_utility::AllocatorWithLimit<Id> allocator{
      ad_utility::makeAllocatorWithLimit<Id>(ad_utility::MemorySize::max())};
  std::vector<size_t> moduli;
  size_t modulus = 10;
  for (size_t col = 0; col < numCols; ++col) {
    moduli.push_back(modulus);
    modulus *= 5;
  }
  std::vector<Result::IdTableVocabPair> blocks;
  blocks.reserve(numBlocks);
  for (size_t block = 0; block < numBlocks; ++block) {
    IdTable table{numCols, allocator};
    table.resize(rowsPerBlock);
    for (size_t col = 0; col < numCols; ++col) {
      auto column = table.getColumn(col);
      for (size_t row = 0; row < rowsPerBlock; ++row) {
        column[row] =
            Id::makeFromVocabIndex(VocabIndex::make(row % moduli[col]));
      }
    }
    blocks.emplace_back(std::move(table), LocalVocab{});
  }
  return blocks;
}

// Time `source.run` over `blocks` inside the benchmark infrastructure and
// record the number of input and output rows as metadata, so that throughput
// can be derived from the reported time with an explicit basis.
void measureRun(BenchmarkResults& results, const std::string& descriptor,
                const VectorStreamSource& source,
                const std::vector<Result::IdTableVocabPair>& blocks,
                size_t numInputRows,
                ql::span<const EqualityFilter> filters = {}) {
  size_t numOutputRows = 0;
  auto sink = [&numOutputRows](const Result::IdTableVocabPair& chunk) {
    numOutputRows += chunk.idTable_.numRows();
  };
  auto& entry = results.addMeasurement(
      descriptor, [&]() { source.run(blocks, sink, filters); });
  entry.metadata().addKeyValuePair("inputRows", numInputRows);
  entry.metadata().addKeyValuePair("outputRows", numOutputRows);
}

// Rechunk 50M rows into chunks of different sizes, without filters.
void benchmarkChunkSizeSweep(BenchmarkResults& results) {
  const size_t totalRows = 50'000'000;
  const size_t numBlocks = 500;
  const auto blocks =
      createSyntheticBlocks(numBlocks, totalRows / numBlocks, 3);
  for (size_t chunkSize : {64, 256, 1024, 4096, 8192, 16384, 65536}) {
    VectorStreamSource source{VectorStreamConfig{RowsPerChunk{chunkSize}}};
    measureRun(results, absl::StrCat("ChunkSize_", chunkSize), source, blocks,
               totalRows);
  }
}

// Rechunk 20M rows with no filter and with an equality filter that keeps 2% of
// the rows (column 1 has 50 distinct values).
void benchmarkFilterSelectivity(BenchmarkResults& results) {
  const size_t totalRows = 20'000'000;
  const size_t numBlocks = 200;
  const auto blocks =
      createSyntheticBlocks(numBlocks, totalRows / numBlocks, 3);
  VectorStreamSource source{VectorStreamConfig{RowsPerChunk{8192}}};
  measureRun(results, "Filter_100%_NoFilter", source, blocks, totalRows);
  const std::array filters{
      EqualityFilter{1, Id::makeFromVocabIndex(VocabIndex::make(42))}};
  measureRun(results, "Filter_2%_ExactMatch", source, blocks, totalRows,
             filters);
}

}  // namespace

class VectorStreamSourceBenchmark : public BenchmarkInterface {
 public:
  std::string name() const final { return "WP2 VectorStreamSource Benchmarks"; }

  BenchmarkResults runAllBenchmarks() final {
    BenchmarkResults results{};
    benchmarkChunkSizeSweep(results);
    benchmarkFilterSelectivity(results);
    return results;
  }
};

AD_REGISTER_BENCHMARK(VectorStreamSourceBenchmark);

}  // namespace ad_benchmark
