// Copyright 2025 The QLever Authors, in particular:
//
// 2025 Julian Mundhahs <mundhahj@tf.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <algorithm>
#include <array>
#include <string>
#include <utility>
#include <vector>

#include "../benchmark/infrastructure/Benchmark.h"
#include "../benchmark/infrastructure/BenchmarkMeasurementContainer.h"
#include "index/LocatedTriples.h"
#include "util/Random.h"

namespace ad_benchmark {

// Benchmarks the effect of
// `BlockSortedLocatedTriplesVectorImpl::kTargetBlockSize` on the performance of
// insert (in batches), copy, and iteration.
//
// Block sizes tested: 10, 20, 50, 85, 200, 500, 1000, 4000, 16000.
// (85 is the largest value such that a block fits in a single memory page.)
class BlockSizeLocatedTriplesBenchmark : public BenchmarkInterface {
 private:
  static constexpr std::array<size_t, 9> kBlockSizes = {
      10, 20, 50, 85, 200, 500, 1000, 4000, 16000};

  // Total element counts for the copy and iterate benchmarks.
  std::vector<size_t> nValues_ = {1'000,     10'000,     100'000,
                                  1'000'000, 10'000'000, 50'000'000};

  // Total element counts for the insert benchmark.
  std::vector<size_t> insertNValues_ = {1'000, 10'000, 100'000, 1'000'000};

  // All candidate batch sizes. A batch size is only used for a given N if
  // N / batchSize <= 50'000 (to keep runtimes manageable) and batchSize <= N.
  std::vector<size_t> allBatchSizes_ = {1, 10, 100, 1'000, 10'000, 100'000};

  static std::string formatNumber(size_t n) {
    if (n >= 1'000'000) {
      return std::to_string(n / 1'000'000) + "M";
    } else if (n >= 1'000) {
      return std::to_string(n / 1'000) + "K";
    } else {
      return std::to_string(n);
    }
  }

  // Returns the subset of `allBatchSizes_` that is valid for a given `N`:
  // batchSize <= N and N / batchSize <= 50'000.
  std::vector<size_t> validBatchSizes(size_t N) const {
    std::vector<size_t> result;
    for (size_t bs : allBatchSizes_) {
      if (bs <= N && N / bs <= 50'000) {
        result.push_back(bs);
      }
    }
    return result;
  }

  std::vector<LocatedTriple> generateTriples(size_t N, uint64_t seed = 42) {
    ad_utility::FastRandomIntGenerator<uint64_t> rng(
        ad_utility::RandomSeed::make(seed));
    std::vector<LocatedTriple> result;
    result.reserve(N);

    auto blockIndex = rng() % 100;
    for (size_t i = 0; i < N; i++) {
      std::array<Id, 4> ids = {Id::makeFromInt(rng()), Id::makeFromInt(rng()),
                               Id::makeFromInt(rng()), Id::makeFromInt(rng())};
      LocatedTriple lt{blockIndex, IdTriple<0>(ids), (i % 2 == 0)};
      result.push_back(lt);
    }
    return result;
  }

  // Generate N sorted triples (for use with fromSorted).
  std::vector<LocatedTriple> generateSortedTriples(size_t N,
                                                   uint64_t seed = 42) {
    auto triples = generateTriples(N, seed);
    ql::ranges::sort(triples, LocatedTripleCompare{});
    // Deduplicate by triple_ to satisfy fromSorted's sorted invariant.
    auto [newEnd, _] = ql::ranges::unique(triples, {}, &LocatedTriple::triple_);
    triples.erase(newEnd, triples.end());
    return triples;
  }

  // Run the insert benchmark for a single block size (given by template param),
  // filling in row `rowIdx` of `table`. Batch sizes are given by `batchSizes`.
  template <size_t BlockSize>
  void measureInsert(ResultTable& table, size_t rowIdx,
                     const std::vector<size_t>& batchSizes, size_t N) {
    // Column 0 is the block-size label.
    table.setEntry(rowIdx, 0, BlockSize);

    for (size_t bsIdx = 0; bsIdx < batchSizes.size(); ++bsIdx) {
      size_t batchSize = batchSizes[bsIdx];
      std::vector<LocatedTriple> allTriples = generateTriples(N, 42);

      table.addMeasurement(rowIdx, bsIdx + 1, [&]() {
        BlockSortedLocatedTriplesVectorImpl<BlockSize> vec;
        for (size_t offset = 0; offset < N; offset += batchSize) {
          size_t end = std::min(offset + batchSize, N);
          for (size_t i = offset; i < end; i++) {
            vec.insert(allTriples[i]);
          }
          vec.consolidate();
        }
      });
    }
  }

  // Run the copy benchmark for a single block size, filling in row `rowIdx` of
  // `table`. Columns correspond to N values.
  template <size_t BlockSize>
  void measureCopy(ResultTable& table, size_t rowIdx) {
    table.setEntry(rowIdx, 0, BlockSize);

    for (size_t nIdx = 0; nIdx < nValues_.size(); ++nIdx) {
      size_t N = nValues_[nIdx];
      std::vector<LocatedTriple> sorted = generateSortedTriples(N);
      BlockSortedLocatedTriplesVectorImpl<BlockSize> vec =
          BlockSortedLocatedTriplesVectorImpl<BlockSize>::fromSorted(
              std::move(sorted));

      table.addMeasurement(rowIdx, nIdx + 1, [&]() {
        BlockSortedLocatedTriplesVectorImpl<BlockSize> copy = vec;
        volatile size_t dummy = copy.size();
        (void)dummy;
      });
    }
  }

  // Run the iterate benchmark for a single block size, filling in row `rowIdx`
  // of `table`. Columns correspond to N values.
  template <size_t BlockSize>
  void measureIterate(ResultTable& table, size_t rowIdx) {
    table.setEntry(rowIdx, 0, BlockSize);

    for (size_t nIdx = 0; nIdx < nValues_.size(); ++nIdx) {
      size_t N = nValues_[nIdx];
      std::vector<LocatedTriple> sorted = generateSortedTriples(N);
      BlockSortedLocatedTriplesVectorImpl<BlockSize> vec =
          BlockSortedLocatedTriplesVectorImpl<BlockSize>::fromSorted(
              std::move(sorted));

      table.addMeasurement(rowIdx, nIdx + 1, [&]() {
        volatile size_t checksum = 0;
        for (const auto& lt : vec) {
          checksum += lt.blockIndex_;
        }
        (void)checksum;
      });
    }
  }

  // Dispatch `fn<kBlockSizes[I]>(rowIdx=I, ...)` for all I in the index
  // sequence.
  template <size_t... Is, typename Fn>
  void forEachBlockSize(std::index_sequence<Is...>, Fn&& fn) {
    (..., fn.template operator()<kBlockSizes[Is]>(Is));
  }

 public:
  std::string name() const final {
    return "BlockSortedLocatedTriplesVector: effect of block size";
  }

  BenchmarkResults runAllBenchmarks() final {
    BenchmarkResults results;

    // Row names: one per block size.
    std::vector<std::string> blockSizeRowNames;
    for (size_t bs : kBlockSizes) {
      blockSizeRowNames.push_back(formatNumber(bs));
    }

    // --- Insert benchmark: one table per N value ---
    for (size_t N : insertNValues_) {
      std::vector<size_t> batchSizes = validBatchSizes(N);
      if (batchSizes.empty()) {
        continue;
      }

      std::vector<std::string> colNames = {"block size"};
      for (size_t bs : batchSizes) {
        colNames.push_back("batch=" + formatNumber(bs));
      }

      ResultTable& table = results.addTable(
          "Insert " + formatNumber(N) + " triples (insert + consolidate)",
          blockSizeRowNames, colNames);

      forEachBlockSize(std::make_index_sequence<kBlockSizes.size()>{},
                       [&]<size_t BlockSize>(size_t rowIdx) {
                         measureInsert<BlockSize>(table, rowIdx, batchSizes, N);
                       });
    }

    // --- Copy benchmark: one table for all N values ---
    {
      std::vector<std::string> colNames = {"block size"};
      for (size_t N : nValues_) {
        colNames.push_back("N=" + formatNumber(N));
      }

      ResultTable& copyTable =
          results.addTable("Copy", blockSizeRowNames, colNames);

      forEachBlockSize(std::make_index_sequence<kBlockSizes.size()>{},
                       [&]<size_t BlockSize>(size_t rowIdx) {
                         measureCopy<BlockSize>(copyTable, rowIdx);
                       });
    }

    // --- Iterate benchmark: one table for all N values ---
    {
      std::vector<std::string> colNames = {"block size"};
      for (size_t N : nValues_) {
        colNames.push_back("N=" + formatNumber(N));
      }

      ResultTable& iterTable =
          results.addTable("Iterate", blockSizeRowNames, colNames);

      forEachBlockSize(std::make_index_sequence<kBlockSizes.size()>{},
                       [&]<size_t BlockSize>(size_t rowIdx) {
                         measureIterate<BlockSize>(iterTable, rowIdx);
                       });
    }

    return results;
  }
};

AD_REGISTER_BENCHMARK(BlockSizeLocatedTriplesBenchmark);

}  // namespace ad_benchmark
