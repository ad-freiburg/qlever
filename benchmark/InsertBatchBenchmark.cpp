// Copyright 2025 The QLever Authors, in particular:
//
// 2025 Julian Mundhahs <mundhahj@tf.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <string>
#include <vector>

#include "../benchmark/infrastructure/Benchmark.h"
#include "../benchmark/infrastructure/BenchmarkMeasurementContainer.h"
#include "index/LocatedTriples.h"
#include "util/Random.h"

namespace ad_benchmark {

// Benchmark inserting triples into SortedLocatedTriplesVector and
// BlockSortedLocatedTriplesVector for different total counts, batch sizes and
// consolidate thresholds.
class InsertBatchBenchmark : public BenchmarkInterface {
 private:
  struct TestConfig {
    size_t totalTriples;
    std::vector<size_t> batchSizes;
  };

  std::vector<TestConfig> configs_ = {
      {10'000, {1, 5, 10}},
      {100'000, {100, 500, 1'000}},
      {2'500'000, {10'000, 50'000, 100'000}},
  };

  // Thresholds for consolidate: 0%, 1%, 10%, 20%
  std::vector<double> thresholds_ = {0.0, 0.01, 0.10, 0.20};

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

  static std::string formatNumber(size_t n) {
    if (n >= 1'000'000) {
      return std::to_string(n / 1'000'000) + "M";
    } else if (n >= 1'000) {
      return std::to_string(n / 1'000) + "K";
    } else {
      return std::to_string(n);
    }
  }

  static std::string formatThreshold(double t) {
    return std::to_string(static_cast<int>(t * 100)) + "%";
  }

  void addTablesForConfig(BenchmarkResults& results, const TestConfig& config) {
    size_t totalTriples = config.totalTriples;
    const auto& batchSizes = config.batchSizes;

    std::vector<std::string> rowNames;
    for (size_t bs : batchSizes) {
      rowNames.push_back(formatNumber(bs));
    }

    // Column names: batch size label + one per threshold
    std::vector<std::string> columnNames = {"batch size"};
    for (double t : thresholds_) {
      columnNames.push_back("threshold=" + formatThreshold(t));
    }

    std::string totalStr = formatNumber(totalTriples);

    ResultTable& table = results.addTable(
        "Insert " + totalStr + " triples (time for insert + consolidate)",
        rowNames, columnNames);

    for (size_t bsIdx = 0; bsIdx < batchSizes.size(); bsIdx++) {
      size_t batchSize = batchSizes[bsIdx];
      table.setEntry(bsIdx, 0, batchSize);

      for (size_t tIdx = 0; tIdx < thresholds_.size(); tIdx++) {
        double threshold = thresholds_[tIdx];

        std::vector<LocatedTriple> allTriples =
            generateTriples(totalTriples, 42);

        table.addMeasurement(bsIdx, tIdx + 1, [&]() {
          SortedLocatedTriplesVector vec;

          for (size_t offset = 0; offset < totalTriples; offset += batchSize) {
            size_t end = std::min(offset + batchSize, totalTriples);
            for (size_t i = offset; i < end; i++) {
              vec.insert(allTriples[i]);
            }
            vec.consolidate(threshold);
          }
        });
      }
    }

    // BlockSortedLocatedTriplesVector table
    std::vector<std::string> blockColumnNames = {"batch size", "BlockSorted"};
    ResultTable& blockTable =
        results.addTable("BlockSorted: Insert " + totalStr +
                             " triples (time for insert + consolidate)",
                         rowNames, blockColumnNames);

    for (size_t bsIdx = 0; bsIdx < batchSizes.size(); bsIdx++) {
      size_t batchSize = batchSizes[bsIdx];
      blockTable.setEntry(bsIdx, 0, batchSize);

      std::vector<LocatedTriple> allTriples = generateTriples(totalTriples, 42);

      blockTable.addMeasurement(bsIdx, 1, [&]() {
        BlockSortedLocatedTriplesVector bvec;

        for (size_t offset = 0; offset < totalTriples; offset += batchSize) {
          size_t end = std::min(offset + batchSize, totalTriples);
          for (size_t i = offset; i < end; i++) {
            bvec.insert(allTriples[i]);
          }
          bvec.consolidate();
        }
      });
    }
  }

 public:
  std::string name() const final {
    return "Insert triples: batch size vs consolidate threshold";
  }

  BenchmarkResults runAllBenchmarks() final {
    BenchmarkResults results;

    for (const auto& config : configs_) {
      addTablesForConfig(results, config);
    }

    return results;
  }
};

AD_REGISTER_BENCHMARK(InsertBatchBenchmark);

}  // namespace ad_benchmark
