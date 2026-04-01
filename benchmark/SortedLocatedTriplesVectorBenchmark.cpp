// Copyright 2025 The QLever Authors, in particular:
//
// 2025 Julian Mundhahs <mundhahj@tf.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <algorithm>
#include <set>
#include <string>
#include <vector>

#include "../benchmark/infrastructure/Benchmark.h"
#include "../benchmark/infrastructure/BenchmarkMeasurementContainer.h"
#include "index/LocatedTriples.h"
#include "util/Random.h"

namespace ad_benchmark {

// Benchmark comparing SortedLocatedTriplesVector with std::set<LocatedTriple>
class SortedLocatedTriplesVectorBenchmark : public BenchmarkInterface {
 private:
  // Test parameters: N values (pre-existing items)
  std::vector<size_t> nValues_ = {5'000,     10'000,     50'000,
                                  100'000,   500'000,    1'000'000,
                                  5'000'000, 10'000'000, 50'000'000};

  // Test parameters: M values (items to insert)
  std::vector<size_t> mValues_ = {10, 5'000, 100'000, 1'000'000};

  // Helper: Generate N pre-existing LocatedTriples, deterministically
  std::vector<LocatedTriple> generatePreExisting(size_t N, uint64_t seed = 42) {
    ad_utility::FastRandomIntGenerator<uint64_t> rng(
        ad_utility::RandomSeed::make(seed));
    std::vector<LocatedTriple> result;
    result.reserve(N);

    auto blockIndex = rng() % 100;  // blockIndex_ in range [0, 99]
    for (size_t i = 0; i < N; i++) {
      // Generate random Ids for the triple
      std::array<Id, 4> ids = {Id::makeFromInt(rng()), Id::makeFromInt(rng()),
                               Id::makeFromInt(rng()), Id::makeFromInt(rng())};

      LocatedTriple lt{
          blockIndex,        // the block index is the same for one
                             // LocatedTriplesSortedVector
          IdTriple<0>(ids),  // triple_
          (i % 2 == 0)       // insertOrDelete_
      };

      result.push_back(lt);
    }

    // Sort using LocatedTripleCompare for fair initialization
    std::sort(result.begin(), result.end(),
              [](const LocatedTriple& a, const LocatedTriple& b) {
                return a.triple_ < b.triple_;
              });

    return result;
  }

  // Helper: Generate M new items to insert, deterministically but different
  // from pre-existing
  std::vector<LocatedTriple> generateNewItems(size_t M, uint64_t seed) {
    ad_utility::FastRandomIntGenerator<uint64_t> rng(
        ad_utility::RandomSeed::make(seed));
    std::vector<LocatedTriple> result;
    result.reserve(M);

    auto blockIndex = rng() % 100;  // blockIndex_ in range [0, 99]
    for (size_t i = 0; i < M; i++) {
      // Generate random Ids for the triple
      std::array<Id, 4> ids = {Id::makeFromInt(rng()), Id::makeFromInt(rng()),
                               Id::makeFromInt(rng()), Id::makeFromInt(rng())};

      LocatedTriple lt{
          blockIndex,        // the block index is the same for one
                             // LocatedTriplesSortedVector
          IdTriple<0>(ids),  // triple_
          (i % 2 == 1)       // insertOrDelete_
      };

      result.push_back(lt);
    }

    return result;
  }

 public:
  std::string name() const final {
    return "SortedLocatedTriplesVector vs std::set<LocatedTriple>";
  }

  BenchmarkResults runAllBenchmarks() final {
    BenchmarkResults results;

    // Create 3 separate tables, one for each M value
    for (size_t mIdx = 0; mIdx < mValues_.size(); mIdx++) {
      size_t M = mValues_[mIdx];

      // Prepare row names (N values as strings)
      std::vector<std::string> rowNames;
      for (size_t N : nValues_) {
        if (N >= 1'000'000) {
          rowNames.push_back(std::to_string(N / 1'000'000) + "M");
        } else if (N >= 1'000) {
          rowNames.push_back(std::to_string(N / 1'000) + "K");
        } else {
          rowNames.push_back(std::to_string(N));
        }
      }

      // Column names
      std::vector<std::string> columnNames = {"N items",
                                              "Vector: Insert only",
                                              "Vector: Integration",
                                              "Set: Insert",
                                              "Vector: Copy",
                                              "Set: Copy",
                                              "Set: Copy (slow)",
                                              "BlockSorted: Insert only",
                                              "BlockSorted: Integration",
                                              "BlockSorted: Copy"};

      // Create table
      std::string tableName = "M=" + std::to_string(M) + " items inserted";
      ResultTable& table = results.addTable(tableName, rowNames, columnNames);

      // Run benchmarks for each N value
      for (size_t nIdx = 0; nIdx < nValues_.size(); nIdx++) {
        size_t N = nValues_[nIdx];

        // Set the N value in the first column
        table.setEntry(nIdx, 0, N);

        // Generate data (same for all measurements in this row)
        std::vector<LocatedTriple> preExisting = generatePreExisting(N);
        std::vector<LocatedTriple> newItems = generateNewItems(M, 1000 + N);

        // Column 1: Vector Raw Insertion (insert M items without integration)
        // Setup: Create vector and insert N pre-existing items (not measured)
        SortedLocatedTriplesVector vec;
        for (const auto& item : preExisting) {
          vec.insert(item);
        }
        // Measure: Insert M new items
        table.addMeasurement(nIdx, 1, [&]() {
          for (const auto& item : newItems) {
            vec.insert(item);
          }
        });

        // Column 2: Vector Integration (measure only ensureIntegration)
        table.addMeasurement(nIdx, 2, [&]() { vec.consolidate(); });

        // Column 3: Set Insertion (insert M items one-by-one)
        std::set<LocatedTriple, LocatedTripleCompare> s(preExisting.begin(),
                                                        preExisting.end());
        table.addMeasurement(nIdx, 3, [&]() {
          for (const auto& item : newItems) {
            s.insert(item);
          }
        });

        // Column 4: Vector Copy (copy after integration)
        table.addMeasurement(nIdx, 4, [&]() {
          SortedLocatedTriplesVector copy = vec;

          // Use result to prevent optimization
          volatile size_t dummy = copy.size();
          (void)dummy;
        });

        // Column 5: Set Copy
        table.addMeasurement(nIdx, 5, [&]() {
          // Measure only the copy
          std::set<LocatedTriple, LocatedTripleCompare> copy = s;

          // Use result to prevent optimization
          volatile size_t dummy = copy.size();
          (void)dummy;
        });

        // Column 6: Set Copy (purposefully slow)
        table.addMeasurement(nIdx, 6, [&]() {
          // Measure only the copy
          std::set<LocatedTriple, LocatedTripleCompare> copy(s.begin(),
                                                             s.end());

          // Use result to prevent optimization
          volatile size_t dummy = copy.size();
          (void)dummy;
        });

        // Column 7: BlockSorted Insert only
        BlockSortedLocatedTriplesVector bvec =
            BlockSortedLocatedTriplesVector::fromSorted(
                std::vector(preExisting));
        table.addMeasurement(nIdx, 7, [&]() {
          for (const auto& item : newItems) {
            bvec.insert(item);
          }
        });

        // Column 8: BlockSorted Integration
        table.addMeasurement(nIdx, 8, [&]() { bvec.consolidate(); });

        // Column 9: BlockSorted Copy
        table.addMeasurement(nIdx, 9, [&]() {
          BlockSortedLocatedTriplesVector copy = bvec;

          volatile size_t dummy = copy.size();
          (void)dummy;
        });
      }
    }

    return results;
  }
};

// Register the benchmark
AD_REGISTER_BENCHMARK(SortedLocatedTriplesVectorBenchmark);

}  // namespace ad_benchmark
