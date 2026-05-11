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

class LocatedTriplesInsertScenariosBenchmark : public BenchmarkInterface {
 private:
  static constexpr size_t kPreExisting = 100'000;
  std::vector<size_t> mValues_ = {10'000, 20, 2};

  static std::vector<LocatedTriple> generatePreExisting(size_t N,
                                                        uint64_t seed = 42) {
    ad_utility::FastRandomIntGenerator<uint64_t> rng(
        ad_utility::RandomSeed::make(seed));
    auto blockIndex = rng() % 100;
    std::vector<LocatedTriple> result;
    for (size_t i = 0; i < N; i++) {
      std::array<Id, 4> ids = {Id::makeFromInt(rng()), Id::makeFromInt(rng()),
                               Id::makeFromInt(rng()), Id::makeFromInt(rng())};
      result.push_back({blockIndex, IdTriple<0>(ids), (i % 2 == 0)});
    }
    ql::ranges::sort(result, {}, &LocatedTriple::triple_);
    return result;
  }

  static std::vector<LocatedTriple> generateNewItems(size_t M, uint64_t seed) {
    ad_utility::FastRandomIntGenerator<uint64_t> rng(
        ad_utility::RandomSeed::make(seed));
    auto blockIndex = rng() % 100;
    std::vector<LocatedTriple> result;
    result.reserve(M);
    for (size_t i = 0; i < M; i++) {
      std::array<Id, 4> ids = {Id::makeFromInt(rng()), Id::makeFromInt(rng()),
                               Id::makeFromInt(rng()), Id::makeFromInt(rng())};
      result.push_back({blockIndex, IdTriple<0>(ids), (i % 2 == 1)});
    }
    return result;
  }

  static std::string formatNumber(size_t n) {
    if (n >= 1'000'000) return std::to_string(n / 1'000'000) + "M";
    if (n >= 1'000) return std::to_string(n / 1'000) + "K";
    return std::to_string(n);
  }

 public:
  std::string name() const final {
    return "SortedLocatedTriplesVector vs BlockSortedLocatedTriplesVector - "
           "Insert Scenarios";
  }

  BenchmarkResults runAllBenchmarks() final {
    using BlockSortedLocatedTriplesVector =
        BlockSortedLocatedTriplesVectorImpl<200>;
    BenchmarkResults results;

    auto preExisting = generatePreExisting(kPreExisting);
    LocatedTriple dummy{0,
                        IdTriple<0>({Id::makeFromInt(0), Id::makeFromInt(0),
                                     Id::makeFromInt(0), Id::makeFromInt(0)}),
                        true};

    std::vector<std::string> insertColNames = {
        "SortedVector: Insert", "SortedVector: Consolidate",
        "BlockSorted: Insert", "BlockSorted: Consolidate"};

    for (size_t M : mValues_) {
      auto newItems = generateNewItems(M, 1000 + M);
      // Target ~100K total operations so the measurement is visible even in
      // release builds (~10ns/op → ~1ms total).
      size_t numReps = std::max(size_t(1), size_t(100'000) / M);

      std::string tableName = "M=" + formatNumber(M) + " insert scenario (x" +
                              std::to_string(numReps) + " reps)";
      ResultTable& table =
          results.addTable(tableName, {"result"}, insertColNames);

      // --- SortedVector ---
      SortedLocatedTriplesVector svec =
          SortedLocatedTriplesVector::fromSorted(preExisting);
      svec.insert(dummy);

      // Insert: loop numReps times; state accumulates in the pending buffer
      // but each push_back is O(1) regardless of pending size.
      table.addMeasurement(0, 0, [&]() {
        for (size_t rep = 0; rep < numReps; rep++) {
          for (const auto& item : newItems) {
            svec.insert(item);
          }
        }
      });

      // Clean up accumulated pending from the insert measurement, then
      // set up exactly M+1 items in pending for the consolidate measurement.
      svec.consolidate();
      svec.insert(dummy);
      for (const auto& item : newItems) {
        svec.insert(item);
      }

      // Consolidate: each iteration merges M+1 pending items, then re-inserts
      // them so the next iteration starts with the same pending size.
      table.addMeasurement(0, 1, [&]() {
        for (size_t rep = 0; rep < numReps; rep++) {
          svec.consolidate();
          svec.insert(dummy);
          for (const auto& item : newItems) {
            svec.insert(item);
          }
        }
        svec.consolidate();
      });

      // --- BlockSorted ---
      BlockSortedLocatedTriplesVector bvec =
          BlockSortedLocatedTriplesVector::fromSorted(preExisting);
      bvec.insert(dummy);

      table.addMeasurement(0, 2, [&]() {
        for (size_t rep = 0; rep < numReps; rep++) {
          for (const auto& item : newItems) {
            bvec.insert(item);
          }
        }
      });

      bvec.consolidate();
      bvec.insert(dummy);
      for (const auto& item : newItems) {
        bvec.insert(item);
      }

      table.addMeasurement(0, 3, [&]() {
        for (size_t rep = 0; rep < numReps; rep++) {
          bvec.consolidate();
          bvec.insert(dummy);
          for (const auto& item : newItems) {
            bvec.insert(item);
          }
        }
        bvec.consolidate();
      });
    }

    // Iteration: traverse all 100k items in a consolidated container.
    {
      constexpr size_t kRepIterate = 10;
      ResultTable& table = results.addTable(
          "Iteration (N=100K, consolidated, x" + std::to_string(kRepIterate) +
              " reps)",
          {"result"}, {"SortedVector: Iterate", "BlockSorted: Iterate"});

      {
        SortedLocatedTriplesVector svec =
            SortedLocatedTriplesVector::fromSorted(preExisting);
        table.addMeasurement(0, 0, [&]() {
          volatile size_t count = 0;
          for (size_t rep = 0; rep < kRepIterate; rep++) {
            for (const auto& item : svec) {
              if (item.insertOrDelete_) count++;
            }
          }
        });
      }

      {
        BlockSortedLocatedTriplesVector bvec =
            BlockSortedLocatedTriplesVector::fromSorted(preExisting);
        table.addMeasurement(0, 1, [&]() {
          volatile size_t count = 0;
          for (size_t rep = 0; rep < kRepIterate; rep++) {
            for (const auto& item : bvec) {
              if (item.insertOrDelete_) count++;
            }
          }
        });
      }
    }

    // Copy: copy-construct a consolidated container with 100k items.
    {
      constexpr size_t kRepCopy = 10;
      ResultTable& table = results.addTable(
          "Copy (N=100K, consolidated, x" + std::to_string(kRepCopy) + " reps)",
          {"result"}, {"SortedVector: Copy", "BlockSorted: Copy"});

      {
        SortedLocatedTriplesVector svec =
            SortedLocatedTriplesVector::fromSorted(preExisting);
        table.addMeasurement(0, 0, [&]() {
          for (size_t rep = 0; rep < kRepCopy; rep++) {
            auto copy = svec;
            volatile auto* p = &copy;
            (void)p;
          }
        });
      }

      {
        BlockSortedLocatedTriplesVector bvec =
            BlockSortedLocatedTriplesVector::fromSorted(preExisting);
        table.addMeasurement(0, 1, [&]() {
          for (size_t rep = 0; rep < kRepCopy; rep++) {
            auto copy = bvec;
            volatile auto* p = &copy;
            (void)p;
          }
        });
      }
    }

    return results;
  }
};

AD_REGISTER_BENCHMARK(LocatedTriplesInsertScenariosBenchmark);

}  // namespace ad_benchmark
