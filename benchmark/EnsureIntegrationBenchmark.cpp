// Copyright 2025 The QLever Authors, in particular:
//
// 2025 Julian Mundhahs <mundhahj@tf.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <algorithm>
#include <string>
#include <vector>

#include "../benchmark/infrastructure/Benchmark.h"
#include "../benchmark/infrastructure/BenchmarkMeasurementContainer.h"
#include "index/LocatedTriples.h"
#include "util/Random.h"

namespace ad_benchmark {

// Benchmark comparing zipSort vs fullSort in ensureIntegration()
class EnsureIntegrationBenchmark : public BenchmarkInterface {
 private:
  // N values (sorted prefix size)
  std::vector<size_t> nValues_ = {0,       1'000,   5'000,     10'000,   50'000,
                                  100'000, 500'000, 1'000'000, 5'000'000};

  // M values (unsorted tail size)
  std::vector<size_t> mValues_ = {10, 100, 1'000, 5'000, 10'000, 100'000};

  // Helper: Generate N pre-existing LocatedTriples, deterministically
  std::vector<LocatedTriple> generatePreExisting(size_t N, uint64_t seed = 42) {
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

    auto blockIndex = rng() % 100;
    for (size_t i = 0; i < M; i++) {
      std::array<Id, 4> ids = {Id::makeFromInt(rng()), Id::makeFromInt(rng()),
                               Id::makeFromInt(rng()), Id::makeFromInt(rng())};
      LocatedTriple lt{blockIndex, IdTriple<0>(ids), (i % 2 == 1)};
      result.push_back(lt);
    }

    return result;
  }

  // Helper: Build a SortedLocatedTriplesVector with N sorted prefix + M
  // unsorted tail, with sortedUntil_ = N and dirty_ = true.
  SortedLocatedTriplesVector prepareVector(
      const std::vector<LocatedTriple>& sortedPrefix,
      const std::vector<LocatedTriple>& unsortedTail) {
    SortedLocatedTriplesVector vec;
    // Insert sorted prefix
    for (const auto& item : sortedPrefix) {
      vec.triples_.push_back(item);
    }
    vec.numItemsLargePart_ = sortedPrefix.size();
    // Insert unsorted tail
    for (const auto& item : unsortedTail) {
      vec.triples_.push_back(item);
    }
    return vec;
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

 public:
  std::string name() const final {
    return "ensureIntegration: zipSort vs fullSort";
  }

  BenchmarkResults runAllBenchmarks() final {
    BenchmarkResults results;

    for (size_t M : mValues_) {
      // Row names from N values
      std::vector<std::string> rowNames;
      for (size_t N : nValues_) {
        rowNames.push_back(formatNumber(N));
      }

      std::vector<std::string> columnNames = {"N items", "zipSort", "fullSort"};

      std::string tableName = "M=" + formatNumber(M) + " unsorted tail items";
      ResultTable& table = results.addTable(tableName, rowNames, columnNames);

      for (size_t nIdx = 0; nIdx < nValues_.size(); nIdx++) {
        size_t N = nValues_[nIdx];

        table.setEntry(nIdx, 0, N);

        // Generate data
        std::vector<LocatedTriple> preExisting = generatePreExisting(N);
        std::vector<LocatedTriple> newItems = generateNewItems(M, 1000 + N);

        {
          // zipSort benchmark
          SortedLocatedTriplesVector vec = prepareVector(preExisting, newItems);
          table.addMeasurement(nIdx, 1, [&]() {
            vec.sortAndMergeParts();
            vec.numItemsLargePart_ = vec.triples_.size();
          });
        }

        {
          // fullSort benchmark
          SortedLocatedTriplesVector vec = prepareVector(preExisting, newItems);
          table.addMeasurement(nIdx, 2, [&]() {
            vec.sortSmallPart();
            vec.numItemsLargePart_ = vec.triples_.size();
          });
        }
      }
    }

    return results;
  }
};

// Register the benchmark
AD_REGISTER_BENCHMARK(EnsureIntegrationBenchmark);

}  // namespace ad_benchmark
