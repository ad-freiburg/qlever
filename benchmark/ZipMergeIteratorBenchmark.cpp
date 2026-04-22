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

// Benchmark comparing plain vector iteration vs ZipMergeIterator iteration
// for different small-part ratios.
class ZipMergeIteratorBenchmark : public BenchmarkInterface {
 private:
  // Total sizes to test.
  std::vector<size_t> totalSizes_ = {10'000,    100'000,    1'000'000,
                                     5'000'000, 10'000'000, 25'000'000};

  // Small-part ratios (as percentages).
  std::vector<size_t> smallPartPercents_ = {0, 1, 10, 25, 50};

  static std::vector<LocatedTriple> generate(size_t N, uint64_t seed) {
    ad_utility::FastRandomIntGenerator<uint64_t> rng(
        ad_utility::RandomSeed::make(seed));
    std::vector<LocatedTriple> result;
    result.reserve(N);

    auto blockIndex = rng() % 100;
    for (size_t i = 0; i < N; i++) {
      std::array<Id, 4> ids = {Id::makeFromInt(rng()), Id::makeFromInt(rng()),
                               Id::makeFromInt(rng()), Id::makeFromInt(rng())};
      result.push_back({blockIndex, IdTriple<0>(ids), (i % 2 == 0)});
    }

    return result;
  }
  // Generate N sorted LocatedTriples deterministically.
  static std::vector<LocatedTriple> generateSorted(size_t N, uint64_t seed) {
    auto result = generate(N, seed);
    ql::ranges::sort(result.begin(), result.end(), {}, &LocatedTriple::triple_);
    return result;
  }

  // Build a SortedLocatedTriplesVector with a sorted large part and a sorted
  // small part (two separate sorted regions, as after sortSmallPart()).
  SortedLocatedTriplesVector prepareVector(
      const std::vector<LocatedTriple>& largePart,
      const std::vector<LocatedTriple>& smallPart) {
    SortedLocatedTriplesVector vec;
    vec.triples_.reserve(largePart.size() + smallPart.size());
    for (const auto& item : largePart) {
      vec.triples_.push_back(item);
    }
    vec.numItemsLargePart_ = largePart.size();
    for (const auto& item : smallPart) {
      vec.triples_.push_back(item);
    }
    vec.smallPartIsSorted_ = true;
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
    return "Vector iteration vs ZipMergeIterator iteration";
  }

  BenchmarkResults runAllBenchmarks() final {
    BenchmarkResults results;

    for (size_t N : totalSizes_) {
      std::string nStr = formatNumber(N);

      // Table 1: Iteration and copy (independent of small part %).
      {
        std::vector<std::string> rowNames = {"result"};
        std::vector<std::string> columnNames = {
            "Vector iteration", "BlockSorted iteration", "Set iteration",
            "Set copy",         "SortedVector copy",     "BlockSorted copy"};

        ResultTable& fixedTable = results.addTable(
            "N=" + nStr + " iteration and copy", rowNames, columnNames);

        auto allSorted = generateSorted(N, 42);

        // Vector iteration (baseline).
        fixedTable.addMeasurement(0, 0, [&]() {
          volatile size_t count = 0;
          for (const auto& item : allSorted) {
            if (item.insertOrDelete_) {
              count++;
            }
          }
        });

        // BlockSorted iteration.
        {
          BlockSortedLocatedTriplesVector bvec =
              BlockSortedLocatedTriplesVector::fromSorted(
                  std::vector(allSorted));
          fixedTable.addMeasurement(0, 1, [&]() {
            volatile size_t count = 0;
            for (const auto& item : bvec) {
              if (item.insertOrDelete_) {
                count++;
              }
            }
          });
        }

        // Set iteration.
        {
          std::set<LocatedTriple, LocatedTripleCompare> s(allSorted.begin(),
                                                          allSorted.end());
          fixedTable.addMeasurement(0, 2, [&]() {
            volatile size_t count = 0;
            for (const auto& item : s) {
              if (item.insertOrDelete_) {
                count++;
              }
            }
          });
        }

        // Set copy.
        {
          std::set<LocatedTriple, LocatedTripleCompare> s(allSorted.begin(),
                                                          allSorted.end());
          fixedTable.addMeasurement(0, 3, [&]() {
            auto copy = s;
            volatile auto* p = &copy;
            (void)p;
          });
        }

        // SortedVector copy.
        {
          SortedLocatedTriplesVector vec = prepareVector(allSorted, {});
          fixedTable.addMeasurement(0, 4, [&]() {
            auto copy = vec;
            volatile auto* p = &copy;
            (void)p;
          });
        }

        // BlockSorted copy.
        {
          BlockSortedLocatedTriplesVector bvec =
              BlockSortedLocatedTriplesVector::fromSorted(
                  std::vector(allSorted));
          fixedTable.addMeasurement(0, 5, [&]() {
            auto copy = bvec;
            volatile auto* p = &copy;
            (void)p;
          });
        }
      }

      // Table 2: Operations that depend on small part %.
      {
        std::vector<std::string> rowNames;
        for (size_t pct : smallPartPercents_) {
          rowNames.push_back(std::to_string(pct) + "%");
        }

        std::vector<std::string> columnNames = {
            "Small part %", "ZipMergeIterator iteration", "zipMerge",
            "sortSmallPart", "BlockSorted consolidate"};

        ResultTable& table = results.addTable(
            "N=" + nStr + " small-part dependent", rowNames, columnNames);

        for (size_t pctIdx = 0; pctIdx < smallPartPercents_.size(); pctIdx++) {
          size_t pct = smallPartPercents_[pctIdx];
          size_t smallSize = N * pct / 100;
          size_t largeSize = N - smallSize;

          table.setEntry(pctIdx, 0, pct);

          auto largePart = generateSorted(largeSize, 42);
          auto smallPart = generateSorted(smallSize, 12345);

          // ZipMergeIterator iteration with varying small part.
          {
            SortedLocatedTriplesVector vec =
                prepareVector(largePart, smallPart);
            table.addMeasurement(pctIdx, 1, [&]() {
              volatile size_t count = 0;
              for (const auto& item : vec) {
                if (item.insertOrDelete_) {
                  count++;
                }
              }
            });
          }

          // zipMerge (merge both parts).
          {
            SortedLocatedTriplesVector vec =
                prepareVector(largePart, smallPart);
            table.addMeasurement(pctIdx, 2, [&]() { vec.sortAndMergeParts(); });
          }

          // sortSmallPart.
          {
            SortedLocatedTriplesVector vec =
                prepareVector(largePart, generate(smallSize, 12345));
            table.addMeasurement(pctIdx, 3, [&]() { vec.sortSmallPart(); });
          }

          // BlockSorted consolidate.
          {
            BlockSortedLocatedTriplesVector bvec =
                BlockSortedLocatedTriplesVector::fromSorted(
                    std::vector(largePart));
            for (const auto& item : generate(smallSize, 12345)) {
              bvec.insert(item);
            }
            table.addMeasurement(pctIdx, 4, [&]() { bvec.consolidate(); });
          }
        }
      }
    }

    return results;
  }
};

// Register the benchmark
AD_REGISTER_BENCHMARK(ZipMergeIteratorBenchmark);

}  // namespace ad_benchmark
