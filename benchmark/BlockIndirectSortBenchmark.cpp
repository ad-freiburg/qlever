// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <boost/asio/thread_pool.hpp>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <random>
#include <string>
#include <thread>
#include <vector>

#include "../benchmark/infrastructure/Benchmark.h"
#include "backports/algorithm.h"
#include "backports/span.h"
#include "engine/idTable/IdTable.h"
#include "global/Id.h"
#include "util/AllocatorWithLimit.h"
#include "util/CancellationHandle.h"
#include "util/CompilerWarnings.h"
#include "util/blockSort/BlockIndirectSort.h"

// `<boost/sort/sort.hpp>` includes the deprecated `<ciso646>`, see
// `util/blockSort/BoostSortHeaders.h`.
DISABLE_PREPROCESSOR_WARNINGS
#include <boost/sort/sort.hpp>
REENABLE_PREPROCESSOR_WARNINGS

// Compare `ad_utility::blockSort::blockIndirectSort` with the original
// `boost::sort::block_indirect_sort` and a single-threaded sort. Only
// meaningful on a machine with many cores.
namespace ad_benchmark {

namespace {
// A big element, so that the cost of moving blocks shows up.
struct WideElement {
  uint64_t key_ = 0;
  std::array<char, 56> payload_{};
  bool operator<(const WideElement& other) const { return key_ < other.key_; }
};

// The element of type `T` that represents `value`.
template <typename T>
T makeElement(uint64_t value);

template <>
uint64_t makeElement<uint64_t>(uint64_t value) {
  return value;
}
template <>
WideElement makeElement<WideElement>(uint64_t value) {
  return WideElement{value, {}};
}
template <>
std::string makeElement<std::string>(uint64_t value) {
  return std::to_string(value) + "_with_some_padding_to_make_it_long";
}

// `numElements` random elements, the same ones in every run.
template <typename T>
std::vector<T> randomElements(size_t numElements) {
  std::mt19937_64 generator{0xC0FFEE};
  std::vector<T> elements;
  elements.reserve(numElements);
  for (size_t i = 0; i < numElements; ++i) {
    elements.push_back(makeElement<T>(generator()));
  }
  return elements;
}
// An `IdTable` with `numRows` random rows of `numColumns` columns, the same
// ones in every run.
IdTable randomIdTable(size_t numRows, size_t numColumns) {
  std::mt19937_64 generator{0xC0FFEE};
  IdTable table{numColumns, ad_utility::makeUnlimitedAllocator<Id>()};
  table.resize(numRows);
  for (auto column : table.getColumns()) {
    for (auto& id : column) {
      id = Id::makeFromVocabIndex(VocabIndex::make(generator() >> 20));
    }
  }
  return table;
}
}  // namespace

class BlockIndirectSortBenchmark : public BenchmarkInterface {
  // Measure the sort of an `IdTable` with `NumColumns` columns (`0` for a
  // dynamic number of columns, then `numColumns`) by its first two columns,
  // like `IdTableUtils::sort` does for the sorts of the queries, with and
  // without a cancellation handle (that is never cancelled), with
  // `numThreads` and in a single task.
  template <size_t NumColumns>
  void addIdTableMeasurements(BenchmarkResults& results, size_t numRows,
                              size_t numColumns, uint32_t numThreads,
                              boost::asio::thread_pool& pool) {
    IdTable pristine = randomIdTable(numRows, numColumns);
    IdTable elements = pristine.clone();
    auto compare = [](const auto& a, const auto& b) {
      if (a[0] != b[0]) {
        return a[0] < b[0];
      }
      return a[1] < b[1];
    };
    auto handle = std::make_shared<ad_utility::CancellationHandle<>>();
    for (uint32_t threads : {numThreads, 1u}) {
      auto suffix =
          absl::StrCat(" (IdTable<", NumColumns, ">, ", numRows, " rows, ",
                       numColumns, " columns, ", threads, " threads)");
      auto sort = [&elements, &pristine, threads, &pool,
                   compare](auto... cancellationHandle) {
        elements = pristine.clone();
        auto table = std::move(elements).toStatic<NumColumns>();
        ad_utility::blockSort::blockIndirectSort(table, compare, threads,
                                                 pool.get_executor(),
                                                 cancellationHandle...);
        elements = std::move(table).toDynamic();
      };
      results.addMeasurement(absl::StrCat("blockIndirectSort", suffix),
                             [&sort]() { sort(); });
      results.addMeasurement(
          absl::StrCat("blockIndirectSort, cancellable", suffix),
          [&sort, &handle]() { sort(handle); });
    }
  }

  // Measure the three sorts on `numElements` random elements of type `T`. All
  // measurements sort the same input.
  template <typename T>
  void addMeasurementsFor(BenchmarkResults& results, const std::string& name,
                          size_t numElements, uint32_t numThreads,
                          boost::asio::thread_pool& pool) {
    auto pristine = randomElements<T>(numElements);
    auto elements = pristine;
    auto suffix = absl::StrCat(" (", name, ", ", numElements, " elements, ",
                               numThreads, " threads)");

    results.addMeasurement(absl::StrCat("blockIndirectSort", suffix),
                           [&elements, &pristine, numThreads, &pool]() {
                             elements = pristine;
                             ad_utility::blockSort::blockIndirectSort(
                                 ql::span<T>{elements}, std::less<T>{},
                                 numThreads, pool.get_executor());
                           });
    results.addMeasurement(absl::StrCat("boost::block_indirect_sort", suffix),
                           [&elements, &pristine, numThreads]() {
                             elements = pristine;
                             boost::sort::block_indirect_sort(
                                 elements.begin(), elements.end(),
                                 std::less<T>{}, numThreads);
                           });
    results.addMeasurement(absl::StrCat("single-threaded sort", suffix),
                           [&elements, &pristine]() {
                             elements = pristine;
                             ql::ranges::sort(elements, std::less<T>{});
                           });
  }

  // The name of this group of benchmarks.
  std::string name() const final {
    return "Benchmarks for the parallel block indirect sort";
  }

  // Measure all element types with one thread per core.
  BenchmarkResults runAllBenchmarks() final {
    BenchmarkResults results{};
    uint32_t numThreads = std::max(std::thread::hardware_concurrency(), 2u);
    boost::asio::thread_pool pool{numThreads};
    addMeasurementsFor<uint64_t>(results, "uint64_t", 50'000'000, numThreads,
                                 pool);
    addMeasurementsFor<WideElement>(results, "64 bytes", 10'000'000, numThreads,
                                    pool);
    addMeasurementsFor<std::string>(results, "std::string", 5'000'000,
                                    numThreads, pool);
    addIdTableMeasurements<3>(results, 20'000'000, 3, numThreads, pool);
    addIdTableMeasurements<0>(results, 10'000'000, 7, numThreads, pool);
    pool.join();
    return results;
  }
};

AD_REGISTER_BENCHMARK(BlockIndirectSortBenchmark);
}  // namespace ad_benchmark
