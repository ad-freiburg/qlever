// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <boost/asio/thread_pool.hpp>
#include <boost/sort/sort.hpp>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <random>
#include <string>
#include <thread>
#include <vector>

#include "../benchmark/infrastructure/Benchmark.h"
#include "backports/algorithm.h"
#include "backports/span.h"
#include "util/blockSort/BlockIndirectSort.h"

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
}  // namespace

class BlockIndirectSortBenchmark : public BenchmarkInterface {
  // All measurements sort the same input.
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

  std::string name() const final {
    return "Benchmarks for the parallel block indirect sort";
  }

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
    pool.join();
    return results;
  }
};

AD_REGISTER_BENCHMARK(BlockIndirectSortBenchmark);
}  // namespace ad_benchmark
