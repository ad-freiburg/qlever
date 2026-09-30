// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <absl/strings/str_cat.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <boost/asio/thread_pool.hpp>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <numeric>
#include <random>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "../util/GTestHelpers.h"
#include "backports/algorithm.h"
#include "backports/asio.h"
#include "backports/span.h"
#include "util/blockSort/BlockIndirectSort.h"

using ad_utility::blockSort::blockIndirectSort;

namespace {

// The thread pool that all tests share. It has to be run by threads other than
// the one that calls the sort, see `blockIndirectSort`.
constexpr uint32_t numPoolThreads = 8;
boost::asio::thread_pool& threadPool() {
  static boost::asio::thread_pool pool{numPoolThreads};
  return pool;
}

// The number of elements above which the block indirect part of the algorithm
// is actually used; below it the sort falls back to sorting in the calling
// thread. Derived from the thresholds in `BlockIndirectSort.h`: the input has
// to have more than one group of blocks per thread.
template <typename T>
constexpr size_t numElementsForParallelPath(uint32_t numThreads) {
  return numThreads *
         size_t{
             ad_utility::blockSort::detail::blockSizeForElements(sizeof(T))} *
         64;
}

// Sort a copy of `input` with `blockIndirectSort` and with `ql::ranges::sort`,
// and expect the two to agree. Returns nothing but reports failures at the
// call site of the caller via `SCOPED_TRACE`.
template <typename T, typename Compare = std::less<T>>
void expectSortedLikeStd(std::vector<T> input, uint32_t nthread,
                         Compare comp = {}) {
  std::vector<T> expected = input;
  ql::ranges::sort(expected, comp);

  std::vector<T> actual = input;
  blockIndirectSort(ql::span<T>{actual}, comp, nthread,
                    threadPool().get_executor());
  EXPECT_EQ(actual, expected);
}

// A random permutation-friendly input: distinct values in random order, so
// that the (unstable) sort has a unique correct answer.
std::vector<uint32_t> randomDistinct(size_t numElements, uint64_t seed) {
  std::vector<uint32_t> values(numElements);
  std::iota(values.begin(), values.end(), uint32_t{0});
  std::shuffle(values.begin(), values.end(), std::mt19937_64{seed});
  return values;
}

}  // namespace

// _____________________________________________________________________________
TEST(BlockIndirectSort, emptyAndTinyInputs) {
  for (uint32_t nthread : {uint32_t{1}, uint32_t{8}}) {
    expectSortedLikeStd<uint32_t>({}, nthread);
    expectSortedLikeStd<uint32_t>({42}, nthread);
    expectSortedLikeStd<uint32_t>({2, 1}, nthread);
    expectSortedLikeStd<uint32_t>({1, 2}, nthread);
    expectSortedLikeStd<uint32_t>({3, 1, 2}, nthread);
    expectSortedLikeStd<uint32_t>({1, 1, 1, 1}, nthread);
  }
}

// _____________________________________________________________________________
// Small inputs take the serial fallback, but they still have to be sorted
// correctly, and the thresholds in between are where off-by-ones would hide.
TEST(BlockIndirectSort, smallInputsOfEverySize) {
  for (size_t numElements : {size_t{5}, size_t{63}, size_t{64}, size_t{65},
                             size_t{4095}, size_t{4096}, size_t{4097}}) {
    for (uint32_t nthread :
         {uint32_t{1}, uint32_t{2}, uint32_t{5}, uint32_t{6}, uint32_t{8}}) {
      SCOPED_TRACE(
          absl::StrCat("numElements=", numElements, " nthread=", nthread));
      expectSortedLikeStd(randomDistinct(numElements, numElements), nthread);
    }
  }
}

// _____________________________________________________________________________
// Big enough that the blocks, their merging and their moving are all used.
TEST(BlockIndirectSort, parallelPathWithDistinctValues) {
  size_t numElements = 2 * numElementsForParallelPath<uint32_t>(numPoolThreads);
  for (uint32_t nthread :
       {uint32_t{5}, uint32_t{6}, uint32_t{8}, uint32_t{16}}) {
    SCOPED_TRACE(absl::StrCat("nthread=", nthread));
    expectSortedLikeStd(randomDistinct(numElements, nthread), nthread);
  }
}

// _____________________________________________________________________________
// The input patterns that the algorithm has special cases for, all of them big
// enough for the parallel path.
TEST(BlockIndirectSort, specialInputPatterns) {
  size_t numElements = numElementsForParallelPath<uint32_t>(numPoolThreads);
  constexpr uint32_t nthread = numPoolThreads;

  std::vector<uint32_t> ascending(numElements);
  std::iota(ascending.begin(), ascending.end(), uint32_t{0});
  expectSortedLikeStd(ascending, nthread);

  std::vector<uint32_t> descending = ascending;
  ql::ranges::reverse(descending);
  expectSortedLikeStd(descending, nthread);

  expectSortedLikeStd(std::vector<uint32_t>(numElements, 7u), nthread);

  // Only a handful of distinct values, so that a lot of elements compare
  // equal.
  std::vector<uint32_t> fewValues(numElements);
  std::mt19937_64 gen{1234};
  for (auto& value : fewValues) {
    value = static_cast<uint32_t>(gen() % 5);
  }
  expectSortedLikeStd(fewValues, nthread);

  // Sorted except for the very last element, which is what makes the merging
  // of the blocks do almost nothing.
  std::vector<uint32_t> almostSorted = ascending;
  almostSorted.back() = 0;
  expectSortedLikeStd(almostSorted, nthread);
}

// _____________________________________________________________________________
// The last block is the only one that may be incomplete, and it is handled
// separately (`tailProcess`), so all of its sizes have to work.
TEST(BlockIndirectSort, incompleteLastBlock) {
  size_t base = numElementsForParallelPath<uint32_t>(numPoolThreads);
  for (size_t extra :
       {size_t{0}, size_t{1}, size_t{2}, size_t{4095}, size_t{4096}}) {
    SCOPED_TRACE(absl::StrCat("extra=", extra));
    expectSortedLikeStd(randomDistinct(base + extra, extra), numPoolThreads);
  }
}

// _____________________________________________________________________________
TEST(BlockIndirectSort, customComparator) {
  size_t numElements = numElementsForParallelPath<uint32_t>(numPoolThreads);
  expectSortedLikeStd(randomDistinct(numElements, 99), numPoolThreads,
                      std::greater<uint32_t>{});
}

// _____________________________________________________________________________
// Elements that are neither trivially copyable nor cheap to move, which is the
// case that the scratch buffers have to get right.
TEST(BlockIndirectSort, stringElements) {
  size_t numElements = numElementsForParallelPath<std::string>(4);
  std::vector<std::string> values;
  values.reserve(numElements);
  std::mt19937_64 gen{7};
  for (size_t i = 0; i < numElements; ++i) {
    values.push_back(absl::StrCat("value_", gen(), "_with_some_padding"));
  }
  expectSortedLikeStd(std::move(values), numPoolThreads);
}

// _____________________________________________________________________________
// An empty executor is a valid argument; the sort then happens in the calling
// thread.
TEST(BlockIndirectSort, emptyExecutorSortsInCallingThread) {
  auto values =
      randomDistinct(numElementsForParallelPath<uint32_t>(numPoolThreads), 5);
  auto expected = values;
  ql::ranges::sort(expected);
  blockIndirectSort(ql::span<uint32_t>{values}, std::less<uint32_t>{},
                    numPoolThreads, ql::any_io_executor{});
  EXPECT_EQ(values, expected);
}

// _____________________________________________________________________________
// A comparator that throws must not deadlock or leak; the exception has to
// reach the caller once all the tasks in flight have finished.
TEST(BlockIndirectSort, exceptionFromComparatorIsPropagated) {
  auto values =
      randomDistinct(numElementsForParallelPath<uint32_t>(numPoolThreads), 11);
  // Throw only after a while, so that the sort is in full swing by then.
  std::atomic<size_t> numComparisons{0};
  auto comp = [&numComparisons](uint32_t a, uint32_t b) {
    if (numComparisons.fetch_add(1, std::memory_order_relaxed) > 100'000) {
      throw std::runtime_error("comparator failed");
    }
    return a < b;
  };
  AD_EXPECT_THROW_WITH_MESSAGE(
      blockIndirectSort(ql::span<uint32_t>{values}, comp, numPoolThreads,
                        threadPool().get_executor()),
      ::testing::HasSubstr("comparator failed"));
}

// _____________________________________________________________________________
// Several sorts that run on the same executor at the same time must not
// interfere; in particular they must not take each other's scratch buffers.
TEST(BlockIndirectSort, concurrentSortsOnTheSameExecutor) {
  constexpr size_t numSorts = 4;
  size_t numElements = numElementsForParallelPath<uint32_t>(numPoolThreads);
  std::vector<std::vector<uint32_t>> inputs;
  for (size_t i = 0; i < numSorts; ++i) {
    inputs.push_back(randomDistinct(numElements, i));
  }
  std::vector<std::thread> callers;
  for (auto& input : inputs) {
    callers.emplace_back([&input] {
      blockIndirectSort(ql::span<uint32_t>{input}, std::less<uint32_t>{},
                        numPoolThreads, threadPool().get_executor());
    });
  }
  for (auto& caller : callers) {
    caller.join();
  }
  for (const auto& input : inputs) {
    EXPECT_TRUE(ql::ranges::is_sorted(input));
  }
}
