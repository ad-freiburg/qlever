// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <array>
#include <boost/asio/thread_pool.hpp>
#include <random>
#include <vector>

#include "backports/algorithm.h"
#include "engine/idTable/BitwiseKeySort.h"
#include "engine/idTable/IdTable.h"
#include "util/AllocatorWithLimit.h"

using namespace ad_utility;

namespace {

// Return a table with `numRows` rows and `numColumns` columns of random IDs.
// The IDs are drawn from a small domain of bit patterns (so that there are
// many duplicates) with random datatype bits.
IdTable randomTable(size_t numRows, size_t numColumns, unsigned seed) {
  std::mt19937_64 engine{seed};
  std::uniform_int_distribution<uint64_t> value{0, 20};
  std::uniform_int_distribution<uint64_t> datatype{0, 3};
  IdTable table{numColumns, makeUnlimitedAllocator<Id>()};
  table.resize(numRows);
  for (size_t i = 0; i < numRows; ++i) {
    for (size_t c = 0; c < numColumns; ++c) {
      table(i, c) = Id::fromBits((datatype(engine) << 60) | value(engine));
    }
  }
  return table;
}

// The expected result: a stable sort by the bits of the `keyColumns`.
template <size_t NumKeys>
IdTable expectedResult(const IdTable& table,
                       const std::array<size_t, NumKeys>& keyColumns) {
  IdTable result = table.clone();
  auto keys = [&keyColumns](const auto& row) {
    std::array<uint64_t, NumKeys> result;
    for (size_t k = 0; k < NumKeys; ++k) {
      result[k] = row[keyColumns[k]].getBits();
    }
    return result;
  };
  ql::ranges::stable_sort(result, [&keys](const auto& a, const auto& b) {
    return keys(a) < keys(b);
  });
  return result;
}

// Sort a random table (see above) with the given `keyColumns` and
// `parallelism`, both as a dynamic and as a static table, and compare the
// results to the expected result.
template <size_t NumKeys, size_t NumStaticCols = 0>
void testSort(size_t numRows, size_t numColumns,
              const std::array<size_t, NumKeys>& keyColumns,
              const BitwiseKeySortParallelism& parallelism, unsigned seed) {
  auto input = randomTable(numRows, numColumns, seed);
  auto expected = expectedResult(input, keyColumns);
  {
    IdTable table = input.clone();
    sortByBitwiseKeys(table, keyColumns, parallelism);
    EXPECT_EQ(table, expected);
  }
  if constexpr (NumStaticCols != 0) {
    auto table = input.clone().toStatic<NumStaticCols>();
    sortByBitwiseKeys(table, keyColumns, parallelism);
    EXPECT_EQ(std::move(table).toDynamic(), expected);
  }
}

// Run the `test` serially and in parallel on a thread pool.
template <typename Test>
void seriallyAndInParallel(const Test& test) {
  test(BitwiseKeySortParallelism{});
  boost::asio::thread_pool pool{8};
  test(BitwiseKeySortParallelism{pool.get_executor(), 8});
  // A parallelism of one is serial, whatever the executor.
  test(BitwiseKeySortParallelism{pool.get_executor(), 1});
  pool.join();
}
}  // namespace

// _____________________________________________________________________________
TEST(BitwiseKeySort, keysAreAllColumns) {
  seriallyAndInParallel([](const auto& parallelism) {
    testSort<4, 4>(1000, 4, {2, 0, 1, 3}, parallelism, 1);
    testSort<1, 1>(1000, 1, {0}, parallelism, 2);
    testSort<2, 2>(777, 2, {1, 0}, parallelism, 3);
  });
}

// _____________________________________________________________________________
TEST(BitwiseKeySort, keysAreSomeColumns) {
  seriallyAndInParallel([](const auto& parallelism) {
    // Payload columns that are not keys.
    testSort<4, 6>(1000, 6, {1, 0, 2, 3}, parallelism, 4);
    testSort<1, 3>(1000, 3, {2}, parallelism, 5);
    // A duplicate key column.
    testSort<3, 3>(1000, 3, {0, 0, 1}, parallelism, 6);
    // All columns are keys, but one of them twice.
    testSort<3, 2>(1000, 2, {1, 0, 1}, parallelism, 7);
  });
}

// _____________________________________________________________________________
TEST(BitwiseKeySort, smallTables) {
  seriallyAndInParallel([](const auto& parallelism) {
    testSort<2, 3>(0, 3, {0, 1}, parallelism, 8);
    testSort<2, 3>(1, 3, {0, 1}, parallelism, 9);
    testSort<2, 2>(2, 2, {1, 0}, parallelism, 10);
  });
}

// _____________________________________________________________________________
TEST(BitwiseKeySort, largeTables) {
  // Large enough for the parallel sort to use its block-wise algorithm and
  // for the parallel gathering of the keys.
  seriallyAndInParallel([](const auto& parallelism) {
    testSort<4, 4>(300'000, 4, {0, 1, 2, 3}, parallelism, 11);
    testSort<3, 5>(200'000, 5, {4, 2, 0}, parallelism, 12);
  });
}

// _____________________________________________________________________________
TEST(BitwiseKeySort, veryLargeTables) {
  // Large enough for the parallel sample sort, see `MIN_ROWS_FOR_SAMPLE_SORT`.
  constexpr size_t numRows =
      ad_utility::bitwiseKeySort::detail::MIN_ROWS_FOR_SAMPLE_SORT + 12345;
  seriallyAndInParallel([](const auto& parallelism) {
    testSort<4, 4>(numRows, 4, {3, 1, 2, 0}, parallelism, 14);
    testSort<2, 5>(numRows, 5, {4, 0}, parallelism, 15);
  });
}

// _____________________________________________________________________________
TEST(BitwiseKeySort, invalidKeyColumn) {
  IdTable table = randomTable(10, 2, 13);
  EXPECT_THROW(sortByBitwiseKeys(table, std::array<size_t, 1>{2}),
               ad_utility::Exception);
}

// _____________________________________________________________________________
namespace {
struct WithKeys {
  static constexpr std::array<size_t, 2> bitwiseKeyColumns{0, 1};
};
struct WithoutKeys {};
}  // namespace
TEST(BitwiseKeySort, hasBitwiseKeyColumns) {
  static_assert(hasBitwiseKeyColumns<WithKeys>);
  static_assert(!hasBitwiseKeyColumns<WithoutKeys>);
}
