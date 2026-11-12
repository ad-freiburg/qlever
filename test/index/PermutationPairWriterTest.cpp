// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gtest/gtest.h>

#include <algorithm>
#include <random>

#include "../util/GTestHelpers.h"
#include "../util/PermutationPairTestHelpers.h"
#include "index/ExternalSortFunctors.h"

// The tests that write a pair of twin permutations from a sorted input and
// check the complete result, for both the classical writer
// (`CompressedRelationWriter::createPermutationPair`) and the chunkwise writer
// (`chunkwisePermutationWriter::createPermutationPair`), which is meant as a
// drop-in replacement for it. The tests that are specific to the chunkwise
// writer are in
// `chunkwisePermutationWriter/ChunkwisePermutationWriterTest.cpp`.
//
// NOTE: The chunkwise writer needs the parallel merge, which only exists in the
// C++20 mode of the build.
#ifndef QLEVER_REDUCED_FEATURE_SET_FOR_CPP17

namespace {
using namespace permutationPairTestHelpers;

// The tests are parameterized by the writer that is tested.
class PermutationPairWriter : public ::testing::TestWithParam<Mode> {
 protected:
  // Run `writeAndCheck` with the writer of the current test.
  template <typename Comparator = CompareAllColumns>
  void check(const TestRows& rows, const Config& config,
             source_location l = AD_CURRENT_SOURCE_LOC()) {
    writeAndCheck<Comparator>(rows, config, GetParam(), l);
  }
};
}  // namespace

// Test empty inputs, with and without the removal of duplicates, and with
// payload columns (indices can be empty, for example the internal ones).
TEST_P(PermutationPairWriter, emptyInput) {
  check({}, Config{});
  Config config;
  config.options_.removeDuplicates_ = false;
  check({}, config);
  config.numColumns_ = 6;
  check({}, config);
}

// _____________________________________________________________________________
TEST_P(PermutationPairWriter, singleSmallRelation) {
  check({{1, 2, 3, 0}, {1, 2, 3, 0}, {1, 2, 4, 0}, {1, 2, 4, 1}}, Config{});
}

// _____________________________________________________________________________
TEST_P(PermutationPairWriter, fewSmallRelationsInSingleBlock) {
  TestRows rows;
  for (int64_t i = 0; i < 30; ++i) {
    rows.push_back({i / 3, i % 7, i % 5, i % 2});
  }
  check(rows, Config{});
}

// _____________________________________________________________________________
TEST_P(PermutationPairWriter, singleLargeRelation) {
  TestRows rows;
  for (int64_t i = 0; i < 1'000; ++i) {
    rows.push_back({7, i % 37, i % 11, i % 3});
  }
  check(rows, Config{});
}

// _____________________________________________________________________________
TEST_P(PermutationPairWriter, randomRows) {
  check(randomRows(100'000, 4, 42), Config{});
}

// _____________________________________________________________________________
TEST_P(PermutationPairWriter, randomRowsWithoutDuplicateRemoval) {
  auto rows = randomRows(100'000, 4, 43);
  ql::ranges::sort(rows);
  rows.erase(std::unique(rows.begin(), rows.end()), rows.end());
  Config config;
  config.options_.removeDuplicates_ = false;
  check(rows, config);
}

// _____________________________________________________________________________
TEST_P(PermutationPairWriter, randomRowsWithPayloadColumns) {
  Config config;
  config.numColumns_ = 6;
  check(randomRows(100'000, 6, 44), config);
}

// _____________________________________________________________________________
TEST_P(PermutationPairWriter, randomRowsWithNonIdentityPermutation) {
  Config config;
  config.permutation_ = qlever::KeyOrder{1, 0, 2, 3};
  // The sorter has to sort by the permutation, which the comparator
  // `SortTriple<1, 0, 2>` does (it also compares the graph column).
  check<SortTriple<1, 0, 2>>(randomRows(100'000, 4, 45), config);
}

// _____________________________________________________________________________
TEST_P(PermutationPairWriter, mediumRelationsAroundTheLargeThreshold) {
  // Relations whose size is around the `0.8 * blocksize` threshold and around
  // the blocksize, so that some of them are small and others large (and for
  // the chunkwise writer, so are the parts at the boundaries of the chunks).
  TestRows rows;
  std::mt19937_64 engine{46};
  for (int64_t col0 = 0; col0 < 3'000; ++col0) {
    auto numRows = std::uniform_int_distribution<int64_t>{60, 140}(engine);
    for (int64_t i = 0; i < numRows; ++i) {
      rows.push_back({col0, i % 50, i, i % 2});
    }
  }
  check(rows, Config{});
}

INSTANTIATE_TEST_SUITE_P(Writers, PermutationPairWriter,
                         ::testing::Values(Mode::Classical, Mode::Chunkwise),
                         [](const ::testing::TestParamInfo<Mode>& info) {
                           return toString(info.param);
                         });

#endif  // QLEVER_REDUCED_FEATURE_SET_FOR_CPP17
