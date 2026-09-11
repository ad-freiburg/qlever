// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Hannah Bast <bast@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <map>
#include <random>
#include <utility>
#include <vector>

#include "util/GTestHelpers.h"
#include "util/SortedSequenceManySortedBlocks.h"
#include "util/TransparentFunctors.h"

namespace ad_utility {
namespace {

using namespace ::testing;
using Pair = std::pair<int, int>;
// A small block size, so that the tests exercise block splitting.
constexpr size_t testBlockSize = 4;
using SV = SortedSequenceManySortedBlocks<
    Pair, std::less<>, MemberProjection<&Pair::first>, testBlockSize>;

// The elements of `s` as a vector (requires a consolidated state).
std::vector<Pair> elementsOf(const SV& s) {
  std::vector<Pair> result;
  ql::ranges::copy(s.getSortedView(), std::back_inserter(result));
  return result;
}
}  // namespace

// _____________________________________________________________________________
TEST(SortedSequenceManySortedBlocksTest, blockSplitting) {
  // Insert far more elements than fit in one block (block size 4), in
  // descending order, across several consolidation rounds.
  SV s;
  for (int round = 0; round < 5; ++round) {
    for (int i = 19 - round; i >= 0; i -= 5) {
      s.insert({i, round});
    }
    s.consolidate();
  }
  // All 20 keys are present, each with the value of the round it was inserted
  // in (each key is inserted exactly once).
  auto elems = elementsOf(s);
  ASSERT_EQ(elems.size(), 20);
  for (int i = 0; i < 20; ++i) {
    EXPECT_EQ(elems[i].first, i);
    EXPECT_EQ(elems[i].second, (19 - i) % 5);
  }
  EXPECT_EQ(s.sizeForTesting(), 20);
}

}  // namespace ad_utility
