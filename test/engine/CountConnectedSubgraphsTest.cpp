//  Copyright 2024, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include <gmock/gmock.h>

#include "engine/CountConnectedSubgraphs.h"
#include "util/BitUtils.h"
#include "util/Exception.h"
#include "util/Log.h"

using namespace countConnectedSubgraphs;

namespace {
// Return a clique with `size` elements.
PlainGraph makeClique(uint64_t size) {
  PlainGraph clique;
  AD_CORRECTNESS_CHECK(size <= 64);
  uint64_t all = ad_utility::bitMaskForLowerBits(size);
  for (uint64_t i = 0; i < size; ++i) {
    uint64_t notSelf = ~(1ull << i);
    clique.emplace_back(all & notSelf);
  }
  return clique;
}

// Make `n` unconnected cliques of size `size` each.
PlainGraph makeUnconnectedCliques(uint64_t size, size_t n) {
  AD_CORRECTNESS_CHECK(64 / n >= size);
  PlainGraph result;
  for (size_t i = 0; i < n; ++i) {
    auto clique = makeClique(size);
    for (auto& v : clique) {
      v.neighbors_ <<= i * size;
    }
    result.insert(result.end(), clique.begin(), clique.end());
  }
  return result;
}

// Make a chain (the n-th element is connected only to the n-1 and n+1-th
// element with `size` elements.
PlainGraph makeChain(uint64_t size) {
  PlainGraph clique;
  AD_CORRECTNESS_CHECK(size <= 64);
  for (uint64_t i = 0; i < size; ++i) {
    uint64_t left = i > 0 ? (1ull << (i - 1)) : 0;
    uint64_t right = i + 1 < size ? (1ull << (i + 1)) : 0;
    clique.emplace_back(left | right);
  }
  return clique;
}
}  // namespace

// The cliques where each node is connected to each other node
TEST(CountConnectedSubgraphs, cliques) {
  // For a clique of size `n` the number of subgraphs is 2^n - 1 (the number
  // of all subsets, minus the empty set, which we don't count as a subgraph.
  EXPECT_EQ(countSubgraphs(makeClique(1), 10), 1);
  EXPECT_EQ(countSubgraphs(makeClique(2), 10), 3);
  EXPECT_EQ(countSubgraphs(makeClique(3), 10), 7);
  EXPECT_EQ(countSubgraphs(makeClique(4), 20), 15);
  EXPECT_EQ(countSubgraphs(makeClique(5), 50), 31);
  EXPECT_EQ(countSubgraphs(makeClique(10), 2000), 1023);
}

// Test that for unconnected components the subraphs are counted separately and
// then added up.
TEST(CountConnectedSubgraphs, unconnectedCliques) {
  uint64_t budget = 1'000'000;
  for (size_t i = 1; i < 12; ++i) {
    EXPECT_EQ(countSubgraphs(makeUnconnectedCliques(i, 3), budget),
              3 * countSubgraphs(makeClique(i), budget));
  }
}

// Test chains.
TEST(CountConnectedSubgraphs, chains) {
  // For a chain of size `n`, the number of subgraphs is `n+(n-1 )+...+1 =
  // n*(n+1)/2`.
  for (size_t i = 1; i < 30; ++i) {
    EXPECT_EQ(countSubgraphs(makeChain(i), 1'000'000), i * (i + 1) / 2)
        << "i is " << i;
  }
}

// _____________________________________________________________________________
TEST(CountConnectedSubgraphs, emptyGraph) {
  EXPECT_EQ(countSubgraphs({}, 30), 0);
}

// Test that the budget is respected.
TEST(CountConnectedSubgraphs, budgetCap) {
  // In case the budget is violated, `budget + 1` is returned.
  EXPECT_EQ(countSubgraphs(makeClique(3), 0), 1);
  // This would run out of memory without the budget, because the full result
  // would be `2^64`.
  EXPECT_EQ(countSubgraphs(makeClique(64), 100), 101);
}

// _____________________________________________________________________________
TEST(CountConnectedSubgraphs, bitsetToString) {
  EXPECT_EQ(toBitsetString(0), "0");
  EXPECT_EQ(toBitsetString(13), "1101");
}
