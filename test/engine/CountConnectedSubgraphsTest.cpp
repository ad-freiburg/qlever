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
Graph makeClique(uint64_t size) {
  Graph clique;
  AD_CORRECTNESS_CHECK(size <= 64);
  uint64_t all = ad_utility::bitMaskForLowerBits(size);
  for (uint64_t i = 0; i < size; ++i) {
    uint64_t notSelf = ~(1ull << i);
    clique.emplace_back(all & notSelf);
  }
  return clique;
}

// Create graph with `n` disjoint cliques of size `k` each.
Graph makeDisjointCliques(size_t n, uint64_t k) {
  AD_CORRECTNESS_CHECK(k * n <= 64);
  Graph result;
  for (size_t i = 0; i < n; ++i) {
    auto clique = makeClique(k);
    for (auto& v : clique) {
      v.neighbors_ <<= i * k;
    }
    result.insert(result.end(), clique.begin(), clique.end());
  }
  return result;
}

// Make a chain graph with `n` nodes, where node 'i' is connected only to nodes
// 'i-1' and 'i+1'.
Graph makeChain(uint64_t n) {
  Graph clique;
  AD_CORRECTNESS_CHECK(n <= 64);
  for (uint64_t i = 0; i < n; ++i) {
    uint64_t left = i > 0 ? (1ull << (i - 1)) : 0;
    uint64_t right = i + 1 < n ? (1ull << (i + 1)) : 0;
    clique.emplace_back(left | right);
  }
  return clique;
}
}  // namespace

// Test `countSubgraphs` for individual cliques. For a clique of size k, the
// number of subgraphs is 2^k - 1 (the number of all subsets, minus the empty
// set, which we don't count as a subgraph).
TEST(CountConnectedSubgraphs, cliques) {
  EXPECT_EQ(countSubgraphs(makeClique(1), 10), 1);
  EXPECT_EQ(countSubgraphs(makeClique(2), 10), 3);
  EXPECT_EQ(countSubgraphs(makeClique(3), 10), 7);
  EXPECT_EQ(countSubgraphs(makeClique(4), 20), 15);
  EXPECT_EQ(countSubgraphs(makeClique(5), 50), 31);
  EXPECT_EQ(countSubgraphs(makeClique(10), 2000), 1023);
}

// Test `countSubgraphs` for disjoint cliques. Then the total number of
// subgraphs is simply the sum of the subgraphs of the individual cliques.
TEST(CountConnectedSubgraphs, unconnectedCliques) {
  uint64_t budget = 1'000'000;
  for (size_t i = 1; i < 12; ++i) {
    EXPECT_EQ(countSubgraphs(makeDisjointCliques(3, i), budget),
              3 * countSubgraphs(makeClique(i), budget));
  }
}

// Test `countSubgraphs` for chains. For a chain of size `n`, the number of
// subgraphs is `n + (n-1) + ... + 1 = n * (n+1) / 2`.
TEST(CountConnectedSubgraphs, chains) {
  for (size_t i = 1; i < 30; ++i) {
    EXPECT_EQ(countSubgraphs(makeChain(i), 1'000'000), i * (i + 1) / 2)
        << "i is " << i;
  }
}

// Test `countSubgraphs` for the empty graph.
TEST(CountConnectedSubgraphs, emptyGraph) {
  EXPECT_EQ(countSubgraphs({}, 30), 0);
}

// Test `countSubgraphs` for a limited budget.
TEST(CountConnectedSubgraphs, limitedBudget) {
  // In case the budget is violated, `budget + 1` is returned.
  EXPECT_EQ(countSubgraphs(makeClique(3), 0), 1);
  // This would run out of memory without the budget, because the full result
  // would be `2^64`.
  EXPECT_EQ(countSubgraphs(makeClique(64), 100), 101);
}

// Test conversion of bitsets to strings.
TEST(CountConnectedSubgraphs, bitsetToString) {
  EXPECT_EQ(toBitsetString(0), "0");
  EXPECT_EQ(toBitsetString(13), "1101");
}
