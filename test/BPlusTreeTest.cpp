// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR

// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gtest/gtest.h>

#include <algorithm>
#include <cstdint>
#include <limits>
#include <numeric>
#include <random>
#include <vector>

#include "util/BPlusTree.h"

using ad_utility::BPlusTree;

// Reference lower_bound expressed as `{value, rank}`.
template <typename T>
std::pair<T, std::size_t> ref_lower_bound(const std::vector<T>& sorted,
                                          T query) {
  auto it = std::lower_bound(sorted.begin(), sorted.end(), query);
  std::size_t rank = static_cast<std::size_t>(it - sorted.begin());
  T value = (it == sorted.end()) ? std::numeric_limits<T>::max() : *it;
  return {value, rank};
}

// Reference upper_bound expressed as `{value, rank}`.
template <typename T>
std::pair<T, std::size_t> ref_upper_bound(const std::vector<T>& sorted,
                                          T query) {
  auto it = std::upper_bound(sorted.begin(), sorted.end(), query);
  std::size_t rank = static_cast<std::size_t>(it - sorted.begin());
  T value = (it == sorted.end()) ? std::numeric_limits<T>::max() : *it;
  return {value, rank};
}

// Build a `BPlusTree<T, CacheLineSize>` and verify `lower_bound`,
// `upper_bound`, and their batch variants (both scalar and SIMD) against the
// reference implementations.
template <typename T, std::size_t CacheLineSize = 64>
void verify(const std::vector<T>& sorted, const std::vector<T>& queries) {
  BPlusTree<T, CacheLineSize> bt(sorted);
  ASSERT_EQ(bt.size(), sorted.size());

  for (T q : queries) {
    // Single lower_bound.
    auto exp_lb = ref_lower_bound(sorted, q);
    auto a0 = bt.template lowerBound<false>(q);
    EXPECT_EQ(a0.first, exp_lb.first) << "scalar lb value, q=" << q;
    EXPECT_EQ(a0.second, exp_lb.second) << "scalar lb rank, q=" << q;
    auto a1 = bt.template lowerBound<true>(q);
    EXPECT_EQ(a1.first, exp_lb.first) << "simd lb value, q=" << q;
    EXPECT_EQ(a1.second, exp_lb.second) << "simd lb rank, q=" << q;

    // Single upper_bound.
    auto exp_ub = ref_upper_bound(sorted, q);
    auto b0 = bt.template upperBound<false>(q);
    EXPECT_EQ(b0.first, exp_ub.first) << "scalar ub value, q=" << q;
    EXPECT_EQ(b0.second, exp_ub.second) << "scalar ub rank, q=" << q;
    auto b1 = bt.template upperBound<true>(q);
    EXPECT_EQ(b1.first, exp_ub.first) << "simd ub value, q=" << q;
    EXPECT_EQ(b1.second, exp_ub.second) << "simd ub rank, q=" << q;
  }

  // Batch lower_bound (scalar).
  {
    std::vector<std::size_t> r(queries.size(), ~std::size_t{0});
    std::size_t i = 0;
    bt.template multiLowerBound<8, false>(
        queries, [&r, &i](std::size_t rank) { r[i++] = rank; });
    for (std::size_t k = 0; k < queries.size(); ++k) {
      EXPECT_EQ(r[k], ref_lower_bound(sorted, queries[k]).second)
          << "multi-scalar lb k=" << k << " q=" << queries[k];
    }
  }

  // Batch lower_bound (SIMD).
  {
    std::vector<std::size_t> r(queries.size(), ~std::size_t{0});
    std::size_t i = 0;
    bt.template multiLowerBound<16, true>(
        queries, [&r, &i](std::size_t rank) { r[i++] = rank; });
    for (std::size_t k = 0; k < queries.size(); ++k) {
      EXPECT_EQ(r[k], ref_lower_bound(sorted, queries[k]).second)
          << "multi-simd lb k=" << k << " q=" << queries[k];
    }
  }

  // Batch upper_bound (scalar).
  {
    std::vector<std::size_t> r(queries.size(), ~std::size_t{0});
    std::size_t i = 0;
    bt.template multiUpperBound<8, false>(
        queries, [&r, &i](std::size_t rank) { r[i++] = rank; });
    for (std::size_t k = 0; k < queries.size(); ++k) {
      EXPECT_EQ(r[k], ref_upper_bound(sorted, queries[k]).second)
          << "multi-scalar ub k=" << k << " q=" << queries[k];
    }
  }

  // Batch upper_bound (SIMD).
  {
    std::vector<std::size_t> r(queries.size(), ~std::size_t{0});
    std::size_t i = 0;
    bt.template multiUpperBound<16, true>(
        queries, [&r, &i](std::size_t rank) { r[i++] = rank; });
    for (std::size_t k = 0; k < queries.size(); ++k) {
      EXPECT_EQ(r[k], ref_upper_bound(sorted, queries[k]).second)
          << "multi-simd ub k=" << k << " q=" << queries[k];
    }
  }
}

// ── Empty / trivial sizes ────────────────────────────────────────────────────

TEST(BPlusTree, Empty) {
  BPlusTree<int> bt(std::vector<int>{});
  EXPECT_EQ(bt.size(), 0u);
  auto [lv, lr] = bt.lowerBound(42);
  EXPECT_EQ(lv, std::numeric_limits<int>::max());
  EXPECT_EQ(lr, 0u);
  auto [uv, ur] = bt.upperBound(42);
  EXPECT_EQ(uv, std::numeric_limits<int>::max());
  EXPECT_EQ(ur, 0u);
}

TEST(BPlusTree, SingleElement) {
  std::vector<int> data = {7};
  verify(data, {0, 6, 7, 8, 100});
}

TEST(BPlusTree, TwoElements) {
  std::vector<int> data = {3, 9};
  verify(data, {0, 3, 5, 9, 10});
}

// ── Sizes around B boundaries ────────────────────────────────────────────────

TEST(BPlusTree, SizeB_minus1) {
  constexpr std::size_t B = 64 / sizeof(int);
  std::vector<int> data(B - 1);
  std::iota(data.begin(), data.end(), 0);
  std::vector<int> queries(B + 5);
  std::iota(queries.begin(), queries.end(), -2);
  verify(data, queries);
}

TEST(BPlusTree, SizeB) {
  constexpr std::size_t B = 64 / sizeof(int);
  std::vector<int> data(B);
  std::iota(data.begin(), data.end(), 10);
  std::vector<int> queries(B + 5);
  std::iota(queries.begin(), queries.end(), 8);
  verify(data, queries);
}

TEST(BPlusTree, SizeB_plus1) {
  constexpr std::size_t B = 64 / sizeof(int);
  std::vector<int> data(B + 1);
  std::iota(data.begin(), data.end(), 5);
  std::vector<int> queries(B + 5);
  std::iota(queries.begin(), queries.end(), 3);
  verify(data, queries);
}

// ── Correctness for various sizes ────────────────────────────────────────────

TEST(BPlusTree, Size100) {
  std::vector<int> data(100);
  std::iota(data.begin(), data.end(), -10);
  std::vector<int> queries(120);
  std::iota(queries.begin(), queries.end(), -15);
  verify(data, queries);
}

TEST(BPlusTree, Size1000) {
  std::vector<int> data(1000);
  std::iota(data.begin(), data.end(), 0);
  std::vector<int> queries(1010);
  std::iota(queries.begin(), queries.end(), -5);
  verify(data, queries);
}

TEST(BPlusTree, Size10000) {
  std::vector<int> data(10000);
  std::iota(data.begin(), data.end(), 0);
  std::vector<int> queries(10010);
  std::iota(queries.begin(), queries.end(), -5);
  verify(data, queries);
}

// ── Edge-case queries ────────────────────────────────────────────────────────

TEST(BPlusTree, QueryBelowMin) {
  std::vector<int> data = {10, 20, 30, 40, 50};
  verify(data, {std::numeric_limits<int>::min(), 0, 9});
}

TEST(BPlusTree, QueryAboveMax) {
  std::vector<int> data = {10, 20, 30};
  auto [lv, lr] = BPlusTree<int>(data).lowerBound(100);
  EXPECT_EQ(lv, std::numeric_limits<int>::max());
  EXPECT_EQ(lr, 3u);
  auto [uv, ur] = BPlusTree<int>(data).upperBound(100);
  EXPECT_EQ(uv, std::numeric_limits<int>::max());
  EXPECT_EQ(ur, 3u);
}

TEST(BPlusTree, QueryEqualsMin) {
  std::vector<int> data = {5, 10, 15};
  verify(data, {5});
}

TEST(BPlusTree, QueryEqualsMax) {
  std::vector<int> data = {5, 10, 15};
  verify(data, {15});
}

// ── upper_bound specific edge cases ─────────────────────────────────────────

TEST(BPlusTree, UpperBoundAllSameAsQuery) {
  // When all elements equal the query, upper_bound rank == size.
  std::vector<int> data(20, 42);
  BPlusTree<int> bt(data);
  auto [uv, ur] = bt.upperBound<false>(42);
  EXPECT_EQ(uv, std::numeric_limits<int>::max());
  EXPECT_EQ(ur, 20u);
  auto [uv2, ur2] = bt.upperBound<true>(42);
  EXPECT_EQ(uv2, std::numeric_limits<int>::max());
  EXPECT_EQ(ur2, 20u);
}

TEST(BPlusTree, UpperBoundBelowAll) {
  // When query < all elements, upper_bound rank == 0 and lower_bound rank == 0.
  std::vector<int> data = {10, 20, 30};
  BPlusTree<int> bt(data);
  auto [uv, ur] = bt.upperBound(5);
  EXPECT_EQ(uv, 10);
  EXPECT_EQ(ur, 0u);
}

TEST(BPlusTree, UpperBoundVsLowerBoundOnDuplicates) {
  // For a run of equal elements, lower_bound points to the first and
  // upper_bound points past the last.
  std::vector<int> data = {1, 3, 3, 3, 7};
  BPlusTree<int> bt(data);
  auto [lv, lr] = bt.lowerBound(3);
  EXPECT_EQ(lv, 3);
  EXPECT_EQ(lr, 1u);
  auto [uv, ur] = bt.upperBound(3);
  EXPECT_EQ(uv, 7);
  EXPECT_EQ(ur, 4u);
}

// ── Duplicates ───────────────────────────────────────────────────────────────

TEST(BPlusTree, AllSame) {
  std::vector<int> data(50, 42);
  verify(data, {40, 41, 42, 43, 44});
}

TEST(BPlusTree, DuplicatesRandom) {
  std::mt19937 rng(12345);
  std::vector<int> data(200);
  for (auto& v : data) v = static_cast<int>(rng() % 20);
  std::sort(data.begin(), data.end());
  std::vector<int> queries(25);
  std::iota(queries.begin(), queries.end(), -2);
  verify(data, queries);
}

// ── Random large test ────────────────────────────────────────────────────────

TEST(BPlusTree, RandomLarge) {
  constexpr std::size_t N = 50'000;
  std::mt19937 rng(99999);
  std::vector<int> data(N);
  for (auto& v : data) v = static_cast<int>(rng());
  std::sort(data.begin(), data.end());

  std::vector<int> queries(1000);
  for (auto& q : queries) q = static_cast<int>(rng());

  queries.push_back(data[0]);
  queries.push_back(data[2]);

  verify(data, queries);
}

// ── int64_t instantiation (B = 8, hits AVX2 specialization) ──────────────────

TEST(BPlusTree, Int64) {
  std::vector<std::int64_t> data(500);
  std::iota(data.begin(), data.end(), std::int64_t{-100});
  std::vector<std::int64_t> queries(510);
  std::iota(queries.begin(), queries.end(), std::int64_t{-105});
  verify(data, queries);
}

TEST(BPlusTree, Int64Random) {
  constexpr std::size_t N = 20'000;
  std::mt19937_64 rng(11111);
  std::vector<std::int64_t> data(N);
  for (auto& v : data) v = static_cast<std::int64_t>(rng());
  std::sort(data.begin(), data.end());

  std::vector<std::int64_t> queries(2000);
  for (auto& q : queries) q = static_cast<std::int64_t>(rng());
  verify(data, queries);
}

// ── Custom B (small) ─────────────────────────────────────────────────────────

TEST(BPlusTree, SmallBranching) {
  std::vector<int> data(50);
  std::iota(data.begin(), data.end(), 0);
  std::vector<int> queries(55);
  std::iota(queries.begin(), queries.end(), -2);
  // Pass `CacheLineSize = B * sizeof(int)` to get the desired branching factor.
  verify<int, 2 * sizeof(int)>(data, queries);
  verify<int, 3 * sizeof(int)>(data, queries);
  verify<int, 4 * sizeof(int)>(data, queries);
}

// ── Tail handling in multi_lower_bound / multi_upper_bound ───────────────────

TEST(BPlusTree, MultiLookupTail) {
  constexpr std::size_t N = 1'000;
  std::vector<int> data(N);
  std::iota(data.begin(), data.end(), 0);
  BPlusTree<int> bt(data);

  // 23 queries with batch size 8: two full batches plus a 7-element tail.
  std::vector<int> q(23);
  std::iota(q.begin(), q.end(), 100);

  std::vector<std::size_t> r_lb(q.size());
  std::size_t i = 0;
  bt.multiLowerBound<8>(q, [&r_lb, &i](std::size_t rank) { r_lb[i++] = rank; });
  for (std::size_t k = 0; k < q.size(); ++k) {
    EXPECT_EQ(r_lb[k], ref_lower_bound(data, q[k]).second)
        << "multi lb tail k=" << k;
  }

  std::vector<std::size_t> r_ub(q.size());
  i = 0;
  bt.multiUpperBound<8>(q, [&r_ub, &i](std::size_t rank) { r_ub[i++] = rank; });
  for (std::size_t k = 0; k < q.size(); ++k) {
    EXPECT_EQ(r_ub[k], ref_upper_bound(data, q[k]).second)
        << "multi ub tail k=" << k;
  }
}

// Verify that `multi_bound<IsUpperBound>` gives identical results to the
// single-query variants for both bound types.
TEST(BPlusTree, MultiBoundMatchesSingle) {
  constexpr std::size_t N = 500;
  std::vector<int> data(N);
  std::iota(data.begin(), data.end(), 0);
  BPlusTree<int> bt(data);

  std::vector<int> q(N + 10);
  std::iota(q.begin(), q.end(), -5);

  // lower_bound.
  std::vector<std::size_t> r_lb(q.size());
  std::size_t i = 0;
  bt.multiBound<false, 16, true>(
      q, [&r_lb, &i](std::size_t rank) { r_lb[i++] = rank; });
  for (std::size_t k = 0; k < q.size(); ++k) {
    EXPECT_EQ(r_lb[k], bt.lowerBound<true>(q[k]).second)
        << "multi vs single lb k=" << k;
  }

  // upper_bound.
  std::vector<std::size_t> r_ub(q.size());
  i = 0;
  bt.multiBound<true, 16, true>(
      q, [&r_ub, &i](std::size_t rank) { r_ub[i++] = rank; });
  for (std::size_t k = 0; k < q.size(); ++k) {
    EXPECT_EQ(r_ub[k], bt.upperBound<true>(q[k]).second)
        << "multi vs single ub k=" << k;
  }
}
