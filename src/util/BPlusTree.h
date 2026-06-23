// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR

// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#pragma once

#include <absl/base/prefetch.h>
#include <hwy/highway.h>

#include <array>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <limits>
#include <new>
#include <ranges>
#include <type_traits>
#include <utility>
#include <vector>

#include "backports/algorithm.h"
#include "backports/concepts.h"
#include "backports/span.h"

namespace ad_utility {

// Custom allocator that returns memory aligned to `Alignment` bytes so that
// every node starts on an alignment boundary.  This guarantees that a single
// cache-line load fetches the full node without straddling two lines, and that
// Highway's `LoadU` receives vector-aligned pointers on all supported ISAs.
// TODO: Replace with the generic `AlignedAllocator` from another PR that
// introduces a more general version of this allocator into `util/`.
template <typename T, std::size_t Alignment = 64>
struct AlignedAllocator {
  using value_type = T;
  template <typename U>
  struct rebind {
    using other = AlignedAllocator<U, Alignment>;
  };

  AlignedAllocator() noexcept = default;
  template <typename U>
  AlignedAllocator(const AlignedAllocator<U, Alignment>&) noexcept {}

  T* allocate(std::size_t n) {
    if (n == 0) return nullptr;
    std::size_t bytes = n * sizeof(T);
    bytes = (bytes + Alignment - 1) & ~(Alignment - 1);
    void* p = std::aligned_alloc(Alignment, bytes);
    if (!p) throw std::bad_alloc();
    return static_cast<T*>(p);
  }
  void deallocate(T* p, std::size_t) noexcept { std::free(p); }
};

template <typename T1, typename T2, std::size_t A>
bool operator==(const AlignedAllocator<T1, A>&,
                const AlignedAllocator<T2, A>&) noexcept {
  return true;
}
template <typename T1, typename T2, std::size_t A>
bool operator!=(const AlignedAllocator<T1, A>&,
                const AlignedAllocator<T2, A>&) noexcept {
  return false;
}

namespace bplus_tree_detail {

// Counts the number of `keys[i]` (for `i` in `[0, B)`) that satisfy the
// predicate determined by `IsUpperBound`: when `false`, counts `keys[i] <
// query` (lower-bound semantics); when `true`, counts `keys[i] <= query`
// (upper-bound semantics).
template <bool IsUpperBound, typename T, std::size_t B>
inline std::size_t findCountScalar(const T* keys, T query) noexcept {
  // IIFE: pick the predicate once at compile time, then call `count_if` once.
  auto pred = [query]() noexcept {
    if constexpr (IsUpperBound) {
      return [query](T k) noexcept { return k <= query; };
    } else {
      return [query](T k) noexcept { return k < query; };
    }
  }();
  return static_cast<std::size_t>(ql::ranges::count_if(keys, keys + B, pred));
}

// SIMD variant of `findCountScalar` using Google Highway.
// `CappedTag<T, B>` picks `min(native lanes, B)` lanes (rounded down to a
// power of two) so the cache-line-sized node tiles cleanly over any vector
// width (SSE 128-b, NEON 128-b, AVX2 256-b, AVX-512 512-b).  `IsUpperBound`
// selects `hn::Lt` (strictly less) or `hn::Le` (less-or-equal).
namespace hn = hwy::HWY_NAMESPACE;

template <bool IsUpperBound, typename T, std::size_t B>
inline std::size_t findCountHwy(const T* keys, T query) noexcept {
  // `CappedTag<T, B>`: selects at most `B` SIMD lanes of type `T`, capped to
  // the native vector width (so at most one cache-line worth of keys per call).
  using D = hn::CappedTag<T, B>;
  D d;
  // `Set(d, query)`: broadcast the scalar `query` into all lanes of a vector.
  const auto q = hn::Set(d, query);
  std::size_t count = 0;
  for (std::size_t i = 0; i < B; i += hn::Lanes(d)) {
    // `LoadU(d, ptr)`: load `Lanes(d)` elements without alignment requirement.
    const auto k = hn::LoadU(d, keys + i);
    if constexpr (IsUpperBound) {
      // `Le(k, q)`: lane-wise mask, lane j = 1 iff keys[i+j] <= query.
      // `CountTrue(d, mask)`: popcount of the mask.
      count += hn::CountTrue(d, hn::Le(k, q));
    } else {
      // `Lt(k, q)`: lane-wise mask, lane j = 1 iff keys[i+j] < query.
      count += hn::CountTrue(d, hn::Lt(k, q));
    }
  }
  return count;
}

// Callable that dispatches to the SIMD or scalar path at compile time.
// The SIMD path requires `B` to be a power of two >= 4 so that the node width
// tiles cleanly over any reasonable SIMD vector width; small branching factors
// and non-arithmetic types always fall back to the scalar path.
// `operator()` is used so call sites read as a function-object invocation.
template <bool IsUpperBound, typename T, std::size_t B, bool USE_SIMD>
struct FindUpperOrLowerBoundInCacheLine {
  static constexpr bool isPowerOfTwo = (B & (B - 1)) == 0;
  inline std::size_t operator()(const T* keys, T query) const noexcept {
    if constexpr (USE_SIMD && B >= 4 && isPowerOfTwo &&
                  std::is_arithmetic_v<T>) {
      return findCountHwy<IsUpperBound, T, B>(keys, query);
    } else {
      return findCountScalar<IsUpperBound, T, B>(keys, query);
    }
  }
};

}  // namespace bplus_tree_detail

// Read-only, cache-line-optimized flat B+ tree for fast `lowerBound` and
// `upperBound` lookups on a static sorted sequence.  All data must be provided
// at construction time via a single bulk load; the tree cannot be modified
// after construction.
//
// All values live in the leaves; internal nodes hold only separator keys.  The
// tree is full and balanced: a sorted input is padded with
// `std::numeric_limits<T>::max()` so every level has the maximum fan-out
// (`branchingFactor+1`).  All nodes (internal nodes first, leaves at the tail)
// are stored in a single flat `cacheLineSize`-byte-aligned array in BFS order,
// so every traversal step is a single cache-line read and child indices are
// computed by arithmetic: no pointers, no branches.
//
// Template parameters:
//   `T`             – key type; must be arithmetic; `numeric_limits<T>::max()`
//                     is used as a padding sentinel.
//   `CacheLineSize` – node size in bytes; defaults to 64 (one cache line on
//                     x86-64 and AArch64).  `sizeof(T)` must divide it evenly;
//                     the branching factor is derived as `CacheLineSize /
//                     sizeof(T)`.
template <typename T, std::size_t CacheLineSize = 64>
class BPlusTree {
  // `sizeof(T)` must evenly divide `CacheLineSize` so that exactly one node
  // fits per cache line with no wasted bytes.
  static_assert(CacheLineSize % sizeof(T) == 0,
                "sizeof(T) must divide CacheLineSize evenly.");
  static_assert(std::is_arithmetic_v<T>, "T must be arithmetic.");

 public:
  // Number of keys per node (= `CacheLineSize / sizeof(T)`).
  static constexpr std::size_t branchingFactor = CacheLineSize / sizeof(T);

  // Node size in bytes used for alignment and branching-factor derivation.
  // Note: `std::hardware_destructive_interference_size` would be the ideal
  // value here but it is not available in Clang 18 without target-specific
  // compilation flags, so the default template argument uses 64 directly.
  static constexpr std::size_t cacheLineSize = CacheLineSize;

 private:
  static constexpr std::size_t B = branchingFactor;

  // `std::aligned_alloc` requires a power-of-2 alignment.  Round up
  // `CacheLineSize` to the next power of two so that tests with small
  // non-power-of-2 node sizes (e.g. B=3, CacheLineSize=12) are valid.
  static constexpr std::size_t kAllocAlignment = []() constexpr noexcept {
    std::size_t p = 1;
    while (p < CacheLineSize) p <<= 1;
    return p;
  }();

  // Vector whose allocator guarantees every element starts on a boundary
  // compatible with `CacheLineSize` so every node occupies exactly one cache
  // line (or a single aligned block in tests with smaller node sizes).
  using AlignedVector = std::vector<T, AlignedAllocator<T, kAllocAlignment>>;

 public:
  BPlusTree() = default;

  // Constructs the tree from any forward range of values convertible to `T`.
  // The range must already be sorted; this is not verified at runtime.
  CPP_template(typename ForwardRange)(
      requires ql::ranges::forward_range<ForwardRange> CPP_and
          std::convertible_to<ql::ranges::range_value_t<ForwardRange>,
                              T>) explicit BPlusTree(ForwardRange&& range)
      : n_(static_cast<std::size_t>(ql::ranges::distance(range))) {
    if (n_ == 0) return;
    // Ceiling integer divide: `numLeaves = ceil(n_ / B)`.
    const std::size_t numLeaves = (n_ + B - 1) / B;
    // Round up the leaf count to a power of `(B+1)` so the tree is perfectly
    // full: every internal node has exactly `B+1` children.  Starting from 1
    // and multiplying by `(B+1)` at each level produces the BFS-complete
    // layout with all leaves at the same depth.
    numLeafNodes_ = 1;
    height_ = 0;
    while (numLeafNodes_ < numLeaves) {
      numLeafNodes_ *= (B + 1);
      ++height_;
    }
    // For a full `(B+1)`-ary tree of `height_` levels, the total number of
    // internal nodes is the geometric sum `1 + (B+1) + ... + (B+1)^(height_-1)
    // = (numLeafNodes_ - 1) / B`.
    numInternalNodes_ = (height_ == 0) ? 0 : (numLeafNodes_ - 1) / B;
    // Allocate one slot per key per node (internal + leaf), padded with
    // `numeric_limits<T>::max()` so unused leaf slots sort to the end.
    data_.assign((numInternalNodes_ + numLeafNodes_) * B,
                 std::numeric_limits<T>::max());
    ql::ranges::copy(
        range, data_.begin() + static_cast<ptrdiff_t>(numInternalNodes_ * B));
    if (numInternalNodes_ > 0) {
      buildInternalRecurse(0, 0, 0);
    }
  }

  std::size_t size() const noexcept { return n_; }
  int height() const noexcept { return height_; }
  std::size_t numInternalNodes() const noexcept { return numInternalNodes_; }
  std::size_t numLeafNodes() const noexcept { return numLeafNodes_; }
  std::size_t totalMemoryBytes() const noexcept {
    return data_.size() * sizeof(T);
  }

  // Returns `{first element >= query, count of elements strictly < query}`.
  // For a query above all stored values returns `{numeric_limits::max,
  // size()}`.
  template <bool USE_SIMD = false>
  std::pair<T, std::size_t> lowerBound(T query) const noexcept {
    return boundImpl<false, USE_SIMD>(query);
  }

  // Returns `{first element > query, count of elements <= query}`.
  // For a query >= all stored values returns `{numeric_limits::max, size()}`.
  template <bool USE_SIMD = false>
  std::pair<T, std::size_t> upperBound(T query) const noexcept {
    return boundImpl<true, USE_SIMD>(query);
  }

  // Processes `queries` in round-robin batches of `BATCH`, issuing a
  // non-temporal prefetch for the next node immediately after determining the
  // child to descend into.  By the time the round trip brings the same query
  // back to the next level, the prefetch has had ~`BATCH` iterations to
  // complete, hiding memory latency.  `IsUpperBound` selects `lowerBound`
  // (`false`) or `upperBound` (`true`) semantics.  `cb` is invoked exactly
  // once per query in input order with the resulting rank (clamped to
  // `size()`).
  template <bool IsUpperBound, std::size_t BATCH, bool USE_SIMD = false,
            typename Callback>
  requires std::invocable<Callback, std::size_t>
  void multiBound(ql::span<const T> queries, Callback cb) const noexcept {
    if (n_ == 0) {
      for (std::size_t i = 0; i < queries.size(); ++i) cb(std::size_t{0});
      return;
    }
    std::size_t i = 0;
    for (; i + BATCH <= queries.size(); i += BATCH) {
      multiBoundBatch<IsUpperBound, BATCH, USE_SIMD>(queries.data() + i, cb);
    }
    // Handle any remaining queries that do not fill a full batch individually.
    for (; i < queries.size(); ++i) {
      cb(boundImpl<IsUpperBound, USE_SIMD>(queries[i]).second);
    }
  }

  // Convenience wrapper: batch `lowerBound` over `queries` (forwards to
  // `multiBound<false>`).
  template <std::size_t BATCH, bool USE_SIMD = false, typename Callback>
  requires std::invocable<Callback, std::size_t>
  void multiLowerBound(ql::span<const T> queries, Callback cb) const noexcept {
    multiBound<false, BATCH, USE_SIMD>(queries, std::move(cb));
  }

  // Convenience wrapper: batch `upperBound` over `queries` (forwards to
  // `multiBound<true>`).
  template <std::size_t BATCH, bool USE_SIMD = false, typename Callback>
  requires std::invocable<Callback, std::size_t>
  void multiUpperBound(ql::span<const T> queries, Callback cb) const noexcept {
    multiBound<true, BATCH, USE_SIMD>(queries, std::move(cb));
  }

 private:
  // Shared implementation for `lowerBound` and `upperBound`.  Applying the
  // same `IsUpperBound` flag at every tree level is correct: an internal node
  // key holds the maximum value in its subtree, so `count(max_key < query)`
  // directs lower-bound traversal and `count(max_key <= query)` directs
  // upper-bound traversal to the leftmost subtree that could contain the
  // sought boundary.
  template <bool IsUpperBound, bool USE_SIMD>
  std::pair<T, std::size_t> boundImpl(T query) const noexcept {
    if (n_ == 0) return {std::numeric_limits<T>::max(), 0};

    using Finder =
        bplus_tree_detail::FindUpperOrLowerBoundInCacheLine<IsUpperBound, T, B,
                                                            USE_SIMD>;
    Finder findOffset{};
    // Count how many keys in the node at `nodeIdx` precede `query` (lower) or
    // are <= `query` (upper).
    auto findOffsetInNode = [&](std::size_t nodeIdx) noexcept {
      return findOffset(&data_[nodeIdx * B], query);
    };

    std::size_t nodeIdx = 0;
    for (int d = 0; d < height_; ++d) {
      const std::size_t j = findOffsetInNode(nodeIdx);
      // In a `(B+1)`-ary BFS layout, child `j` of node `nodeIdx` is at index
      // `(B+1)*nodeIdx + j + 1`.
      nodeIdx = (B + 1) * nodeIdx + j + 1;
    }
    const std::size_t off = findOffsetInNode(nodeIdx);
    // The 0-based leaf index among all padded leaves is
    // `nodeIdx - numInternalNodes_`.  Multiplying by `B` gives the rank of
    // the first key in this leaf; adding `off` gives the absolute rank of the
    // result key.
    const std::size_t leafIdx = nodeIdx - numInternalNodes_;
    const std::size_t rank = leafIdx * B + off;
    if (rank >= n_) return {std::numeric_limits<T>::max(), n_};
    return {data_[nodeIdx * B + off], rank};
  }

  // Processes exactly `BATCH` queries through the tree in lock-step,
  // prefetching the next node for each query after each level to hide memory
  // latency.
  template <bool IsUpperBound, std::size_t BATCH, bool USE_SIMD,
            typename Callback>
  void multiBoundBatch(const T* queries, Callback& cb) const noexcept {
    using Finder =
        bplus_tree_detail::FindUpperOrLowerBoundInCacheLine<IsUpperBound, T, B,
                                                            USE_SIMD>;
    Finder findOffset{};
    // Count how many keys in the node at `nodeIdx` precede `queries[qi]`
    // (lower) or are <= `queries[qi]` (upper).
    auto findOffsetInNode = [&](std::size_t nodeIdx, std::size_t qi) noexcept {
      return findOffset(&data_[nodeIdx * B], queries[qi]);
    };

    std::array<std::size_t, BATCH> nodeIdx{};
    if (height_ == 0) {
      for (std::size_t i = 0; i < BATCH; ++i) {
        const std::size_t off = findOffsetInNode(0, i);
        cb((off < n_) ? off : n_);
      }
      return;
    }
    for (int d = 0; d < height_; ++d) {
      for (std::size_t i = 0; i < BATCH; ++i) {
        const std::size_t j = findOffsetInNode(nodeIdx[i], i);
        nodeIdx[i] = (B + 1) * nodeIdx[i] + j + 1;
        absl::PrefetchToLocalCacheNta(&data_[nodeIdx[i] * B]);
      }
    }
    for (std::size_t i = 0; i < BATCH; ++i) {
      const std::size_t off = findOffsetInNode(nodeIdx[i], i);
      const std::size_t leafIdx = nodeIdx[i] - numInternalNodes_;
      const std::size_t rank = leafIdx * B + off;
      cb((rank < n_) ? rank : n_);
    }
  }

  // Recursively fills the internal node at BFS index `nodeIdx` (tree depth
  // `depth`, 0 = root) with separator keys derived from the leaf data.
  // `leafBase` is the 0-based index of the first leaf in this subtree within
  // the padded leaf array.  Each separator key at slot `j` is the maximum key
  // of the `(j+1)`-th leaf block within the subtree, which is also the
  // rightmost key of the last leaf in that block.
  void buildInternalRecurse(std::size_t nodeIdx, int depth,
                            std::size_t leafBase) {
    if (depth >= height_) return;
    // Number of leaves covered by each child subtree at this depth:
    // `(B+1)^(height_ - depth - 1)`.
    std::size_t L = 1;
    for (int i = 0; i < height_ - depth - 1; ++i) L *= (B + 1);
    T* keys = &data_[nodeIdx * B];
    for (std::size_t j = 0; j < B; ++j) {
      // Last leaf in child `j`'s subtree is at index `leafBase + (j+1)*L - 1`.
      // Its last (maximum) key sits at position
      // `(numInternalNodes_ + lastLeaf + 1) * B - 1` in `data_`.
      const std::size_t lastLeafInSubtree = leafBase + (j + 1) * L - 1;
      keys[j] = data_[(numInternalNodes_ + lastLeafInSubtree + 1) * B - 1];
    }
    // Recurse into each of the `B+1` children.
    for (std::size_t j = 0; j <= B; ++j) {
      buildInternalRecurse((B + 1) * nodeIdx + j + 1, depth + 1,
                           leafBase + j * L);
    }
  }

  AlignedVector data_;
  std::size_t n_ = 0;
  int height_ = 0;
  std::size_t numLeafNodes_ = 0;
  std::size_t numInternalNodes_ = 0;
};

}  // namespace ad_utility
