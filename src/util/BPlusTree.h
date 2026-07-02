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
  size_t res = 0;
  auto end = keys + B;
  for (; keys != end; ++keys) {
    res += pred(*keys);
  }
  return res;
  // return static_cast<std::size_t>(ql::ranges::count_if(keys, keys + B,
  // pred));
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
  std::size_t operator()(const T* keys, T query) const noexcept {
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
// All values live in the leaves; internal nodes hold only separator keys.
// Internal nodes form a perfectly full `numChildrenPerNode`-ary tree stored in
// BFS order; the leaf array immediately follows and holds only `ceil(n/B)`
// actual leaf nodes (no padding).  Traversal clamps the BFS leaf index to the
// last actual leaf when the virtual descent would overshoot the allocation.
// All nodes are stored in a single flat `CacheLineSize`-byte-aligned array so
// every traversal step is a single cache-line read and child indices are
// computed by arithmetic: no pointers, no branches.
//
// Template parameters:
//   `T`             – key type; must be arithmetic; `numeric_limits<T>::max()`
//                     is used as a padding sentinel.
//   `CacheLineSize` – node size in bytes; defaults to 64 (one cache line on
//                     x86-64 and AArch64); must be a power of two and
//                     `sizeof(T)` must divide it evenly; the branching factor
//                     is derived as `CacheLineSize / sizeof(T)`.
template <typename T, std::size_t CacheLineSize = 64>
class BPlusTree {
  // `sizeof(T)` must evenly divide `CacheLineSize` so that exactly one node
  // fits per cache line with no wasted bytes.
  static_assert(CacheLineSize % sizeof(T) == 0,
                "sizeof(T) must divide CacheLineSize evenly.");
  // `std::aligned_alloc` requires a power-of-two alignment.
  static_assert((CacheLineSize & (CacheLineSize - 1)) == 0,
                "CacheLineSize must be a power of two.");
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

  // Fan-out: each internal node has `numChildrenPerNode` children, with `B`
  // separator keys routing among them.
  static constexpr std::size_t numChildrenPerNode = B + 1;

  // Vector whose allocator guarantees every element starts on a
  // `CacheLineSize`-byte boundary so every node occupies exactly one cache
  // line.
  using AlignedVector = std::vector<T, AlignedAllocator<T, CacheLineSize>>;

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
    // Ceiling integer divide: `numActualLeaves = ceil(n_ / B)`.
    const std::size_t numActualLeaves = (n_ + B - 1) / B;
    // Round up the leaf count to a power of `numChildrenPerNode` so the
    // internal-node tree is perfectly full.  Accumulate `numInternalNodes_` as
    // the geometric partial sum 1 + numChildrenPerNode + ... before each
    // multiply; the loop terminates when `numLeavesPadded` first reaches or
    // exceeds `numActualLeaves`.
    std::size_t numLeavesPadded = 1;
    height_ = 0;
    numInternalNodes_ = 0;
    while (numLeavesPadded < numActualLeaves) {
      numInternalNodes_ += numLeavesPadded;
      numLeavesPadded *= numChildrenPerNode;
      ++height_;
    }
    // Allocate only `numActualLeaves` leaf nodes (no padding), initialised
    // with `numeric_limits<T>::max()` so unused slots in the last partial leaf
    // sort to the end.
    data_.assign((numInternalNodes_ + numActualLeaves) * B,
                 std::numeric_limits<T>::max());
    ql::ranges::copy(
        range, data_.begin() + static_cast<ptrdiff_t>(numInternalNodes_ * B));
    if (numInternalNodes_ > 0) {
      buildInternalNodes(numLeavesPadded, numActualLeaves);
    }
  }

  std::size_t size() const noexcept { return n_; }
  int height() const noexcept { return height_; }
  std::size_t numInternalNodes() const noexcept { return numInternalNodes_; }
  // Returns the number of actual (non-padded) leaf nodes.
  std::size_t numLeafNodes() const noexcept {
    return (n_ == 0) ? 0 : (n_ + B - 1) / B;
  }
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
  CPP_template(bool IsUpperBound, std::size_t BATCH, bool USE_SIMD = false,
               typename Callback)(
      requires ql::concepts::invocable<
          Callback, std::size_t>) void multiBound(ql::span<const T> queries,
                                                  Callback cb) const noexcept {
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
  CPP_template(std::size_t BATCH, bool USE_SIMD = false, typename Callback)(
      requires ql::concepts::invocable<
          Callback,
          std::size_t>) void multiLowerBound(ql::span<const T> queries,
                                             Callback cb) const noexcept {
    multiBound<false, BATCH, USE_SIMD>(queries, std::move(cb));
  }

  // Convenience wrapper: batch `upperBound` over `queries` (forwards to
  // `multiBound<true>`).
  CPP_template(std::size_t BATCH, bool USE_SIMD = false, typename Callback)(
      requires ql::concepts::invocable<
          Callback,
          std::size_t>) void multiUpperBound(ql::span<const T> queries,
                                             Callback cb) const noexcept {
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
    // Count how many keys in the node at `nodeIdx` precede `query` (lower) or
    // are <= `query` (upper).
    auto findOffsetInNode = [&](std::size_t nodeIdx) noexcept {
      return Finder{}(&data_[nodeIdx * B], query);
    };

    std::size_t nodeIdx = 0;
    for (int d = 0; d < height_; ++d) {
      const std::size_t j = findOffsetInNode(nodeIdx);
      // In a `numChildrenPerNode`-ary BFS layout, child `j` of node `nodeIdx`
      // is at index `numChildrenPerNode * nodeIdx + j + 1`.
      nodeIdx = numChildrenPerNode * nodeIdx + j + 1;
    }
    // Clamp to the last actual leaf to guard against upper_bound with the
    // sentinel query `numeric_limits<T>::max()` navigating beyond the
    // (unpadded) leaf allocation.
    const std::size_t numActualLeaves = (n_ + B - 1) / B;
    nodeIdx = std::min(nodeIdx, numInternalNodes_ + numActualLeaves - 1);
    const std::size_t off = findOffsetInNode(nodeIdx);
    // The 0-based leaf index is `nodeIdx - numInternalNodes_`.  Multiplying by
    // `B` gives the rank of the first key in this leaf; adding `off` gives the
    // absolute rank of the result key.
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
    // Count how many keys in the node at `nodeIdx` precede `queries[qi]`
    // (lower) or are <= `queries[qi]` (upper).
    auto findOffsetInNode = [&](std::size_t nodeIdx, std::size_t qi) noexcept {
      return Finder{}(&data_[nodeIdx * B], queries[qi]);
    };

    std::array<std::size_t, BATCH> nodeIdx{};
    if (height_ == 0) {
      for (std::size_t i = 0; i < BATCH; ++i) {
        const std::size_t off = findOffsetInNode(0, i);
        cb((off < n_) ? off : n_);
      }
      return;
    }
    const std::size_t numActualLeaves = (n_ + B - 1) / B;
    const std::size_t lastLeafBfs = numInternalNodes_ + numActualLeaves - 1;
    for (int d = 0; d < height_; ++d) {
      for (std::size_t i = 0; i < BATCH; ++i) {
        const std::size_t j = findOffsetInNode(nodeIdx[i], i);
        nodeIdx[i] = numChildrenPerNode * nodeIdx[i] + j + 1;
        // Clamp the prefetch address so it never escapes the allocated array.
        const std::size_t safeIdx = std::min(nodeIdx[i], lastLeafBfs);
        absl::PrefetchToLocalCacheNta(&data_[safeIdx * B]);
      }
    }
    for (std::size_t i = 0; i < BATCH; ++i) {
      // Clamp to the last actual leaf (guards against sentinel queries).
      nodeIdx[i] = std::min(nodeIdx[i], lastLeafBfs);
      const std::size_t off = findOffsetInNode(nodeIdx[i], i);
      const std::size_t leafIdx = nodeIdx[i] - numInternalNodes_;
      const std::size_t rank = leafIdx * B + off;
      cb((rank < n_) ? rank : n_);
    }
  }

  // Fills all internal nodes iteratively in BFS order.  `numLeavesPadded` is
  // the smallest power of `numChildrenPerNode` that is >= `numActualLeaves`;
  // it determines the virtual tree shape used to compute child assignments.
  // When the last leaf in a child subtree is beyond `numActualLeaves`, the
  // separator key is set to `numeric_limits<T>::max()` (the sentinel), which
  // correctly routes any query < max() to the leftmost (and only populated)
  // child of that node.
  void buildInternalNodes(std::size_t numLeavesPadded,
                          std::size_t numActualLeaves) {
    std::size_t levelStart = 0;
    std::size_t levelCount = 1;
    std::size_t leavesInSubtree = numLeavesPadded;
    for (int d = 0; d < height_; ++d) {
      const std::size_t leavesPerChildSubtree =
          leavesInSubtree / numChildrenPerNode;
      for (std::size_t i = 0; i < levelCount; ++i) {
        const std::size_t nodeIdx = levelStart + i;
        T* keys = &data_[nodeIdx * B];
        // First padded leaf covered by node `i` at this level.
        const std::size_t leafBase = i * leavesInSubtree;
        for (std::size_t j = 0; j < B; ++j) {
          // The separator at slot `j` equals the last key of child `j`'s
          // subtree: the rightmost key of padded leaf
          // `leafBase + (j+1)*leavesPerChildSubtree - 1`.
          const std::size_t lastLeaf =
              leafBase + (j + 1) * leavesPerChildSubtree - 1;
          keys[j] = (lastLeaf < numActualLeaves)
                        ? data_[(numInternalNodes_ + lastLeaf + 1) * B - 1]
                        : std::numeric_limits<T>::max();
        }
      }
      levelStart += levelCount;
      levelCount *= numChildrenPerNode;
      leavesInSubtree = leavesPerChildSubtree;
    }
  }

  AlignedVector data_;
  std::size_t n_ = 0;
  int height_ = 0;
  std::size_t numInternalNodes_ = 0;
};

}  // namespace ad_utility
