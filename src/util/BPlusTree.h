// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR

// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#pragma once

#include <absl/base/prefetch.h>
#include <hwy/highway.h>

#include <algorithm>
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

namespace ad_utility {

// Custom allocator that returns memory at 64-byte boundaries so that every
// node starts at a cache-line boundary.  This guarantees that a single
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

template <typename T>
using AlignedVector = std::vector<T, AlignedAllocator<T, 64>>;

namespace detail {

// Counts the number of `keys[i]` that satisfy the predicate determined by
// `IsUpperBound`: when `false`, counts `keys[i] < query` (lower-bound
// semantics); when `true`, counts `keys[i] <= query` (upper-bound semantics).
template <bool IsUpperBound, typename T, std::size_t B>
inline std::size_t findCountScalar(const T* keys, T query) noexcept {
  if constexpr (IsUpperBound) {
    return static_cast<std::size_t>(ql::ranges::count_if(
        keys, keys + B, [query](T k) noexcept { return k <= query; }));
  } else {
    return static_cast<std::size_t>(ql::ranges::count_if(
        keys, keys + B, [query](T k) noexcept { return k < query; }));
  }
}

// SIMD variant of `findCountScalar` using Google Highway.
// `CappedTag<T, B>` picks `min(native lanes, B)` lanes (rounded down to a
// power of two) so the cache-line-sized node tiles cleanly over any vector
// width (SSE 128-b, NEON 128-b, AVX2 256-b, AVX-512 512-b).  `IsUpperBound`
// selects `hn::Lt` (strictly less) or `hn::Le` (less-or-equal).
namespace hn = hwy::HWY_NAMESPACE;

template <bool IsUpperBound, typename T, std::size_t B>
inline std::size_t findCountHwy(const T* keys, T query) noexcept {
  using D = hn::CappedTag<T, B>;
  D d;
  const auto q = hn::Set(d, query);
  std::size_t count = 0;
  for (std::size_t i = 0; i < B; i += hn::Lanes(d)) {
    const auto k = hn::LoadU(d, keys + i);
    if constexpr (IsUpperBound) {
      count += hn::CountTrue(d, hn::Le(k, q));
    } else {
      count += hn::CountTrue(d, hn::Lt(k, q));
    }
  }
  return count;
}

// Dispatch struct that selects the SIMD or scalar path at compile time.
// The SIMD path requires `B` to be a power of two >= 4 so that the
// cache-line node tiles cleanly over any reasonable vector width.  Small
// branching factors (2, 3) and non-arithmetic types always use the scalar path.
template <bool IsUpperBound, typename T, std::size_t B, bool USE_SIMD>
struct FindCount {
  static constexpr bool isPowerOfTwo = (B & (B - 1)) == 0;
  static inline std::size_t apply(const T* keys, T query) noexcept {
    if constexpr (USE_SIMD && B >= 4 && isPowerOfTwo &&
                  std::is_arithmetic_v<T>) {
      return findCountHwy<IsUpperBound, T, B>(keys, query);
    } else {
      return findCountScalar<IsUpperBound, T, B>(keys, query);
    }
  }
};

}  // namespace detail

// Read-only, cache-line-optimized flat B+ tree for fast `lowerBound` and
// `upperBound` lookups on a static sorted sequence.  All data must be provided
// at construction time via a single bulk load; the tree cannot be modified
// after construction.
//
// All values live in the leaves; internal nodes hold only separator keys.  The
// tree is full and balanced: a sorted input is padded with
// `std::numeric_limits<T>::max()` so every level has the maximum fan-out
// (`B+1`).  All nodes (internal nodes first, leaves at the tail) are stored in
// a single flat 64-byte-aligned array in BFS order, so every traversal step is
// a single cache-line read and child indices are computed by index arithmetic:
// no pointers, no branches.
//
// Template parameters:
//   `T` – key type; must be arithmetic and have a `std::numeric_limits::max`
//         sentinel used for padding.
//   `B` – keys per node; defaults to one 64-byte cache line.
template <typename T, std::size_t B = 64 / sizeof(T)>
class BPlusTree {
  static_assert(B >= 2, "B must be at least 2.");
  static_assert(std::is_arithmetic_v<T>, "T must be arithmetic.");

 public:
  static constexpr std::size_t branching = B;

  BPlusTree() = default;

  explicit BPlusTree(const std::vector<T>& sorted) : n_(sorted.size()) {
    if (n_ == 0) return;
    const std::size_t numLeaves = (n_ + B - 1) / B;
    height_ = 0;
    numLeavesPadded_ = 1;
    while (numLeavesPadded_ < numLeaves) {
      numLeavesPadded_ *= (B + 1);
      ++height_;
    }
    totalInternal_ = (height_ == 0) ? 0 : (numLeavesPadded_ - 1) / B;
    data_.assign((totalInternal_ + numLeavesPadded_) * B,
                 std::numeric_limits<T>::max());
    std::copy(sorted.begin(), sorted.end(),
              data_.begin() + static_cast<ptrdiff_t>(totalInternal_ * B));
    if (totalInternal_ > 0) {
      buildInternalRecurse(0, 0, 0);
    }
  }

  // Constructs the tree from any forward range of values convertible to `T`.
  // The range must already be sorted; no sorted check is performed here.
  // Excluded for `std::vector<T>` to avoid ambiguity with the vector overload.
  template <std::ranges::forward_range R>
  requires(std::convertible_to<std::ranges::range_value_t<R>, T> &&
           !std::same_as<std::remove_cvref_t<R>, std::vector<T>>)
  explicit BPlusTree(R&& range)
      : BPlusTree(std::vector<T>(std::ranges::begin(range),
                                 std::ranges::end(range))) {}

  std::size_t size() const noexcept { return n_; }
  int height() const noexcept { return height_; }
  std::size_t internalNodeCount() const noexcept { return totalInternal_; }
  std::size_t leafNodeCount() const noexcept { return numLeavesPadded_; }
  std::size_t totalMemoryBytes() const noexcept {
    return data_.size() * sizeof(T);
  }

  // Returns `{first element >= query, count of elements strictly < query}`.
  // For queries above all stored values returns `{numeric_limits::max,
  // size()}`.
  template <bool USE_SIMD = false>
  std::pair<T, std::size_t> lowerBound(T query) const noexcept {
    return boundImpl<false, USE_SIMD>(query);
  }

  // Returns `{first element > query, count of elements <= query}`.
  // For queries >= all stored values returns `{numeric_limits::max, size()}`.
  template <bool USE_SIMD = false>
  std::pair<T, std::size_t> upperBound(T query) const noexcept {
    return boundImpl<true, USE_SIMD>(query);
  }

  // Processes `n` queries in round-robin batches of `BATCH`, issuing a
  // non-temporal prefetch for the next node immediately after determining the
  // child to descend into.  By the time the round trip brings the same query
  // back to the next level, the prefetch has had ~`BATCH` iterations to
  // complete, hiding memory latency.  `IsUpperBound` selects `lowerBound`
  // (`false`) or `upperBound` (`true`) semantics.  `cb` is invoked exactly
  // once per query in input order with the resulting rank (clamped to
  // `size()`).
  template <bool IsUpperBound, std::size_t BATCH, bool USE_SIMD = false,
            typename Callback>
  void multiBound(const T* queries, std::size_t n, Callback cb) const noexcept {
    if (n_ == 0) {
      for (std::size_t i = 0; i < n; ++i) cb(std::size_t{0});
      return;
    }
    std::size_t i = 0;
    for (; i + BATCH <= n; i += BATCH) {
      multiBoundBatch<IsUpperBound, BATCH, USE_SIMD>(queries + i, cb);
    }
    // Handle any remaining queries individually.
    for (; i < n; ++i) {
      cb(boundImpl<IsUpperBound, USE_SIMD>(queries[i]).second);
    }
  }

  // Convenience wrapper for batch `lowerBound` (forwards to `multiBound`).
  template <std::size_t BATCH, bool USE_SIMD = false, typename Callback>
  void multiLowerBound(const T* queries, std::size_t n,
                       Callback cb) const noexcept {
    multiBound<false, BATCH, USE_SIMD>(queries, n, std::move(cb));
  }

  // Convenience wrapper for batch `upperBound` (forwards to `multiBound`).
  template <std::size_t BATCH, bool USE_SIMD = false, typename Callback>
  void multiUpperBound(const T* queries, std::size_t n,
                       Callback cb) const noexcept {
    multiBound<true, BATCH, USE_SIMD>(queries, n, std::move(cb));
  }

 private:
  // Shared implementation for `lowerBound` and `upperBound`.  Using the same
  // `IsUpperBound` flag at every level is correct: an internal node key holds
  // the maximum of its subtree, so `count(max_key < query)` directs lower-bound
  // traversal and `count(max_key <= query)` directs upper-bound traversal to
  // the leftmost subtree that could still hold the sought boundary.
  template <bool IsUpperBound, bool USE_SIMD>
  std::pair<T, std::size_t> boundImpl(T query) const noexcept {
    if (n_ == 0) return {std::numeric_limits<T>::max(), 0};
    std::size_t nodeIdx = 0;
    for (int d = 0; d < height_; ++d) {
      const T* keys = &data_[nodeIdx * B];
      const std::size_t j =
          detail::FindCount<IsUpperBound, T, B, USE_SIMD>::apply(keys, query);
      nodeIdx = (B + 1) * nodeIdx + j + 1;
    }
    const T* leaf = &data_[nodeIdx * B];
    const std::size_t off =
        detail::FindCount<IsUpperBound, T, B, USE_SIMD>::apply(leaf, query);
    const std::size_t leafIdx = nodeIdx - totalInternal_;
    const std::size_t rank = leafIdx * B + off;
    if (rank >= n_) return {std::numeric_limits<T>::max(), n_};
    return {leaf[off], rank};
  }

  // Processes exactly `BATCH` queries through the tree in lock-step,
  // prefetching the next node for each query after each level to hide memory
  // latency.
  template <bool IsUpperBound, std::size_t BATCH, bool USE_SIMD,
            typename Callback>
  void multiBoundBatch(const T* queries, Callback& cb) const noexcept {
    std::array<std::size_t, BATCH> nodeIdx{};
    if (height_ == 0) {
      for (std::size_t i = 0; i < BATCH; ++i) {
        const std::size_t off =
            detail::FindCount<IsUpperBound, T, B, USE_SIMD>::apply(data_.data(),
                                                                   queries[i]);
        cb((off < n_) ? off : n_);
      }
      return;
    }
    for (int d = 0; d < height_; ++d) {
      for (std::size_t i = 0; i < BATCH; ++i) {
        const T* keys = &data_[nodeIdx[i] * B];
        const std::size_t j =
            detail::FindCount<IsUpperBound, T, B, USE_SIMD>::apply(keys,
                                                                   queries[i]);
        nodeIdx[i] = (B + 1) * nodeIdx[i] + j + 1;
        absl::PrefetchToLocalCacheNta(&data_[nodeIdx[i] * B]);
      }
    }
    for (std::size_t i = 0; i < BATCH; ++i) {
      const T* leaf = &data_[nodeIdx[i] * B];
      const std::size_t off =
          detail::FindCount<IsUpperBound, T, B, USE_SIMD>::apply(leaf,
                                                                 queries[i]);
      const std::size_t leafIdx = nodeIdx[i] - totalInternal_;
      const std::size_t rank = leafIdx * B + off;
      cb((rank < n_) ? rank : n_);
    }
  }

  void buildInternalRecurse(std::size_t nodeIdx, int depth,
                            std::size_t leafBase) {
    if (depth >= height_) return;
    std::size_t L = 1;
    for (int i = 0; i < height_ - depth - 1; ++i) L *= (B + 1);
    T* keys = &data_[nodeIdx * B];
    for (std::size_t j = 0; j < B; ++j) {
      const std::size_t lastLeafInSubtree = leafBase + (j + 1) * L - 1;
      keys[j] = data_[(totalInternal_ + lastLeafInSubtree + 1) * B - 1];
    }
    for (std::size_t j = 0; j <= B; ++j) {
      buildInternalRecurse((B + 1) * nodeIdx + j + 1, depth + 1,
                           leafBase + j * L);
    }
  }

  AlignedVector<T> data_;
  std::size_t n_ = 0;
  int height_ = 0;
  std::size_t numLeavesPadded_ = 0;
  std::size_t totalInternal_ = 0;
};

}  // namespace ad_utility
