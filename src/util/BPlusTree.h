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
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <limits>
#include <new>
#include <type_traits>
#include <utility>
#include <vector>

namespace ad_utility {

// Custom allocator that returns memory at 64-byte boundaries so that every
// node starts at a cache-line boundary.  This guarantees that a single
// cache-line load fetches the full node without straddling two lines, and that
// Highway's `LoadU` receives vector-aligned pointers on all supported ISAs.
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

// Non-temporal prefetch via Abseil for cross-platform cache warming.
inline void prefetch_nta(const void* p) noexcept {
  absl::PrefetchToLocalCacheNta(p);
}

// Counts the number of `keys[i]` that satisfy the predicate determined by
// `IsUpperBound`: when `false`, counts `keys[i] < query` (lower-bound
// semantics); when `true`, counts `keys[i] <= query` (upper-bound semantics).
// The loop is branchless and unrolls to a tight straight-line sequence.
template <bool IsUpperBound, typename T, std::size_t B>
inline std::size_t find_count_scalar(const T* keys, T query) noexcept {
  std::size_t count = 0;
  for (std::size_t i = 0; i < B; ++i) {
    if constexpr (IsUpperBound) {
      count += (keys[i] <= query);
    } else {
      count += (keys[i] < query);
    }
  }
  return count;
}

// SIMD variant of `find_count_scalar` using Google Highway.
// `CappedTag<T, B>` picks `min(native lanes, B)` lanes (rounded down to a
// power of two) so the cache-line-sized node tiles cleanly over any vector
// width (SSE 128-b, NEON 128-b, AVX2 256-b, AVX-512 512-b).  `IsUpperBound`
// selects `hn::Lt` (strictly less) or `hn::Le` (less-or-equal).
namespace hn = hwy::HWY_NAMESPACE;

template <bool IsUpperBound, typename T, std::size_t B>
inline std::size_t find_count_hwy(const T* keys, T query) noexcept {
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
// The SIMD path requires `B` to be a power of two ≥ 4 so that the
// cache-line node tiles cleanly over any reasonable vector width.  Small
// branching factors (2, 3) and non-arithmetic types always use the scalar path.
template <bool IsUpperBound, typename T, std::size_t B, bool USE_SIMD>
struct FindCount {
  static inline std::size_t apply(const T* keys, T query) noexcept {
    if constexpr (USE_SIMD && B >= 4 && (B & (B - 1)) == 0 &&
                  std::is_arithmetic<T>::value) {
      return find_count_hwy<IsUpperBound, T, B>(keys, query);
    } else {
      return find_count_scalar<IsUpperBound, T, B>(keys, query);
    }
  }
};

}  // namespace detail

// Cache-line-optimized flat B+ tree for fast `lower_bound` and `upper_bound`
// lookups on a static sorted sequence.
//
// All values live in the leaves; internal nodes hold only separator keys.  The
// tree is full and balanced: a sorted input is padded with
// `std::numeric_limits<T>::max()` so every level has the maximum fan-out
// (`B+1`).  Storage is two flat 64-byte-aligned arrays (internal nodes in BFS
// order, leaves contiguously), so every traversal step is a single cache-line
// read and child indices are computed by index arithmetic — no pointers, no
// branches.
//
// Template parameters:
//   `T` – key type; must be arithmetic and have a `std::numeric_limits::max`
//         sentinel used for padding.
//   `B` – keys per node; defaults to one 64-byte cache line.
template <typename T, std::size_t B = 64 / sizeof(T)>
class BPlusTree {
  static_assert(B >= 2, "B must be at least 2.");
  static_assert(std::is_arithmetic<T>::value, "T must be arithmetic.");

 public:
  static constexpr std::size_t branching = B;

  BPlusTree() = default;

  explicit BPlusTree(const std::vector<T>& sorted) : n_(sorted.size()) {
    if (n_ == 0) return;

    const std::size_t num_leaves = (n_ + B - 1) / B;

    height_ = 0;
    num_leaves_padded_ = 1;
    while (num_leaves_padded_ < num_leaves) {
      num_leaves_padded_ *= (B + 1);
      ++height_;
    }

    total_internal_ = (height_ == 0) ? 0 : (num_leaves_padded_ - 1) / B;

    leaves_.assign(num_leaves_padded_ * B, std::numeric_limits<T>::max());
    std::copy(sorted.begin(), sorted.end(), leaves_.begin());

    if (total_internal_ > 0) {
      internal_.assign(total_internal_ * B, std::numeric_limits<T>::max());
      build_internal_recurse(0, 0, 0);
    }
  }

  std::size_t size() const noexcept { return n_; }
  int height() const noexcept { return height_; }
  std::size_t internal_node_count() const noexcept { return total_internal_; }
  std::size_t leaf_node_count() const noexcept { return num_leaves_padded_; }
  std::size_t total_memory_bytes() const noexcept {
    return (internal_.size() + leaves_.size()) * sizeof(T);
  }

  // Returns `{first element >= query, count of elements strictly < query}`.
  // For queries above all stored values returns `{numeric_limits::max,
  // size()}`.
  template <bool USE_SIMD = false>
  std::pair<T, std::size_t> lower_bound(T query) const noexcept {
    return bound_impl<false, USE_SIMD>(query);
  }

  // Returns `{first element > query, count of elements <= query}`.
  // For queries >= all stored values returns `{numeric_limits::max, size()}`.
  template <bool USE_SIMD = false>
  std::pair<T, std::size_t> upper_bound(T query) const noexcept {
    return bound_impl<true, USE_SIMD>(query);
  }

  // Processes `n` queries in round-robin batches of `BATCH`, issuing a
  // non-temporal prefetch for the next node immediately after determining the
  // child to descend into.  By the time the round trip brings the same query
  // back to the next level, the prefetch has had ~`BATCH` iterations to
  // complete, hiding memory latency.  `IsUpperBound` selects `lower_bound`
  // (`false`) or `upper_bound` (`true`) semantics.  `cb` is invoked exactly
  // once per query in input order with the resulting rank (clamped to
  // `size()`).
  template <bool IsUpperBound, std::size_t BATCH, bool USE_SIMD = false,
            typename Callback>
  void multi_bound(const T* queries, std::size_t n,
                   Callback cb) const noexcept {
    if (n_ == 0) {
      for (std::size_t i = 0; i < n; ++i) cb(std::size_t{0});
      return;
    }
    std::size_t i = 0;
    for (; i + BATCH <= n; i += BATCH) {
      multi_bound_batch<IsUpperBound, BATCH, USE_SIMD>(queries + i, cb);
    }
    // Handle any remaining queries individually.
    for (; i < n; ++i) {
      cb(bound_impl<IsUpperBound, USE_SIMD>(queries[i]).second);
    }
  }

  // Convenience wrapper for batch `lower_bound` (forwards to `multi_bound`).
  template <std::size_t BATCH, bool USE_SIMD = false, typename Callback>
  void multi_lower_bound(const T* queries, std::size_t n,
                         Callback cb) const noexcept {
    multi_bound<false, BATCH, USE_SIMD>(queries, n, std::move(cb));
  }

  // Convenience wrapper for batch `upper_bound` (forwards to `multi_bound`).
  template <std::size_t BATCH, bool USE_SIMD = false, typename Callback>
  void multi_upper_bound(const T* queries, std::size_t n,
                         Callback cb) const noexcept {
    multi_bound<true, BATCH, USE_SIMD>(queries, n, std::move(cb));
  }

 private:
  // Shared implementation for `lower_bound` and `upper_bound`.  Using the same
  // `IsUpperBound` flag at every level is correct: an internal node key holds
  // the maximum of its subtree, so `count(max_key < query)` directs lower-bound
  // traversal and `count(max_key <= query)` directs upper-bound traversal to
  // the leftmost subtree that could still hold the sought boundary.
  template <bool IsUpperBound, bool USE_SIMD>
  std::pair<T, std::size_t> bound_impl(T query) const noexcept {
    if (n_ == 0) return {std::numeric_limits<T>::max(), 0};

    std::size_t leaf_idx = 0;
    std::size_t node_idx = 0;
    for (int d = 0; d < height_; ++d) {
      const T* keys = &internal_[node_idx * B];
      const std::size_t j =
          detail::FindCount<IsUpperBound, T, B, USE_SIMD>::apply(keys, query);
      leaf_idx = leaf_idx * (B + 1) + j;
      node_idx = (B + 1) * node_idx + j + 1;
    }
    const T* leaf = &leaves_[leaf_idx * B];
    const std::size_t off =
        detail::FindCount<IsUpperBound, T, B, USE_SIMD>::apply(leaf, query);
    const std::size_t rank = leaf_idx * B + off;
    if (rank >= n_) return {std::numeric_limits<T>::max(), n_};
    return {leaves_[rank], rank};
  }

  // Processes exactly `BATCH` queries through the tree in lock-step.
  template <bool IsUpperBound, std::size_t BATCH, bool USE_SIMD,
            typename Callback>
  void multi_bound_batch(const T* queries, Callback& cb) const noexcept {
    std::array<std::size_t, BATCH> node_idx{};
    std::array<std::size_t, BATCH> leaf_idx{};

    if (height_ == 0) {
      for (std::size_t i = 0; i < BATCH; ++i) {
        const std::size_t off =
            detail::FindCount<IsUpperBound, T, B, USE_SIMD>::apply(
                leaves_.data(), queries[i]);
        cb((off < n_) ? off : n_);
      }
      return;
    }

    for (int d = 0; d < height_; ++d) {
      const bool next_is_leaf = (d + 1 >= height_);
      for (std::size_t i = 0; i < BATCH; ++i) {
        const T* keys = &internal_[node_idx[i] * B];
        const std::size_t j =
            detail::FindCount<IsUpperBound, T, B, USE_SIMD>::apply(keys,
                                                                   queries[i]);
        leaf_idx[i] = leaf_idx[i] * (B + 1) + j;
        node_idx[i] = (B + 1) * node_idx[i] + j + 1;
        if (next_is_leaf) {
          detail::prefetch_nta(&leaves_[leaf_idx[i] * B]);
        } else {
          detail::prefetch_nta(&internal_[node_idx[i] * B]);
        }
      }
    }

    for (std::size_t i = 0; i < BATCH; ++i) {
      const T* leaf = &leaves_[leaf_idx[i] * B];
      const std::size_t off =
          detail::FindCount<IsUpperBound, T, B, USE_SIMD>::apply(leaf,
                                                                 queries[i]);
      const std::size_t rank = leaf_idx[i] * B + off;
      cb((rank < n_) ? rank : n_);
    }
  }

  void build_internal_recurse(std::size_t node_idx, int depth,
                              std::size_t leaf_base) {
    if (depth >= height_) return;

    std::size_t L = 1;
    for (int i = 0; i < height_ - depth - 1; ++i) L *= (B + 1);

    T* keys = &internal_[node_idx * B];
    for (std::size_t j = 0; j < B; ++j) {
      const std::size_t last_leaf_in_subtree = leaf_base + (j + 1) * L - 1;
      keys[j] = leaves_[(last_leaf_in_subtree + 1) * B - 1];
    }
    for (std::size_t j = 0; j <= B; ++j) {
      build_internal_recurse((B + 1) * node_idx + j + 1, depth + 1,
                             leaf_base + j * L);
    }
  }

  AlignedVector<T> internal_;
  AlignedVector<T> leaves_;
  std::size_t n_ = 0;
  int height_ = 0;
  std::size_t num_leaves_padded_ = 0;
  std::size_t total_internal_ = 0;
};

}  // namespace ad_utility
