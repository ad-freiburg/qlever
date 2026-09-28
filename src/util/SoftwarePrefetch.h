// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_UTIL_SOFTWAREPREFETCH_H
#define QLEVER_SRC_UTIL_SOFTWAREPREFETCH_H

#include <algorithm>
#include <cstddef>
#include <string_view>

#if defined(__x86_64__) || defined(_M_X64) || defined(__i386__) || \
    defined(_M_IX86)
#include <xmmintrin.h>
#endif

#include "backports/span.h"
#include "util/CompactStringVector.h"
#include "util/Exception.h"

namespace ad_utility {

// _____________________________________________________________________________
// Issue a non-blocking CPU prefetch of the cache line at `address` into the L1
// data cache (`_MM_HINT_T0`, temporal locality 3). A null `address` is
// ignored.
inline void prefetchForRead(const void* address) noexcept {
  if (address == nullptr) {
    return;
  }
#if defined(__x86_64__) || defined(_M_X64) || defined(__i386__) || \
    defined(_M_IX86)
  _mm_prefetch(static_cast<const char*>(address), _MM_HINT_T0);
#elif defined(__GNUC__) || defined(__clang__)
  __builtin_prefetch(address, 0, 3);
#endif
}

// _____________________________________________________________________________
// Call `f(i, indices[i], words[indices[i]])` for every `i` in order, while
// software-prefetching the offset entry of `indices[i + distance]` and the
// first data line of `indices[i + distance / 2]`. This hides the DRAM latency
// of the random accesses into `words` when it is much larger than the CPU
// caches. Every index must be `< words.size()` (checked).
template <typename CharType, typename F>
void forEachWordPrefetched(const CompactVectorOfStrings<CharType>& words,
                           ql::span<const size_t> indices, size_t distance,
                           F&& f) {
  if (indices.empty() || !words.ready()) {
    return;
  }

  const size_t n = indices.size();
  const auto offsets = words.offsetsSpan();
  const auto data = words.dataSpan();

  // Warm-up: prefetch the offset entries of the first `distance` items.
  for (size_t k = 0; k < std::min(distance, n); ++k) {
    const size_t idx = indices[k];
    if (idx < offsets.size()) {
      prefetchForRead(&offsets[idx]);
    }
  }

  for (size_t i = 0; i < n; ++i) {
    // 1. Prefetch the offset entry of item `i + distance`. The guard is
    // subtraction-based because `i + distance` would wrap for a huge
    // `distance` (`i < n`, so `n - i` cannot underflow).
    if (distance < n - i) {
      const size_t pfIdx = indices[i + distance];
      if (pfIdx < offsets.size()) {
        prefetchForRead(&offsets[pfIdx]);
      }
    }

    // 2. Prefetch the first data line of item `i + distance / 2`, whose offset
    // entry was prefetched `distance / 2` iterations ago. `midIdx` is checked
    // first: for `midIdx == SIZE_MAX` the successor would wrap to zero.
    if ((distance / 2) < n - i) {
      const size_t midIdx = indices[i + (distance / 2)];
      if (midIdx < offsets.size() && midIdx + 1 < offsets.size()) {
        const auto strOffset = offsets[midIdx];
        if (strOffset < data.size()) {
          prefetchForRead(data.data() + strOffset);
        }
      }
    }

    // 3. Resolve item `i`. `curIdx` must not be the final (sentinel) offset.
    const size_t curIdx = indices[i];
    AD_CORRECTNESS_CHECK(curIdx < offsets.size() &&
                         curIdx + 1 < offsets.size());
    const auto curOffset = offsets[curIdx];
    const size_t strLen = offsets[curIdx + 1] - curOffset;
    f(i, curIdx,
      std::basic_string_view<CharType>{data.data() + curOffset, strLen});
  }
}

}  // namespace ad_utility

#endif  // QLEVER_SRC_UTIL_SOFTWAREPREFETCH_H
