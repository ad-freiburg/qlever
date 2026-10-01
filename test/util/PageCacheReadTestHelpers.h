// Copyright 2026, The QLever Authors, in particular:
//
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_TEST_UTIL_PAGECACHEREADTESTHELPERS_H
#define QLEVER_TEST_UTIL_PAGECACHEREADTESTHELPERS_H

#include <cerrno>
#include <cstdint>
#include <utility>

#include "util/IoUringManager.h"

namespace pageCacheReadTestHelpers {

// Replace the `preadv2(RWF_NOWAIT)` call of `ad_utility::readPageCacheHits`
// with `function` for the lifetime of this object. On destruction the
// previous function is restored and an injected `EOPNOTSUPP` is undone, so
// later tests see the fast path as supported again.
class ScopedPageCacheRead {
 public:
  explicit ScopedPageCacheRead(ad_utility::detail::PageCacheRead function)
      : previous_{
            std::exchange(ad_utility::detail::pageCacheRead(), function)} {}
  ~ScopedPageCacheRead() {
    ad_utility::detail::pageCacheRead() = previous_;
    ad_utility::detail::resetPageCacheFastPathSupport();
  }
  ScopedPageCacheRead(const ScopedPageCacheRead&) = delete;
  ScopedPageCacheRead& operator=(const ScopedPageCacheRead&) = delete;

 private:
  ad_utility::detail::PageCacheRead previous_;
};

// A page-cache read that finds nothing cached (`EAGAIN`).
inline int64_t nothingCached(int, const ::iovec*, int, int64_t) {
  errno = EAGAIN;
  return -1;
}

// A page-cache read on a file system that rejects `RWF_NOWAIT`.
inline int64_t notSupported(int, const ::iovec*, int, int64_t) {
  errno = EOPNOTSUPP;
  return -1;
}

}  // namespace pageCacheReadTestHelpers

#endif  // QLEVER_TEST_UTIL_PAGECACHEREADTESTHELPERS_H
