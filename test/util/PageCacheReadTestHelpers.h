// Copyright 2026, The QLever Authors, in particular:
//
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_TEST_UTIL_PAGECACHEREADTESTHELPERS_H
#define QLEVER_TEST_UTIL_PAGECACHEREADTESTHELPERS_H

#include <sys/uio.h>

#include <cerrno>
#include <cstdint>
#include <utility>

#include "util/IoUringManager.h"
#include "util/PageCacheReadHook.h"

namespace pageCacheReadTestHelpers {

// Replace the `preadv2(RWF_NOWAIT)` call of `ad_utility::readPageCacheHits`
// with `function` for the lifetime of this object, and restore the state
// that the test found on destruction. Two pieces of global state are
// involved, and both are restored:
//
// 1. The read function itself: the constructor swaps `function` into
//    `detail::pageCacheRead()` and remembers the previous one in
//    `previous_`; the destructor writes it back.
// 2. The process-wide fast-path latch (`pageCacheFastPathIsSupported()`):
//    an injected `EOPNOTSUPP` read switches the latch off for the rest of
//    the process (that is what `switchOffFastPathIfUnsupported` does with
//    a real failing call, too). `wasSupported_` remembers whether the
//    latch was on when this object was constructed. If it was, the
//    destructor calls `resetPageCacheFastPathSupport()` to switch it on
//    again, undoing an `EOPNOTSUPP` that the test itself injected. If it
//    was already off (a platform without `RWF_NOWAIT`), nothing is
//    reset: the test must not pretend the fast path works there.
class ScopedPageCacheRead {
 public:
  explicit ScopedPageCacheRead(ad_utility::detail::PageCacheRead function)
      : previous_{
            std::exchange(ad_utility::detail::pageCacheRead(), function)} {}
  ~ScopedPageCacheRead() {
    // Restore 1: the read function that was in place before.
    ad_utility::detail::pageCacheRead() = previous_;
    // Restore 2: undo an injected `EOPNOTSUPP`, but only if the fast
    // path was supported before this test (see the class comment).
    if (wasSupported_) {
      ad_utility::detail::resetPageCacheFastPathSupport();
    }
  }
  ScopedPageCacheRead(const ScopedPageCacheRead&) = delete;
  ScopedPageCacheRead& operator=(const ScopedPageCacheRead&) = delete;
  // Deleted: moving would restore the previous read twice.
  ScopedPageCacheRead(ScopedPageCacheRead&&) = delete;
  ScopedPageCacheRead& operator=(ScopedPageCacheRead&&) = delete;

 private:
  const ad_utility::detail::PageCacheRead previous_;
  bool wasSupported_ = ad_utility::pageCacheFastPathIsSupported();
};

//______________________________________________________________________________
// Report that nothing is cached (`EAGAIN`).
inline int64_t nothingCached(int, const ::iovec*, int, int64_t) noexcept {
  errno = EAGAIN;
  return -1;
}

//______________________________________________________________________________
// Report that `RWF_NOWAIT` is not supported (`EOPNOTSUPP`).
inline int64_t notSupported(int, const ::iovec*, int, int64_t) noexcept {
  errno = EOPNOTSUPP;
  return -1;
}

}  // namespace pageCacheReadTestHelpers

#endif  // QLEVER_TEST_UTIL_PAGECACHEREADTESTHELPERS_H
