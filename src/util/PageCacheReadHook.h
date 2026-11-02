// Copyright 2026, The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_UTIL_PAGECACHEREADHOOK_H
#define QLEVER_SRC_UTIL_PAGECACHEREADHOOK_H

#include <sys/uio.h>

#include <cstdint>

namespace ad_utility {

// In `detail` because production code must call `readPageCacheHits` and
// must not call or replace this hook; it exists only for the tests.
namespace detail {
// The one page-cache read call per run that
// `readPageCacheHits` makes, with the same contract (the number of bytes read,
// or -1 with `errno` set). A replaceable function pointer so that unit tests
// can inject `EAGAIN`, short reads and `EOPNOTSUPP`; production code never
// changes it.
using PageCacheRead = int64_t (*)(int fd, const ::iovec* iov, int iovcnt,
                                  int64_t offset) noexcept;
// The default: the system call. Where it is not available it fails with
// `EOPNOTSUPP` (it is never called there).
int64_t systemPageCacheRead(int fd, const ::iovec* iov, int iovcnt,
                            int64_t offset) noexcept;
// The function `readPageCacheHits` calls (initially `systemPageCacheRead`).
PageCacheRead& pageCacheRead() noexcept;
// Undo the effect of an `EOPNOTSUPP` on `pageCacheFastPathIsSupported()`, for
// tests that injected one.
void resetPageCacheFastPathSupport() noexcept;
}  // namespace detail

}  // namespace ad_utility

#endif  // QLEVER_SRC_UTIL_PAGECACHEREADHOOK_H
