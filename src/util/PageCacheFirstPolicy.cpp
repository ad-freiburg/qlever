// Copyright 2026, The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "util/PageCacheFirstPolicy.h"

#include <sys/uio.h>
#include <unistd.h>

#include <atomic>
#include <cerrno>
#include <climits>
#include <cstdint>
#include <vector>

#include "backports/algorithm.h"
#include "backports/span.h"
#include "util/Exception.h"
#include "util/Log.h"
#include "util/PageCacheReadHook.h"

namespace ad_utility {

// `preadv2` with `RWF_NOWAIT` exists on Linux only. Emscripten's headers
// define `RWF_NOWAIT`, but its libc has no `preadv2`; there, and wherever
// `RWF_NOWAIT` is missing, the fast path is compiled out.
#if defined(__linux__) && defined(RWF_NOWAIT) && !defined(__EMSCRIPTEN__)
#define QL_PAGE_CACHE_FAST_PATH
#endif

namespace {
#ifdef QL_PAGE_CACHE_FAST_PATH
// Cleared once a `preadv2(RWF_NOWAIT)` fails with `EOPNOTSUPP` or `ENOSYS`.
// The flag is a best-effort hint without associated data; the default
// sequential consistency (Sonar cpp:S8417) keeps the publication of the
// disabled state obvious.
std::atomic<bool> pageCacheFastPathSupported{true};
#endif
}  // namespace

namespace detail {
//______________________________________________________________________________
int64_t systemPageCacheRead(int fd, const ::iovec* iov, int iovcnt,
                            int64_t offset) noexcept {
#ifdef QL_PAGE_CACHE_FAST_PATH
  return preadv2(fd, iov, iovcnt, static_cast<off_t>(offset), RWF_NOWAIT);
#else
  (void)fd;
  (void)iov;
  (void)iovcnt;
  (void)offset;
  errno = EOPNOTSUPP;
  return -1;
#endif
}

//______________________________________________________________________________
PageCacheRead& pageCacheRead() noexcept {
  static PageCacheRead function = &systemPageCacheRead;
  return function;
}

//______________________________________________________________________________
void resetPageCacheFastPathSupport() noexcept {
#ifdef QL_PAGE_CACHE_FAST_PATH
  pageCacheFastPathSupported.store(true);
#endif
}
}  // namespace detail

//______________________________________________________________________________
bool pageCacheFastPathIsSupported() noexcept {
#ifdef QL_PAGE_CACHE_FAST_PATH
  return pageCacheFastPathSupported.load();
#else
  return false;
#endif
}

// The fast-path helpers below only exist where `preadv2(RWF_NOWAIT)` is
// compiled in; without it, `readPageCacheHits` returns every position.
#ifdef QL_PAGE_CACHE_FAST_PATH
namespace {
// The elements of the range of reads in `readPageCacheHits`: the position of a
// read in its batch, its number of bytes, its file offset and its buffer.
constexpr auto position = [](const auto& read) { return std::get<0>(read); };
constexpr auto numBytesOf = [](const auto& read) { return std::get<1>(read); };
constexpr auto fileOffsetOf = [](const auto& read) {
  return std::get<2>(read);
};
constexpr auto toIovec = [](const auto& read) {
  return ::iovec{std::get<3>(read), numBytesOf(read)};
};
// True if the file range of `next` starts where that of `previous` ends.
constexpr auto areAdjacent = [](const auto& previous, const auto& next) {
  return fileOffsetOf(previous) + numBytesOf(previous) == fileOffsetOf(next);
};

//______________________________________________________________________________
// Log once that the kernel or file system rejects `RWF_NOWAIT` (`EOPNOTSUPP`)
// or that `preadv2` does not exist (`ENOSYS`, Linux < 4.6 or a seccomp filter),
// and switch the fast path off for the rest of the process.
void switchOffFastPathIfUnsupported(int64_t numBytesRead) {
  if (numBytesRead < 0 && (errno == EOPNOTSUPP || errno == ENOSYS) &&
      pageCacheFastPathSupported.exchange(false)) {
    AD_LOG_WARN << "preadv2 with RWF_NOWAIT is not supported (EOPNOTSUPP or "
                   "ENOSYS); reading without the page-cache fast path "
                   "from now on"
                << std::endl;
  }
}

//______________________________________________________________________________
// Read a `run` of adjacent reads with one `preadv2(RWF_NOWAIT)` call and return
// how many of its reads, from the front, were read completely. `EAGAIN`, a
// short read and any other error leave the remaining reads unserved.
template <typename Run>
size_t serveRun(int fd, Run run) {
  const auto iovecs = ::ranges::to<std::vector<::iovec>>(
      run | ::ranges::views::transform(toIovec));
  const int64_t numBytesRead = detail::pageCacheRead()(
      fd, iovecs.data(), static_cast<int>(iovecs.size()),
      static_cast<int64_t>(fileOffsetOf(*::ranges::begin(run))));
  switchOffFastPathIfUnsupported(numBytesRead);
  if (numBytesRead < 0) {
    return 0;
  }
  // The end of each read within the run; a read is served if its end was read.
  auto readEnds = iovecs | ::ranges::views::transform(&::iovec::iov_len) |
                  ::ranges::views::partial_sum;
  const auto numBytesReadUnsigned = static_cast<size_t>(numBytesRead);
  return static_cast<size_t>(
      ::ranges::count_if(readEnds, [numBytesReadUnsigned](size_t end) {
        return end <= numBytesReadUnsigned;
      }));
}
}  // namespace
#endif

//______________________________________________________________________________
std::vector<size_t> readPageCacheHits(int fd, ql::span<const size_t> numBytes,
                                      ql::span<const uint64_t> offsets,
                                      ql::span<char*> buffers) {
  AD_CONTRACT_CHECK(
      offsets.size() == numBytes.size() && buffers.size() == numBytes.size(),
      "The three spans describe the same reads, so they "
      "must have the same length.");
  const auto positions = ::ranges::views::iota(size_t{0}, numBytes.size());
#ifdef QL_PAGE_CACHE_FAST_PATH
  // View over the three spans and `positions`; it is only used inside
  // this call, while all four are alive.
  auto reads = ::ranges::views::zip(positions, numBytes, offsets, buffers);
  std::vector<size_t> notServed;
  notServed.reserve(numBytes.size());
  // One `preadv2` call reads one contiguous range, so reads are grouped
  // into maximal runs of adjacent file ranges and served run by run.
  for (auto adjacentReads : reads | ::ranges::views::chunk_by(areAdjacent)) {
    // One `preadv2` call takes at most `IOV_MAX` buffers.
    for (auto run :
         adjacentReads | ::ranges::views::chunk(static_cast<size_t>(IOV_MAX))) {
      const size_t numServed =
          pageCacheFastPathIsSupported() ? serveRun(fd, run) : 0;
      ::ranges::push_back(notServed, run | ::ranges::views::drop(numServed) |
                                         ::ranges::views::transform(position));
    }
  }
  return notServed;
#else
  (void)fd;
  return ::ranges::to<std::vector<size_t>>(positions);
#endif  // QL_PAGE_CACHE_FAST_PATH
}

}  // namespace ad_utility
