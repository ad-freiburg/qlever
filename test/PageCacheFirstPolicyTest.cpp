// Copyright 2026, The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gtest/gtest.h>
#include <unistd.h>

#include <climits>
#include <cstddef>
#include <cstdint>
#include <string>
#include <utility>
#include <vector>

#include "util/Exception.h"
#include "util/GTestHelpers.h"
#include "util/IoUringManager.h"
#include "util/IoUringManagerTestHelpers.h"
#include "util/PageCacheFirstPolicy.h"
#include "util/PageCacheReadHook.h"
#include "util/PageCacheReadTestHelpers.h"
#include "util/Views.h"

namespace {

using namespace ::testing;
using namespace ioUringManagerTestHelpers;

// Run `readPageCacheHits` on `reads` (pairs of file offset and size) of `fd`
// and return, in this order, the positions of the reads that were not
// served (in ascending order) and the buffers (filled with '-' before the
// call).
std::pair<std::vector<size_t>, std::vector<std::string>> notServedAndBuffers(
    int fd, const std::vector<std::pair<uint64_t, size_t>>& reads) {
  std::vector<size_t> numBytes;
  numBytes.reserve(reads.size());
  std::vector<uint64_t> offsets;
  offsets.reserve(reads.size());
  std::vector<std::string> buffers;
  buffers.reserve(reads.size());
  for (const auto& [offset, size] : reads) {
    offsets.push_back(offset);
    numBytes.push_back(size);
    buffers.emplace_back(size, '-');
  }
  std::vector<char*> targets;
  for (auto& buffer : buffers) {
    targets.push_back(buffer.data());
  }
  auto notServed =
      ad_utility::readPageCacheHits(fd, numBytes, offsets, targets);
  return {std::move(notServed), std::move(buffers)};
}

// All positions `0 .. n - 1`.
std::vector<size_t> allPositions(size_t n) {
  return ::ranges::to<std::vector<size_t>>(ad_utility::integerRange(n));
}

// Reads that cannot be served completely (here: beyond the end of the file)
// are all returned, whether or not `RWF_NOWAIT` is supported.
TEST(ReadPageCacheHits, missOnlyBatch) {
  auto [tmp, fd] = makeTempFile("AAAABBBBCCCCDDDD");
  auto [notServed, buffers] =
      notServedAndBuffers(fd, {{16, 4}, {40, 4}, {20, 4}});
  EXPECT_EQ(notServed, allPositions(3));
}

// Fault injection for `readPageCacheHits`: the tests below replace its
// `preadv2(RWF_NOWAIT)` call. They need the fast path to be compiled in.
using pageCacheReadTestHelpers::ScopedPageCacheRead;

// Number of calls of the injected page-cache reads below.
size_t numPageCacheReads = 0;

// Read into the buffers of `iov` with plain blocking `pread` calls (which work
// on any file system, unlike `preadv2` with `RWF_NOWAIT`) and return the
// number of bytes read (short at the end of the file), or -1 on a real I/O
// error. This lets the
// tests below exercise the batching of `readPageCacheHits` without depending
// on `RWF_NOWAIT` being supported for the test files (it is rejected with
// `EOPNOTSUPP` in the docker builds, for example on overlayfs).
int64_t blockingFill(int fd, const ::iovec* iov, int iovcnt,
                     int64_t offset) noexcept {
  int64_t total = 0;
  for (const ::iovec& iovec :
       ql::span<const ::iovec>{iov, static_cast<size_t>(iovcnt)}) {
    auto* base = static_cast<char*>(iovec.iov_base);
    size_t remaining = iovec.iov_len;
    while (remaining > 0) {
      const ssize_t numBytesRead = ::pread(fd, base, remaining, offset + total);
      if (numBytesRead < 0) {
        return -1;
      }
      if (numBytesRead == 0) {
        return total;
      }
      base += numBytesRead;
      remaining -= static_cast<size_t>(numBytesRead);
      total += numBytesRead;
    }
  }
  return total;
}

// A fully cached run, counted.
int64_t countedCachedRead(int fd, const ::iovec* iov, int iovcnt,
                          int64_t offset) noexcept {
  ++numPageCacheReads;
  return blockingFill(fd, iov, iovcnt, offset);
}

// `EAGAIN` (nothing cached), counted.
int64_t countedNothingCached(int fd, const ::iovec* iov, int iovcnt,
                             int64_t offset) noexcept {
  ++numPageCacheReads;
  return pageCacheReadTestHelpers::nothingCached(fd, iov, iovcnt, offset);
}

// `EOPNOTSUPP`, counted.
int64_t countedNotSupported(int fd, const ::iovec* iov, int iovcnt,
                            int64_t offset) noexcept {
  ++numPageCacheReads;
  return pageCacheReadTestHelpers::notSupported(fd, iov, iovcnt, offset);
}

// Only the first 6 bytes of every run are "cached": the whole run is read, but
// only 6 bytes are reported as read.
int64_t sixBytesCached(int fd, const ::iovec* iov, int iovcnt,
                       int64_t offset) noexcept {
  const int64_t numBytesRead = blockingFill(fd, iov, iovcnt, offset);
  return numBytesRead < 0 ? numBytesRead : std::min<int64_t>(numBytesRead, 6);
}

// `RWF_NOWAIT` may return 0 before the end of the file (readv(2)).
int64_t zeroBytes(int, const ::iovec*, int, int64_t) noexcept { return 0; }

// Skip a test if the page-cache fast path is compiled out. Otherwise switch it
// on: an earlier real `preadv2(RWF_NOWAIT)` in this process may have switched
// it off, but the tests below inject their own page-cache read.
#define SKIP_WITHOUT_PAGE_CACHE_FAST_PATH()                   \
  ad_utility::detail::resetPageCacheFastPathSupport();        \
  if (!ad_utility::pageCacheFastPathIsSupported()) {          \
    GTEST_SKIP() << "preadv2(RWF_NOWAIT) is not compiled in"; \
  }

// A batch whose reads are all in the page cache is served completely;
// adjacent ranges (the first two reads) are read in one call. The page-cache
// read is injected (a blocking `pread` that serves every byte that exists),
// so the test does not depend on the real `preadv2(RWF_NOWAIT)` being
// supported for the test file or on the file still being cached.
TEST(ReadPageCacheHits, hitOnlyBatch) {
  SKIP_WITHOUT_PAGE_CACHE_FAST_PATH();
  auto [tmp, fd] = makeTempFile("AAAABBBBCCCCDDDD");
  ScopedPageCacheRead inject{&countedCachedRead};
  auto [notServed, buffers] =
      notServedAndBuffers(fd, {{0, 4}, {4, 4}, {12, 4}, {4, 0}});
  EXPECT_TRUE(notServed.empty());
  EXPECT_EQ(buffers[0], "AAAA");
  EXPECT_EQ(buffers[1], "BBBB");
  EXPECT_EQ(buffers[2], "DDDD");
  EXPECT_EQ(buffers[3], "");
}

// A mixed batch: a short read in the middle of a run of adjacent ranges
// returns the incomplete read and the rest of its run, the complete reads
// of the run and of other runs are served. The page-cache read is injected
// as in `hitOnlyBatch`, so no real `preadv2(RWF_NOWAIT)` is involved.
TEST(ReadPageCacheHits, mixedBatchWithShortRead) {
  SKIP_WITHOUT_PAGE_CACHE_FAST_PATH();
  auto [tmp, fd] = makeTempFile("AAAABBBBCCCCDDDD");
  ScopedPageCacheRead inject{&countedCachedRead};
  auto [notServed, buffers] = notServedAndBuffers(
      fd, {{0, 4}, {8, 4}, {12, 2}, {14, 4}, {18, 4}, {4, 4}});
  EXPECT_EQ(notServed, (std::vector<size_t>{3, 4}));
  EXPECT_EQ(buffers[0], "AAAA");
  EXPECT_EQ(buffers[1], "CCCC");
  EXPECT_EQ(buffers[2], "DD");
  EXPECT_EQ(buffers[5], "BBBB");
}

// Reads that are not cached (`EAGAIN`) are all returned, their buffers stay
// untouched, and the fast path stays enabled.
TEST(ReadPageCacheHits, notCachedReadsAreReturned) {
  SKIP_WITHOUT_PAGE_CACHE_FAST_PATH();
  auto [tmp, fd] = makeTempFile("AAAABBBBCCCCDDDD");
  ScopedPageCacheRead inject{&pageCacheReadTestHelpers::nothingCached};
  auto [notServed, buffers] =
      notServedAndBuffers(fd, {{0, 4}, {4, 4}, {12, 4}});
  EXPECT_EQ(notServed, allPositions(3));
  EXPECT_EQ(buffers[0], "----");
  EXPECT_TRUE(ad_utility::pageCacheFastPathIsSupported());
}

// A partial read serves the reads it covers completely; the incomplete read
// and the rest of its run are returned. A read of 0 bytes serves nothing.
TEST(ReadPageCacheHits, partialAndZeroByteReads) {
  SKIP_WITHOUT_PAGE_CACHE_FAST_PATH();
  auto [tmp, fd] = makeTempFile("AAAABBBBCCCCDDDD");
  {
    ScopedPageCacheRead inject{&sixBytesCached};
    auto [notServed, buffers] =
        notServedAndBuffers(fd, {{0, 4}, {4, 4}, {12, 4}});
    EXPECT_EQ(notServed, (std::vector<size_t>{1}));
    EXPECT_EQ(buffers[0], "AAAA");
    EXPECT_EQ(buffers[2], "DDDD");
  }
  {
    ScopedPageCacheRead inject{&zeroBytes};
    auto [notServed, buffers] =
        notServedAndBuffers(fd, {{0, 4}, {4, 4}, {12, 4}});
    EXPECT_EQ(notServed, allPositions(3));
  }
}

// `EOPNOTSUPP` disables the fast path for the process: the failed run and all
// later runs of the batch are returned without further calls, and so are all
// reads of later batches.
TEST(ReadPageCacheHits, notSupportedDisablesTheFastPath) {
  SKIP_WITHOUT_PAGE_CACHE_FAST_PATH();
  auto [tmp, fd] = makeTempFile("AAAABBBBCCCCDDDD");
  numPageCacheReads = 0;
  {
    ScopedPageCacheRead inject{&countedNotSupported};
    auto [notServed, buffers] =
        notServedAndBuffers(fd, {{0, 4}, {8, 4}, {12, 2}});
    EXPECT_EQ(notServed, allPositions(3));
    EXPECT_EQ(numPageCacheReads, 1u);
    EXPECT_FALSE(ad_utility::pageCacheFastPathIsSupported());
    auto [notServed2, unusedBuffers] = notServedAndBuffers(fd, {{0, 4}});
    EXPECT_EQ(notServed2, allPositions(1));
    EXPECT_EQ(numPageCacheReads, 1u);
  }
  // The guard re-enabled the fast path.
  EXPECT_TRUE(ad_utility::pageCacheFastPathIsSupported());
}

// A run of more than `IOV_MAX` adjacent reads is split into several calls.
TEST(ReadPageCacheHits, runsAreSplitAtIovMax) {
  SKIP_WITHOUT_PAGE_CACHE_FAST_PATH();
  const size_t numReads = static_cast<size_t>(IOV_MAX) + 1;
  std::string content(numReads, 'x');
  content.back() = 'y';
  auto [tmp, fd] = makeTempFile(content);
  std::vector<std::pair<uint64_t, size_t>> reads;
  reads.reserve(numReads);
  for (size_t i : ad_utility::integerRange(numReads)) {
    reads.emplace_back(i, 1);
  }
  numPageCacheReads = 0;
  ScopedPageCacheRead inject{&countedCachedRead};
  auto [notServed, buffers] = notServedAndBuffers(fd, reads);
  EXPECT_TRUE(notServed.empty());
  EXPECT_EQ(numPageCacheReads, 2u);
  EXPECT_EQ(buffers.front(), "x");
  EXPECT_EQ(buffers.back(), "y");
}

// An empty batch is trivially served, and spans of different lengths are
// rejected.
TEST(ReadPageCacheHits, emptyBatchAndContract) {
  auto [tmp, fd] = makeTempFile("AAAA");
  EXPECT_TRUE(ad_utility::readPageCacheHits(fd, {}, {}, {}).empty());
  std::vector<size_t> numBytes{4, 4};
  std::vector<uint64_t> offsets{0};
  std::string buffer(8, '-');
  std::vector<char*> targets{buffer.data(), buffer.data() + 4};
  AD_EXPECT_THROW_WITH_MESSAGE(
      ad_utility::readPageCacheHits(fd, numBytes, offsets, targets),
      HasSubstr("offsets.size() == numBytes.size()"));
}

// A `ReadPolicy` that reads synchronously and records the reads it receives.
// The records are static because a `BatchManager` owns its policy.
struct RecordingPolicy {
  using BatchHandle = uint64_t;
  // The number of reads of every `addBatch` call.
  static inline std::vector<size_t> batchSizes;
  // The file offsets of all reads, in the order of arrival.
  static inline std::vector<uint64_t> receivedOffsets;
  static void reset() {
    batchSizes.clear();
    receivedOffsets.clear();
  }

  explicit RecordingPolicy([[maybe_unused]] unsigned ringSize) {}
  void addBatch(int fd, ql::span<const size_t> numBytes,
                ql::span<const uint64_t> offsets, ql::span<char*> buffers,
                [[maybe_unused]] BatchHandle handle) {
    batchSizes.push_back(numBytes.size());
    for (auto&& [numberOfBytesToRead, fileOffset, targetBuffer] :
         ::ranges::views::zip(numBytes, offsets, buffers)) {
      receivedOffsets.push_back(fileOffset);
      ad_utility::SyncIoPolicy::readFullyOrThrow(
          fd, targetBuffer, numberOfBytesToRead, fileOffset);
    }
  }
  void wait([[maybe_unused]] BatchHandle handle) const {}
};

// A manager whose (page-cache-first) policy is the `RecordingPolicy`
// above, so tests can observe which reads reached the policy.
using RecordedManager =
    ad_utility::BatchManager<ad_utility::PageCacheFirstPolicy<RecordingPolicy>>;

// Only the first run of adjacent reads is "cached", all later runs are not.
int64_t firstRunCached(int fd, const ::iovec* iov, int iovcnt,
                       int64_t offset) noexcept {
  if (numPageCacheReads++ == 0) {
    return blockingFill(fd, iov, iovcnt, offset);
  }
  return pageCacheReadTestHelpers::nothingCached(fd, iov, iovcnt, offset);
}

// The reads that the page-cache read serves are not passed on to the decorated
// policy; the others are, in their original order. The offsets `0, 4` are
// adjacent and form one run, `12` and `8` are runs of their own.
TEST(PageCacheFirstPolicy, onlyTheMissesAreForwarded) {
  SKIP_WITHOUT_PAGE_CACHE_FAST_PATH();
  auto [tmp, fd] = makeTempFile("AAAABBBBCCCCDDDD");
  RecordingPolicy::reset();
  numPageCacheReads = 0;
  ScopedPageCacheRead inject{&firstRunCached};
  RecordedManager manager(64);
  ReadBatchForTesting batch;
  batch.add({{0, 4}, {4, 4}, {12, 4}, {8, 4}});
  manager.wait(batch.submitTo(manager, fd));
  EXPECT_THAT(batch.result(), ElementsAre("AAAA", "BBBB", "DDDD", "CCCC"));
  EXPECT_THAT(RecordingPolicy::batchSizes, ElementsAre(size_t{2}));
  EXPECT_THAT(RecordingPolicy::receivedOffsets,
              ElementsAre(uint64_t{12}, uint64_t{8}));
  EXPECT_EQ(numPageCacheReads, size_t{3});
}

// If the page-cache read serves every read, the decorated policy receives an
// empty batch, and waiting for the batch returns immediately.
TEST(PageCacheFirstPolicy, aBatchThatIsServedCompletelyHasNothingToWaitFor) {
  SKIP_WITHOUT_PAGE_CACHE_FAST_PATH();
  auto [tmp, fd] = makeTempFile("AAAABBBBCCCCDDDD");
  RecordingPolicy::reset();
  numPageCacheReads = 0;
  ScopedPageCacheRead inject{&countedCachedRead};
  RecordedManager manager(64);
  ReadBatchForTesting batch;
  batch.add({{0, 4}, {8, 4}});
  manager.wait(batch.submitTo(manager, fd));
  EXPECT_THAT(batch.result(), ElementsAre("AAAA", "CCCC"));
  EXPECT_THAT(RecordingPolicy::batchSizes, ElementsAre(size_t{0}));
  EXPECT_TRUE(RecordingPolicy::receivedOffsets.empty());
  EXPECT_EQ(numPageCacheReads, size_t{2});
}

// If the page-cache read serves nothing, the decorated policy receives the
// batch unchanged.
TEST(PageCacheFirstPolicy, aBatchThatIsNotServedIsPassedOnUnchanged) {
  SKIP_WITHOUT_PAGE_CACHE_FAST_PATH();
  auto [tmp, fd] = makeTempFile("AAAABBBBCCCCDDDD");
  RecordingPolicy::reset();
  numPageCacheReads = 0;
  ScopedPageCacheRead inject{&countedNothingCached};
  RecordedManager manager(64);
  ReadBatchForTesting batch;
  batch.add({{8, 4}, {0, 4}, {12, 4}});
  manager.wait(batch.submitTo(manager, fd));
  EXPECT_THAT(batch.result(), ElementsAre("CCCC", "AAAA", "DDDD"));
  EXPECT_THAT(RecordingPolicy::batchSizes, ElementsAre(size_t{3}));
  EXPECT_THAT(RecordingPolicy::receivedOffsets,
              ElementsAre(uint64_t{8}, uint64_t{0}, uint64_t{12}));
  // One page-cache read per run; the three reads are not adjacent.
  EXPECT_EQ(numPageCacheReads, size_t{3});
}

// With the fast path switched off through the manager, the page-cache read is
// not called and all reads are passed on; switching it on again restores it.
TEST(PageCacheFirstPolicy, theFastPathCanBeSwitchedOffAndOn) {
  SKIP_WITHOUT_PAGE_CACHE_FAST_PATH();
  auto [tmp, fd] = makeTempFile("AAAABBBBCCCCDDDD");
  RecordingPolicy::reset();
  numPageCacheReads = 0;
  ScopedPageCacheRead inject{&countedCachedRead};
  RecordedManager manager(64);
  ad_utility::BatchManagerBase& base = manager;
  base.setPageCacheFastPath(false);
  {
    ReadBatchForTesting batch;
    batch.add({{0, 4}, {8, 4}});
    manager.wait(batch.submitTo(manager, fd));
    EXPECT_THAT(batch.result(), ElementsAre("AAAA", "CCCC"));
    EXPECT_EQ(numPageCacheReads, size_t{0});
    EXPECT_THAT(RecordingPolicy::batchSizes, ElementsAre(size_t{2}));
  }
  base.setPageCacheFastPath(true);
  {
    ReadBatchForTesting batch;
    batch.add({{0, 4}, {8, 4}});
    manager.wait(batch.submitTo(manager, fd));
    EXPECT_THAT(batch.result(), ElementsAre("AAAA", "CCCC"));
    EXPECT_EQ(numPageCacheReads, size_t{2});
    EXPECT_THAT(RecordingPolicy::batchSizes, ElementsAre(size_t{2}, size_t{0}));
  }
}

// If the kernel or file system rejects `RWF_NOWAIT`, the first batch is passed
// on completely, and later batches do not even try the page-cache read.
TEST(PageCacheFirstPolicy, anUnsupportedFastPathIsLeftAlone) {
  SKIP_WITHOUT_PAGE_CACHE_FAST_PATH();
  auto [tmp, fd] = makeTempFile("AAAABBBBCCCCDDDD");
  RecordingPolicy::reset();
  numPageCacheReads = 0;
  ScopedPageCacheRead inject{&countedNotSupported};
  RecordedManager manager(64);
  for (int round = 0; round < 2; ++round) {
    ReadBatchForTesting batch;
    batch.add({{0, 4}, {8, 4}});
    manager.wait(batch.submitTo(manager, fd));
    EXPECT_THAT(batch.result(), ElementsAre("AAAA", "CCCC"));
    EXPECT_FALSE(ad_utility::pageCacheFastPathIsSupported());
  }
  EXPECT_EQ(numPageCacheReads, size_t{1});
  EXPECT_THAT(RecordingPolicy::batchSizes, ElementsAre(size_t{2}, size_t{2}));
}

}  // namespace
