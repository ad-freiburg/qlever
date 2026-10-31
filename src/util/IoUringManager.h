// Copyright 2026, The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_UTIL_IOURINGMANAGER_H
#define QLEVER_SRC_UTIL_IOURINGMANAGER_H

#include <gtest/gtest_prod.h>
#include <sys/uio.h>

#include <cstdint>
#include <memory>
#include <type_traits>
#include <unordered_map>
#include <utility>
#include <vector>

#include "backports/algorithm.h"
#include "backports/concepts.h"
#include "util/Exception.h"
#include "util/HashMap.h"
#include "util/Log.h"

#ifdef QLEVER_HAS_IO_URING
#include <liburing.h>
// `liburing.h` pulls in the kernel header `<linux/fs.h>`, which defines a macro
// `BLOCK_SIZE`. That is a very common identifier, and a macro of that name
// breaks every header that later declares something with that name (for
// example `boost/sort/common/util/circular_buffer.hpp`, which is pulled in by
// `util/blockSort/BoostSortHeaders.h`). QLever does not use the macro, so we
// simply get rid of it again.
#undef BLOCK_SIZE
#endif

#include "backports/span.h"

namespace ad_utility {

template <typename T>
CPP_requires(ReadPolicy_,
             requires(T& policy, int fd, ql::span<const size_t> numBytes,
                      ql::span<const uint64_t> offsets, ql::span<char*> buffers,
                      typename T::BatchHandle handle)(
                 concepts::constructible_from<T, unsigned>,
                 // Must provide `addBatch` with the following parameters.
                 policy.addBatch(fd, numBytes, offsets, buffers, handle),
                 // Must provide a `wait` method with the following interface.
                 policy.wait(handle)));

// The pluggable I/O backend of `BatchManager`: it specifies how the reads in a
// batch are carried out. See `IoUringPolicy` (asynchronous, via io_uring) and
// `SyncIoPolicy` (blocking `pread` fallback) below. Must additionally be
// constructible from a ringsize (`unsigned`). Also require that `T` must expose
// an unsigned integral `BatchHandle` type.
template <typename T>
CPP_concept ReadPolicyConcept =
    concepts::constructible_from<T, unsigned> &&
    concepts::unsigned_integral<typename T::BatchHandle> &&
    CPP_requires_ref(ReadPolicy_, T);

// Abstract base of `BatchManager` so a pool can hold managers of different
// `ReadPolicy`s behind a unified interface and pick the backend at runtime.
class BatchManagerBase {
 public:
  using BatchHandle = uint64_t;
  virtual ~BatchManagerBase() = default;

  [[nodiscard]] virtual BatchHandle addBatch(int fd,
                                             ql::span<const size_t> numBytes,
                                             ql::span<const uint64_t> offsets,
                                             ql::span<char*> buffers) = 0;

  virtual void wait(BatchHandle handle) = 0;

  // Switch the page-cache fast path of this manager on or off (see
  // `PageCacheFirstPolicy`). Takes effect for the batches added afterwards.
  // Managers without a page-cache fast path ignore it.
  virtual void setPageCacheFastPath([[maybe_unused]] bool enabled) {}
};

namespace detail {
// Satisfied iff `Policy` has a member callable as `setPageCacheFastPath(true)`,
// i.e. a switchable page-cache fast path.
template <typename Policy>
CPP_requires(HasPageCacheFastPathSwitch,
             requires(Policy& policy)(policy.setPageCacheFastPath(true)));
}  // namespace detail

// `BatchManager` owns the batch bookkeeping (minting a `BatchHandle` per batch,
// validating the input spans) and delegates the reads from the underlying
// Vocabulary to the `Policy`, which must satisfy the `ReadPolicy` concept
// above.
template <typename ReadPolicy>
class BatchManager final : public BatchManagerBase {
  static_assert(
      ReadPolicyConcept<ReadPolicy>,
      "BatchManager's ReadPolicy must satisfy the ReadPolicyConcept concept.");
  // The policy is only valid if the policy's handle type matches the base's.
  static_assert(
      std::is_same_v<typename ReadPolicy::BatchHandle,
                     BatchManagerBase::BatchHandle>,
      "ReadPolicy::BatchHandle must match BatchManagerBase::BatchHandle.");

 public:
  using BatchHandle = typename BatchManagerBase::BatchHandle;

  explicit BatchManager(unsigned ringSize = 256) : policy_(ringSize) {}

  BatchManager(const BatchManager&) = delete;
  BatchManager& operator=(const BatchManager&) = delete;

  [[nodiscard]] BatchHandle addBatch(int fd, ql::span<const size_t> numBytes,
                                     ql::span<const uint64_t> offsets,
                                     ql::span<char*> buffers) override {
    validateSameLength(numBytes, offsets, buffers);

    BatchHandle handle = nextBatchHandle_++;

    // Delegate the I/O work to the policy.
    policy_.addBatch(fd, numBytes, offsets, buffers, handle);

    return handle;
  }

  // Block until every read in `handle` has completed.
  void wait(BatchHandle handle) override { policy_.wait(handle); }

  // Forward the switch to the policy if it has one.
  void setPageCacheFastPath([[maybe_unused]] bool enabled) override {
    if constexpr (CPP_requires_ref(detail::HasPageCacheFastPathSwitch,
                                   ReadPolicy)) {
      policy_.setPageCacheFastPath(enabled);
    }
  }

 private:
  [[no_unique_address]] ReadPolicy policy_;
  BatchHandle nextBatchHandle_ = 0;

  template <typename Span0, typename... Spans>
  static void validateSameLength(const Span0& first, const Spans&... rest) {
    const auto n = ql::ranges::size(first);
    auto valid = ((ql::ranges::size(rest) == n) && ...);
    if (!valid) {
      AD_THROW("spans must have same length");
    }
    return;
  }
};

// Fallback implementation for the `IoUringPolicy` below. Schedules pread calls
// in a synchronous (blocking) manner. Single-threaded use only.
struct SyncIoPolicy {
  using BatchHandle = uint64_t;

  // `ringSize` is ignored; it exists only so the policy is constructible the
  // same way as `IoUringPolicy`.
  //
  // NOTE: GCC rejects `[[maybe_unused]]` on a defaulted parameter; cast to
  // void.
  explicit SyncIoPolicy(unsigned ringSize = 256) { (void)ringSize; }

  ~SyncIoPolicy() = default;
  SyncIoPolicy(const SyncIoPolicy&) = delete;
  SyncIoPolicy& operator=(const SyncIoPolicy&) = delete;

  // Immediately execute a batch of reads synchronously. This blocks the calling
  // thread. Read `i` reads `numBytesToReadPerRequest[i]` bytes from file
  // descriptor `fd`, starting at offset `fileOffsetPerRequest[i]` (from the
  // start of the file), into the buffer starting at
  // `targetBufferPerRequest[i]`. `handle` is unused (the batch completes before
  // `addBatch` returns).
  void addBatch(int fd, ql::span<const size_t> numBytesToReadPerRequest,
                ql::span<const uint64_t> fileOffsetPerRequest,
                ql::span<char*> targetBufferPerRequest,
                BatchHandle handle) const;

  void wait(BatchHandle) const {
    // No-op: `addBatch` already completed all reads synchronously.
  }

  // Read exactly `numBytes` bytes from file descriptor `fd` at `fileOffset`
  // (from the start of the file) into `targetBuffer`. Throws exception if the
  // read fails or returns fewer bytes than requested (a partial read or end of
  // file), since every read must be fully satisfied.
  static void readFullyOrThrow(int fd, char* targetBuffer, size_t numBytes,
                               uint64_t fileOffset);
};

// Persistent io_uring manager that accepts multiple named batches of indices to
// be read from the underlying storage medium, submits all SQEs in `addBatch`
// (blocking if the ring is full), and lets the caller block on a specific batch
// via `wait()`. Single-threaded use only. See https://github.com/axboe/liburing
// for more details.
#ifdef QLEVER_HAS_IO_URING

class IoUringPolicy {
 public:
  using BatchHandle = uint64_t;

 private:
  io_uring ring_{};
  unsigned ringSize_;

  // Total number of outstanding reads: reads that are prepared (SQE filled in,
  // not yet submitted), in flight (submitted to the kernel, not yet
  // completed), or completed but not yet reaped via a completion queue entry
  // (CQE). `addBatch` keeps it at most `ringSize_`, so that a free SQE is
  // always available and the completion queue (twice as large as the
  // submission queue) cannot overflow.
  size_t numOutstandingReadRequests_ = 0;

  // The same outstanding reads as `numOutstandingReadRequests_`, but broken
  // down per batch: maps a batch handle to the number of its reads that have
  // not yet been reaped. An entry is removed as soon as the last read of its
  // batch is reaped: usually in the `wait()` of that batch, but also in
  // `addBatch` or in the `wait()` of another batch, which reap every ready CQE.
  ad_utility::HashMap<BatchHandle, size_t> numOutstandingReadRequestsPerBatch_;

  // Batches with a failed or short read, mapped to the message of their first
  // failure. `addBatch` and the `wait()` of another batch also reap CQEs, so a
  // failure is kept here until the `wait()` of its own batch throws it.
  ad_utility::HashMap<BatchHandle, const char*> failedBatches_;

  // Per-read metadata needed when a completion is reaped: which batch the read
  // belongs to, and how many bytes it was supposed to read (so that reading
  // fewer bytes than expected can be detected). See
  // `outstandingReadsByRequestId_`.
  struct OutstandingRead {
    BatchHandle batchHandle;
    size_t expectedNumBytes;
  };

  // Monotonically increasing counter that mints a unique request id for each
  // individual read. The id is stored in the SQE's `user_data` and recovered
  // from the matching CQE to look up the read's `OutstandingRead` metadata.
  uint64_t nextRequestIdToAssign_ = 0;

  // Maps a read's request id to its metadata. An entry is inserted when the
  // read is prepared in `addBatch` and erased when its completion is reaped.
  ad_utility::HashMap<uint64_t, OutstandingRead> outstandingReadsByRequestId_;

  // The size of a wave: when the ring is full or `wait()` blocks, wait for up
  // to this many completions (fewer if fewer requests can complete), then reap
  // all ready ones in one pass. This amortizes `io_uring_enter` and the
  // CQ-head update over the wave.
  static constexpr unsigned REAP_WAVE = 8;

  // Block until at least `minComplete` CQEs are ready, then reap every ready
  // CQE. `minComplete` must be > 0 and at most the number of submitted reads
  // that are not yet reaped: a read that is only prepared (SQE filled in, not
  // yet submitted) never completes, so both callers submit before they drain.
  void drainAtLeast(unsigned minComplete);

  // Apply one completion to the bookkeeping of the outstanding reads. Always
  // updates the counts, also for a failed read, and records a failed or short
  // read in `failedBatches_`.
  void processCqe(int numBytesRead, uint64_t requestId);

 public:
  IoUringPolicy(const IoUringPolicy&) = delete;
  IoUringPolicy& operator=(const IoUringPolicy&) = delete;

  // `ringSize` must be > 0 (power of 2 preferred; liburing rounds up).
  explicit IoUringPolicy(unsigned ringSize);
  ~IoUringPolicy();

  // Enqueue a batch of read requests and submit them to the kernel. Blocks the
  // calling thread only when the submission queue is full, in order to drain
  // completion queue entries and free slots in the submission queue. Read `i`
  // reads `numBytesToRead[i]` bytes from file descriptor `fd`, starting at
  // offset `offsets[i]` (from the start of the file), into the buffer starting
  // at `buffers[i]`. The reads are tracked under `handle`, which can be passed
  // to `wait()` to block until this batch has completed.
  void addBatch(int fd, ql::span<const size_t> numBytesToRead,
                ql::span<const uint64_t> offsets, ql::span<char*> buffers,
                BatchHandle handle);

  // Block until every read in the batch that is represented by the `handle` has
  // completed. (The `handle` was submitted along the read requests using
  // `addBatch`.) Throws if a read of this batch failed or was short, after all
  // of its reads are reaped.
  void wait(BatchHandle handle);
};

using BatchIoManager = BatchManager<IoUringPolicy>;
#else
using BatchIoManager = BatchManager<SyncIoPolicy>;
#endif

// Serve the reads of a batch that are completely in the page cache with
// non-blocking `preadv2(RWF_NOWAIT)` calls, one call per run of adjacent file
// ranges (split into chunks of at most `IOV_MAX` reads), and return the
// positions of the other reads in ascending order. The caller issues those
// through its regular path, which also reports errors. A call that fails with
// `EOPNOTSUPP` or `ENOSYS` switches the fast path off for the whole process
// (see `pageCacheFastPathIsSupported`).
// Precondition: the three spans have the same length.
std::vector<size_t> readPageCacheHits(int fd, ql::span<const size_t> numBytes,
                                      ql::span<const uint64_t> offsets,
                                      ql::span<char*> buffers);

// Return whether `readPageCacheHits` may use `preadv2(RWF_NOWAIT)`: false if it
// is not compiled in, or once a call has failed with `EOPNOTSUPP` or `ENOSYS`.
// That latch is process-wide.
bool pageCacheFastPathIsSupported();

namespace detail {
// The one `preadv2(fd, iov, iovcnt, offset, RWF_NOWAIT)` call per run that
// `readPageCacheHits` makes, with the same contract (the number of bytes read,
// or -1 with `errno` set). A replaceable function pointer so that unit tests
// can inject `EAGAIN`, short reads and `EOPNOTSUPP`; production code never
// changes it.
using PageCacheRead = int64_t (*)(int fd, const ::iovec* iov, int iovcnt,
                                  int64_t offset);
// The default: the system call. Where it is not available it fails with
// `EOPNOTSUPP` (it is never called there).
int64_t systemPageCacheRead(int fd, const ::iovec* iov, int iovcnt,
                            int64_t offset);
// The function `readPageCacheHits` calls (initially `systemPageCacheRead`).
PageCacheRead& pageCacheRead();
// Undo the effect of an `EOPNOTSUPP` on `pageCacheFastPathIsSupported()`, for
// tests that injected one.
void resetPageCacheFastPathSupport();
}  // namespace detail

namespace detail {
// Owning copies of the reads of a batch at the indices `positions` into the
// three equally long spans, in the order of `positions`, as the `numBytes`,
// `offsets` and `buffers` arguments of `Policy::addBatch`.
struct SelectedReads {
  std::vector<size_t> numBytes_;
  std::vector<uint64_t> offsets_;
  std::vector<char*> buffers_;

  SelectedReads(ql::span<const size_t> numBytes,
                ql::span<const uint64_t> offsets, ql::span<char*> buffers,
                ql::span<const size_t> positions)
      : numBytes_{selectAt(numBytes, positions)},
        offsets_{selectAt(offsets, positions)},
        buffers_{selectAt(buffers, positions)} {}

 private:
  template <typename T>
  static std::vector<std::remove_const_t<T>> selectAt(
      ql::span<T> values, ql::span<const size_t> positions) {
    return ::ranges::to<std::vector<std::remove_const_t<T>>>(
        positions |
        ql::views::transform([values](size_t i) { return values[i]; }));
  }
};
}  // namespace detail

// A `ReadPolicy` that serves the reads of a batch that are in the page cache
// with `readPageCacheHits` and passes only the others on to `Policy`.
template <typename Policy>
class PageCacheFirstPolicy {
  static_assert(
      ReadPolicyConcept<Policy>,
      "PageCacheFirstPolicy's Policy must satisfy the ReadPolicyConcept.");

 public:
  using BatchHandle = typename Policy::BatchHandle;

  // `ringSize` is forwarded to the constructor of the wrapped `Policy`.
  explicit PageCacheFirstPolicy(unsigned ringSize) : policy_(ringSize) {}

  PageCacheFirstPolicy(const PageCacheFirstPolicy&) = delete;
  PageCacheFirstPolicy& operator=(const PageCacheFirstPolicy&) = delete;

  // Switch the fast path on (the default) or off for the batches that are added
  // afterwards.
  void setPageCacheFastPath(bool enabled) { enabled_ = enabled; }

  // Serve the reads of the batch that are in the page cache and pass the others
  // on to the decorated policy. The arguments have the contract of
  // `readPageCacheHits` and `Policy::addBatch`.
  void addBatch(int fd, ql::span<const size_t> numBytes,
                ql::span<const uint64_t> offsets, ql::span<char*> buffers,
                BatchHandle handle) {
    if (!enabled_ || !pageCacheFastPathIsSupported()) {
      policy_.addBatch(fd, numBytes, offsets, buffers, handle);
      return;
    }
    const std::vector<size_t> notServed =
        readPageCacheHits(fd, numBytes, offsets, buffers);
    if (notServed.size() == numBytes.size()) {
      // Nothing was served, so the batch is passed on unchanged.
      policy_.addBatch(fd, numBytes, offsets, buffers, handle);
      return;
    }
    detail::SelectedReads missed{numBytes, offsets, buffers, notServed};
    policy_.addBatch(fd, missed.numBytes_, missed.offsets_, missed.buffers_,
                     handle);
  }

  // Block until every read of the batch `handle` has completed.
  void wait(BatchHandle handle) { policy_.wait(handle); }

 private:
  Policy policy_;
  bool enabled_ = true;
};

namespace detail {
// Build a `BatchManager` for `Policy`, decorated with `PageCacheFirstPolicy` if
// the page-cache fast path is supported.
template <typename Policy>
std::unique_ptr<BatchManagerBase> makeBatchManagerFor(unsigned ringSize) {
  if (pageCacheFastPathIsSupported()) {
    return std::make_unique<BatchManager<PageCacheFirstPolicy<Policy>>>(
        ringSize);
  }
  return std::make_unique<BatchManager<Policy>>(ringSize);
}
}  // namespace detail

// Build a batch manager. When io_uring is compiled in and the runtime flag
// `preferIoUring` is set, try to build an `IoUringManager`. If its setup
// syscall fails at runtime clear `preferIoUring` and fall back to a
// `SyncIoManager`. Passing the flag by reference makes this probe-once: after
// the first failure, every subsequent call goes straight to the sync manager,
// so we don't repeat a failing syscall. Where `preadv2(RWF_NOWAIT)` is
// available, the manager serves page-cache hits first (`PageCacheFirstPolicy`,
// switched with `BatchManagerBase::setPageCacheFastPath`).
inline std::unique_ptr<BatchManagerBase> makeBatchManager(
    bool& preferIoUring, unsigned ringSize = 256) {
#ifdef QLEVER_HAS_IO_URING
  if (preferIoUring) {
    try {
      return detail::makeBatchManagerFor<IoUringPolicy>(ringSize);
    } catch (const std::exception& e) {
      preferIoUring = false;
      AD_LOG_WARN << "io_uring is compiled in but unavailable at runtime ("
                  << e.what()
                  << "); falling back to synchronous pread for vocabulary "
                     "lookups"
                  << std::endl;
    }
  }
#else
  preferIoUring = false;
#endif
  return detail::makeBatchManagerFor<SyncIoPolicy>(ringSize);
}

}  // namespace ad_utility

#endif  // QLEVER_SRC_UTIL_IOURINGMANAGER_H
