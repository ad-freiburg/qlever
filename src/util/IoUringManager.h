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
#include <limits>
#include <memory>
#include <optional>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include "backports/algorithm.h"
#include "backports/concepts.h"
#include "util/Exception.h"
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
// batch are carried out. See `IoUringPolicy` (via io_uring) and
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

#ifdef QLEVER_HAS_IO_URING

// The operations of an io_uring instance that `IoUringPolicy` uses. An
// interface so that unit tests can run the policy against a simulated ring
// (`test/util/FakeIoUring.h`) that takes only some of the submitted requests,
// fails submits and completes requests in any order.
class IoUringInterface {
 public:
  // A reaped completion: the request id of its SQE and its result (`res`).
  struct Completion {
    uint64_t requestId;
    int result;
  };

  virtual ~IoUringInterface() = default;

  // Fill a free SQE with a read of `numBytes` bytes at `offset` of `fd` into
  // `buffer`, tagged with `requestId`. Return false if no SQE is free.
  virtual bool prepareRead(int fd, char* buffer, size_t numBytes,
                           uint64_t offset, uint64_t requestId) = 0;

  // The number of prepared SQEs that the kernel has not taken yet.
  virtual size_t numQueued() const = 0;

  // `io_uring_submit`: hand the queued SQEs to the kernel, which takes them in
  // the order in which they were prepared. Return the number of SQEs taken, or
  // `-errno`.
  virtual int submit() = 0;

  // Block until at least `minComplete` completions are ready. Return 0 or
  // `-errno`. Must neither hand queued SQEs to the kernel nor consume
  // completions: `IoUringPolicy` learns which SQEs the kernel took only from
  // `submit`, and counts every completion that `popCompletions` returns.
  virtual int waitForCompletions(unsigned minComplete) = 0;

  // Move up to `completions.size()` ready completions into `completions` and
  // release their CQ slots. Return how many were moved.
  virtual size_t popCompletions(ql::span<Completion> completions) = 0;

  // Rewrite every queued SQE into a no-op tagged with `noOpRequestId`, so that
  // it no longer refers to a buffer. Return the request ids of the rewritten
  // SQEs that were not no-ops already, in queue order.
  virtual std::vector<uint64_t> cancelQueued(uint64_t noOpRequestId) = 0;

  // Wait a moment before a submit that failed with `EAGAIN` is retried
  // without any completion to wait for.
  virtual void backOff() = 0;
};

// An `IoUringInterface` backed by liburing. Throws if the ring cannot be set
// up, for example because `io_uring_setup` is blocked by seccomp.
std::unique_ptr<IoUringInterface> makeLiburingRing(unsigned ringSize);

// Reads batches with io_uring, one batch at a time: `addBatch` returns (or
// throws) only once the kernel no longer holds any read of the batch, so the
// caller may free the target buffers right away. `wait()` has nothing left to
// do. Single-threaded use only. See https://github.com/axboe/liburing.
//
// Each read of a batch goes through the states of `ReadState`:
//
//   NotQueued --prepare--> Queued --kernel takes it--> InKernel --CQE--> Done
//                            |
//                            +--- submit fails for good --> Done (no-op)
//
// Between two `addBatch` calls the ring holds no read, only no-ops of reads
// that were cancelled (they refer to no buffer). Errors are reported only
// after the batch has left the ring: a failed or short read throws at the end
// of `addBatch`, and so does an `io_uring_submit` that keeps failing with
// `EAGAIN` (the kernel is out of memory for requests). A failure that leaves
// the kernel holding reads, so that throwing would let the caller free buffers
// the kernel still writes into, terminates the process instead: any other
// submit error, and a failed wait for completions.
class IoUringPolicy {
 public:
  using BatchHandle = uint64_t;

  IoUringPolicy(const IoUringPolicy&) = delete;
  IoUringPolicy& operator=(const IoUringPolicy&) = delete;

  // `ringSize` must be > 0 (power of 2 preferred; liburing rounds up).
  explicit IoUringPolicy(unsigned ringSize);

  // Run on `ring` instead of a liburing ring. `ring` must have room for at
  // least `ringSize` SQEs and `2 * ringSize` CQEs. For tests.
  IoUringPolicy(unsigned ringSize, std::unique_ptr<IoUringInterface> ring);

  // Read the batch: read `i` reads `numBytesToRead[i]` bytes from `fd` at
  // offset `offsets[i]` into `buffers[i]`; the three spans must have the same
  // length. `handle` is unused. Returns once every read has completed. Throws
  // if a read failed or was short, or if its reads could not be submitted, but
  // only once the kernel holds none of its reads.
  void addBatch(int fd, ql::span<const size_t> numBytesToRead,
                ql::span<const uint64_t> offsets, ql::span<char*> buffers,
                BatchHandle handle);

  // Nothing to do: `addBatch` has already completed the batch.
  void wait([[maybe_unused]] BatchHandle handle) {}

 private:
  enum class ReadState : uint8_t { NotQueued, Queued, InKernel, Done };

  // The batch that `addBatch` is reading. Reads are queued in index order, and
  // the kernel takes queued SQEs in order, so the queued reads are always
  // `[numTaken, numQueued)`; the reads before `numTaken` are `InKernel` or
  // `Done`.
  struct Batch {
    ql::span<const size_t> numBytes;
    std::vector<ReadState> states;
    size_t numQueued = 0;
    size_t numTaken = 0;
    size_t numDone = 0;
    std::optional<std::string> error;

    // The reads that the kernel took and whose completion has not been reaped
    // yet (they may still write into their buffers).
    size_t numReadsInKernel() const { return numTaken - numDone; }
    // The ring slots that the batch's reads occupy (queued, in the kernel,
    // or completed but not reaped).
    size_t numSlots() const { return numQueued - numDone; }
  };

  // The request id of a no-op. A read's request id is its index in the batch,
  // so no read gets this id. It is not `uint64_t` max: that is liburing's
  // `LIBURING_UDATA_TIMEOUT`, whose completions liburing consumes itself on
  // kernels without `IORING_FEAT_EXT_ARG` (before 5.11), so the no-op would
  // never be reaped.
  static constexpr uint64_t NO_OP_REQUEST_ID =
      std::numeric_limits<uint64_t>::max() - 1;

 public:
  // The request id that `IoUringPolicy` gives the no-ops of cancelled reads.
  static constexpr uint64_t noOpRequestId() { return NO_OP_REQUEST_ID; }

 private:
  // The size of a wave: wait for up to this many completions at once (fewer if
  // fewer requests are in the kernel), then reap all ready ones in one pass.
  // This amortizes `io_uring_enter` and the CQ-head update over the wave.
  static constexpr unsigned REAP_WAVE = 8;

  // How often in a row an `io_uring_submit` that fails with `EAGAIN` is
  // retried before the queued reads are cancelled.
  static constexpr unsigned MAX_SUBMIT_RETRIES = 16;

  std::unique_ptr<IoUringInterface> ring_;
  unsigned ringSize_;

  // No-ops of cancelled reads (of this or an earlier batch) that are queued or
  // in the kernel. They occupy ring slots until their CQE is reaped.
  size_t numNoOpsQueued_ = 0;
  size_t numNoOpsInKernel_ = 0;

  // Fill the free ring slots with reads, submit them and reap waves of
  // completions until the batch is done or has failed and left the ring.
  void run(int fd, ql::span<const uint64_t> offsets, ql::span<char*> buffers,
           Batch& batch);

  // Submit the queued SQEs, retrying `EINTR`, and `EAGAIN` up to
  // `MAX_SUBMIT_RETRIES` times. If `EAGAIN` persists, cancel the queued reads.
  // Terminates on any other error.
  void submit(Batch& batch);

  // The kernel took `numTaken` SQEs: first the queued no-ops, then reads.
  void onTaken(Batch& batch, size_t numTaken);

  // Rewrite the queued reads into no-ops and fail the batch with
  // `errorMessage`.
  void cancelQueuedReads(Batch& batch, std::string errorMessage);

  // Block until at least `minComplete` requests have completed, then reap all
  // ready completions. Terminates if waiting fails.
  void reap(Batch& batch, unsigned minComplete);

  // Check that the ring's queue holds exactly what `batch` and the no-op
  // counts say.
  void checkQueue(const Batch& batch) const;
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
