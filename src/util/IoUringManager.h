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

#include <atomic>
#include <cstdint>
#include <limits>
#include <optional>
#include <unordered_map>
#include <vector>

#include "backports/algorithm.h"
#include "backports/concepts.h"
#include "util/Exception.h"
#include "util/HashMap.h"
#include "util/RegisteredIoUringReader.h"

#ifdef QLEVER_HAS_IO_URING
#include <liburing.h>
#endif

#include "backports/span.h"

namespace ad_utility {

// How the reads of one batch are carried out. The default (all members off)
// is a plain read into each target buffer.
struct BatchReadOptions {
  // Read into the slots of a pinned arena that is registered with the ring
  // (`IORING_REGISTER_BUFFERS`, served by `IORING_OP_READ_FIXED`) and copy
  // each result into its target buffer. Reads that do not fit into a slot use
  // a plain read. Only honored by `IoUringPolicy`.
  bool useRegisteredBuffers = false;
  // If `useRegisteredBuffers` is set and this is a valid descriptor: the same
  // file as the `fd` of the batch, opened with `O_DIRECT`. Each read then
  // fetches the enclosing aligned blocks from this descriptor (bypassing the
  // page cache) and copies the requested bytes out of the slot. Consecutive
  // requests whose blocks lie in the same slot share one read.
  int directIoFd = -1;
};

// Process-wide switches for the `BatchReadOptions` of vocabulary batch reads.
// They are set by the runtime parameters
// `vocabulary-iouring-registered-buffers` and `vocabulary-iouring-direct-io`
// (see `RuntimeParameters`), like `setRuntimeLogLevel` in `Log.h`.
inline std::atomic<bool> useRegisteredBuffersForVocabularyReads{false};
inline std::atomic<bool> useDirectIoForVocabularyReads{false};

template <typename T>
CPP_requires(
    ReadPolicy_,
    requires(T& policy, int fd, ql::span<const size_t> numBytes,
             ql::span<const uint64_t> offsets, ql::span<char*> buffers,
             typename T::BatchHandle handle, const BatchReadOptions& options)(
        concepts::constructible_from<T, unsigned>,
        // Must provide `addBatch` with the following parameters.
        policy.addBatch(fd, numBytes, offsets, buffers, handle, options),
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

  // Submit the reads of one batch (read `i` reads `numBytes[i]` bytes at
  // `offsets[i]` of `fd` into `buffers[i]`), carried out as specified by
  // `options`. Returns the handle to pass to `wait`.
  [[nodiscard]] virtual BatchHandle addBatch(
      int fd, ql::span<const size_t> numBytes, ql::span<const uint64_t> offsets,
      ql::span<char*> buffers, const BatchReadOptions& options) = 0;

  // Same as above, with the default (plain) `BatchReadOptions`.
  [[nodiscard]] BatchHandle addBatch(int fd, ql::span<const size_t> numBytes,
                                     ql::span<const uint64_t> offsets,
                                     ql::span<char*> buffers) {
    return addBatch(fd, numBytes, offsets, buffers, BatchReadOptions{});
  }

  virtual void wait(BatchHandle handle) = 0;
};

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

  // Keep the overload without `BatchReadOptions` visible.
  using BatchManagerBase::addBatch;

  [[nodiscard]] BatchHandle addBatch(int fd, ql::span<const size_t> numBytes,
                                     ql::span<const uint64_t> offsets,
                                     ql::span<char*> buffers,
                                     const BatchReadOptions& options) override {
    validateSameLength(numBytes, offsets, buffers);

    BatchHandle handle = nextBatchHandle_++;

    // Delegate the I/O work to the policy.
    policy_.addBatch(fd, numBytes, offsets, buffers, handle, options);

    return handle;
  }

  // Block until every read in `handle` has completed.
  void wait(BatchHandle handle) override { policy_.wait(handle); }

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
  // `addBatch` returns). `options` is ignored: the blocking fallback always
  // reads plainly into the target buffers.
  void addBatch(int fd, ql::span<const size_t> numBytesToReadPerRequest,
                ql::span<const uint64_t> fileOffsetPerRequest,
                ql::span<char*> targetBufferPerRequest, BatchHandle handle,
                const BatchReadOptions& options) const;

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

  // Total number of reads that occupy a ring slot but have not yet been reaped
  // via a completion queue entry (CQE), i.e. that are prepared or submitted but
  // not yet completed. Used to detect whether the ring is full.
  size_t numInFlightReadRequests_ = 0;

  // The same in-flight reads as `numInFlight_`, but broken down per batch:
  // maps a batch handle to the number of its reads that have not yet completed
  // (are "in flight"). An entry for a batch (identified by `BatchHandle`) is
  // removed once `wait()` has observed all of its reads complete.
  ad_utility::HashMap<BatchHandle, size_t> numInFlightReadRequestsPerBatch_;

  // Marks an `InFlightRead` that reads directly into its target buffer.
  static constexpr uint32_t kNoSlot = std::numeric_limits<uint32_t>::max();

  // Per-read metadata needed when a completion is reaped: which batch the read
  // belongs to, how many bytes it must at least have read (so that reading
  // fewer bytes than expected can be detected), and the slot of the registered
  // arena it reads into (if any). See `inFlightReadsByRequestId_`.
  struct InFlightRead {
    BatchHandle batchHandle;
    size_t minNumBytes;
    uint32_t slot = kNoSlot;
  };

  // A range of a slot that has to be copied to a target buffer once the read
  // into the slot has completed.
  struct CopyFromSlot {
    char* target;
    size_t offsetInSlot;
    size_t numBytes;
  };

  // An `O_DIRECT` read into a slot that is still being extended: consecutive
  // requests of a batch whose enclosing blocks fit into the same slot share
  // one read, so a block is not read once per word that lies in it.
  struct OpenDirectRead {
    uint32_t slot;
    uint64_t blockBegin;
    uint64_t blockEnd;
    size_t minNumBytes;
  };

  // Size of one slot of the registered arena. With `O_DIRECT`, a read fetches
  // the aligned blocks that enclose the requested bytes, so a read fits into
  // a slot if these blocks do.
  static constexpr size_t kRegisteredSlotSize =
      export_prototypes::kDirectIoBlockSize;

  // The arena for `BatchReadOptions::useRegisteredBuffers`: one slot per ring
  // entry, so that every in-flight read can own a slot. Allocated and
  // registered with the ring on the first batch that asks for it; if that
  // fails (e.g. because of `RLIMIT_MEMLOCK`), all reads stay plain.
  enum class Registration { NotTried, Registered, Failed };
  Registration registration_ = Registration::NotTried;
  std::optional<export_prototypes::PinnedArena> arena_;
  std::vector<uint32_t> freeSlots_;
  // The copies to do when the read into a slot completes, indexed by slot.
  std::vector<std::vector<CopyFromSlot>> copiesPerSlot_;

  // Return true if the registered arena is available, registering it first
  // if this has not been tried yet. Registration is only attempted while no
  // read is in flight.
  bool registeredBuffersAvailable();

  // Return an SQE for the next read, first submitting the prepared SQEs and
  // draining completions if the ring is full.
  io_uring_sqe* claimSqe();

  // Tag the prepared `sqe` with a new request id and account for it as an
  // in-flight read of `read.batchHandle`.
  void trackSqe(io_uring_sqe* sqe, const InFlightRead& read);

  // Return a free slot of the arena, draining completions until one is free.
  uint32_t acquireSlot();

  // Submit `read` as one fixed-buffer read from `directIoFd`.
  void submitDirectRead(const OpenDirectRead& read, int directIoFd,
                        BatchHandle handle);

  // Monotonically increasing counter that mints a unique request id for each
  // individual read. The id is stored in the SQE's `user_data` and recovered
  // from the matching CQE to look up the read's `InFlightRead` metadata.
  uint64_t nextRequestIdToAssign_ = 0;

  // Maps a read's request id to its metadata. An entry is inserted when the
  // read is prepared in `addBatch` and erased when its completion is reaped in
  // `drainOneCqe`.
  ad_utility::HashMap<uint64_t, InFlightRead> inFlightReadsByRequestId_;

  // Wait for one CQE and update the in-flight bookkeeping.
  void drainOneCqe();

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
  // to `wait()` to block until this batch has completed. See
  // `BatchReadOptions` for how `options` changes the way the reads are done;
  // the bytes that end up in `buffers` are the same.
  void addBatch(int fd, ql::span<const size_t> numBytesToRead,
                ql::span<const uint64_t> offsets, ql::span<char*> buffers,
                BatchHandle handle, const BatchReadOptions& options);

  // Block until every read in the batch that is represented by the `handle` has
  // completed. (The `handle` was submitted along the read requests using
  // `addBatch`.) Throws on any I/O error.
  void wait(BatchHandle handle);
};

using BatchIoManager = BatchManager<IoUringPolicy>;
#else
using BatchIoManager = BatchManager<SyncIoPolicy>;
#endif

// Build a batch manager. When io_uring is compiled in and the runtime flag
// `preferIoUring` is set, try to build an `IoUringManager`. If its setup
// syscall fails at runtime clear `preferIoUring` and fall back to a
// `SyncIoManager`. Passing the flag by reference makes this probe-once: after
// the first failure, every subsequent call goes straight to the sync manager,
// so we don't repeat a failing syscall.
inline std::unique_ptr<BatchManagerBase> makeBatchManager(
    bool& preferIoUring, unsigned ringSize = 256) {
#ifdef QLEVER_HAS_IO_URING
  if (preferIoUring) {
    try {
      return std::make_unique<BatchManager<IoUringPolicy>>(ringSize);
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
  return std::make_unique<BatchManager<SyncIoPolicy>>(ringSize);
}

}  // namespace ad_utility

#endif  // QLEVER_SRC_UTIL_IOURINGMANAGER_H
