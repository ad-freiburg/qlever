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

#include <cstdint>
#include <unordered_map>

#include "backports/algorithm.h"
#include "backports/concepts.h"
#include "util/Exception.h"
#include "util/HashMap.h"

#ifdef QLEVER_HAS_IO_URING
#include <liburing.h>
#endif

#include "backports/span.h"

namespace ad_utility {

template <typename T>
CPP_requires(ReadPolicy_,
             requires(T& policy, int fd, ql::span<const size_t> numBytes,
                      ql::span<const uint64_t> offsets, ql::span<char*> buffers,
                      typename T::BatchHandle handle)(
                 // Must expose a `BatchHandle` type (used as a parameter
                 // above). Must be constructible from a ring size.
                 concepts::constructible_from<T, unsigned>,
                 // Must provide `addBatch` with the following parameters.
                 policy.addBatch(fd, numBytes, offsets, buffers, handle),
                 // Must provide a `wait` method with the following interface.
                 policy.wait(handle)));

// The pluggable I/O backend of `BatchManager`: it specifies how the reads in a
// batch are carried out. See `IoUringPolicy` (asynchronous, via io_uring) and
// `SyncIoPolicy` (blocking `pread` fallback) below.
template <typename T>
CPP_concept ReadPolicyConcept = CPP_requires_ref(ReadPolicy_, T);

// `BatchManager` owns the batch bookkeeping (minting a `BatchHandle` per batch,
// validating the input spans) and delegates the reads from the underlying
// Vocabulary to the `Policy`, which must satisfy the `ReadPolicy` concept
// above.
template <typename ReadPolicy>
class BatchManager {
  static_assert(ReadPolicyConcept<ReadPolicy>,
                "BatchManager's Policy must satisfy the ReadPolicy concept.");

 public:
  using BatchHandle = ReadPolicy::BatchHandle;

  explicit BatchManager(unsigned ringSize = 256) : policy_(ringSize) {}

  BatchManager(const BatchManager&) = delete;
  BatchManager& operator=(const BatchManager&) = delete;

  [[nodiscard]] BatchHandle addBatch(int fd, ql::span<const size_t> numBytes,
                                     ql::span<const uint64_t> offsets,
                                     ql::span<char*> buffers) {
    if (!validateSameLength(numBytes, offsets, buffers)) {
      AD_THROW("spans should have same length");
    }

    BatchHandle handle = nextBatchHandle_++;

    // Delegate the I/O work to the policy.
    policy_.addBatch(fd, numBytes, offsets, buffers, handle);

    return handle;
  }

  // Block until every read in `handle` has completed.
  void wait(BatchHandle handle) { policy_.wait(handle); }

 private:
  ReadPolicy policy_;
  BatchHandle nextBatchHandle_ = 0;

  template <typename Span0, typename... Spans>
  static bool validateSameLength(const Span0& first, const Spans&... rest) {
    const auto n = ql::ranges::size(first);
    return ((ql::ranges::size(rest) == n) && ...);
  }
};

// Fallback implementation for the `IoUringPolicy` below. Schedules pread calls
// in a synchronous (blocking) manner. Single-threaded use only.
struct SyncIoPolicy {
  using BatchHandle = uint64_t;
  // `ringSize` is ignored; it exists only so the policy is constructible the
  // same way as `IoUringPolicy`.
  explicit SyncIoPolicy([[maybe_unused]] unsigned ringSize = 256) {}

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
  };

 private:
  // Read exactly `numBytes` bytes from file descriptor `fd` at `fileOffset`
  // (from the start of the file) into `targetBuffer`. Throws exception if the
  // read fails or returns fewer bytes than requested (a partial read or end of
  // file), since every read must be fully satisfied.
  static void readFullyOrThrow(int fd, char* targetBuffer, size_t numBytes,
                               uint64_t fileOffset);

  FRIEND_TEST(ReadFullyOrThrow, FullReadSucceeds);
  FRIEND_TEST(ReadFullyOrThrow, ShortReadThrows);
  FRIEND_TEST(ReadFullyOrThrow, InvalidFdThrows);
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

  // Per-read metadata needed when a completion is reaped: which batch the read
  // belongs to, and how many bytes it was supposed to read (so that reading
  // fewer bytes than expected can be detected). See
  // `inFlightReadsByRequestId_`.
  struct InFlightRead {
    BatchHandle batchHandle;
    size_t expectedNumBytes;
  };

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
  // to `wait()` to block until this batch has completed.
  void addBatch(int fd, ql::span<const size_t> numBytesToRead,
                ql::span<const uint64_t> offsets, ql::span<char*> buffers,
                BatchHandle handle);

  // Block until every read in the batch that is represented by the `handle` has
  // completed. (The `handle` was submitted along the read requests using
  // `addBatch`.) Throws on any I/O error.
  void wait(BatchHandle handle);
};

using BatchIoManager = BatchManager<IoUringPolicy>;
#else
using BatchIoManager = BatchManager<SyncIoPolicy>;
#endif

}  // namespace ad_utility

#endif  // QLEVER_SRC_UTIL_IOURINGMANAGER_H
