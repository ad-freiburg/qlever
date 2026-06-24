// Copyright 2025, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach <johannes.kalmbach@gmail.com>

#ifndef QLEVER_SRC_UTIL_IOURINGMANAGER_H
#define QLEVER_SRC_UTIL_IOURINGMANAGER_H

#include <cstdint>
#include <unordered_map>

#ifdef QLEVER_HAS_IO_URING
#include <liburing.h>
#endif

#include "backports/span.h"

namespace ad_utility {

// Synchronous (pread) fallback implementation. Single-threaded use only.
class SyncIoManager {
 public:
  using BatchHandle = uint64_t;

  explicit SyncIoManager(unsigned /*ringSize*/ = 256) {}
  ~SyncIoManager() = default;
  SyncIoManager(const SyncIoManager&) = delete;
  SyncIoManager& operator=(const SyncIoManager&) = delete;

  // Enqueue and immediately execute a batch of reads synchronously.
  // This blocks the thread. Returns a handle for consistency with
  // `IoUringManager`. The three spans must all have the same length, which
  // implicitly defines the number of reads: read `i` reads up to
  // `numBytesToRead[i]` bytes from file descriptor `fd`, starting at offset
  // `fileOffsets[i]` (from the start of the file), into the buffer starting at
  // `targetBuffers[i]`.
  BatchHandle addBatch(int fd, ql::span<const size_t> numBytesToRead,
                       ql::span<const uint64_t> fileOffsets,
                       ql::span<char*> targetBuffers);

  // No-op: `addBatch` already completed all reads synchronously.
  void wait(BatchHandle) {}

 private:
  uint64_t nextHandle_ = 0;
};

// Persistent io_uring manager that accepts multiple named batches, submits
// all SQEs in `addBatch` (blocking if the ring is full), and lets the caller
// block on a specific batch via `wait()`.
// Single-threaded use only. See https://github.com/axboe/liburing for more
// details.
#ifdef QLEVER_HAS_IO_URING

class IoUringManager {
 public:
  using BatchHandle = uint64_t;

  // `ringSize` must be > 0 (power of 2 preferred; liburing rounds up).
  explicit IoUringManager(unsigned ringSize = 256);
  ~IoUringManager();

  // Non-copyable, non-movable (owns ring resources).
  IoUringManager(const IoUringManager&) = delete;
  IoUringManager& operator=(const IoUringManager&) = delete;

  // Enqueue a batch of reads and submit them to the kernel, blocking to drain
  // completions only when the ring is full. The three spans must all have the
  // same length, which implicitly defines the number of reads: read `i` reads
  // up to `numBytesToRead[i]` bytes from file descriptor `fd`, starting at
  // offset `fileOffsets[i]` (from the start of the file), into the buffer
  // starting at `targetBuffers[i]`. Returns a handle that can be passed to
  // `wait()` to block until this batch has completed.
  BatchHandle addBatch(int fd, ql::span<const size_t> numBytesToRead,
                       ql::span<const uint64_t> fileOffsets,
                       ql::span<char*> targetBuffers);

  // Block until every read in `handle` has completed.
  // Throws `std::runtime_error` on any I/O error.
  void wait(BatchHandle handle);

 private:
  io_uring ring_{};
  unsigned ringSize_;

  // Total number of reads that occupy a ring slot but have not yet been reaped
  // via a completion queue entry (CQE), i.e. that are prepared or submitted but
  // not yet completed. Used to detect when the ring is full.
  size_t numInFlight_ = 0;

  // The handle that will be assigned to the next batch.
  uint64_t nextHandle_ = 0;

  // The same in-flight reads as `numInFlight_`, but broken down per batch:
  // maps a batch handle to the number of its reads that have not yet completed.
  // An entry for a batch (identified by `BatchHandle`) is removed once `wait()`
  // has observed all of its reads complete.
  std::unordered_map<BatchHandle, size_t> inFlightPerBatch_;

  // Wait for one CQE and update the in-flight bookkeeping.
  void drainOneCqe();
};

using BatchIoManager = IoUringManager;
#else
using BatchIoManager = SyncIoManager;
#endif

}  // namespace ad_utility

#endif  // QLEVER_SRC_UTIL_IOURINGMANAGER_H
