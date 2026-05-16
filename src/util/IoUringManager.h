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

// ── SyncIoManager ──────────────────────────────────────────────────────────
// Synchronous (pread) fallback implementation. Single-threaded use only.
class SyncIoManager {
 public:
  using BatchHandle = uint64_t;

  explicit SyncIoManager(unsigned /*ringSize*/ = 256) {}
  ~SyncIoManager() = default;
  SyncIoManager(const SyncIoManager&) = delete;
  SyncIoManager& operator=(const SyncIoManager&) = delete;

  // Enqueue and immediately execute a batch of reads synchronously.
  // Returns a handle for consistency with IoUringManager.
  BatchHandle addBatch(int fd, ql::span<const size_t> sizes,
                       ql::span<const uint64_t> fileOffsets,
                       ql::span<char*> targetPointers);

  // No-op: addBatch already completed all reads synchronously.
  void wait(BatchHandle) {}

 private:
  uint64_t nextHandle_ = 0;
};

// ── IoUringManager ─────────────────────────────────────────────────────────
// Persistent io_uring manager that accepts multiple named batches, submits
// all SQEs in addBatch (blocking if the ring is full), and lets the caller
// block on a specific batch via wait().
// Single-threaded use only.
#ifdef QLEVER_HAS_IO_URING

class IoUringManager {
 public:
  using BatchHandle = uint64_t;

  // ringSize must be > 0 (power of 2 preferred; liburing rounds up).
  explicit IoUringManager(unsigned ringSize = 256);
  ~IoUringManager();

  // Non-copyable, non-movable (owns ring resources).
  IoUringManager(const IoUringManager&) = delete;
  IoUringManager& operator=(const IoUringManager&) = delete;

  // Enqueue a batch of reads. Submits all SQEs, blocking to drain CQEs
  // when the ring is full. Returns a handle that can be passed to wait().
  BatchHandle addBatch(int fd, ql::span<const size_t> sizes,
                       ql::span<const uint64_t> fileOffsets,
                       ql::span<char*> targetPointers);

  // Block until every read in `handle` has completed.
  // Throws std::runtime_error on any I/O error.
  void wait(BatchHandle handle);

 private:
  io_uring ring_{};
  unsigned ringSize_;
  size_t inFlight_ = 0;
  uint64_t nextHandle_ = 0;

  // Maps batch ID to number of not-yet-completed reads.
  std::unordered_map<BatchHandle, size_t> remaining_;

  // Wait for one CQE and update bookkeeping.
  void drainOneCqe();
};

using BatchIoManager = IoUringManager;
#else
using BatchIoManager = SyncIoManager;
#endif

}  // namespace ad_utility

#endif  // QLEVER_SRC_UTIL_IOURINGMANAGER_H
