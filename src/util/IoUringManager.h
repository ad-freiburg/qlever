// Copyright 2025, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach <johannes.kalmbach@gmail.com>

#ifndef QLEVER_SRC_UTIL_IOURINGMANAGER_H
#define QLEVER_SRC_UTIL_IOURINGMANAGER_H

#include <cstdint>
#include <deque>
#include <vector>

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
// Persistent io_uring manager that accepts multiple named batches, drip-feeds
// SQEs when the ring fills up, and lets the caller block on a specific batch.
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

  // Enqueue a batch of reads. Immediately submits as many SQEs as fit.
  // Returns a handle that can be passed to wait().
  BatchHandle addBatch(int fd, ql::span<const size_t> sizes,
                       ql::span<const uint64_t> fileOffsets,
                       ql::span<char*> targetPointers);

  // Block until every read in `handle` has completed.
  // Throws std::runtime_error on any I/O error.
  // If the batch was already cleaned up (already done), this is a no-op.
  void wait(BatchHandle handle);

 private:
  struct ReadRequest {
    size_t size;
    uint64_t fileOffset;
    char* target;
  };

  struct Batch {
    BatchHandle id;
    int fd;
    std::vector<ReadRequest> reads;
    size_t submitted = 0;
    size_t completed = 0;
    bool isDone() const { return completed == reads.size(); }
  };

  io_uring ring_{};
  unsigned ringSize_;
  size_t inFlight_ = 0;
  uint64_t nextHandle_ = 0;
  std::deque<Batch> pending_;

  void submitFromPending();
  void drainOneCqe();
  void cleanupFront();
  Batch* findBatch(BatchHandle handle);
};

using BatchIoManager = IoUringManager;
#else
using BatchIoManager = SyncIoManager;
#endif

}  // namespace ad_utility

#endif  // QLEVER_SRC_UTIL_IOURINGMANAGER_H
