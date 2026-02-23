// Copyright 2025, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach <johannes.kalmbach@gmail.com>

#ifndef QLEVER_SRC_UTIL_IOURINGMANAGER_H
#define QLEVER_SRC_UTIL_IOURINGMANAGER_H

#include <unistd.h>

#include <algorithm>
#include <cstdint>
#include <deque>
#include <numeric>
#include <stdexcept>
#include <vector>

#ifdef QLEVER_HAS_IO_URING
#include <liburing.h>
#endif

#include "backports/span.h"

namespace ad_utility {

// Persistent io_uring manager that accepts multiple named batches, drip-feeds
// SQEs when the ring fills up, and lets the caller block on a specific batch.
// Single-threaded use only.
class IoUringManager {
 public:
  using BatchHandle = uint64_t;

  // ringSize must be > 0 (power of 2 preferred; liburing rounds up).
  explicit IoUringManager(unsigned ringSize = 256) {
#ifdef QLEVER_HAS_IO_URING
    ringSize_ = ringSize;
    int ret = io_uring_queue_init(ringSize_, &ring_, 0);
    if (ret < 0) {
      throw std::runtime_error("io_uring_queue_init failed in IoUringManager");
    }
#else
    (void)ringSize;
#endif
  }

  ~IoUringManager() {
#ifdef QLEVER_HAS_IO_URING
    io_uring_queue_exit(&ring_);
#endif
  }

  // Non-copyable, non-movable (owns ring resources).
  IoUringManager(const IoUringManager&) = delete;
  IoUringManager& operator=(const IoUringManager&) = delete;

  // Enqueue a batch of reads. Immediately submits as many SQEs as fit.
  // Returns a handle that can be passed to wait().
  BatchHandle addBatch(int fd, ql::span<const size_t> sizes,
                       ql::span<const uint64_t> fileOffsets,
                       ql::span<char*> targetPointers) {
    BatchHandle handle = nextHandle_++;
    const size_t n = sizes.size();

    // Build ReadRequest list sorted by file offset for sequential I/O.
    std::vector<size_t> perm(n);
    std::iota(perm.begin(), perm.end(), size_t{0});
    std::sort(perm.begin(), perm.end(), [&](size_t a, size_t b) {
      return fileOffsets[a] < fileOffsets[b];
    });

    Batch batch;
    batch.id = handle;
    batch.fd = fd;
    batch.reads.reserve(n);
    for (size_t pi = 0; pi < n; ++pi) {
      size_t i = perm[pi];
      batch.reads.push_back({sizes[i], fileOffsets[i], targetPointers[i]});
    }

#ifdef QLEVER_HAS_IO_URING
    if (n == 0) {
      // Empty batch: mark as immediately done.
      batch.submitted = 0;
      batch.completed = 0;
      pending_.push_back(std::move(batch));
      cleanupFront();
      return handle;
    }

    pending_.push_back(std::move(batch));
    submitFromPending();
#else
    // Fallback: execute synchronously.
    for (const auto& r : batch.reads) {
      size_t bytesRead = 0;
      while (bytesRead < r.size) {
        ssize_t ret = pread(
            fd, r.target + bytesRead, r.size - bytesRead,
            static_cast<off_t>(r.fileOffset) + static_cast<off_t>(bytesRead));
        if (ret < 0) {
          throw std::runtime_error("pread failed in IoUringManager::addBatch");
        }
        bytesRead += static_cast<size_t>(ret);
      }
    }
    batch.submitted = n;
    batch.completed = n;
    pending_.push_back(std::move(batch));
    cleanupFront();
#endif

    return handle;
  }

  // Block until every read in `handle` has completed.
  // Throws std::runtime_error on any I/O error.
  // If the batch was already cleaned up (already done), this is a no-op.
  void wait(BatchHandle handle) {
#ifdef QLEVER_HAS_IO_URING
    // Find the batch; if absent it was already completed and cleaned up.
    Batch* b = findBatch(handle);
    if (b == nullptr) {
      return;
    }
    while (!b->isDone()) {
      drainOneCqe();
      submitFromPending();
      // Re-fetch pointer since deque may have been modified (but only
      // cleanupFront removes from front; our batch is still there until done).
      b = findBatch(handle);
      if (b == nullptr) {
        return;  // completed inside drainOneCqe
      }
    }
    cleanupFront();
#else
    // Synchronous fallback: batch already done in addBatch.
    (void)handle;
#endif
  }

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

#ifdef QLEVER_HAS_IO_URING
  io_uring ring_{};
  unsigned ringSize_;
  size_t inFlight_ = 0;
#endif
  uint64_t nextHandle_ = 0;
  std::deque<Batch> pending_;

  Batch* findBatch(BatchHandle handle) {
    for (auto& b : pending_) {
      if (b.id == handle) return &b;
    }
    return nullptr;
  }

#ifdef QLEVER_HAS_IO_URING
  // Submit as many SQEs as fit from the front of pending_.
  void submitFromPending() {
    bool submitted = false;
    for (auto& b : pending_) {
      while (inFlight_ < ringSize_ && b.submitted < b.reads.size()) {
        struct io_uring_sqe* sqe = io_uring_get_sqe(&ring_);
        if (!sqe) break;
        const auto& r = b.reads[b.submitted];
        io_uring_prep_read(sqe, b.fd, r.target, static_cast<unsigned>(r.size),
                           static_cast<__u64>(r.fileOffset));
        io_uring_sqe_set_data64(sqe, b.id);
        b.submitted++;
        inFlight_++;
        submitted = true;
      }
      if (inFlight_ >= ringSize_) break;
    }
    if (submitted) {
      io_uring_submit(&ring_);
    }
  }

  // Block for one CQE and process it.
  void drainOneCqe() {
    struct io_uring_cqe* cqe = nullptr;
    int ret = io_uring_wait_cqe(&ring_, &cqe);
    if (ret < 0) {
      throw std::runtime_error("io_uring_wait_cqe failed in IoUringManager");
    }
    if (cqe->res < 0) {
      io_uring_cqe_seen(&ring_, cqe);
      throw std::runtime_error("I/O error in IoUringManager read operation");
    }
    BatchHandle batchId = io_uring_cqe_get_data64(cqe);
    io_uring_cqe_seen(&ring_, cqe);
    inFlight_--;

    Batch* b = findBatch(batchId);
    if (b != nullptr) {
      b->completed++;
    }
  }
#endif

  // Remove fully completed batches from the front of pending_.
  void cleanupFront() {
    while (!pending_.empty() && pending_.front().isDone()) {
      pending_.pop_front();
    }
  }
};

}  // namespace ad_utility

#endif  // QLEVER_SRC_UTIL_IOURINGMANAGER_H
