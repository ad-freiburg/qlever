// Copyright 2025, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach <johannes.kalmbach@gmail.com>

#include "util/IoUringManager.h"

#include <unistd.h>

#include <algorithm>
#include <numeric>
#include <stdexcept>

namespace ad_utility {

// ── SyncIoManager methods ──────────────────────────────────────────────────

SyncIoManager::BatchHandle SyncIoManager::addBatch(
    int fd, ql::span<const size_t> sizes, ql::span<const uint64_t> fileOffsets,
    ql::span<char*> targetPointers) {
  for (size_t i = 0; i < sizes.size(); ++i) {
    size_t bytesRead = 0;
    while (bytesRead < sizes[i]) {
      ssize_t ret = pread(
          fd, targetPointers[i] + bytesRead, sizes[i] - bytesRead,
          static_cast<off_t>(fileOffsets[i]) + static_cast<off_t>(bytesRead));
      if (ret < 0) {
        throw std::runtime_error("pread failed in SyncIoManager::addBatch");
      }
      bytesRead += static_cast<size_t>(ret);
    }
  }
  return nextHandle_++;
}

#ifdef QLEVER_HAS_IO_URING

// ── IoUringManager methods ─────────────────────────────────────────────────

IoUringManager::IoUringManager(unsigned ringSize) : ringSize_(ringSize) {
  int ret = io_uring_queue_init(ringSize_, &ring_, 0);
  if (ret < 0) {
    throw std::runtime_error("io_uring_queue_init failed in IoUringManager");
  }
}

IoUringManager::~IoUringManager() { io_uring_queue_exit(&ring_); }

IoUringManager::BatchHandle IoUringManager::addBatch(
    int fd, ql::span<const size_t> sizes, ql::span<const uint64_t> fileOffsets,
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

  if (n == 0) {
    // Empty batch: mark as immediately done.
    pending_.push_back(std::move(batch));
    cleanupFront();
    return handle;
  }

  pending_.push_back(std::move(batch));
  submitFromPending();
  return handle;
}

void IoUringManager::wait(BatchHandle handle) {
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
}

void IoUringManager::submitFromPending() {
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

void IoUringManager::drainOneCqe() {
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

void IoUringManager::cleanupFront() {
  while (!pending_.empty() && pending_.front().isDone()) {
    pending_.pop_front();
  }
}

IoUringManager::Batch* IoUringManager::findBatch(BatchHandle handle) {
  for (auto& b : pending_) {
    if (b.id == handle) return &b;
  }
  return nullptr;
}

#endif  // QLEVER_HAS_IO_URING

}  // namespace ad_utility
