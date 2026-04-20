// Copyright 2025, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach <johannes.kalmbach@gmail.com>

#include "util/IoUringManager.h"

#include <unistd.h>

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

  if (n == 0) {
    return handle;
  }

  remaining_[handle] = n;

  for (size_t i = 0; i < n; ++i) {
    // When the ring is full, submit pending SQEs and drain CQEs to free space.
    if (inFlight_ >= ringSize_) {
      io_uring_submit(&ring_);
      while (inFlight_ >= ringSize_) {
        drainOneCqe();
      }
    }

    struct io_uring_sqe* sqe = io_uring_get_sqe(&ring_);
    io_uring_prep_read(sqe, fd, targetPointers[i],
                       static_cast<unsigned>(sizes[i]),
                       static_cast<__u64>(fileOffsets[i]));
    io_uring_sqe_set_data64(sqe, handle);
    inFlight_++;
  }

  io_uring_submit(&ring_);
  return handle;
}

void IoUringManager::wait(BatchHandle handle) {
  while (remaining_.count(handle) && remaining_[handle] > 0) {
    drainOneCqe();
  }
  remaining_.erase(handle);
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

  if (remaining_.count(batchId)) {
    remaining_[batchId]--;
  }
}

#endif  // QLEVER_HAS_IO_URING

}  // namespace ad_utility
