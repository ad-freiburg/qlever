// Copyright 2026, The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "util/IoUringManager.h"

#include <unistd.h>

#include <stdexcept>

#include "util/Exception.h"

namespace ad_utility {

//______________________________________________________________________________
SyncIoManager::BatchHandle SyncIoManager::addBatch(
    int fd, ql::span<const size_t> numBytesToRead,
    ql::span<const uint64_t> fileOffsets, ql::span<char*> targetBuffers) {
  for (size_t i = 0; i < numBytesToRead.size(); ++i) {
    size_t totalBytesRead = 0;

    while (totalBytesRead < numBytesToRead[i]) {
      void* buf = targetBuffers[i] + totalBytesRead;
      size_t count = numBytesToRead[i] - totalBytesRead;
      off_t offset = static_cast<off_t>(fileOffsets[i]) +
                     static_cast<off_t>(totalBytesRead);
      // reads up to `count` bytes from file descriptor `fd` at offset `offset`
      // (from the start of the file) into the buffer starting at `buf`. The
      // file offset is not changed. On success, `pread()` returns the number of
      // bytes read (a return of zero indicates end of file). On error, -1 is
      // returned and `errno` is set to indicate the error. See
      // https://man7.org/linux/man-pages/man2/pread.2.html for more details.
      ssize_t numBytesRead = pread(fd, buf, count, offset);

      if (numBytesRead < 0) {
        throw std::runtime_error("pread failed in SyncIoManager::addBatch");
      }
      totalBytesRead += static_cast<size_t>(numBytesRead);
    }
  }
  return nextHandle_++;
}

#ifdef QLEVER_HAS_IO_URING

//______________________________________________________________________________
IoUringManager::IoUringManager(unsigned ringSize) : ringSize_(ringSize) {
  // Set up the submission and completion queues, shared between this process
  // and the kernel, with (at least) `ringSize_` submission slots. liburing
  // rounds the requested size up to a power of two, so the actual ring may be
  // larger; `ringSize_` is therefore a conservative bound for the "ring full"
  // check below. See io_uring_queue_init(3).
  int ret = io_uring_queue_init(ringSize_, &ring_, /*flags=*/0);
  if (ret < 0) {
    throw std::runtime_error("io_uring_queue_init failed in IoUringManager");
  }
}

//______________________________________________________________________________
IoUringManager::~IoUringManager() { io_uring_queue_exit(&ring_); }

//______________________________________________________________________________
IoUringManager::BatchHandle IoUringManager::addBatch(
    int fd, ql::span<const size_t> numBytesToReadPerRequest,
    ql::span<const uint64_t> fileOffsets, ql::span<char*> targetBuffers) {
  const BatchHandle handle = nextHandle_++;
  const size_t numReadRequestsToPerform = numBytesToReadPerRequest.size();

  if (numReadRequestsToPerform == 0) {
    return handle;
  }
  numInFlightReadRequestsPerBatch_[handle] = numReadRequestsToPerform;

  for (size_t i = 0; i < numReadRequestsToPerform; ++i) {
    // The ring has no free slot, so make room: submit what we have prepared so
    // far and block until enough completions have been drained.
    if (numInFlightReadRequests_ >= ringSize_) {
      io_uring_submit(&ring_);
      while (numInFlightReadRequests_ >= ringSize_) {
        drainOneCqe();
      }
    }

    // Grab a free submission queue entry (SQE) and prepare it. The check above
    // guarantees a slot in the ring buffer is available, so `io_uring_get_sqe`
    // cannot return `nullptr` here.
    io_uring_sqe* sqe = io_uring_get_sqe(&ring_);
    AD_CORRECTNESS_CHECK(sqe != nullptr);
    io_uring_prep_read(sqe, fd, targetBuffers[i],
                       static_cast<unsigned>(numBytesToReadPerRequest[i]),
                       static_cast<__u64>(fileOffsets[i]));
    // Tag the request with its batch handle (by value, not by pointer) so that
    // the matching completion can be attributed back to this batch in
    // `drainOneCqe`.
    io_uring_sqe_set_data64(sqe, handle);
    numInFlightReadRequests_++;
  }

  io_uring_submit(&ring_);
  return handle;
}

//______________________________________________________________________________
void IoUringManager::wait(BatchHandle handle) {
  auto it = numInFlightReadRequestsPerBatch_.find(handle);
  while (it != numInFlightReadRequestsPerBatch_.end() && it->second > 0) {
    drainOneCqe();
    // `drainOneCqe` may have erased the batch (when it completed) or rehashed
    // the map, so re-look up the entry on every iteration.
    it = numInFlightReadRequestsPerBatch_.find(handle);
  }
  numInFlightReadRequestsPerBatch_.erase(handle);
}

//______________________________________________________________________________
void IoUringManager::drainOneCqe() {
  // Block until at least one completion queue entry (CQE) is available.
  io_uring_cqe* cqe = nullptr;
  int ret = io_uring_wait_cqe(&ring_, &cqe);
  if (ret < 0) {
    throw std::runtime_error("io_uring_wait_cqe failed in IoUringManager");
  }

  // `cqe->res` is the read's return value: < 0 is `-errno`, >= 0 is the number
  // of bytes read. Mark the CQE consumed before throwing so its slot is freed.
  if (cqe->res < 0) {
    io_uring_cqe_seen(&ring_, cqe);
    throw std::runtime_error("I/O error in IoUringManager read operation");
  }

  // Recover the batch handle we stored in the SQE and consume the CQE.
  const BatchHandle handle = io_uring_cqe_get_data64(cqe);
  io_uring_cqe_seen(&ring_, cqe);
  numInFlightReadRequests_--;

  // The batch may already have been erased by `wait()`, so look before
  // decrementing.
  auto it = numInFlightReadRequestsPerBatch_.find(handle);
  if (it != numInFlightReadRequestsPerBatch_.end()) {
    it->second--;
  }
}

#endif  // QLEVER_HAS_IO_URING

}  // namespace ad_utility
