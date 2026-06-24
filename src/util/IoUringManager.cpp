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
    int fd, ql::span<const size_t> numBytesToReadPerRequest,
    ql::span<const uint64_t> fileOffsetPerRequest,
    ql::span<char*> targetBufferPerRequest) {
  for (size_t i = 0; i < numBytesToReadPerRequest.size(); ++i) {
    // `pread` reads up to `numBytesToReadPerRequest[i]` bytes from file
    // descriptor `fd` at offset `fileOffsetPerRequest[i]` (from the start of
    // the file) into `targetBufferPerRequest[i]`. The file offset is not
    // changed. On success, it returns the number of bytes read (0 indicates end
    // of file); on error it returns -1 and sets `errno`. See
    // https://man7.org/linux/man-pages/man2/pread.2.html for more details.
    const ssize_t numBytesRead =
        pread(fd, targetBufferPerRequest[i], numBytesToReadPerRequest[i],
              static_cast<off_t>(fileOffsetPerRequest[i]));

    if (numBytesRead < 0) {
      throw std::runtime_error("pread failed in SyncIoManager::addBatch");
    }
    // A result smaller than requested (a partial read, or 0 at end of file)
    // means we read fewer bytes than expected, which we treat as an error.
    if (static_cast<size_t>(numBytesRead) != numBytesToReadPerRequest[i]) {
      throw std::runtime_error(
          "read fewer bytes than requested in SyncIoManager::addBatch");
    }
  }
  return nextBatchHandleToAssign_++;
}

#ifdef QLEVER_HAS_IO_URING

//______________________________________________________________________________
IoUringManager::IoUringManager(unsigned ringSize) : ringSize_(ringSize) {
  // Set up the submission and completion queues, shared between this process
  // and the kernel, with (at least) `ringSize_` submission slots in the
  // submission queue. liburing rounds the requested size up to a power of two,
  // so the actual ring may be larger than `ringSize`; `ringSize_` is therefore
  // a conservative (lower) bound for the "ring full"
  // check below. See io_uring_queue_init(3).
  // See https://man7.org/linux/man-pages/man3/io_uring_queue_init.3.html for
  // details.
  int ret = io_uring_queue_init(ringSize_, &ring_, /*flags=*/0);
  if (ret < 0) {
    throw std::runtime_error("io_uring_queue_init failed in IoUringManager");
  }
}

//______________________________________________________________________________
IoUringManager::~IoUringManager() { io_uring_queue_exit(&ring_); }

//______________________________________________________________________________
void IoUringManager::prepareAndTagRead(uint64_t requestId, int fd,
                                       char* targetBuffer, size_t numBytes,
                                       uint64_t fileOffset) {
  // Claim the next free SQE. The caller guarantees a slot is available, so
  // `io_uring_get_sqe` must not return `nullptr` here.
  io_uring_sqe* sqe = io_uring_get_sqe(&ring_);
  AD_CORRECTNESS_CHECK(sqe != nullptr);

  // Record the read's parameters in the SQE (this only sets the SQE's fields;
  // the request is not handed to the kernel until a later `io_uring_submit`),
  // tag it with the request id (copied verbatim into the matching CQE), and
  // count it as in flight.
  io_uring_prep_read(sqe, fd, targetBuffer, static_cast<unsigned>(numBytes),
                     static_cast<__u64>(fileOffset));
  io_uring_sqe_set_data64(sqe, requestId);
  numInFlightReadRequests_++;
}

//______________________________________________________________________________
IoUringManager::BatchHandle IoUringManager::addBatch(
    int fd, ql::span<const size_t> numBytesToReadPerRequest,
    ql::span<const uint64_t> fileOffsetPerRequest,
    ql::span<char*> targetBufferPerRequest) {
  const BatchHandle handle = nextBatchHandleToAssign_++;
  const size_t numReadRequestsToPerform = numBytesToReadPerRequest.size();

  if (numReadRequestsToPerform == 0) {
    return handle;
  }
  numInFlightReadRequestsPerBatch_[handle] = numReadRequestsToPerform;

  for (size_t i = 0; i < numReadRequestsToPerform; ++i) {
    // The ring has no free slot, so make room: submit what we have prepared so
    // far and block until enough completions have been drained.
    if (numInFlightReadRequests_ >= ringSize_) {
      // FLush the SQEs prepared so far to the kernel so the kernel can start
      // servicing them. Their completions will free up submission slots.
      io_uring_submit(&ring_);
      while (numInFlightReadRequests_ >= ringSize_) {
        drainOneCqe();
      }
    }

    // Record this read's metadata under a unique request id, then prepare and
    // tag its SQE. io_uring copies the request id (the SQE's `user_data`)
    // verbatim into the matching completion, so `drainOneCqe` can recover it.
    const uint64_t requestId = nextRequestIdToAssign_++;
    inFlightReadsByRequestId_[requestId] =
        InFlightRead{handle,
                     fd,
                     targetBufferPerRequest[i],
                     fileOffsetPerRequest[i],
                     numBytesToReadPerRequest[i],
                     0};
    prepareAndTagRead(requestId, fd, targetBufferPerRequest[i],
                      numBytesToReadPerRequest[i], fileOffsetPerRequest[i]);
  }
  // Flush the remaining prepared SQEs to the kernel (the loop above only
  // submits when the submission queue is full, so the last group of SQEs has
  // not yet been submitted).
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

  // Recover the read's result (`cqe->res`) and the request id we stored in the
  // SQE, then consume the CQE so its slot is freed. Do this before any throw.
  const int numBytesRead = cqe->res;
  const uint64_t requestId = io_uring_cqe_get_data64(cqe);
  io_uring_cqe_seen(&ring_, cqe);
  numInFlightReadRequests_--;

  // Every reaped CQE corresponds to exactly one in-flight read whose id we
  // inserted in `addBatch`, so the entry must be present.
  auto reqIt = inFlightReadsByRequestId_.find(requestId);
  AD_CORRECTNESS_CHECK(reqIt != inFlightReadsByRequestId_.end());
  InFlightRead& inFlightRead = reqIt->second;

  // `cqe->res` < 0 is `-errno`.
  if (numBytesRead < 0) {
    throw std::runtime_error("I/O error in IoUringManager read operation");
  }
  // 0 means end of file: no bytes were read although more were requested, so we
  // cannot make progress. The file is shorter than expected; treat as an error.
  if (numBytesRead == 0) {
    throw std::runtime_error(
        "read fewer bytes than requested in IoUringManager");
  }

  // The kernel may satisfy a read in several steps, returning fewer bytes than
  // requested without that being an error. If bytes remain, re-issue a read for
  // the unread bytes (reusing the same request id and metadata). We just reaped
  // a CQE, so a submission slot is free.
  if (inFlightRead.numBytesReadSoFar < inFlightRead.expectedNumBytes) {
    const size_t numBytesRemaining =
        inFlightRead.expectedNumBytes - inFlightRead.numBytesReadSoFar;
    prepareAndTagRead(
        requestId, inFlightRead.fd,
        inFlightRead.targetBuffer + inFlightRead.numBytesReadSoFar,
        numBytesRemaining,
        inFlightRead.fileOffset + inFlightRead.numBytesReadSoFar);
    io_uring_submit(&ring_);
    return;
  }

  // The read is fully satisfied. Attribute the completion to its batch,
  // decrement that batch's in-flight count, and drop the per-read metadata. The
  // batch entry must still be present: a batch is only erased (in `wait()`)
  // after its count reaches zero, which happens only once all of its reads have
  // completed.
  const BatchHandle batchHandle = inFlightRead.batchHandle;
  inFlightReadsByRequestId_.erase(reqIt);
  auto it = numInFlightReadRequestsPerBatch_.find(batchHandle);
  AD_CORRECTNESS_CHECK(it != numInFlightReadRequestsPerBatch_.end());
  it->second--;
}

#endif  // QLEVER_HAS_IO_URING

}  // namespace ad_utility
