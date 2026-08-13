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

#include <cerrno>
#include <cstring>
#include <stdexcept>

#include "util/Exception.h"
#include "util/Log.h"

namespace ad_utility {

//______________________________________________________________________________
void SyncIoPolicy::readFullyOrThrow(int fd, char* targetBuffer, size_t numBytes,
                                    uint64_t fileOffset) {
  // `pread` reads up to `numBytes` bytes from file descriptor `fd` at offset
  // `fileOffset` (from the start of the file) into `targetBuffer`. The file
  // offset is not changed. On success, it returns the number of bytes read (0
  // indicates end of file); on error it returns -1 and sets `errno`. See
  // https://man7.org/linux/man-pages/man2/pread.2.html for more details.
  const ssize_t numBytesRead =
      pread(fd, targetBuffer, numBytes, static_cast<off_t>(fileOffset));

  if (numBytesRead < 0) {
    AD_THROW("pread failed in readFullyOrThrow");
  }
  // A result smaller than requested (a partial read, or 0 at end of file) means
  // we read fewer bytes than expected, which we treat as an error.
  if (static_cast<size_t>(numBytesRead) != numBytes) {
    AD_THROW("read fewer bytes than requested in readFullyOrThrow");
  }
}

//______________________________________________________________________________
void SyncIoPolicy::addBatch(int fd,
                            ql::span<const size_t> numBytesToReadPerRequest,
                            ql::span<const uint64_t> fileOffsetPerRequest,
                            ql::span<char*> targetBufferPerRequest,
                            [[maybe_unused]] BatchHandle handle) const {
  for (const auto& [numBytesToRead, fileOffset, targetBuf] :
       ::ranges::views::zip(numBytesToReadPerRequest, fileOffsetPerRequest,
                            targetBufferPerRequest)) {
    SyncIoPolicy::readFullyOrThrow(fd, targetBuf, numBytesToRead, fileOffset);
  }
}

#ifdef QLEVER_HAS_IO_URING

//______________________________________________________________________________
IoUringPolicy::IoUringPolicy(unsigned ringSize, size_t registeredBufferSize)
    : ringSize_(ringSize), registeredBufferSize_(registeredBufferSize) {
  // Set up the submission and completion queues, shared between this process
  // and the kernel, with (at least) `ringSize_` submission slots in the
  // submission queue. liburing rounds the requested size up to a power of two,
  // so the actual ring may be larger than `ringSize`; `ringSize_` is therefore
  // a conservative (lower) bound for the "ring full" check below. See
  // https://man7.org/linux/man-pages/man3/io_uring_queue_init.3.html for
  // details.
  int ret = io_uring_queue_init(ringSize_, &ring_, /*flags=*/0);
  if (ret < 0) {
    AD_THROW("io_uring_queue_init failed in IoUringManager");
  }

  // --- Set up the registered buffer pool -----------------------------------
  // Pre-allocate one buffer per ring slot so that every in-flight SQE can
  // target its own registered buffer. Registering the buffers lets the kernel
  // pin their pages once instead of on every read, which removes the
  // per-request `get_user_pages`/unpin work from the I/O path. Note that this
  // does *not* remove the page-cache-to-user copy for buffered reads; the
  // per-completion `memcpy` into the caller's buffer is an additional cost
  // that this design trades against the cheaper submission path, so the
  // approach only pays off if it is measured to.
  registeredBufferPool_.resize(ringSize_ * registeredBufferSize_, '\0');
  registeredIovecs_.reserve(ringSize_);
  freeBufferIndices_.reserve(ringSize_);
  for (unsigned i = 0; i < ringSize_; ++i) {
    registeredIovecs_.push_back(
        {registeredBufferPool_.data() + i * registeredBufferSize_,
         registeredBufferSize_});
    freeBufferIndices_.push_back(i);
  }

  // Registered buffers are pinned, so they count against `RLIMIT_MEMLOCK`,
  // whose default is small (often 8 MiB) on many systems. Rather than
  // refusing to open the vocabulary, fall back to a ring without registered
  // buffers; `addBatch` then reads straight into the caller's buffers.
  ret = io_uring_register_buffers(&ring_, registeredIovecs_.data(),
                                  registeredIovecs_.size());
  if (ret == -ENOMEM || ret == -EPERM) {
    AD_LOG_INFO << "io_uring: could not register "
                << registeredIovecs_.size() * registeredBufferSize_
                << " bytes of buffers (RLIMIT_MEMLOCK is likely too low), "
                   "falling back to unregistered reads.\n";
    registeredBufferPool_.clear();
    registeredIovecs_.clear();
    freeBufferIndices_.clear();
    ret = 0;
  }
  if (ret < 0) {
    // The ring is already initialized; release its resources before throwing,
    // since the destructor does not run for a constructor that throws.
    io_uring_queue_exit(&ring_);
    AD_THROW("io_uring_register_buffers failed in IoUringManager");
  }
}

//______________________________________________________________________________
IoUringPolicy::~IoUringPolicy() {
  if (numInFlightReadRequests_ > 0) {
    AD_LOG_WARN << "IoUringPolicy destroyed with " << numInFlightReadRequests_
                << " read request(s) still in flight; all batches should be "
                   "`wait()`ed before destroying the policy. Draining them now "
                   "so the kernel stops writing into the registered buffers.\n";
  }
  // Reap the outstanding completions before tearing down the ring, so the
  // kernel is no longer writing into any registered buffer once we return. We
  // deliberately do not call `drainOneCqe` here: it throws on I/O errors,
  // and a destructor must not throw. We also stop if `io_uring_wait_cqe`
  // fails, to avoid spinning forever (it would not decrement the in-flight
  // count).
  while (numInFlightReadRequests_ > 0) {
    io_uring_cqe* cqe = nullptr;
    if (io_uring_wait_cqe(&ring_, &cqe) < 0) {
      break;
    }
    // Reap the completion and return the buffer to the free list. Reads that
    // bypassed the pool hold no buffer (`noPoolBuffer`).
    const uint64_t requestId = io_uring_cqe_get_data64(cqe);
    auto it = inFlightReadsByRequestId_.find(requestId);
    if (it != inFlightReadsByRequestId_.end()) {
      if (it->second.poolBufferIndex != noPoolBuffer) {
        freeBufferIndices_.push_back(it->second.poolBufferIndex);
      }
      inFlightReadsByRequestId_.erase(it);
    }
    io_uring_cqe_seen(&ring_, cqe);
    --numInFlightReadRequests_;
  }
  if (!registeredBufferPool_.empty()) {
    io_uring_unregister_buffers(&ring_);
  }
  io_uring_queue_exit(&ring_);
}

//______________________________________________________________________________
size_t IoUringPolicy::allocatePoolBuffer() {
  // If no free buffer is available, drain completions until one is freed.
  // This cannot deadlock: whenever the pool is exhausted, every pool buffer
  // is held by an in-flight read that returns it on completion, so draining
  // completions eventually frees a buffer.
  while (freeBufferIndices_.empty()) {
    drainOneCqe();
  }
  size_t idx = freeBufferIndices_.back();
  freeBufferIndices_.pop_back();
  return idx;
}

//______________________________________________________________________________
void IoUringPolicy::freePoolBuffer(size_t index) {
  freeBufferIndices_.push_back(index);
}

//______________________________________________________________________________
void IoUringPolicy::addBatch(int fd,
                             ql::span<const size_t> numBytesToReadPerRequest,
                             ql::span<const uint64_t> fileOffsetPerRequest,
                             ql::span<char*> targetBufferPerRequest,
                             BatchHandle handle) {
  const size_t numReadRequestsToPerform = numBytesToReadPerRequest.size();

  if (numReadRequestsToPerform == 0) {
    return;
  }
  numInFlightReadRequestsPerBatch_[handle] = numReadRequestsToPerform;

  for (const auto& [numBytesToRead, fileOffset, targetBuf] :
       ::ranges::views::zip(numBytesToReadPerRequest, fileOffsetPerRequest,
                            targetBufferPerRequest)) {
    // A read only uses the registered pool if it fits into one pool buffer.
    // Vocabulary words have no fixed upper length, and a fixed-buffer read
    // whose length exceeds the registered buffer is rejected by the kernel
    // with `-EFAULT`, so anything larger reads straight into the caller's
    // buffer instead. The pool is also empty when buffer registration failed
    // at construction time.
    const bool usePool = !registeredBufferPool_.empty() &&
                         numBytesToRead <= registeredBufferSize_;

    // The ring has limited capacity. If it is exhausted, flush the prepared
    // SQEs and drain completions until a slot is available again. The buffer
    // pool needs no separate check: it holds exactly `ringSize_` buffers, one
    // per ring slot, and a buffer can only be held by an in-flight read, so
    // the pool can only be exhausted while the ring is full. (When `usePool`
    // is false the read needs no pool buffer at all.)
    auto mustWait = [this]() { return numInFlightReadRequests_ >= ringSize_; };
    if (mustWait()) {
      io_uring_submit(&ring_);
      while (mustWait()) {
        drainOneCqe();
      }
    }

    // Claim a free registered buffer (if any) before the SQE, so that a
    // `drainOneCqe` inside `allocatePoolBuffer` can never leave a claimed but
    // unfilled SQE behind for the next `io_uring_submit` to pick up.
    const size_t poolIdx = usePool ? allocatePoolBuffer() : noPoolBuffer;
    io_uring_sqe* sqe = io_uring_get_sqe(&ring_);
    AD_CORRECTNESS_CHECK(sqe != nullptr);

    if (usePool) {
      // Record the read's parameters in the SQE using a fixed-buffer read:
      // The buffer is selected via `poolIdx`; `addr` must point into that
      // registered buffer (here its base, so the read starts at offset 0).
      io_uring_prep_read_fixed(
          sqe, fd,
          registeredBufferPool_.data() + poolIdx * registeredBufferSize_,
          static_cast<unsigned>(numBytesToRead), static_cast<__u64>(fileOffset),
          poolIdx);
    } else {
      io_uring_prep_read(sqe, fd, targetBuf,
                         static_cast<unsigned>(numBytesToRead),
                         static_cast<__u64>(fileOffset));
    }

    // Tag the SQE with a unique request id and record its metadata.
    const uint64_t requestId = nextRequestIdToAssign_++;
    inFlightReadsByRequestId_[requestId] =
        InFlightRead{handle, numBytesToRead, poolIdx, targetBuf};
    io_uring_sqe_set_data64(sqe, requestId);
    numInFlightReadRequests_++;
  }
  // Flush the remaining prepared SQEs to the kernel (the loop above only
  // submits when the submission queue or buffer pool is exhausted, so the
  // last group of SQEs has not yet been submitted).
  io_uring_submit(&ring_);
}

//______________________________________________________________________________
void IoUringPolicy::wait(BatchHandle handle) {
  // Drain completions until this batch is gone. `drainOneCqe` erases a batch as
  // soon as its last read completes, so a present entry always still has
  // outstanding reads.
  while (numInFlightReadRequestsPerBatch_.find(handle) !=
         numInFlightReadRequestsPerBatch_.end()) {
    drainOneCqe();
  }
}

//______________________________________________________________________________
void ad_utility::IoUringPolicy::drainOneCqe() {
  // Block until at least one completion queue entry (CQE) is available.
  io_uring_cqe* cqe = nullptr;
  int ret = io_uring_wait_cqe(&ring_, &cqe);
  if (ret < 0) {
    AD_THROW("io_uring_wait_cqe failed in IoUringPolicy");
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
  const InFlightRead inFlightRead = reqIt->second;
  inFlightReadsByRequestId_.erase(reqIt);

  // Reads that were too large for a pool buffer went directly into the
  // caller's buffer and hold no pool buffer to return.
  const bool usedPool = inFlightRead.poolBufferIndex != noPoolBuffer;

  // `cqe->res` < 0 is `-errno`.
  if (numBytesRead < 0) {
    if (usedPool) {
      freePoolBuffer(inFlightRead.poolBufferIndex);
    }
    AD_THROW("I/O error in IoUringPolicy read operation");
  }
  // A result smaller than requested (a partial read, or 0 at end of file) means
  // we read fewer bytes than expected, which we treat as an error.
  if (static_cast<size_t>(numBytesRead) != inFlightRead.expectedNumBytes) {
    if (usedPool) {
      freePoolBuffer(inFlightRead.poolBufferIndex);
    }
    AD_THROW("read fewer bytes than requested in IoUringPolicy");
  }

  if (usedPool) {
    // Copy the data from the registered buffer into the caller's target
    // buffer, then return the registered buffer to the free pool. This
    // user-space memcpy is the cost of the bounce-buffer design; it is paid in
    // addition to the page-cache-to-user copy that a buffered read performs
    // anyway, and is the reason this policy has to be measured rather than
    // assumed to be faster.
    std::memcpy(inFlightRead.targetBuffer,
                registeredBufferPool_.data() +
                    inFlightRead.poolBufferIndex * registeredBufferSize_,
                inFlightRead.expectedNumBytes);
    freePoolBuffer(inFlightRead.poolBufferIndex);
  }

  // Attribute the completion to its batch and decrement that batch's in-flight
  // count, erasing the batch once its last read completes. The entry must still
  // be present here: the read we are processing belongs to this batch and was
  // outstanding, so the batch's count was at least one and it had not yet been
  // erased.
  auto it = numInFlightReadRequestsPerBatch_.find(inFlightRead.batchHandle);
  AD_CORRECTNESS_CHECK(it != numInFlightReadRequestsPerBatch_.end());
  if (--it->second == 0) {
    numInFlightReadRequestsPerBatch_.erase(it);
  }
}

#endif  // QLEVER_HAS_IO_URING

}  // namespace ad_utility
