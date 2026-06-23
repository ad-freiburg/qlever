// Copyright 2025, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach <johannes.kalmbach@gmail.com>

#include "util/IoUringManager.h"

#include <unistd.h>

#include <stdexcept>

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

namespace {
// Prepares an IO read request. The submission queue entry `sqe` is set up to
// use  the file descriptor `fd` to start reading `nbytes` into the buffer `buf`
// at the specified `offset`.
// See https://man7.org/linux/man-pages/man3/io_uring_prep_read.3.html for more
// details.
void prepareSQEFOrReadRequest(io_uring_sqe* sqe, int fd, void* buffer,
                              size_t numBytes, uint64_t fileOffset) {
  io_uring_prep_read(sqe, fd, buffer, static_cast<unsigned>(numBytes),
                     static_cast<__u64>(fileOffset));
}

// Stores a 64-bit `data` value with the submission queue entry `sqe`.
// After the caller has requested a submission queue entry (SQE) with
// `io_uring_get_sqe`, they can associate a data pointer or value with the SQE.
// Once the completion arrives, the function `io_uring_cqe_get_data` or
// `io_uring_cqe_get_data64` can be called to retrieve the data pointer or value
// associated with the submitted request.
// See https://man7.org/linux/man-pages/man3/io_uring_sqe_set_data64.3.html for
// more details.
void setUserData(io_uring_sqe* sqe, void* user_data) {
  io_uring_sqe_set_data(sqe, user_data);
}

// The `io_uring_queue_init` system function executes the `io_uring_setup`
// system call to initialize the submission and completion queues in the kernel
// with at least `entries` entries in the submission queue and then maps the
// resulting file descriptor to memory shared between the application and the
// kernel. On success `io_uring_queue_init` returns 0 and `ring` will point to
// the shared memory containing the io_uring queues. On failure -errno is
// returned. `flags` will be passed through to the `io_uring_setup` syscall
// (see https://man7.org/linux/man-pages/man2/io_uring_setup.2.html).
// On success, the resources held by `ring` should be released via a
// corresponding call to `io_uring_queue_exit`.
// See https://man7.org/linux/man-pages/man3/io_uring_queue_init.3.html for more
// details.
int setupSubmissionAndCompletionQueues(unsigned entries, struct io_uring* ring,
                                       unsigned flags) {
  return io_uring_queue_init(entries, ring, flags);
}

// Submits the next events to the submission queue belonging to the `ring`.
// After the caller retrieves a submission queue entry (SQE) with
// `io_uring_get_sqe` and prepares the SQE using one of the provided helpers,
// it can be submitted with `io_uring_submit`.
// See https://man7.org/linux/man-pages/man3/io_uring_submit.3.html for details.
int submit(struct io_uring* ring) { return io_uring_submit(ring); }
// See https://man7.org/linux/man-pages/man3/io_uring_wait_cqe.3.html
int waitForCompletionEvent(struct io_uring* ring,
                           struct io_uring_cqe** cqe_ptr) {
  return io_uring_wait_cqe(ring, cqe_ptr);
}

// Marks the IO completion `cqe` belonging to the `ring` param as consumed.
// After the caller has submitted a request with `io_uring_submit`, the
// application can retrieve the completion with `io_uring_wait_cqe`,
// `io_uring_peek_cqe`, or any of the other CQE retrieval helpers, and mark it
// as consumed with `io_uring_cqe_seen`. Completions must be marked as
// completed so their slot can be reused.
// See https://man7.org/linux/man-pages/man3/io_uring_cqe_seen.3.html for
// details.
void markAsConsumed(struct io_uring* ring, struct io_uring_cqe* cqe) {
  io_uring_cqe_seen(ring, cqe);
}

// After the caller has received a completion queue entry (CQE) with
// `io_uring_wait_cqe`, the application can call `io_uring_cqe_get_data` or
// `io_uring_get_data64` function to retrieve the `user_data` value. This
// requires that `user_data` has been set earlier with the function
// `io_uring_sqe_set_data` or `io_uring_sqe_set_data64`.
// See https://man7.org/linux/man-pages/man3/io_uring_cqe_get_data64.3.html for
// more details.
__u64 getUserDataForCompletionEvent(struct io_uring_cqe* cqe) {
  return io_uring_cqe_get_data64(cqe);
}

}  // namespace

//______________________________________________________________________________
IoUringManager::IoUringManager(unsigned ringSize) : ringSize_(ringSize) {
  int ret = setupSubmissionAndCompletionQueues(ringSize_, &ring_, 0);
  if (ret < 0) {
    throw std::runtime_error("io_uring_queue_init failed in IoUringManager");
  }
}

//______________________________________________________________________________
IoUringManager::~IoUringManager() { io_uring_queue_exit(&ring_); }

//______________________________________________________________________________
IoUringManager::BatchHandle IoUringManager::addBatch(
    int fd, ql::span<const size_t> sizes, ql::span<const uint64_t> fileOffsets,
    ql::span<char*> buffers) {
  BatchHandle handle = nextHandle_++;
  const size_t numberOfRequests = sizes.size();

  if (numberOfRequests == 0) {
    return handle;
  }

  remaining_[handle] = numberOfRequests;

  for (size_t i = 0; i < numberOfRequests; ++i) {
    // When the ring is full, submit pending SQEs and drain CQEs to free space.
    if (inFlight_ >= ringSize_) {
      submit(&ring_);
      while (inFlight_ >= ringSize_) {
        drainOneCqe();
      }
    }

    struct io_uring_sqe* sqe = io_uring_get_sqe(&ring_);
    // `io_uring_prep_read` prepares an IO read request. The submission queue
    // entry `sqe` is is setup to
    prepareSQEFOrReadRequest(sqe, fd, buffers[i], sizes[i], fileOffsets[i]);
    setUserData(sqe, &handle);
    inFlight_++;
  }

  submit(&ring_);
  return handle;
}

//______________________________________________________________________________
void IoUringManager::wait(BatchHandle handle) {
  while (remaining_.count(handle) && remaining_[handle] > 0) {
    drainOneCqe();
  }
  remaining_.erase(handle);
}

//______________________________________________________________________________
void IoUringManager::drainOneCqe() {
  struct io_uring_cqe* cqe = nullptr;
  int ret = waitForCompletionEvent(&ring_, &cqe);
  if (ret < 0) {
    throw std::runtime_error("io_uring_wait_cqe failed in IoUringManager");
  }
  if (cqe->res < 0) {
    markAsConsumed(&ring_, cqe);
    throw std::runtime_error("I/O error in IoUringManager read operation");
  }
  BatchHandle batchId = getUserDataForCompletionEvent(cqe);
  markAsConsumed(&ring_, cqe);
  inFlight_--;

  if (remaining_.count(batchId)) {
    remaining_[batchId]--;
  }
}

#endif  // QLEVER_HAS_IO_URING

}  // namespace ad_utility
