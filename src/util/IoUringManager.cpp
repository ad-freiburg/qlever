// Copyright 2026, The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "util/IoUringManager.h"

#include <absl/strings/str_cat.h>
#include <sys/uio.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <cerrno>
#include <climits>
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

namespace {
#ifdef RWF_NOWAIT
// Cleared once a `preadv2(RWF_NOWAIT)` fails with `EOPNOTSUPP`.
std::atomic<bool> pageCacheFastPathSupported{true};
#endif
}  // namespace

//______________________________________________________________________________
bool pageCacheFastPathIsSupported() {
#ifdef RWF_NOWAIT
  return pageCacheFastPathSupported.load(std::memory_order_relaxed);
#else
  return false;
#endif
}

//______________________________________________________________________________
std::vector<size_t> readPageCacheHits(int fd, ql::span<const size_t> numBytes,
                                      ql::span<const uint64_t> offsets,
                                      ql::span<char*> buffers) {
  AD_CONTRACT_CHECK(offsets.size() == numBytes.size() &&
                    buffers.size() == numBytes.size());
  std::vector<size_t> notServed;
#ifdef RWF_NOWAIT
  const size_t numReads = numBytes.size();
  std::vector<iovec> iovecs;
  size_t runBegin = 0;
  while (runBegin < numReads) {
    // The run `[runBegin, runEnd)` holds exactly adjacent file ranges, at most
    // `IOV_MAX` of them (the limit of one `preadv2` call).
    size_t runEnd = runBegin + 1;
    while (runEnd < numReads &&
           runEnd - runBegin < static_cast<size_t>(IOV_MAX) &&
           offsets[runEnd - 1] + numBytes[runEnd - 1] == offsets[runEnd]) {
      ++runEnd;
    }
    if (!pageCacheFastPathSupported.load(std::memory_order_relaxed)) {
      for (size_t i = runBegin; i < numReads; ++i) {
        notServed.push_back(i);
      }
      return notServed;
    }
    iovecs.clear();
    for (size_t i = runBegin; i < runEnd; ++i) {
      iovecs.push_back(iovec{buffers[i], numBytes[i]});
    }
    // See https://man7.org/linux/man-pages/man2/preadv2.2.html: with
    // `RWF_NOWAIT`, the call fails with `EAGAIN` (or returns fewer bytes)
    // instead of waiting for the storage device when data is not cached.
    const ssize_t numBytesRead =
        preadv2(fd, iovecs.data(), static_cast<int>(iovecs.size()),
                static_cast<off_t>(offsets[runBegin]), RWF_NOWAIT);
    if (numBytesRead < 0 && errno == EOPNOTSUPP) {
      if (pageCacheFastPathSupported.exchange(false)) {
        AD_LOG_WARN << "preadv2 with RWF_NOWAIT is not supported for the "
                       "vocabulary files; reading them without the "
                       "page-cache fast path"
                    << std::endl;
      }
    }
    // Reads that were read completely are served, the others (from the first
    // incomplete one on) are left to the caller.
    size_t remaining = numBytesRead < 0 ? 0 : static_cast<size_t>(numBytesRead);
    size_t i = runBegin;
    for (; i < runEnd && remaining >= numBytes[i]; ++i) {
      remaining -= numBytes[i];
    }
    for (; i < runEnd; ++i) {
      notServed.push_back(i);
    }
    runBegin = runEnd;
  }
#else
  (void)fd;
  for (size_t i = 0; i < numBytes.size(); ++i) {
    notServed.push_back(i);
  }
#endif
  return notServed;
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
IoUringPolicy::IoUringPolicy(unsigned ringSize) : ringSize_(ringSize) {
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
}

//______________________________________________________________________________
IoUringPolicy::~IoUringPolicy() {
  if (numInFlightReadRequests_ > 0) {
    AD_LOG_WARN << "IoUringPolicy destroyed with " << numInFlightReadRequests_
                << " read request(s) still in flight; all batches should be "
                   "`wait()`ed before destroying the policy. Draining them now "
                   "so the kernel stops writing into the target buffers.\n";
  }
  // Reap the outstanding completions before tearing down the ring, so the
  // kernel is no longer writing into any target buffer once we return. We
  // deliberately do not call `drainAtLeast` here: it throws on I/O errors, and
  // a destructor must not throw. We also stop if `io_uring_wait_cqe` fails, to
  // avoid spinning forever (it would not decrement the in-flight count).
  //
  // A failed `io_uring_submit` (see `submitOrThrow`) can leave prepared SQEs
  // that the kernel has not consumed. They produce no completion, so retry
  // submitting them once and wait only for the reads the kernel has actually
  // received; the rest are discarded by `io_uring_queue_exit`.
  if (io_uring_sq_ready(&ring_) > 0) {
    io_uring_submit(&ring_);
  }
  const size_t numNeverSubmitted = io_uring_sq_ready(&ring_);
  while (numInFlightReadRequests_ > numNeverSubmitted) {
    io_uring_cqe* cqe = nullptr;
    if (io_uring_wait_cqe(&ring_, &cqe) < 0) {
      break;
    }
    io_uring_cqe_seen(&ring_, cqe);
    --numInFlightReadRequests_;
  }
  io_uring_queue_exit(&ring_);
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

  // Reads prepared since the last `io_uring_submit` and reads of this batch
  // still waiting to be prepared (including the current one). Only used when
  // adaptive batch sizing is enabled.
  size_t numPreparedSinceSubmit = 0;
  size_t numRemaining = numReadRequestsToPerform;

  for (const auto& [numBytesToRead, fileOffset, targetBuf] :
       ::ranges::views::zip(numBytesToReadPerRequest, fileOffsetPerRequest,
                            targetBufferPerRequest)) {
    // The ring has no free slot, so make room: submit what we have prepared so
    // far and block until enough completions have been drained. This hard
    // safety bound applies with and without the controller.
    if (numInFlightReadRequests_ >= ringSize_) {
      // Flush the SQEs prepared so far to the kernel so the kernel can start
      // servicing them. Their completions will free up submission slots.
      // Wait for a wave of completions instead of one, and reap every ready
      // CQE, so a large batch refills the ring in waves rather than one SQE
      // per reaped CQE.
      submitOrThrow();
      while (numInFlightReadRequests_ >= ringSize_) {
        drainAtLeast(static_cast<unsigned>(
            std::min<size_t>(REAP_WAVE, numInFlightReadRequests_)));
      }
      numPreparedSinceSubmit = 0;
    } else if (adaptiveBatchController_.has_value()) {
      const AdaptiveBatchController& controller = *adaptiveBatchController_;
      // Clamp the deferred group to the configured maximum so a very large
      // batch still submits incrementally and never exceeds the ring.
      // Otherwise ask the controller once the minimum group size is reached:
      // flush early when little work remains, defer while many I/Os are
      // already in flight to increase amortization. `outstanding` spans all
      // batches by design (every submitted read occupies device queue
      // depth), but excludes the prepared reads of the current group, which
      // the kernel has not seen yet. `pending` is this batch's remainder.
      if (numPreparedSinceSubmit >= controller.maxBatchSize_) {
        submitOrThrow();
        numPreparedSinceSubmit = 0;
      } else if (numPreparedSinceSubmit >= controller.minBatchSize_ &&
                 controller.shouldFlush(
                     numInFlightReadRequests_ - numPreparedSinceSubmit,
                     numRemaining)) {
        submitOrThrow();
        numPreparedSinceSubmit = 0;
      }
    }

    // Claim the next free SQE. The check above guarantees a slot is available,
    // so `io_uring_get_sqe` must not return `nullptr` here.
    io_uring_sqe* sqe = io_uring_get_sqe(&ring_);
    AD_CORRECTNESS_CHECK(sqe != nullptr);

    // Record the read's parameters in the SQE (this only sets the SQE's fields;
    // the request is not handed to the kernel until a later `io_uring_submit`).
    io_uring_prep_read(sqe, fd, targetBuf,
                       static_cast<unsigned>(numBytesToRead),
                       static_cast<__u64>(fileOffset));

    // Tag the SQE with a unique request id and record its metadata (the batch
    // it belongs to and how many bytes it should read). io_uring copies the
    // request id (the SQE's `user_data`) verbatim into the matching completion,
    // so `processCqe` can recover it.
    const uint64_t requestId = nextRequestIdToAssign_++;
    inFlightReadsByRequestId_[requestId] = InFlightRead{handle, numBytesToRead};
    io_uring_sqe_set_data64(sqe, requestId);
    numInFlightReadRequests_++;
    numPreparedSinceSubmit++;
    numRemaining--;
  }
  // Flush the remaining prepared SQEs to the kernel (the loop above submits
  // only when the ring is full or, with an adaptive controller, when a group
  // is flushed early, so the last group of SQEs has not yet been submitted).
  submitOrThrow();
}

//______________________________________________________________________________
void IoUringPolicy::wait(BatchHandle handle) {
  // Drain completions until this batch is gone. `processCqe` erases a batch as
  // soon as its last read completes, so a present entry always still has
  // outstanding reads. Waiting for up to `REAP_WAVE` CQEs never waits longer
  // than this batch needs: it cannot finish before its own remaining reads
  // complete, and any CQE (also of other batches) counts towards the wave.
  for (auto it = numInFlightReadRequestsPerBatch_.find(handle);
       it != numInFlightReadRequestsPerBatch_.end();
       it = numInFlightReadRequestsPerBatch_.find(handle)) {
    drainAtLeast(
        static_cast<unsigned>(std::min<size_t>(REAP_WAVE, it->second)));
  }
}

//______________________________________________________________________________
void IoUringPolicy::submitOrThrow() {
  // `io_uring_submit` returns the number of submitted SQEs or `-errno`. On
  // failure the prepared SQEs stay in the submission queue; `drainAtLeast`
  // and the destructor submit them again before waiting for completions.
  const int ret = io_uring_submit(&ring_);
  if (ret < 0) {
    AD_THROW(absl::StrCat("io_uring_submit failed in IoUringPolicy: ",
                          std::strerror(-ret)));
  }
}

//______________________________________________________________________________
void IoUringPolicy::drainAtLeast(unsigned minComplete) {
  AD_CORRECTNESS_CHECK(minComplete > 0);
  AD_CORRECTNESS_CHECK(minComplete <= numInFlightReadRequests_);
  // Submit SQEs that an earlier failed or partial `io_uring_submit` left in
  // the submission queue. Without this, waiting for their completions would
  // block forever, because the kernel has never seen them.
  if (io_uring_sq_ready(&ring_) > 0) {
    submitOrThrow();
  }
  // Only reads the kernel has received can complete, so never wait for more
  // CQEs than that.
  const size_t numSubmitted =
      numInFlightReadRequests_ - io_uring_sq_ready(&ring_);
  AD_CORRECTNESS_CHECK(numSubmitted > 0);
  const unsigned numToWaitFor =
      static_cast<unsigned>(std::min<size_t>(minComplete, numSubmitted));

  // Block until at least `numToWaitFor` completion queue entries (CQEs) are
  // ready. This costs at most one `io_uring_enter` for the whole wave.
  io_uring_cqe* cqe = nullptr;
  int ret = 0;
  do {
    ret = io_uring_wait_cqes(&ring_, &cqe, numToWaitFor, nullptr, nullptr);
  } while (ret == -EINTR);
  if (ret < 0) {
    AD_THROW(absl::StrCat("io_uring_wait_cqes failed in IoUringPolicy: ",
                          std::strerror(-ret)));
  }

  // Reap every ready CQE in chunks. `io_uring_peek_batch_cqe` does not block;
  // `io_uring_cq_advance` releases a whole chunk with one CQ-head update
  // instead of one `io_uring_cqe_seen` per CQE. Every CQE of the wave is
  // applied to the bookkeeping before any error is thrown, so the in-flight
  // counts stay consistent and no CQE is processed twice.
  const char* firstErrorMessage = nullptr;
  std::array<io_uring_cqe*, 64> cqes{};
  while (true) {
    const unsigned n = io_uring_peek_batch_cqe(
        &ring_, cqes.data(), static_cast<unsigned>(cqes.size()));
    if (n == 0) {
      break;
    }
    for (unsigned i = 0; i < n; ++i) {
      // Recover the id via the 64-bit `user_data` field, see `addBatch`.
      const char* errorMessage =
          processCqe(cqes[i]->res, io_uring_cqe_get_data64(cqes[i]));
      if (firstErrorMessage == nullptr) {
        firstErrorMessage = errorMessage;
      }
    }
    io_uring_cq_advance(&ring_, n);
  }
  if (firstErrorMessage != nullptr) {
    AD_THROW(firstErrorMessage);
  }
}

//______________________________________________________________________________
const char* IoUringPolicy::processCqe(int numBytesRead, uint64_t requestId) {
  --numInFlightReadRequests_;

  // Every reaped CQE corresponds to exactly one in-flight read whose id we
  // inserted in `addBatch`, so the entry must be present.
  auto reqIt = inFlightReadsByRequestId_.find(requestId);
  AD_CORRECTNESS_CHECK(reqIt != inFlightReadsByRequestId_.end());
  const InFlightRead inFlightRead = reqIt->second;
  inFlightReadsByRequestId_.erase(reqIt);

  // Attribute the completion to its batch and decrement that batch's in-flight
  // count, erasing the batch once its last read completes. This happens also
  // for a failed read, so a batch whose read failed does not stay in flight.
  auto it = numInFlightReadRequestsPerBatch_.find(inFlightRead.batchHandle);
  AD_CORRECTNESS_CHECK(it != numInFlightReadRequestsPerBatch_.end());
  if (--it->second == 0) {
    numInFlightReadRequestsPerBatch_.erase(it);
  }

  // `cqe->res` < 0 is `-errno`.
  if (numBytesRead < 0) {
    return "I/O error in IoUringPolicy read operation";
  }
  // A result smaller than requested (a partial read, or 0 at end of file) means
  // we read fewer bytes than expected, which we treat as an error.
  if (static_cast<size_t>(numBytesRead) != inFlightRead.expectedNumBytes) {
    return "read fewer bytes than requested in IoUringPolicy";
  }
  return nullptr;
}

#endif  // QLEVER_HAS_IO_URING

}  // namespace ad_utility
