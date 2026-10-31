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

// `preadv2` with `RWF_NOWAIT` exists on Linux only. Emscripten's headers
// define `RWF_NOWAIT`, but its libc has no `preadv2`; there, and wherever
// `RWF_NOWAIT` is missing, the fast path is compiled out.
#if defined(__linux__) && defined(RWF_NOWAIT) && !defined(__EMSCRIPTEN__)
#define QL_PAGE_CACHE_FAST_PATH
#endif

namespace {
#ifdef QL_PAGE_CACHE_FAST_PATH
// Cleared once a `preadv2(RWF_NOWAIT)` fails with `EOPNOTSUPP` or `ENOSYS`.
// The flag is a best-effort hint without associated data; the default
// sequential consistency (Sonar cpp:S8417) keeps the publication of the
// disabled state obvious.
std::atomic<bool> pageCacheFastPathSupported{true};
#endif
}  // namespace

namespace detail {
//______________________________________________________________________________
int64_t systemPageCacheRead(int fd, const ::iovec* iov, int iovcnt,
                            int64_t offset) {
#ifdef QL_PAGE_CACHE_FAST_PATH
  return preadv2(fd, iov, iovcnt, static_cast<off_t>(offset), RWF_NOWAIT);
#else
  (void)fd;
  (void)iov;
  (void)iovcnt;
  (void)offset;
  errno = EOPNOTSUPP;
  return -1;
#endif
}

//______________________________________________________________________________
PageCacheRead& pageCacheRead() {
  static PageCacheRead function = &systemPageCacheRead;
  return function;
}

//______________________________________________________________________________
void resetPageCacheFastPathSupport() {
#ifdef QL_PAGE_CACHE_FAST_PATH
  pageCacheFastPathSupported.store(true);
#endif
}
}  // namespace detail

//______________________________________________________________________________
bool pageCacheFastPathIsSupported() {
#ifdef QL_PAGE_CACHE_FAST_PATH
  return pageCacheFastPathSupported.load();
#else
  return false;
#endif
}

#ifdef QL_PAGE_CACHE_FAST_PATH
namespace {
// The elements of the range of reads in `readPageCacheHits`: the position of a
// read in its batch, its number of bytes, its file offset and its buffer.
constexpr auto position = [](const auto& read) { return std::get<0>(read); };
constexpr auto toIovec = [](const auto& read) {
  return ::iovec{std::get<3>(read), std::get<1>(read)};
};
// True if the file range of `next` starts where the one of `previous` ends.
constexpr auto areAdjacent = [](const auto& previous, const auto& next) {
  return std::get<2>(previous) + std::get<1>(previous) == std::get<2>(next);
};

// Log once that the kernel or file system rejects `RWF_NOWAIT` (`EOPNOTSUPP`)
// or that `preadv2` does not exist (`ENOSYS`, Linux < 4.6 or a seccomp filter),
// and switch the fast path off for the rest of the process.
void disableIfNotSupported(int64_t numBytesRead) {
  if (numBytesRead < 0 && (errno == EOPNOTSUPP || errno == ENOSYS) &&
      pageCacheFastPathSupported.exchange(false)) {
    AD_LOG_WARN << "preadv2 with RWF_NOWAIT is not supported for the "
                   "vocabulary files; reading them without the "
                   "page-cache fast path"
                << std::endl;
  }
}

// Read a `run` of adjacent reads with one `preadv2(RWF_NOWAIT)` call and return
// how many of its reads, from the front, were read completely. `EAGAIN`, a
// short read and any other error leave the remaining reads unserved.
template <typename Run>
size_t serveRun(int fd, Run run) {
  const auto iovecs = ::ranges::to<std::vector<::iovec>>(
      run | ::ranges::views::transform(toIovec));
  const int64_t numBytesRead = detail::pageCacheRead()(
      fd, iovecs.data(), static_cast<int>(iovecs.size()),
      static_cast<int64_t>(std::get<2>(*::ranges::begin(run))));
  disableIfNotSupported(numBytesRead);
  // The end of each read within the run; a read is served if its end was read.
  auto readEnds = iovecs | ::ranges::views::transform(&::iovec::iov_len) |
                  ::ranges::views::partial_sum;
  return static_cast<size_t>(::ranges::count_if(readEnds, [&](size_t end) {
    return numBytesRead >= 0 && end <= static_cast<size_t>(numBytesRead);
  }));
}
}  // namespace
#endif

//______________________________________________________________________________
std::vector<size_t> readPageCacheHits(int fd, ql::span<const size_t> numBytes,
                                      ql::span<const uint64_t> offsets,
                                      ql::span<char*> buffers) {
  AD_CONTRACT_CHECK(offsets.size() == numBytes.size() &&
                    buffers.size() == numBytes.size());
  auto positions = ::ranges::views::iota(size_t{0}, numBytes.size());
#ifdef QL_PAGE_CACHE_FAST_PATH
  auto reads = ::ranges::views::zip(positions, numBytes, offsets, buffers);
  std::vector<size_t> notServed;
  for (auto adjacentReads : reads | ::ranges::views::chunk_by(areAdjacent)) {
    // One `preadv2` call takes at most `IOV_MAX` buffers.
    for (auto run :
         adjacentReads | ::ranges::views::chunk(static_cast<size_t>(IOV_MAX))) {
      const size_t numServed =
          pageCacheFastPathSupported.load() ? serveRun(fd, run) : 0;
      ::ranges::push_back(notServed, run | ::ranges::views::drop(numServed) |
                                         ::ranges::views::transform(position));
    }
  }
  return notServed;
#else
  (void)fd;
  return ::ranges::to<std::vector<size_t>>(positions);
#endif
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
  if (numOutstandingReadRequests_ > 0) {
    AD_LOG_WARN << "IoUringPolicy destroyed with "
                << numOutstandingReadRequests_
                << " read request(s) still outstanding; all batches should be "
                   "`wait()`ed before destroying the policy. Draining them now "
                   "so the kernel stops writing into the target buffers.\n";
  }
  // Reap the outstanding completions before tearing down the ring, so the
  // kernel is no longer writing into any target buffer once we return. We
  // deliberately do not call `drainAtLeast` here: it throws on I/O errors, and
  // a destructor must not throw. We also stop if `io_uring_wait_cqe` fails, to
  // avoid spinning forever (it would not decrement the outstanding count).
  while (numOutstandingReadRequests_ > 0) {
    io_uring_cqe* cqe = nullptr;
    if (io_uring_wait_cqe(&ring_, &cqe) < 0) {
      break;
    }
    io_uring_cqe_seen(&ring_, cqe);
    --numOutstandingReadRequests_;
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
  numOutstandingReadRequestsPerBatch_[handle] = numReadRequestsToPerform;

  for (const auto& [numBytesToRead, fileOffset, targetBuf] :
       ::ranges::views::zip(numBytesToReadPerRequest, fileOffsetPerRequest,
                            targetBufferPerRequest)) {
    // The ring has no free slot, so make room: submit what we have prepared so
    // far and block until enough completions have been drained.
    if (numOutstandingReadRequests_ >= ringSize_) {
      // Flush the SQEs prepared so far to the kernel so the kernel can start
      // servicing them. Their completions will free up submission slots.
      // Wait for a wave of completions instead of one, and reap every ready
      // CQE, so a large batch refills the ring in waves rather than one SQE
      // per reaped CQE.
      io_uring_submit(&ring_);
      while (numOutstandingReadRequests_ >= ringSize_) {
        drainAtLeast(static_cast<unsigned>(
            std::min<size_t>(REAP_WAVE, numOutstandingReadRequests_)));
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
    outstandingReadsByRequestId_[requestId] =
        OutstandingRead{handle, numBytesToRead};
    io_uring_sqe_set_data64(sqe, requestId);
    numOutstandingReadRequests_++;
  }
  // Flush the remaining prepared SQEs to the kernel (the loop above only
  // submits when the submission queue is full, so the last group of SQEs has
  // not yet been submitted).
  io_uring_submit(&ring_);
}

//______________________________________________________________________________
void IoUringPolicy::wait(BatchHandle handle) {
  // Drain completions until this batch is gone. `processCqe` erases a batch as
  // soon as its last read completes, so a present entry always still has
  // outstanding reads. Waiting for up to `REAP_WAVE` CQEs never waits longer
  // than this batch needs: it cannot finish before its own remaining reads
  // complete, and any CQE (also of other batches) counts towards the wave.
  for (auto it = numOutstandingReadRequestsPerBatch_.find(handle);
       it != numOutstandingReadRequestsPerBatch_.end();
       it = numOutstandingReadRequestsPerBatch_.find(handle)) {
    drainAtLeast(
        static_cast<unsigned>(std::min<size_t>(REAP_WAVE, it->second)));
  }
  // Throw only now that every read of the batch is reaped, so the kernel no
  // longer writes into its buffers.
  if (auto failed = failedBatches_.find(handle);
      failed != failedBatches_.end()) {
    const char* errorMessage = failed->second;
    failedBatches_.erase(failed);
    AD_THROW(errorMessage);
  }
}

//______________________________________________________________________________
void IoUringPolicy::drainAtLeast(unsigned minComplete) {
  AD_CORRECTNESS_CHECK(minComplete > 0);
  AD_CORRECTNESS_CHECK(minComplete <= numOutstandingReadRequests_);

  // Block until at least `minComplete` completion queue entries (CQEs) are
  // ready. This typically costs one `io_uring_enter` for the whole wave (and
  // one more after each `EINTR`).
  io_uring_cqe* cqe = nullptr;
  int ret = 0;
  do {
    ret = io_uring_wait_cqes(&ring_, &cqe, minComplete, nullptr, nullptr);
  } while (ret == -EINTR);
  if (ret < 0) {
    AD_THROW(absl::StrCat("io_uring_wait_cqes failed in IoUringPolicy: ",
                          std::strerror(-ret)));
  }

  // Reap every ready CQE in chunks. `io_uring_peek_batch_cqe` does not block;
  // `io_uring_cq_advance` releases a whole chunk with one CQ-head update
  // instead of one `io_uring_cqe_seen` per CQE. Every CQE of the wave is
  // applied to the bookkeeping, so the outstanding counts stay consistent and
  // no CQE is processed twice.
  std::array<io_uring_cqe*, 64> cqes{};
  while (true) {
    const unsigned n = io_uring_peek_batch_cqe(
        &ring_, cqes.data(), static_cast<unsigned>(cqes.size()));
    if (n == 0) {
      break;
    }
    for (const io_uring_cqe* cqe :
         ql::span<io_uring_cqe* const>(cqes.data(), n)) {
      // Recover the id via the 64-bit `user_data` field, see `addBatch`.
      processCqe(cqe->res, io_uring_cqe_get_data64(cqe));
    }
    io_uring_cq_advance(&ring_, n);
  }
}

//______________________________________________________________________________
void IoUringPolicy::processCqe(int numBytesRead, uint64_t requestId) {
  --numOutstandingReadRequests_;

  // Every reaped CQE corresponds to exactly one outstanding read whose id we
  // inserted in `addBatch`, so the entry must be present.
  auto reqIt = outstandingReadsByRequestId_.find(requestId);
  AD_CORRECTNESS_CHECK(reqIt != outstandingReadsByRequestId_.end());
  const OutstandingRead outstandingRead = reqIt->second;
  outstandingReadsByRequestId_.erase(reqIt);

  // Attribute the completion to its batch and decrement that batch's
  // outstanding count, erasing the batch once its last read is reaped. This
  // happens also for a failed read, so a batch whose read failed does not stay
  // outstanding.
  auto it =
      numOutstandingReadRequestsPerBatch_.find(outstandingRead.batchHandle);
  AD_CORRECTNESS_CHECK(it != numOutstandingReadRequestsPerBatch_.end());
  if (--it->second == 0) {
    numOutstandingReadRequestsPerBatch_.erase(it);
  }

  // `cqe->res` < 0 is `-errno`. A result smaller than requested (a partial
  // read, or 0 at end of file) means we read fewer bytes than expected, which
  // we treat as an error.
  const char* errorMessage = nullptr;
  if (numBytesRead < 0) {
    errorMessage = "I/O error in IoUringPolicy read operation";
  } else if (static_cast<size_t>(numBytesRead) !=
             outstandingRead.expectedNumBytes) {
    errorMessage = "read fewer bytes than requested in IoUringPolicy";
  }
  if (errorMessage != nullptr) {
    // Keep the first failure of the batch; `wait()` of that batch throws it.
    failedBatches_.try_emplace(outstandingRead.batchHandle, errorMessage);
  }
}

#endif  // QLEVER_HAS_IO_URING

}  // namespace ad_utility
