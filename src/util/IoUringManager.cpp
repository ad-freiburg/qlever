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
#include <chrono>
#include <climits>
#include <cstring>
#include <exception>
#include <iostream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <utility>

#include "util/Exception.h"
#include "util/ExceptionHandling.h"
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
constexpr auto numBytesOf = [](const auto& read) { return std::get<1>(read); };
constexpr auto fileOffsetOf = [](const auto& read) {
  return std::get<2>(read);
};
constexpr auto toIovec = [](const auto& read) {
  return ::iovec{std::get<3>(read), numBytesOf(read)};
};
// True if the file range of `next` starts where that of `previous` ends.
constexpr auto areAdjacent = [](const auto& previous, const auto& next) {
  return fileOffsetOf(previous) + numBytesOf(previous) == fileOffsetOf(next);
};

//______________________________________________________________________________
// Log once that the kernel or file system rejects `RWF_NOWAIT` (`EOPNOTSUPP`)
// or that `preadv2` does not exist (`ENOSYS`, Linux < 4.6 or a seccomp filter),
// and switch the fast path off for the rest of the process.
void disableIfNotSupported(int64_t numBytesRead) {
  if (numBytesRead < 0 && (errno == EOPNOTSUPP || errno == ENOSYS) &&
      pageCacheFastPathSupported.exchange(false)) {
    AD_LOG_WARN << "preadv2 with RWF_NOWAIT is not supported (EOPNOTSUPP or "
                   "ENOSYS); reading without the page-cache fast path "
                   "from now on"
                << std::endl;
  }
}

//______________________________________________________________________________
// Read a `run` of adjacent reads with one `preadv2(RWF_NOWAIT)` call and return
// how many of its reads, from the front, were read completely. `EAGAIN`, a
// short read and any other error leave the remaining reads unserved.
template <typename Run>
size_t serveRun(int fd, Run run) {
  const auto iovecs = ::ranges::to<std::vector<::iovec>>(
      run | ::ranges::views::transform(toIovec));
  const int64_t numBytesRead = detail::pageCacheRead()(
      fd, iovecs.data(), static_cast<int>(iovecs.size()),
      static_cast<int64_t>(fileOffsetOf(*::ranges::begin(run))));
  disableIfNotSupported(numBytesRead);
  if (numBytesRead < 0) {
    return 0;
  }
  // The end of each read within the run; a read is served if its end was read.
  auto readEnds = iovecs | ::ranges::views::transform(&::iovec::iov_len) |
                  ::ranges::views::partial_sum;
  const auto numBytesReadUnsigned = static_cast<size_t>(numBytesRead);
  return static_cast<size_t>(
      ::ranges::count_if(readEnds, [numBytesReadUnsigned](size_t end) {
        return end <= numBytesReadUnsigned;
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
  notServed.reserve(numBytes.size());
  for (auto adjacentReads : reads | ::ranges::views::chunk_by(areAdjacent)) {
    // One `preadv2` call takes at most `IOV_MAX` buffers.
    for (auto run :
         adjacentReads | ::ranges::views::chunk(static_cast<size_t>(IOV_MAX))) {
      const size_t numServed =
          pageCacheFastPathIsSupported() ? serveRun(fd, run) : 0;
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

namespace {
// Log that `operation` failed with `error` and terminate the process. Used
// where the kernel may still hold reads of a batch while the ring cannot be
// used any more: throwing would let the caller free buffers that the kernel
// still writes into.
[[noreturn]] void terminateBecauseRingIsUnusable(std::string_view operation,
                                                 int error) {
  const std::string message = absl::StrCat(
      operation, " failed in IoUringPolicy: ", std::strerror(error),
      ". The kernel may still write into the buffers of submitted reads, so "
      "the process cannot continue safely. Terminating");
  AD_LOG_ERROR << message << std::endl;
  std::cerr << message << std::endl;
  std::terminate();
}

// `IoUringInterface` via liburing.
class LiburingRing final : public IoUringInterface {
 public:
  explicit LiburingRing(unsigned ringSize) {
    // Set up the submission and completion queues, shared between this
    // process and the kernel, with (at least) `ringSize` submission slots.
    // liburing rounds the requested size up to a power of two, so the actual
    // ring may be larger. See
    // https://man7.org/linux/man-pages/man3/io_uring_queue_init.3.html for
    // details.
    if (io_uring_queue_init(ringSize, &ring_, /*flags=*/0) < 0) {
      AD_THROW("io_uring_queue_init failed in IoUringManager");
    }
  }

  // `IoUringPolicy` leaves no read in the ring between batches, only no-ops,
  // so no read can write into a buffer after the ring is torn down.
  ~LiburingRing() override { io_uring_queue_exit(&ring_); }

  LiburingRing(const LiburingRing&) = delete;
  LiburingRing& operator=(const LiburingRing&) = delete;

  bool prepareRead(int fd, char* buffer, size_t numBytes, uint64_t offset,
                   uint64_t requestId) override {
    io_uring_sqe* sqe = io_uring_get_sqe(&ring_);
    if (sqe == nullptr) {
      return false;
    }
    // This only fills in the SQE; the kernel sees it at the next
    // `io_uring_submit`. io_uring copies the SQE's `user_data` verbatim into
    // the matching completion.
    io_uring_prep_read(sqe, fd, buffer, static_cast<unsigned>(numBytes),
                       static_cast<__u64>(offset));
    io_uring_sqe_set_data64(sqe, requestId);
    return true;
  }

  size_t numQueued() const override { return io_uring_sq_ready(&ring_); }

  int submit() override { return io_uring_submit(&ring_); }

  int waitForCompletions(unsigned minComplete) override {
    io_uring_cqe* cqe = nullptr;
    return io_uring_wait_cqes(&ring_, &cqe, minComplete, nullptr, nullptr);
  }

  size_t popCompletions(ql::span<Completion> completions) override {
    // `io_uring_peek_batch_cqe` does not block; `io_uring_cq_advance` releases
    // all of them with one CQ-head update. They are copied first, so nothing
    // that happens with the results can leave a released CQE unprocessed or a
    // processed one in the queue.
    std::array<io_uring_cqe*, 64> cqes{};
    const unsigned numReady = io_uring_peek_batch_cqe(
        &ring_, cqes.data(),
        static_cast<unsigned>(std::min(cqes.size(), completions.size())));
    for (unsigned i = 0; i < numReady; ++i) {
      completions[i] =
          Completion{io_uring_cqe_get_data64(cqes[i]), cqes[i]->res};
    }
    io_uring_cq_advance(&ring_, numReady);
    return numReady;
  }

  std::vector<uint64_t> cancelQueued(uint64_t noOpRequestId) override {
    // Without `IORING_SETUP_SQPOLL`, the kernel reads SQEs only inside our own
    // `io_uring_enter` calls, so the SQEs it has not taken can safely be
    // rewritten between two calls.
    AD_CORRECTNESS_CHECK((ring_.flags & IORING_SETUP_SQPOLL) == 0);
    // The queued SQEs are the `io_uring_sq_ready` positions from the kernel's
    // head on, and liburing hands out the SQE of position `p` as
    // `sqes[p & mask]` (`io_uring_get_sqe`, in every version). The SQ array is
    // not used: liburing <= 2.2 fills it only when it flushes the SQEs to the
    // kernel. `*kring_mask` exists in every version, `ring_mask` only since
    // 2.3.
    const unsigned mask = *ring_.sq.kring_mask;
    const unsigned head = io_uring_smp_load_acquire(ring_.sq.khead);
    const unsigned numReady = io_uring_sq_ready(&ring_);
    std::vector<uint64_t> cancelled;
    for (const unsigned i : ql::views::iota(0u, numReady)) {
      io_uring_sqe* sqe = &ring_.sq.sqes[(head + i) & mask];
      if (sqe->user_data != noOpRequestId) {
        cancelled.push_back(sqe->user_data);
      }
      io_uring_prep_nop(sqe);
      io_uring_sqe_set_data64(sqe, noOpRequestId);
    }
    return cancelled;
  }

  void backOff() override {
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }

 private:
  io_uring ring_{};
};
}  // namespace

//______________________________________________________________________________
std::unique_ptr<IoUringInterface> makeLiburingRing(unsigned ringSize) {
  return std::make_unique<LiburingRing>(ringSize);
}

//______________________________________________________________________________
IoUringPolicy::IoUringPolicy(unsigned ringSize)
    : IoUringPolicy(ringSize, makeLiburingRing(ringSize)) {}

//______________________________________________________________________________
IoUringPolicy::IoUringPolicy(unsigned ringSize,
                             std::unique_ptr<IoUringInterface> ring)
    : ring_{std::move(ring)}, ringSize_{ringSize} {
  AD_CONTRACT_CHECK(ringSize_ > 0 && ring_ != nullptr);
}

//______________________________________________________________________________
void IoUringPolicy::addBatch(int fd, ql::span<const size_t> numBytesToRead,
                             ql::span<const uint64_t> offsets,
                             ql::span<char*> buffers,
                             [[maybe_unused]] BatchHandle handle) {
  if (numBytesToRead.empty()) {
    return;
  }
  Batch batch;
  batch.numBytes = numBytesToRead;
  batch.states.assign(numBytesToRead.size(), ReadState::NotQueued);
  // `run` throws only if a correctness check fails or memory runs out. The
  // bookkeeping can then no longer tell which reads the kernel still holds, so
  // the exception must not reach the caller, who would free the buffers.
  ad_utility::terminateIfThrows(
      [&]() { run(fd, offsets, buffers, batch); },
      "IoUringPolicy::addBatch must not throw while the kernel may hold reads");
  if (batch.error.has_value()) {
    AD_THROW(std::move(batch.error).value());
  }
}

//______________________________________________________________________________
void IoUringPolicy::run(int fd, ql::span<const uint64_t> offsets,
                        ql::span<char*> buffers, Batch& batch) {
  const size_t numReads = batch.numBytes.size();
  while (true) {
    // Fill the free ring slots with the next reads, unless the batch has
    // failed (then `addBatch` throws anyway). At most `ringSize_` requests are
    // in the ring, so a free SQE is always available and the completion queue
    // (twice as large as the submission queue) cannot overflow.
    while (!batch.error.has_value() && batch.numQueued < numReads &&
           batch.numSlots() + numNoOpsQueued_ + numNoOpsInKernel_ < ringSize_) {
      const size_t i = batch.numQueued;
      AD_CORRECTNESS_CHECK(
          ring_->prepareRead(fd, buffers[i], batch.numBytes[i], offsets[i], i));
      batch.states[i] = ReadState::Queued;
      ++batch.numQueued;
    }
    submit(batch);
    checkQueue(batch);

    const size_t numInKernel = batch.numReadsInKernel() + numNoOpsInKernel_;
    if (numInKernel == 0) {
      // The queue is empty or holds only no-ops (`submit` cancels the reads it
      // cannot submit), so the batch is done or has failed.
      AD_CORRECTNESS_CHECK(batch.numDone == numReads ||
                           batch.error.has_value());
      return;
    }
    // Wait for a wave of completions, but never for more requests than the
    // kernel holds.
    reap(batch,
         static_cast<unsigned>(std::min<size_t>(REAP_WAVE, numInKernel)));
  }
}

//______________________________________________________________________________
void IoUringPolicy::submit(Batch& batch) {
  // `io_uring_submit` returns the number of SQEs that the kernel took, or
  // `-errno`. The SQEs it did not take stay queued. Of the errors listed in
  // io_uring_enter(2), only `EINTR` and `EAGAIN` can occur for this ring (no
  // setup flags, and never more requests than CQ slots); the others mean that
  // the ring is broken. See
  // https://man7.org/linux/man-pages/man2/io_uring_enter.2.html
  unsigned numRetries = 0;
  while (ring_->numQueued() > 0) {
    const int ret = ring_->submit();
    if (ret > 0) {
      onTaken(batch, static_cast<size_t>(ret));
      continue;
    }
    if (ret == -EINTR) {
      continue;
    }
    // A submit that takes nothing although SQEs are queued did not make
    // progress either, so it is retried like `EAGAIN`.
    if (ret != 0 && ret != -EAGAIN) {
      terminateBecauseRingIsUnusable("io_uring_submit", -ret);
    }
    if (numRetries == MAX_SUBMIT_RETRIES) {
      cancelQueuedReads(
          batch, absl::StrCat("io_uring_submit failed in IoUringPolicy: ",
                              std::strerror(EAGAIN), " (after ",
                              MAX_SUBMIT_RETRIES, " retries)"));
      return;
    }
    ++numRetries;
    // The kernel could not allocate memory for the requests. Completed
    // requests free some, so wait for one if there is any.
    if (batch.numReadsInKernel() + numNoOpsInKernel_ > 0) {
      reap(batch, 1);
    } else {
      ring_->backOff();
    }
  }
}

//______________________________________________________________________________
void IoUringPolicy::onTaken(Batch& batch, size_t numTaken) {
  // The no-ops were queued before any read of this batch: those of earlier
  // batches before it started, and those of this batch's cancelled reads after
  // its last queued read.
  const size_t numNoOps = std::min(numTaken, numNoOpsQueued_);
  numNoOpsQueued_ -= numNoOps;
  numNoOpsInKernel_ += numNoOps;
  for (size_t k = numNoOps; k < numTaken; ++k) {
    AD_CORRECTNESS_CHECK(batch.numTaken < batch.numQueued);
    ReadState& state = batch.states[batch.numTaken++];
    AD_CORRECTNESS_CHECK(state == ReadState::Queued);
    state = ReadState::InKernel;
  }
}

//______________________________________________________________________________
void IoUringPolicy::cancelQueuedReads(Batch& batch, std::string errorMessage) {
  const std::vector<uint64_t> cancelled = ring_->cancelQueued(NO_OP_REQUEST_ID);
  AD_CORRECTNESS_CHECK(cancelled.size() == batch.numQueued - batch.numTaken);
  for (const uint64_t requestId : cancelled) {
    AD_CORRECTNESS_CHECK(requestId >= batch.numTaken &&
                         requestId < batch.numQueued);
    ReadState& state = batch.states[requestId];
    AD_CORRECTNESS_CHECK(state == ReadState::Queued);
    state = ReadState::Done;
  }
  // The cancelled reads are done; their SQEs stay queued as no-ops.
  batch.numTaken = batch.numQueued;
  batch.numDone += cancelled.size();
  numNoOpsQueued_ += cancelled.size();
  if (!batch.error.has_value()) {
    batch.error = std::move(errorMessage);
  }
}

//______________________________________________________________________________
void IoUringPolicy::reap(Batch& batch, unsigned minComplete) {
  AD_CORRECTNESS_CHECK(minComplete > 0 &&
                       minComplete <=
                           batch.numReadsInKernel() + numNoOpsInKernel_);
  int ret = 0;
  do {
    ret = ring_->waitForCompletions(minComplete);
  } while (ret == -EINTR);
  if (ret < 0) {
    terminateBecauseRingIsUnusable("io_uring_wait_cqes", -ret);
  }

  std::array<IoUringInterface::Completion, 64> completions{};
  while (true) {
    const size_t numReady = ring_->popCompletions(completions);
    if (numReady == 0) {
      return;
    }
    for (const auto& [requestId, result] :
         ql::views::take(completions, numReady)) {
      if (requestId == NO_OP_REQUEST_ID) {
        AD_CORRECTNESS_CHECK(numNoOpsInKernel_ > 0);
        --numNoOpsInKernel_;
        continue;
      }
      // A read's request id is its index in the batch.
      AD_CORRECTNESS_CHECK(requestId < batch.numTaken);
      ReadState& state = batch.states[requestId];
      AD_CORRECTNESS_CHECK(state == ReadState::InKernel);
      state = ReadState::Done;
      ++batch.numDone;
      // `result` < 0 is `-errno`. A result smaller than requested (a partial
      // read, or 0 at end of file) means we read fewer bytes than expected,
      // which we treat as an error. Only the first error of a batch is kept.
      const size_t expectedNumBytes = batch.numBytes[requestId];
      if (batch.error.has_value()) {
        continue;
      }
      if (result < 0) {
        batch.error =
            absl::StrCat("I/O error in IoUringPolicy read operation: ",
                         std::strerror(-result));
      } else if (static_cast<size_t>(result) != expectedNumBytes) {
        batch.error =
            absl::StrCat("read fewer bytes than requested in IoUringPolicy (",
                         result, " of ", expectedNumBytes, ")");
      }
    }
  }
}

//______________________________________________________________________________
void IoUringPolicy::checkQueue(const Batch& batch) const {
  AD_CORRECTNESS_CHECK(ring_->numQueued() ==
                       numNoOpsQueued_ + (batch.numQueued - batch.numTaken));
}

#endif  // QLEVER_HAS_IO_URING

}  // namespace ad_utility
