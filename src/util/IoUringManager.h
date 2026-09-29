// Copyright 2026, The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_UTIL_IOURINGMANAGER_H
#define QLEVER_SRC_UTIL_IOURINGMANAGER_H

#include <gtest/gtest_prod.h>

#include <array>
#include <cstdint>
#include <functional>
#include <optional>
#include <type_traits>
#include <unordered_map>
#include <utility>
#include <vector>

#include "backports/algorithm.h"
#include "backports/concepts.h"
#include "util/AdaptiveBatchController.h"
#include "util/Exception.h"
#include "util/HashMap.h"

#ifdef QLEVER_HAS_IO_URING
#include <liburing.h>
#endif

#include "backports/span.h"

namespace ad_utility {

// Default number of submission slots of an io_uring ring (and the fixed
// submission window of a `BatchManager` without an adaptive controller).
inline constexpr unsigned DEFAULT_IO_URING_RING_SIZE = 256;

template <typename T>
CPP_requires(ReadPolicy_,
             requires(T& policy, int fd, ql::span<const size_t> numBytes,
                      ql::span<const uint64_t> offsets, ql::span<char*> buffers,
                      typename T::BatchHandle handle)(
                 concepts::constructible_from<T, unsigned>,
                 // Must provide `addBatch` with the following parameters.
                 policy.addBatch(fd, numBytes, offsets, buffers, handle),
                 // Must provide a `wait` method with the following interface.
                 policy.wait(handle)));

// The pluggable I/O backend of `BatchManager`: it specifies how the reads in a
// batch are carried out. See `IoUringPolicy` (asynchronous, via io_uring) and
// `SyncIoPolicy` (blocking `pread` fallback) below. Must additionally be
// constructible from a ringsize (`unsigned`). Also require that `T` must expose
// an unsigned integral `BatchHandle` type.
template <typename T>
CPP_concept ReadPolicyConcept =
    concepts::constructible_from<T, unsigned> &&
    concepts::unsigned_integral<typename T::BatchHandle> &&
    CPP_requires_ref(ReadPolicy_, T);

// Abstract base of `BatchManager` so a pool can hold managers of different
// `ReadPolicy`s behind a unified interface and pick the backend at runtime.
class BatchManagerBase {
 public:
  using BatchHandle = uint64_t;
  virtual ~BatchManagerBase() = default;

  [[nodiscard]] virtual BatchHandle addBatch(int fd,
                                             ql::span<const size_t> numBytes,
                                             ql::span<const uint64_t> offsets,
                                             ql::span<char*> buffers) = 0;

  virtual void wait(BatchHandle handle) = 0;
};

// `BatchManager` owns the batch bookkeeping (minting a `BatchHandle` per batch,
// validating the input spans) and delegates the reads from the underlying
// Vocabulary to the `Policy`, which must satisfy the `ReadPolicy` concept
// above.

// Detection helpers for optional controller support (`IoUringPolicy` has
// it, `SyncIoPolicy` does not). Written with `std::void_t` instead of a
// C++20 requires-expression so the C++17 backports build keeps working;
// see the same idiom in `util/Synchronized.h`.
template <typename T, typename = void>
struct HasAdaptiveBatchControllerSetter : std::false_type {};
template <typename T>
struct HasAdaptiveBatchControllerSetter<
    T, std::void_t<decltype(std::declval<T&>().setAdaptiveBatchController(
           std::declval<AdaptiveBatchController>()))>> : std::true_type {};
template <typename T, typename = void>
struct HasAdaptiveBatchControllerGetter : std::false_type {};
template <typename T>
struct HasAdaptiveBatchControllerGetter<
    T,
    std::void_t<decltype(std::declval<const T&>().adaptiveBatchController())>>
    : std::true_type {};

template <typename ReadPolicy>
class BatchManager final : public BatchManagerBase {
  static_assert(
      ReadPolicyConcept<ReadPolicy>,
      "BatchManager's ReadPolicy must satisfy the ReadPolicyConcept concept.");
  // The policy is only valid if the policy's handle type matches the base's.
  static_assert(
      std::is_same_v<typename ReadPolicy::BatchHandle,
                     BatchManagerBase::BatchHandle>,
      "ReadPolicy::BatchHandle must match BatchManagerBase::BatchHandle.");

 public:
  using BatchHandle = typename BatchManagerBase::BatchHandle;

  explicit BatchManager(unsigned ringSize = DEFAULT_IO_URING_RING_SIZE)
      : policy_(ringSize) {}

  BatchManager(const BatchManager&) = delete;
  BatchManager& operator=(const BatchManager&) = delete;

  [[nodiscard]] BatchHandle addBatch(int fd, ql::span<const size_t> numBytes,
                                     ql::span<const uint64_t> offsets,
                                     ql::span<char*> buffers) override {
    validateSameLength(numBytes, offsets, buffers);

    BatchHandle handle = nextBatchHandle_++;

    // Delegate the I/O work to the policy.
    policy_.addBatch(fd, numBytes, offsets, buffers, handle);

    return handle;
  }

  // Block until every read in `handle` has completed.
  void wait(BatchHandle handle) override { policy_.wait(handle); }

  // Enable adaptive batch sizing on the policy. Only policies with
  // controller support (`IoUringPolicy`) accept it; for policies without a
  // `setAdaptiveBatchController` member (`SyncIoPolicy`, whose blocking
  // reads have nothing to pace) this method throws.
  void setAdaptiveBatchController(AdaptiveBatchController controller) {
    if constexpr (HasAdaptiveBatchControllerSetter<ReadPolicy>::value) {
      policy_.setAdaptiveBatchController(std::move(controller));
    } else {
      AD_THROW(
          "adaptive batch sizing is not supported by this read policy: "
          "blocking reads cannot be paced");
    }
  }

  // Return the policy's controller, or `std::nullopt` when adaptive batch
  // sizing is disabled or the policy has no `adaptiveBatchController`
  // member (`SyncIoPolicy`).
  [[nodiscard]] std::optional<AdaptiveBatchController> adaptiveBatchController()
      const {
    if constexpr (HasAdaptiveBatchControllerGetter<ReadPolicy>::value) {
      return policy_.adaptiveBatchController();
    } else {
      return std::nullopt;
    }
  }

 private:
  [[no_unique_address]] ReadPolicy policy_;
  BatchHandle nextBatchHandle_ = 0;

  template <typename Span0, typename... Spans>
  static void validateSameLength(const Span0& first, const Spans&... rest) {
    const auto n = ql::ranges::size(first);
    auto valid = ((ql::ranges::size(rest) == n) && ...);
    if (!valid) {
      AD_THROW("spans must have same length");
    }
    return;
  }
};

// Fallback implementation for the `IoUringPolicy` below. Schedules pread calls
// in a synchronous (blocking) manner. Single-threaded use only.
struct SyncIoPolicy {
  using BatchHandle = uint64_t;

  // `ringSize` is ignored; it exists only so the policy is constructible the
  // same way as `IoUringPolicy`.
  //
  // NOTE: GCC rejects `[[maybe_unused]]` on a defaulted parameter; cast to
  // void.
  explicit SyncIoPolicy(unsigned ringSize = DEFAULT_IO_URING_RING_SIZE) {
    (void)ringSize;
  }

  ~SyncIoPolicy() = default;
  SyncIoPolicy(const SyncIoPolicy&) = delete;
  SyncIoPolicy& operator=(const SyncIoPolicy&) = delete;

  // Immediately execute a batch of reads synchronously. This blocks the calling
  // thread. Read `i` reads `numBytesToReadPerRequest[i]` bytes from file
  // descriptor `fd`, starting at offset `fileOffsetPerRequest[i]` (from the
  // start of the file), into the buffer starting at
  // `targetBufferPerRequest[i]`. `handle` is unused (the batch completes before
  // `addBatch` returns).
  void addBatch(int fd, ql::span<const size_t> numBytesToReadPerRequest,
                ql::span<const uint64_t> fileOffsetPerRequest,
                ql::span<char*> targetBufferPerRequest,
                BatchHandle handle) const;

  void wait(BatchHandle) const {
    // No-op: `addBatch` already completed all reads synchronously.
  }

  // Read exactly `numBytes` bytes from file descriptor `fd` at `fileOffset`
  // (from the start of the file) into `targetBuffer`. Throws exception if the
  // read fails or returns fewer bytes than requested (a partial read or end of
  // file), since every read must be fully satisfied.
  static void readFullyOrThrow(int fd, char* targetBuffer, size_t numBytes,
                               uint64_t fileOffset);
};

// The fixed-file slot table of one `io_uring` ring, without the ring itself:
// which caller descriptor occupies which slot, and the `dup`ed descriptor that
// the ring holds for it. The `dup` keeps the ring's table entry alive
// independently of the caller's descriptor. The ring-specific step (installing
// a descriptor into a slot) and the descriptor system calls are injected, so
// the bookkeeping and its error paths do not need a live ring.
class FixedFileSlots {
 public:
  // The vocabulary path serves exactly two stable files (the offsets file and
  // the word-data file).
  static constexpr size_t NUM_SLOTS = 2;

  // Install `registeredFd` into ring slot `slot`. Return a non-negative value
  // on success and a negative `errno` value on failure, like
  // `io_uring_register_files_update`.
  using InstallFunction = std::function<int(unsigned slot, int registeredFd)>;
  // Duplicate `fd` with `dup` semantics: a new descriptor, or -1 and `errno`.
  using DupFunction = std::function<int(int fd)>;
  // Close a descriptor that this table `dup`ed.
  using CloseFunction = std::function<void(int fd)>;

 private:
  // One slot: the caller's descriptor (`ownerFd`, never closed here) and its
  // `dup` held by the ring (`registeredFd`). `-1` marks an unused slot.
  struct Slot {
    int ownerFd = -1;
    int registeredFd = -1;
  };
  // The slots in registration order.
  std::array<Slot, NUM_SLOTS> slots_;
  InstallFunction install_;
  DupFunction dup_;
  CloseFunction close_;

 public:
  explicit FixedFileSlots(InstallFunction install,
                          DupFunction dupFd = defaultDup,
                          CloseFunction closeFd = defaultClose);
  // Close every `dup`ed descriptor (see `releaseAll`).
  ~FixedFileSlots();
  FixedFileSlots(const FixedFileSlots&) = delete;
  FixedFileSlots& operator=(const FixedFileSlots&) = delete;

  // Return the slot of `fd`, and on first use `dup` it and install the
  // duplicate into a free slot. Throw when all slots hold other descriptors,
  // so a third file fails loudly instead of being read without registration,
  // and when `dup` or the install fails; the table is then unchanged and no
  // descriptor leaks. Slots are keyed by descriptor number, so the caller must
  // keep `fd` open (and referring to the same file) while the table lives.
  unsigned slotFor(int fd);

  // Close every `dup`ed descriptor and mark all slots unused. Call it only
  // after the ring's file table was unregistered. Calling it again is a no-op.
  void releaseAll() noexcept;

  // The number of slots that currently hold a descriptor.
  size_t numUsedSlots() const;

  // The `dup` and `close` system calls, the defaults of the constructor.
  static int defaultDup(int fd);
  static void defaultClose(int fd);
};

// Persistent io_uring manager that accepts multiple named batches of indices to
// be read from the underlying storage medium, submits all SQEs in `addBatch`
// (blocking if the ring is full), and lets the caller block on a specific batch
// via `wait()`. Single-threaded use only. See https://github.com/axboe/liburing
// for more details.
#ifdef QLEVER_HAS_IO_URING

class IoUringPolicy {
 public:
  using BatchHandle = uint64_t;

 private:
  io_uring ring_{};
  unsigned ringSize_;

  // Optional ratio controller for adaptive batch sizing. Disabled
  // (`std::nullopt`) by default, in which case `addBatch` keeps the exact
  // fixed-window behavior. Enabled via `setAdaptiveBatchController`.
  std::optional<AdaptiveBatchController> adaptiveBatchController_;

  // Total number of reads that occupy a ring slot but have not yet been reaped
  // via a completion queue entry (CQE), i.e. that are prepared or submitted but
  // not yet completed. Used to detect whether the ring is full.
  size_t numInFlightReadRequests_ = 0;

  // The same in-flight reads as `numInFlight_`, but broken down per batch:
  // maps a batch handle to the number of its reads that have not yet completed
  // (are "in flight"). An entry for a batch (identified by `BatchHandle`) is
  // removed once `wait()` has observed all of its reads complete.
  ad_utility::HashMap<BatchHandle, size_t> numInFlightReadRequestsPerBatch_;

  // Per-read metadata needed when a completion is reaped: which batch the read
  // belongs to, and how many bytes it was supposed to read (so that reading
  // fewer bytes than expected can be detected). See
  // `inFlightReadsByRequestId_`.
  struct InFlightRead {
    BatchHandle batchHandle;
    size_t expectedNumBytes;
  };

  // Monotonically increasing counter that mints a unique request id for each
  // individual read. The id is stored in the SQE's `user_data` and recovered
  // from the matching CQE to look up the read's `InFlightRead` metadata.
  uint64_t nextRequestIdToAssign_ = 0;

  // Maps a read's request id to its metadata. An entry is inserted when the
  // read is prepared in `addBatch` and erased when its completion is reaped.
  ad_utility::HashMap<uint64_t, InFlightRead> inFlightReadsByRequestId_;

  // Block until at least `minComplete` CQEs are ready (capped at the number
  // of reads the kernel has received), then reap every ready CQE. Throw after
  // the whole wave is reaped if any read in it failed or was short.
  // `minComplete` must be > 0 and at most `numInFlightReadRequests_`.
  void drainAtLeast(unsigned minComplete);

  // Apply one completion to the in-flight bookkeeping. Always updates the
  // counts, also for a failed read. Return a static error message if the read
  // failed or was short, and `nullptr` otherwise.
  [[nodiscard]] const char* processCqe(int numBytesRead, uint64_t requestId);

  // Submit all prepared SQEs to the kernel. Throw if `io_uring_submit`
  // fails, including the error description in the message.
  void submitOrThrow();

  // Drain completions until the ring has a free submission slot. Called from
  // a fiber body this cooperates via `FiberIoScheduler` instead of parking
  // the thread; called from a plain thread it blocks in `drainAtLeast`.
  void drainUntilSlotFree();

  // The vocabulary path that uses this policy serves exactly two stable
  // files (the offsets file and the word-data file).
  static constexpr size_t NUM_FIXED_FILES = FixedFileSlots::NUM_SLOTS;

  // The ring's fixed-file table. Each new descriptor is installed into its
  // slot with `IORING_REGISTER_FILES_UPDATE`: re-registering the whole table
  // over an already registered one fails with `EBUSY`, and updating one slot
  // leaves the other slots (and reads in flight on them) untouched.
  FixedFileSlots fixedFileSlots_{[this](unsigned slot, int registeredFd) {
    return io_uring_register_files_update(&ring_, slot, &registeredFd, 1);
  }};

 public:
  IoUringPolicy(const IoUringPolicy&) = delete;
  IoUringPolicy& operator=(const IoUringPolicy&) = delete;

  // `ringSize` must be > 0 (power of 2 preferred; liburing rounds up).
  // Throws when the kernel does not support fixed files
  // (`IORING_REGISTER_FILES`), so `makeBatchManager` falls back to
  // synchronous reads on such kernels.
  explicit IoUringPolicy(unsigned ringSize);
  ~IoUringPolicy();

  // Enable adaptive batch sizing with `controller`. The bounds are
  // normalized against `ringSize_` (see `AdaptiveBatchController::
  // normalized`); this is the single normalization boundary, so callers
  // may pass raw configured values.
  void setAdaptiveBatchController(AdaptiveBatchController controller) {
    adaptiveBatchController_ =
        controller.normalized(static_cast<size_t>(ringSize_));
  }

  // The controller from `setAdaptiveBatchController`, or `std::nullopt`
  // when adaptive batch sizing is disabled (the default).
  [[nodiscard]] std::optional<AdaptiveBatchController> adaptiveBatchController()
      const {
    return adaptiveBatchController_;
  }

  // Minimum number of completions to wait for when the ring is full or
  // `wait()` blocks. Waiting for several CQEs and reaping all ready ones in
  // one pass amortizes `io_uring_enter` and the CQ-head update over the wave.
  static constexpr unsigned REAP_WAVE = 8;

  // Enqueue a batch of read requests and submit them to the kernel. Blocks the
  // calling thread only when the submission queue is full, in order to drain
  // completion queue entries and free slots in the submission queue. Read `i`
  // reads `numBytesToRead[i]` bytes from the file registered for `fd` (see
  // `NUM_FIXED_FILES`), starting at offset `offsets[i]` (from the start of the
  // file), into the buffer starting at `buffers[i]`. Every read uses
  // `IOSQE_FIXED_FILE`, so the kernel skips the per-request file-table lookup.
  // The reads are tracked under `handle`, which can be passed to `wait()` to
  // block until this batch has completed.
  void addBatch(int fd, ql::span<const size_t> numBytesToRead,
                ql::span<const uint64_t> offsets, ql::span<char*> buffers,
                BatchHandle handle);

  // Block until every read in the batch that is represented by the `handle` has
  // completed. (The `handle` was submitted along the read requests using
  // `addBatch`.) Throws on any I/O error. When called from inside a
  // `FiberIoScheduler` fiber (and fiber support is compiled in, see
  // `QLEVER_HAS_FIBER_IO`), this cooperates instead of parking the thread:
  // it reaps available completions and yields to sibling fibers while the
  // batch is still in flight, parking only as a last resort. Called from a
  // plain thread it keeps the current blocking behavior.
  void wait(BatchHandle handle);

  // Block until at least one completion is available, then reap every ready
  // completion and attribute it to its batch (not necessarily the awaited
  // one). Throws on I/O errors exactly like `wait`. Used for the last-resort
  // park in `FiberIoScheduler::waitUntil`: a parking fiber still serves its
  // siblings while parked. Requires at least one read in flight.
  void drainOneCqe();

  // Try to reap a single available completion without blocking. Returns true
  // if a completion was reaped (and attributed to its batch), false if no
  // completion was currently available. Never blocks: it peeks at the
  // completion queue instead of waiting. Throws on I/O errors exactly like
  // `wait`.
  bool tryReapOneCqe();

  // Reap every currently available completion without blocking. Returns the
  // number of completions reaped. Used by the cooperative fiber scheduler
  // (`FiberIoScheduler`) to drain a full round of completions before deciding
  // whether to yield or park.
  size_t reapAvailableCompletions();

  // True once every read of `handle` has completed, i.e. the batch entry is
  // gone. A handle that was never submitted (or an empty batch, which
  // `addBatch` returns early for without recording) also reports complete,
  // matching the `wait` loop it backs.
  bool isBatchComplete(BatchHandle handle) const;

  // Number of reads that occupy a ring slot but have not yet been reaped.
  size_t numOutstandingReads() const { return numInFlightReadRequests_; }

  // True once submitting one more read would exceed the ring capacity.
  bool isRingFull() const { return numInFlightReadRequests_ >= ringSize_; }
};

using BatchIoManager = BatchManager<IoUringPolicy>;
#else
using BatchIoManager = BatchManager<SyncIoPolicy>;
#endif

// Serve the reads of a batch that are fully in the page cache with
// non-blocking `preadv2(RWF_NOWAIT)` calls, and return the positions (indices
// into the three spans, ascending) of the reads that were not served. The
// caller must issue those through its regular path, which also reports real
// errors. Reads whose file ranges are exactly adjacent (`offsets[i] +
// numBytes[i] == offsets[i + 1]`) are coalesced into one `preadv2` call with
// one `iovec` per read. A read is served only if all of its bytes were read:
// `EAGAIN` (not cached), a short read (end of file, or only a prefix cached)
// or any other error leaves the read (and, for a failed call, the rest of its
// run) to the caller. If the kernel or file system rejects `RWF_NOWAIT`
// (`EOPNOTSUPP`), the fast path is disabled for the rest of the process (see
// `pageCacheFastPathIsSupported`), which is logged once. Where `preadv2` with
// `RWF_NOWAIT` is not available (outside Linux, and in Emscripten builds),
// the function exists but serves nothing: every read is returned, and
// `pageCacheFastPathIsSupported()` is false.
// Precondition: the three spans have the same length.
// `preadv2` and `RWF_NOWAIT` (Linux >= 4.14), including the caveat that a
// `RWF_NOWAIT` read may return 0 before the end of the file (such a read is
// treated as not served): readv(2),
// https://web.archive.org/web/20260828220215/https://man7.org/linux/man-pages/man2/readv.2.html
std::vector<size_t> readPageCacheHits(int fd, ql::span<const size_t> numBytes,
                                      ql::span<const uint64_t> offsets,
                                      ql::span<char*> buffers);

// False once `readPageCacheHits` found that `RWF_NOWAIT` is not supported, or
// if it is not available at compile time.
bool pageCacheFastPathIsSupported();

// Build a batch manager. When io_uring is compiled in and the runtime flag
// `preferIoUring` is set, try to build an `IoUringManager`. If its setup
// syscall fails at runtime clear `preferIoUring` and fall back to a
// `SyncIoManager`. Passing the flag by reference makes this probe-once: after
// the first failure, every subsequent call goes straight to the sync manager,
// so we don't repeat a failing syscall. The optional `adaptiveBatchController`
// opts the io_uring backend into adaptive batch sizing; the default
// (`std::nullopt`) keeps the fixed submission window.
inline std::unique_ptr<BatchManagerBase> makeBatchManager(
    bool& preferIoUring, unsigned ringSize = DEFAULT_IO_URING_RING_SIZE,
    std::optional<AdaptiveBatchController> adaptiveBatchController =
        std::nullopt) {
#ifdef QLEVER_HAS_IO_URING
  if (preferIoUring) {
    try {
      auto manager = std::make_unique<BatchManager<IoUringPolicy>>(ringSize);
      // The controller only paces io_uring submissions; without it the
      // manager keeps the fixed-window behavior.
      if (adaptiveBatchController.has_value()) {
        manager->setAdaptiveBatchController(*adaptiveBatchController);
      }
      return manager;
    } catch (const std::exception& e) {
      preferIoUring = false;
      AD_LOG_WARN << "io_uring is compiled in but unavailable at runtime ("
                  << e.what()
                  << "); falling back to synchronous pread for vocabulary "
                     "lookups"
                  << std::endl;
    }
  }
#else
  preferIoUring = false;
#endif
  // The synchronous fallback performs blocking reads, which have nothing to
  // pace. Say so loudly when a controller was requested: silently dropping
  // it would mislead the caller into believing pacing is active.
  if (adaptiveBatchController.has_value()) {
    AD_LOG_WARN << "adaptive batch sizing requested, but vocabulary lookups "
                   "fall back to synchronous pread; continuing without pacing"
                << std::endl;
  }
  return std::make_unique<BatchManager<SyncIoPolicy>>(ringSize);
}

}  // namespace ad_utility

#endif  // QLEVER_SRC_UTIL_IOURINGMANAGER_H
