// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_UTIL_ZEROCOPYSOCKETSENDER_H
#define QLEVER_SRC_UTIL_ZEROCOPYSOCKETSENDER_H

#include <fcntl.h>
#include <poll.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/uio.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <limits>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "backports/concepts.h"
#include "backports/span.h"
#include "util/AlignedAllocator.h"
#include "util/Exception.h"
#include "util/Log.h"

// Use `io_uring` only when CMake found and linked liburing (it then defines
// `QLEVER_HAS_IO_URING`). A present `<liburing.h>` alone is not enough: with
// `USE_IO_URING=OFF` the library is not linked, and every build without it
// must still compile and link against the synchronous `send()` fallback.
#ifdef QLEVER_HAS_IO_URING
#define QLEVER_HAS_LIBURING 1
#include <liburing.h>
#endif

// IORING_OP_SEND_ZC plus the sqe/cqe 64-bit data helpers need liburing
// >= 2.5 (Ubuntu 22.04 ships 2.1). Older toolchains transparently fall back
// to regular async sends below. The version macros are absent before 2.2,
// hence the defined() guard.
#if defined(QLEVER_HAS_LIBURING) && defined(IO_URING_VERSION_MAJOR) && \
    (IO_URING_VERSION_MAJOR > 2 ||                                     \
     (IO_URING_VERSION_MAJOR == 2 && IO_URING_VERSION_MINOR >= 5))
#define QLEVER_HAS_LIBURING_SEND_ZC 1
#endif

namespace ad_utility {

// Flag that suppresses `SIGPIPE` for a single `send()` to a closed peer.
// `MSG_NOSIGNAL` is not available on every platform (e.g. older macOS SDKs);
// there, Boost.Asio already sets `SO_NOSIGPIPE` on the sockets it creates.
#ifdef MSG_NOSIGNAL
inline constexpr int kSendNoSignalFlag = MSG_NOSIGNAL;
#else
inline constexpr int kSendNoSignalFlag = 0;
#endif

// 4KB memory page alignment constant for DMA and zero-copy kernel pinning.
inline constexpr size_t kZeroCopyPageAlignment = 4096;

// Upper bound for a single wait on the peer (socket writability or an
// `io_uring` send completion). A stalled peer must surface as an error instead
// of blocking the sending thread forever.
inline constexpr int kZeroCopyPeerStallTimeoutSeconds = 30;

// _____________________________________________________________________________
// Configuration parameters for ZeroCopySocketSender.
struct ZeroCopySenderConfig {
  size_t ringEntries = 512;
  size_t numBuffers = 64;
  size_t bufferSizeBytes = 64 * 1024;  // 64 KB per slot
  bool useRegisteredBuffers = true;
  bool useZeroCopy = true;
  // Extra flags for io_uring_queue_init (e.g. IORING_SETUP_SQPOLL). Invalid
  // flags are not fatal: initRing logs a warning and falls back to
  // synchronous send().
  unsigned int additionalFlags = 0;
};

// _____________________________________________________________________________
// Preallocated, page-aligned fixed memory buffer pool.
// Manages zero-copy DMA buffers registered with the kernel via
// IORING_REGISTER_BUFFERS. Provides O(1) buffer acquisition and recycling with
// zero runtime dynamic allocations.
class ZeroCopyBufferPool {
 private:
  void* rawBuffer_ = nullptr;
  size_t numBuffers_ = 0;
  size_t bufferSizeBytes_ = 0;
  size_t totalBytes_ = 0;

  std::vector<iovec> iovecs_;
  std::vector<uint32_t> freeSlots_;
  std::vector<bool> slotInUse_;

 public:
  ZeroCopyBufferPool() = default;

  // Allocate a pool of `numBuffers` slots, each `bufferSizeBytes` bytes.
  // Both total size and buffer sizes are 4KB-aligned.
  ZeroCopyBufferPool(size_t numBuffers, size_t bufferSizeBytes) {
    AD_CONTRACT_CHECK(numBuffers > 0);
    AD_CONTRACT_CHECK(bufferSizeBytes > 0);
    AD_CONTRACT_CHECK((bufferSizeBytes % kZeroCopyPageAlignment) == 0);
    // The pool size multiplication must not wrap: the pool below allocates
    // `totalBytes_` but builds `numBuffers` iovecs over it.
    AD_CONTRACT_CHECK(numBuffers <=
                      std::numeric_limits<size_t>::max() / bufferSizeBytes);

    numBuffers_ = numBuffers;
    bufferSizeBytes_ = bufferSizeBytes;
    totalBytes_ = numBuffers_ * bufferSizeBytes_;

    int ret = posix_memalign(&rawBuffer_, kZeroCopyPageAlignment, totalBytes_);
    if (ret != 0 || rawBuffer_ == nullptr) {
      AD_THROW(
          "posix_memalign failed to allocate zero-copy pinned buffer pool");
    }

    // Pre-fault memory pages before registration to avoid soft page faults
    // during async DMA transmission.
    std::memset(rawBuffer_, 0, totalBytes_);

    iovecs_.reserve(numBuffers_);
    freeSlots_.reserve(numBuffers_);
    slotInUse_.assign(numBuffers_, false);

    auto* basePtr = static_cast<char*>(rawBuffer_);
    for (size_t i = 0; i < numBuffers_; ++i) {
      // NOTE: positional (not designated) initialization: this header must
      // also compile as C++17, where designated initializers are unavailable.
      iovec iov{};
      iov.iov_base = basePtr + (i * bufferSizeBytes_);
      iov.iov_len = bufferSizeBytes_;
      iovecs_.push_back(iov);
      freeSlots_.push_back(static_cast<uint32_t>(numBuffers_ - 1 - i));
    }
  }

  ~ZeroCopyBufferPool() {
    if (rawBuffer_ != nullptr) {
      std::free(rawBuffer_);
      rawBuffer_ = nullptr;
    }
  }

  ZeroCopyBufferPool(const ZeroCopyBufferPool&) = delete;
  ZeroCopyBufferPool& operator=(const ZeroCopyBufferPool&) = delete;

  ZeroCopyBufferPool(ZeroCopyBufferPool&& other) noexcept
      : rawBuffer_{std::exchange(other.rawBuffer_, nullptr)},
        numBuffers_{std::exchange(other.numBuffers_, 0)},
        bufferSizeBytes_{std::exchange(other.bufferSizeBytes_, 0)},
        totalBytes_{std::exchange(other.totalBytes_, 0)},
        iovecs_{std::move(other.iovecs_)},
        freeSlots_{std::move(other.freeSlots_)},
        slotInUse_{std::move(other.slotInUse_)} {}

  ZeroCopyBufferPool& operator=(ZeroCopyBufferPool&& other) noexcept {
    if (this != &other) {
      if (rawBuffer_ != nullptr) {
        std::free(rawBuffer_);
      }
      rawBuffer_ = std::exchange(other.rawBuffer_, nullptr);
      numBuffers_ = std::exchange(other.numBuffers_, 0);
      bufferSizeBytes_ = std::exchange(other.bufferSizeBytes_, 0);
      totalBytes_ = std::exchange(other.totalBytes_, 0);
      iovecs_ = std::move(other.iovecs_);
      freeSlots_ = std::move(other.freeSlots_);
      slotInUse_ = std::move(other.slotInUse_);
    }
    return *this;
  }

  // Acquire a free buffer slot with zero dynamic allocations.
  // Returns std::nullopt if all buffer slots are currently in flight.
  [[nodiscard]] std::optional<uint32_t> acquireSlot() {
    if (freeSlots_.empty()) {
      return std::nullopt;
    }
    uint32_t slot = freeSlots_.back();
    freeSlots_.pop_back();
    AD_CORRECTNESS_CHECK(slot < numBuffers_);
    AD_CORRECTNESS_CHECK(!slotInUse_[slot]);
    slotInUse_[slot] = true;
    return slot;
  }

  // Release and recycle a buffer slot back into the pool.
  void releaseSlot(uint32_t slot) {
    AD_CONTRACT_CHECK(slot < numBuffers_);
    AD_CONTRACT_CHECK(slotInUse_[slot]);
    slotInUse_[slot] = false;
    freeSlots_.push_back(slot);
  }

  // Obtain a writable span view over slot `slotIndex`.
  [[nodiscard]] ql::span<char> getSlotSpan(uint32_t slotIndex) {
    AD_CONTRACT_CHECK(slotIndex < numBuffers_);
    AD_CONTRACT_CHECK(slotInUse_[slotIndex]);
    auto* ptr = static_cast<char*>(rawBuffer_) + (slotIndex * bufferSizeBytes_);
    return {ptr, bufferSizeBytes_};
  }

  // Obtain a const span view over slot `slotIndex`.
  [[nodiscard]] ql::span<const char> getSlotSpan(uint32_t slotIndex) const {
    AD_CONTRACT_CHECK(slotIndex < numBuffers_);
    AD_CONTRACT_CHECK(slotInUse_[slotIndex]);
    const auto* ptr =
        static_cast<const char*>(rawBuffer_) + (slotIndex * bufferSizeBytes_);
    return {ptr, bufferSizeBytes_};
  }

  [[nodiscard]] ql::span<const iovec> iovecs() const noexcept {
    return {iovecs_.data(), iovecs_.size()};
  }

  [[nodiscard]] size_t numBuffers() const noexcept { return numBuffers_; }
  [[nodiscard]] size_t bufferSizeBytes() const noexcept {
    return bufferSizeBytes_;
  }
  [[nodiscard]] size_t availableSlots() const noexcept {
    return freeSlots_.size();
  }
  [[nodiscard]] bool isSlotInUse(uint32_t slot) const {
    AD_CONTRACT_CHECK(slot < numBuffers_);
    return slotInUse_[slot];
  }
};

// _____________________________________________________________________________
// High-performance asynchronous zero-copy network socket sender utilizing
// Linux io_uring IORING_OP_SEND_ZC and IORING_REGISTER_BUFFERS.
//
// Manages submission queue entries (SQEs), tracks dual completion queue
// notifications (transmission completion + buffer release notification), and
// achieves zero runtime memory allocation on the transmission fast path.
//
// NOT thread-safe: all public operations must be called from a single thread
// (or serialized externally); in particular the io_uring ring must never be
// submitted to or drained from concurrently.
//
// Lifecycle contract: call `flushAndDrainAll()` before destruction (or move)
// whenever `sendChunk` was used, so that every queued SQE is submitted and
// every in-flight request runs its full completion lifecycle. The destructor
// only reaps already-submitted completions on a best-effort basis and cannot
// report drain errors.
class ZeroCopySocketSender {
 private:
  ZeroCopySenderConfig config_;
  ZeroCopyBufferPool bufferPool_;

#ifdef QLEVER_HAS_LIBURING
  io_uring ring_{};
  bool ringInitialized_ = false;
  bool buffersRegistered_ = false;
#endif

  // Internal fixed metadata for an in-flight socket send request.
  struct InFlightRequest {
    uint32_t bufferIndex = 0;
    size_t expectedBytes = 0;
    bool waitingForNotification = false;
    bool active = false;
  };

  // Preallocated tracking table mapped by (requestId % tableSize)
  std::vector<InFlightRequest> inFlightTable_;
  size_t numInFlightRequests_ = 0;  // Requests in SQ/CQ
  size_t numInFlightBuffers_ = 0;   // Buffers currently pinned by kernel
  uint64_t nextRequestId_ = 0;

  size_t totalBytesSent_ = 0;
  size_t totalPacketsSent_ = 0;

 public:
  explicit ZeroCopySocketSender(ZeroCopySenderConfig config = {})
      : config_{config},
        bufferPool_{config.numBuffers, config.bufferSizeBytes} {
    AD_CONTRACT_CHECK(config_.ringEntries > 0);
    AD_CONTRACT_CHECK(config_.numBuffers > 0);
    AD_CONTRACT_CHECK(config_.bufferSizeBytes > 0);
    // `ringEntries * 2` must not wrap, and `ringEntries` must survive the
    // `unsigned int` cast at `io_uring_queue_init` below.
    AD_CONTRACT_CHECK(config_.ringEntries <=
                      std::numeric_limits<unsigned int>::max() / 2);

    inFlightTable_.resize(config_.ringEntries * 2);

    initRing();
  }

  ~ZeroCopySocketSender() { teardown(); }

  ZeroCopySocketSender(const ZeroCopySocketSender&) = delete;
  ZeroCopySocketSender& operator=(const ZeroCopySocketSender&) = delete;

  // The moved-from object must not be used until it is move-assigned a new
  // value: its kernel ring (if any) now belongs to the destination.
  ZeroCopySocketSender(ZeroCopySocketSender&& other) noexcept
      : config_{other.config_},
        bufferPool_{std::move(other.bufferPool_)},
#ifdef QLEVER_HAS_LIBURING
        ring_{other.ring_},
        ringInitialized_{std::exchange(other.ringInitialized_, false)},
        buffersRegistered_{std::exchange(other.buffersRegistered_, false)},
#endif
        inFlightTable_{std::move(other.inFlightTable_)},
        numInFlightRequests_{std::exchange(other.numInFlightRequests_, 0)},
        numInFlightBuffers_{std::exchange(other.numInFlightBuffers_, 0)},
        nextRequestId_{std::exchange(other.nextRequestId_, 0)},
        totalBytesSent_{std::exchange(other.totalBytesSent_, 0)},
        totalPacketsSent_{std::exchange(other.totalPacketsSent_, 0)} {
#ifdef QLEVER_HAS_LIBURING
    std::memset(&other.ring_, 0, sizeof(other.ring_));
#endif
  }

  ZeroCopySocketSender& operator=(ZeroCopySocketSender&& other) noexcept {
    if (this != &other) {
      teardown();
      config_ = other.config_;
      bufferPool_ = std::move(other.bufferPool_);
#ifdef QLEVER_HAS_LIBURING
      ring_ = other.ring_;
      std::memset(&other.ring_, 0, sizeof(other.ring_));
      ringInitialized_ = std::exchange(other.ringInitialized_, false);
      buffersRegistered_ = std::exchange(other.buffersRegistered_, false);
#endif
      inFlightTable_ = std::move(other.inFlightTable_);
      numInFlightRequests_ = std::exchange(other.numInFlightRequests_, 0);
      numInFlightBuffers_ = std::exchange(other.numInFlightBuffers_, 0);
      nextRequestId_ = std::exchange(other.nextRequestId_, 0);
      totalBytesSent_ = std::exchange(other.totalBytesSent_, 0);
      totalPacketsSent_ = std::exchange(other.totalPacketsSent_, 0);
    }
    return *this;
  }

  // ___________________________________________________________________________
  // Acquire an available buffer slot from the pool.
  // If all buffer slots are currently occupied by in-flight transmissions,
  // flushes pending SQEs to the kernel and reaps CQEs until a slot is released.
  // Guaranteed zero heap allocation.
  [[nodiscard]] uint32_t acquireBuffer() {
    while (true) {
      auto slotOpt = bufferPool_.acquireSlot();
      if (slotOpt.has_value()) {
        return slotOpt.value();
      }

#ifdef QLEVER_HAS_LIBURING
      if (ringInitialized_) {
        // Pool exhausted: submit pending queue and drain completions
        io_uring_submit(&ring_);
        drainOneCqe();
        continue;
      }
#endif
      AD_THROW("Buffer pool exhausted with no asynchronous engine initialized");
    }
  }

  // Obtain writable span for the allocated buffer slot.
  [[nodiscard]] ql::span<char> getSlotSpan(uint32_t slotIndex) {
    return bufferPool_.getSlotSpan(slotIndex);
  }

  // ___________________________________________________________________________
  // Prepare and enqueue an asynchronous zero-copy socket send request.
  // If the submission queue is saturated, automatically flushes SQEs to the
  // kernel and drains CQEs.
  void sendChunk(int sockfd, uint32_t bufferIndex, size_t numBytes,
                 int flags = 0, [[maybe_unused]] unsigned int zcFlags = 0) {
    (void)zcFlags;
    // Suppress SIGPIPE on a closed peer for every path (the sync fallback
    // already ORs this in; the io_uring paths receive the same flags).
    flags |= kSendNoSignalFlag;
    AD_CONTRACT_CHECK(sockfd >= 0);
    AD_CONTRACT_CHECK(bufferIndex < config_.numBuffers);
    AD_CONTRACT_CHECK(numBytes > 0);
    AD_CONTRACT_CHECK(numBytes <= config_.bufferSizeBytes);

#ifdef QLEVER_HAS_LIBURING
    if (!ringInitialized_) {
      sendChunkSync(sockfd, bufferIndex, numBytes, flags);
      return;
    }

    // If submission ring is full, submit and reap CQEs to free ring entries
    if (numInFlightRequests_ >= config_.ringEntries) {
      io_uring_submit(&ring_);
      while (numInFlightRequests_ >= config_.ringEntries) {
        drainOneCqe();
      }
    }

    io_uring_sqe* sqe = io_uring_get_sqe(&ring_);
    AD_CORRECTNESS_CHECK(sqe != nullptr);

    const auto slotSpan = bufferPool_.getSlotSpan(bufferIndex);

    if (config_.useZeroCopy) {
#ifdef QLEVER_HAS_LIBURING_SEND_ZC
      if (buffersRegistered_ && config_.useRegisteredBuffers) {
        // Zero-Copy Send with Registered Fixed Buffer (Opcode:
        // IORING_OP_SEND_ZC)
        io_uring_prep_send_zc_fixed(sqe, sockfd, slotSpan.data(), numBytes,
                                    flags, zcFlags, bufferIndex);
      } else {
        // Zero-Copy Send with Unpinned Buffer
        io_uring_prep_send_zc(sqe, sockfd, slotSpan.data(), numBytes, flags,
                              zcFlags);
      }
#else
      // liburing predates SEND_ZC: regular async send (no zero-copy).
      io_uring_prep_send(sqe, sockfd, slotSpan.data(), numBytes, flags);
#endif
    } else {
      // Standard asynchronous io_uring send
      io_uring_prep_send(sqe, sockfd, slotSpan.data(), numBytes, flags);
    }

    const uint64_t reqId = nextRequestId_++;
    const size_t tableIdx = reqId % inFlightTable_.size();
    AD_CORRECTNESS_CHECK(!inFlightTable_[tableIdx].active);

    // NOTE: positional initialization (see above): no C++20 designated
    // initializers, this header must compile as C++17.
    InFlightRequest request;
    request.bufferIndex = bufferIndex;
    request.expectedBytes = numBytes;
    request.waitingForNotification = false;
    request.active = true;
    inFlightTable_[tableIdx] = request;

#ifdef QLEVER_HAS_LIBURING_SEND_ZC
    io_uring_sqe_set_data64(sqe, reqId);
#else
    io_uring_sqe_set_data(sqe, reinterpret_cast<void*>(reqId));
#endif
    ++numInFlightRequests_;
    ++numInFlightBuffers_;
#else
    sendChunkSync(sockfd, bufferIndex, numBytes, flags);
#endif
  }

  // ___________________________________________________________________________
  // Flush all queued SQEs to the kernel.
  void submit() {
#ifdef QLEVER_HAS_LIBURING
    if (ringInitialized_) {
      int ret = io_uring_submit(&ring_);
      if (ret < 0 && ret != -EAGAIN && ret != -EBUSY) {
        AD_THROW(absl::StrCat("io_uring_submit failed (errno: ", -ret, ")"));
      }
    }
#endif
  }

  // ___________________________________________________________________________
  // Wait for and drain all in-flight requests and kernel notifications,
  // ensuring all buffers are recycled back to the pool.
  void flushAndDrainAll() {
#ifdef QLEVER_HAS_LIBURING
    if (!ringInitialized_) {
      return;
    }

    submit();
    while (numInFlightRequests_ > 0 || numInFlightBuffers_ > 0) {
      drainOneCqe();
    }
#endif
  }

  // ___________________________________________________________________________
  // Drain at least `minCompletions` from the completion queue.
  void drainCompletions(size_t minCompletions = 1) {
#ifdef QLEVER_HAS_LIBURING
    if (!ringInitialized_) {
      return;
    }

    size_t reaped = 0;
    while (numInFlightRequests_ > 0 && reaped < minCompletions) {
      drainOneCqe();
      ++reaped;
    }
#else
    (void)minCompletions;
#endif
  }

  // ___________________________________________________________________________
  // Accessors
  [[nodiscard]] const ZeroCopyBufferPool& bufferPool() const noexcept {
    return bufferPool_;
  }
  [[nodiscard]] ZeroCopyBufferPool& bufferPool() noexcept {
    return bufferPool_;
  }

  [[nodiscard]] size_t inFlightRequests() const noexcept {
    return numInFlightRequests_;
  }
  [[nodiscard]] size_t inFlightBuffers() const noexcept {
    return numInFlightBuffers_;
  }
  [[nodiscard]] size_t totalBytesSent() const noexcept {
    return totalBytesSent_;
  }
  [[nodiscard]] size_t totalPacketsSent() const noexcept {
    return totalPacketsSent_;
  }
  [[nodiscard]] bool isBuffersRegistered() const noexcept {
#ifdef QLEVER_HAS_LIBURING
    return buffersRegistered_;
#else
    return false;
#endif
  }
  [[nodiscard]] bool isRingInitialized() const noexcept {
#ifdef QLEVER_HAS_LIBURING
    return ringInitialized_;
#else
    return false;
#endif
  }

 private:
  void initRing() {
#ifdef QLEVER_HAS_LIBURING
    int ret =
        io_uring_queue_init(static_cast<unsigned int>(config_.ringEntries),
                            &ring_, config_.additionalFlags);
    if (ret < 0) {
      ringInitialized_ = false;
      AD_LOG_WARN << "io_uring_queue_init failed (errno: " << -ret
                  << "), falling back to synchronous send()\n";
      return;
    }
    ringInitialized_ = true;

    if (config_.useRegisteredBuffers) {
      const auto iov = bufferPool_.iovecs();
      int regRet = io_uring_register_buffers(
          &ring_, iov.data(), static_cast<unsigned int>(iov.size()));
      if (regRet < 0) {
        AD_LOG_WARN << "io_uring_register_buffers failed (errno: " << -regRet
                    << "), falling back to unregistered zero-copy send\n";
        buffersRegistered_ = false;
      } else {
        buffersRegistered_ = true;
      }
    }
#endif
  }

  void teardown() noexcept {
#ifdef QLEVER_HAS_LIBURING
    if (ringInitialized_) {
      // Best-effort: submit queued SQEs, then reuse `drainOneCqe()` so the
      // MORE/NOTIF lifecycle and buffer release stay consistent. Drain
      // errors cannot propagate from the destructor, and a stall (e.g.
      // CQEs already consumed) must not hang destruction.
      io_uring_submit(&ring_);
      while (numInFlightRequests_ > 0 || numInFlightBuffers_ > 0) {
        if (io_uring_cq_ready(&ring_) == 0) {
          break;
        }
        try {
          drainOneCqe();
        } catch (...) {
          break;
        }
      }

      // Only unregister while no buffer is still pinned by the kernel:
      // unregistering with in-flight zero-copy buffers would free user memory
      // (rawBuffer_) that the kernel may still DMA into.
      if (buffersRegistered_ && numInFlightBuffers_ == 0) {
        io_uring_unregister_buffers(&ring_);
        buffersRegistered_ = false;
      } else if (buffersRegistered_) {
        AD_LOG_WARN << "ZeroCopySocketSender destroyed with buffers still "
                       "pinned by the kernel; skipping unregister, call "
                       "flushAndDrainAll() before destruction\n";
        buffersRegistered_ = false;
      }

      io_uring_queue_exit(&ring_);
      ringInitialized_ = false;
    }
#endif
  }

#ifdef QLEVER_HAS_LIBURING
  // ___________________________________________________________________________
  // Drain a single CQE and handle zero-copy dual notification lifecycle.
  void drainOneCqe() {
    io_uring_cqe* cqe = nullptr;
    __kernel_timespec timeout{};
    timeout.tv_sec = kZeroCopyPeerStallTimeoutSeconds;
    int ret = io_uring_wait_cqe_timeout(&ring_, &cqe, &timeout);
    if (ret == -ETIME) {
      AD_THROW(
          "timed out waiting for an io_uring send completion (peer stalled)");
    }
    if (ret < 0) {
      AD_THROW(absl::StrCat("io_uring_wait_cqe failed (errno: ", -ret, ")"));
    }

    const int res = cqe->res;
    const unsigned int flags = cqe->flags;
#ifdef QLEVER_HAS_LIBURING_SEND_ZC
    const uint64_t reqId = io_uring_cqe_get_data64(cqe);
#else
    const uint64_t reqId =
        reinterpret_cast<uint64_t>(io_uring_cqe_get_data(cqe));
#endif
    io_uring_cqe_seen(&ring_, cqe);

    const size_t tableIdx = reqId % inFlightTable_.size();
    auto& entry = inFlightTable_[tableIdx];
    AD_CORRECTNESS_CHECK(entry.active);

    if (entry.waitingForNotification) {
      // CQE 2: Kernel buffer release notification (IORING_CQE_F_NOTIF).
      // Buffer can now be safely recycled for subsequent writes.
      bufferPool_.releaseSlot(entry.bufferIndex);
      AD_CORRECTNESS_CHECK(numInFlightBuffers_ > 0);
      AD_CORRECTNESS_CHECK(numInFlightRequests_ > 0);
      --numInFlightBuffers_;
      --numInFlightRequests_;
      entry.active = false;
      entry.waitingForNotification = false;
      return;
    }

    if (res < 0) {
      if (flags & IORING_CQE_F_MORE) {
        // The kernel still holds the buffer and will deliver a NOTIF CQE
        // that recycles it; keep the entry alive for that CQE and only
        // report the error.
        entry.waitingForNotification = true;
      } else {
        bufferPool_.releaseSlot(entry.bufferIndex);
        AD_CORRECTNESS_CHECK(numInFlightBuffers_ > 0);
        AD_CORRECTNESS_CHECK(numInFlightRequests_ > 0);
        --numInFlightBuffers_;
        --numInFlightRequests_;
        entry.active = false;
      }
      AD_THROW(absl::StrCat("io_uring send error (res: ", res,
                            ", errno: ", -res, ": ", std::strerror(-res), ")"));
    }

    totalBytesSent_ += static_cast<size_t>(res);
    ++totalPacketsSent_;

    if (flags & IORING_CQE_F_MORE) {
      // Kernel is holding the buffer for zero-copy DMA; wait for CQE 2 (NOTIF)
      // Below we still validate the byte count, mirroring the non-MORE path:
      // a truncated send must not be counted as success and freed on NOTIF.
      if (static_cast<size_t>(res) != entry.expectedBytes) {
        entry.waitingForNotification = true;
        AD_THROW(absl::StrCat("io_uring short send with MORE (res: ", res,
                              ", expected: ", entry.expectedBytes, ")"));
      }
      entry.waitingForNotification = true;
    } else {
      if (static_cast<size_t>(res) != entry.expectedBytes) {
        bufferPool_.releaseSlot(entry.bufferIndex);
        AD_CORRECTNESS_CHECK(numInFlightBuffers_ > 0);
        AD_CORRECTNESS_CHECK(numInFlightRequests_ > 0);
        --numInFlightBuffers_;
        --numInFlightRequests_;
        entry.active = false;
        AD_THROW(absl::StrCat("io_uring short send (res: ", res,
                              ", expected: ", entry.expectedBytes, ")"));
      }
      // Standard completion or synchronous copy; release buffer immediately
      bufferPool_.releaseSlot(entry.bufferIndex);
      AD_CORRECTNESS_CHECK(numInFlightBuffers_ > 0);
      AD_CORRECTNESS_CHECK(numInFlightRequests_ > 0);
      --numInFlightBuffers_;
      --numInFlightRequests_;
      entry.active = false;
    }
  }
#endif

  // Synchronous send fallback. Guarantees full delivery or throws: loops
  // over short sends, retries `EINTR`, and polls for writability on
  // `EAGAIN`/`EWOULDBLOCK` (callers run on non-blocking Beast sockets).
  void sendChunkSync(int sockfd, uint32_t bufferIndex, size_t numBytes,
                     int flags) {
    const auto slotSpan = bufferPool_.getSlotSpan(bufferIndex);
    size_t sent = 0;
    while (sent < numBytes) {
      ssize_t bytesSent = ::send(sockfd, slotSpan.data() + sent,
                                 numBytes - sent, flags | kSendNoSignalFlag);
      if (bytesSent < 0) {
        if (errno == EINTR) {
          continue;
        }
        if (errno == EAGAIN || errno == EWOULDBLOCK) {
          pollfd pfd{sockfd, POLLOUT, 0};
          // Bounded wait (matching the session's 30s read timeout): a stalled
          // peer must surface as an error, not hang the session forever.
          int ret = ::poll(&pfd, 1, kZeroCopyPeerStallTimeoutSeconds * 1000);
          if (ret == 0) {
            bufferPool_.releaseSlot(bufferIndex);
            AD_THROW("send timed out waiting for socket writability");
          }
          if (ret < 0) {
            if (errno == EINTR) {
              continue;
            }
            bufferPool_.releaseSlot(bufferIndex);
            AD_THROW(
                absl::StrCat("send failed (errno: ", strerror(errno), ")"));
          }
          if (pfd.revents == 0) {
            // Spurious wakeup, keep waiting within the timeout above.
            continue;
          }
          if (pfd.revents & (POLLERR | POLLHUP | POLLNVAL)) {
            bufferPool_.releaseSlot(bufferIndex);
            AD_THROW("send failed (peer closed)");
          }
          continue;
        }
        bufferPool_.releaseSlot(bufferIndex);
        AD_THROW(absl::StrCat("send failed (errno: ", strerror(errno), ")"));
      }
      if (bytesSent == 0) {
        bufferPool_.releaseSlot(bufferIndex);
        AD_THROW("send returned 0 (peer closed)");
      }
      sent += static_cast<size_t>(bytesSent);
    }

    totalBytesSent_ += sent;
    ++totalPacketsSent_;
    bufferPool_.releaseSlot(bufferIndex);
  }
};

}  // namespace ad_utility

#endif  // QLEVER_SRC_UTIL_ZEROCOPYSOCKETSENDER_H
