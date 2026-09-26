// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_UTIL_EXPORT_PROTOTYPES_REGISTEREDIOURINGREADER_H
#define QLEVER_SRC_UTIL_EXPORT_PROTOTYPES_REGISTEREDIOURINGREADER_H

#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/uio.h>
#include <unistd.h>

#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <optional>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "backports/concepts.h"
#include "backports/memory_resource.h"
#include "backports/span.h"
#include "util/AlignedAllocator.h"
#include "util/Exception.h"
#include "util/HashMap.h"
#include "util/Log.h"

#if defined(__has_include)
#if __has_include(<liburing.h>)
#define QLEVER_HAS_LIBURING 1
#include <liburing.h>
#endif
#endif

#if defined(QLEVER_HAS_IO_URING) && !defined(QLEVER_HAS_LIBURING)
#define QLEVER_HAS_LIBURING 1
#include <liburing.h>
#endif

namespace ad_utility::export_prototypes {

// Direct I/O block alignment constants for NVMe and modern Linux kernels.
inline constexpr size_t kDirectIoBlockSize = 4096;
inline constexpr size_t kDirectIoAlignment = 4096;

// _____________________________________________________________________________
// Helper to check whether a pointer or size is 4KB page/block aligned.
[[nodiscard]] constexpr bool isBlockAligned(uint64_t val) noexcept {
  return (val % kDirectIoBlockSize) == 0;
}

[[nodiscard]] inline bool isPointerAligned(
    const void* ptr, size_t alignment = kDirectIoAlignment) noexcept {
  return (reinterpret_cast<uintptr_t>(ptr) % alignment) == 0;
}

// _____________________________________________________________________________
// RAII wrapper for an open file descriptor with Direct I/O (O_DIRECT) support.
class DirectIoFile {
 private:
  int fd_ = -1;
  bool isDirect_ = false;
  uint64_t fileSize_ = 0;
  std::string path_;

 public:
  DirectIoFile() = default;

  DirectIoFile(std::string_view path, bool useDirectIo, bool readOnly = true) {
    open(path, useDirectIo, readOnly);
  }

  ~DirectIoFile() { close(); }

  DirectIoFile(const DirectIoFile&) = delete;
  DirectIoFile& operator=(const DirectIoFile&) = delete;

  DirectIoFile(DirectIoFile&& other) noexcept
      : fd_{std::exchange(other.fd_, -1)},
        isDirect_{other.isDirect_},
        fileSize_{other.fileSize_},
        path_{std::move(other.path_)} {}

  DirectIoFile& operator=(DirectIoFile&& other) noexcept {
    if (this != &other) {
      close();
      fd_ = std::exchange(other.fd_, -1);
      isDirect_ = other.isDirect_;
      fileSize_ = other.fileSize_;
      path_ = std::move(other.path_);
    }
    return *this;
  }

  void open(std::string_view path, bool useDirectIo, bool readOnly = true) {
    close();
    path_ = std::string(path);
    isDirect_ = useDirectIo;

    int flags = readOnly ? O_RDONLY : O_RDWR;
#ifdef O_DIRECT
    if (useDirectIo) {
      flags |= O_DIRECT;
    }
#endif
#ifdef O_NOATIME
    flags |= O_NOATIME;
#endif

    fd_ = ::open(path_.c_str(), flags);
#ifdef O_NOATIME
    // `O_NOATIME` is only permitted for the owner of the file.
    if (fd_ < 0 && errno == EPERM) {
      fd_ = ::open(path_.c_str(), flags & ~O_NOATIME);
    }
#endif
    if (fd_ < 0) {
      AD_THROW(absl::StrCat("Failed to open file: ", path_,
                            " (errno: ", strerror(errno), ")"));
    }

    struct stat st {};
    if (::fstat(fd_, &st) != 0) {
      close();
      AD_THROW(absl::StrCat("Failed to stat file: ", path_));
    }
    fileSize_ = static_cast<uint64_t>(st.st_size);
  }

  void close() noexcept {
    if (fd_ >= 0) {
      ::close(fd_);
      fd_ = -1;
    }
  }

  [[nodiscard]] int fd() const noexcept { return fd_; }
  [[nodiscard]] bool isOpen() const noexcept { return fd_ >= 0; }
  [[nodiscard]] bool isDirect() const noexcept { return isDirect_; }
  [[nodiscard]] uint64_t size() const noexcept { return fileSize_; }
  [[nodiscard]] const std::string& path() const noexcept { return path_; }
};

// _____________________________________________________________________________
// Aligned PMR Memory Arena for DMA and io_uring fixed buffer registration.
// Guarantees 4KB page alignment for Direct I/O and zero-copy DMA pinning.
class PinnedArena {
 private:
  void* rawBuffer_ = nullptr;
  size_t totalBytes_ = 0;
  size_t slotSize_ = 0;
  size_t numSlots_ = 0;
  std::vector<iovec> iovecs_;

 public:
  PinnedArena() = default;

  // Allocate an arena of `numSlots` slots, each of size `slotSizeBytes`.
  // `slotSizeBytes` must be a multiple of 4KB for O_DIRECT alignment.
  PinnedArena(size_t numSlots, size_t slotSizeBytes = kDirectIoBlockSize) {
    AD_CONTRACT_CHECK(numSlots > 0);
    AD_CONTRACT_CHECK(slotSizeBytes > 0);
    AD_CONTRACT_CHECK(isBlockAligned(slotSizeBytes));

    slotSize_ = slotSizeBytes;
    numSlots_ = numSlots;
    totalBytes_ = numSlots * slotSizeBytes;

    int ret = posix_memalign(&rawBuffer_, kDirectIoAlignment, totalBytes_);
    if (ret != 0 || rawBuffer_ == nullptr) {
      AD_THROW("posix_memalign failed to allocate pinned buffer arena");
    }

    // Zero out memory to pre-fault pages before kernel DMA registration.
    std::memset(rawBuffer_, 0, totalBytes_);

    iovecs_.reserve(numSlots_);
    auto* basePtr = static_cast<char*>(rawBuffer_);
    for (size_t i = 0; i < numSlots_; ++i) {
      iovecs_.push_back(
          iovec{.iov_base = basePtr + (i * slotSize_), .iov_len = slotSize_});
    }
  }

  ~PinnedArena() {
    if (rawBuffer_ != nullptr) {
      std::free(rawBuffer_);
      rawBuffer_ = nullptr;
    }
  }

  PinnedArena(const PinnedArena&) = delete;
  PinnedArena& operator=(const PinnedArena&) = delete;

  PinnedArena(PinnedArena&& other) noexcept
      : rawBuffer_{std::exchange(other.rawBuffer_, nullptr)},
        totalBytes_{std::exchange(other.totalBytes_, 0)},
        slotSize_{std::exchange(other.slotSize_, 0)},
        numSlots_{std::exchange(other.numSlots_, 0)},
        iovecs_{std::move(other.iovecs_)} {}

  PinnedArena& operator=(PinnedArena&& other) noexcept {
    if (this != &other) {
      if (rawBuffer_ != nullptr) {
        std::free(rawBuffer_);
      }
      rawBuffer_ = std::exchange(other.rawBuffer_, nullptr);
      totalBytes_ = std::exchange(other.totalBytes_, 0);
      slotSize_ = std::exchange(other.slotSize_, 0);
      numSlots_ = std::exchange(other.numSlots_, 0);
      iovecs_ = std::move(other.iovecs_);
    }
    return *this;
  }

  [[nodiscard]] size_t numSlots() const noexcept { return numSlots_; }
  [[nodiscard]] size_t slotSize() const noexcept { return slotSize_; }
  [[nodiscard]] size_t totalBytes() const noexcept { return totalBytes_; }
  [[nodiscard]] char* data() noexcept { return static_cast<char*>(rawBuffer_); }
  [[nodiscard]] const char* data() const noexcept {
    return static_cast<const char*>(rawBuffer_);
  }

  // Access a specific block slot as a span.
  [[nodiscard]] ql::span<char> getSlotSpan(size_t slotIndex) {
    AD_CONTRACT_CHECK(slotIndex < numSlots_);
    auto* slotPtr = static_cast<char*>(rawBuffer_) + (slotIndex * slotSize_);
    return {slotPtr, slotSize_};
  }

  [[nodiscard]] ql::span<const char> getSlotSpan(size_t slotIndex) const {
    AD_CONTRACT_CHECK(slotIndex < numSlots_);
    const auto* slotPtr =
        static_cast<const char*>(rawBuffer_) + (slotIndex * slotSize_);
    return {slotPtr, slotSize_};
  }

  [[nodiscard]] ql::span<const iovec> iovecs() const noexcept {
    return {iovecs_.data(), iovecs_.size()};
  }
};

// _____________________________________________________________________________
// Invariant-proven descriptor for a block read request.
struct BlockReadRequest {
  uint32_t fileIndex = 0;       // Registered file index (or raw fd if unpinned)
  uint64_t fileOffset = 0;      // File byte offset (4KB aligned for O_DIRECT)
  uint32_t bufferIndex = 0;     // Registered buffer index
  uint32_t bufferOffset = 0;    // Offset within registered buffer (4KB aligned)
  uint32_t numBytes = 0;        // Number of bytes to read (multiple of 4KB)
  char* destination = nullptr;  // Target memory address (4KB aligned)

  BlockReadRequest() = default;

  BlockReadRequest(uint32_t fIndex, uint64_t fOffset, uint32_t bufIndex,
                   uint32_t bufOffset, uint32_t bytes, char* dest,
                   bool requireDirectIoAlignment = true)
      : fileIndex{fIndex},
        fileOffset{fOffset},
        bufferIndex{bufIndex},
        bufferOffset{bufOffset},
        numBytes{bytes},
        destination{dest} {
    AD_CONTRACT_CHECK(numBytes > 0);
    AD_CONTRACT_CHECK(destination != nullptr);
    if (requireDirectIoAlignment) {
      AD_CONTRACT_CHECK(isBlockAligned(fileOffset));
      AD_CONTRACT_CHECK(isBlockAligned(bufferOffset));
      AD_CONTRACT_CHECK(isBlockAligned(numBytes));
      AD_CONTRACT_CHECK(isPointerAligned(destination));
    }
  }
};

// _____________________________________________________________________________
// Result of a completed I/O batch.
struct BatchResult {
  size_t requestsCompleted = 0;
  size_t totalBytesRead = 0;
  bool success = true;
};

// _____________________________________________________________________________
// Configuration options for RegisteredIoUringReader.
struct RegisteredReaderConfig {
  unsigned ringEntries = 512;
  bool useDirectIo = true;
  bool useRegisteredFiles = true;
  bool useRegisteredBuffers = true;
  unsigned additionalFlags = 0;
};

// _____________________________________________________________________________
// Deep Module: RegisteredIoUringReader
//
// Encapsulates kernel fixed-file table registration (`IORING_REGISTER_FILES`),
// fixed-buffer DMA page-pinning (`IORING_REGISTER_BUFFERS`), Direct I/O
// alignment enforcement (`O_DIRECT`), submission queue batching, and completion
// queue reaping behind a clean, zero-bookkeeping interface.
class RegisteredIoUringReader {
 public:
  using BatchId = uint64_t;

 private:
  RegisteredReaderConfig config_;
#ifdef QLEVER_HAS_LIBURING
  io_uring ring_{};
  bool ringInitialized_ = false;
#endif
  bool filesRegistered_ = false;
  bool buffersRegistered_ = false;

  std::vector<int> registeredFds_;
  std::vector<iovec> registeredIovecs_;

  size_t numInFlightRequests_ = 0;
  BatchId nextBatchId_ = 1;

  struct InFlightMeta {
    BatchId batchId;
    size_t expectedBytes;
  };
  ad_utility::HashMap<uint64_t, InFlightMeta> inFlightByReqId_;
  ad_utility::HashMap<BatchId, size_t> inFlightByBatchId_;
  uint64_t nextReqId_ = 0;

 public:
  explicit RegisteredIoUringReader(
      RegisteredReaderConfig config = RegisteredReaderConfig{})
      : config_{config} {
    initRing();
  }

  ~RegisteredIoUringReader() { teardown(); }

  RegisteredIoUringReader(const RegisteredIoUringReader&) = delete;
  RegisteredIoUringReader& operator=(const RegisteredIoUringReader&) = delete;

  RegisteredIoUringReader(RegisteredIoUringReader&& other) noexcept
      : config_{other.config_},
#ifdef QLEVER_HAS_LIBURING
        ring_{other.ring_},
        ringInitialized_{other.ringInitialized_},
#endif
        filesRegistered_{other.filesRegistered_},
        buffersRegistered_{other.buffersRegistered_},
        registeredFds_{std::move(other.registeredFds_)},
        registeredIovecs_{std::move(other.registeredIovecs_)},
        numInFlightRequests_{other.numInFlightRequests_},
        nextBatchId_{other.nextBatchId_},
        inFlightByReqId_{std::move(other.inFlightByReqId_)},
        inFlightByBatchId_{std::move(other.inFlightByBatchId_)},
        nextReqId_{other.nextReqId_} {
#ifdef QLEVER_HAS_LIBURING
    other.ringInitialized_ = false;
#endif
    other.filesRegistered_ = false;
    other.buffersRegistered_ = false;
    other.numInFlightRequests_ = 0;
  }

  RegisteredIoUringReader& operator=(RegisteredIoUringReader&& other) noexcept {
    if (this != &other) {
      teardown();
      config_ = other.config_;
#ifdef QLEVER_HAS_LIBURING
      ring_ = other.ring_;
      ringInitialized_ = other.ringInitialized_;
      other.ringInitialized_ = false;
#endif
      filesRegistered_ = other.filesRegistered_;
      buffersRegistered_ = other.buffersRegistered_;
      registeredFds_ = std::move(other.registeredFds_);
      registeredIovecs_ = std::move(other.registeredIovecs_);
      numInFlightRequests_ = other.numInFlightRequests_;
      nextBatchId_ = other.nextBatchId_;
      inFlightByReqId_ = std::move(other.inFlightByReqId_);
      inFlightByBatchId_ = std::move(other.inFlightByBatchId_);
      nextReqId_ = other.nextReqId_;

      other.filesRegistered_ = false;
      other.buffersRegistered_ = false;
      other.numInFlightRequests_ = 0;
    }
    return *this;
  }

  // ___________________________________________________________________________
  // IORING_REGISTER_FILES: Pre-register open file descriptors into the kernel
  // io_uring file table, eliminating fget()/fput() locking overhead per I/O.
  void registerFiles(ql::span<const int> fds) {
    AD_CONTRACT_CHECK(!fds.empty());

#ifdef QLEVER_HAS_LIBURING
    if (!ringInitialized_) {
      AD_THROW("io_uring is not initialized");
    }

    if (filesRegistered_) {
      unregisterFiles();
    }

    registeredFds_.assign(fds.begin(), fds.end());
    int ret = io_uring_register_files(
        &ring_, registeredFds_.data(),
        static_cast<unsigned int>(registeredFds_.size()));
    if (ret < 0) {
      registeredFds_.clear();
      AD_THROW(
          absl::StrCat("io_uring_register_files failed (errno: ", -ret, ")"));
    }
    filesRegistered_ = true;
#else
    registeredFds_.assign(fds.begin(), fds.end());
    filesRegistered_ = true;
#endif
  }

  void unregisterFiles() noexcept {
#ifdef QLEVER_HAS_LIBURING
    if (ringInitialized_ && filesRegistered_) {
      io_uring_unregister_files(&ring_);
      filesRegistered_ = false;
      registeredFds_.clear();
    }
#else
    filesRegistered_ = false;
    registeredFds_.clear();
#endif
  }

  // ___________________________________________________________________________
  // IORING_REGISTER_BUFFERS: Pre-register and page-pin PMR arena buffers for
  // direct zero-copy DMA, eliminating get_user_pages() and TLB shootdowns.
  void registerBuffers(ql::span<const iovec> iovecs) {
    AD_CONTRACT_CHECK(!iovecs.empty());

#ifdef QLEVER_HAS_LIBURING
    if (!ringInitialized_) {
      AD_THROW("io_uring is not initialized");
    }

    if (buffersRegistered_) {
      unregisterBuffers();
    }

    registeredIovecs_.assign(iovecs.begin(), iovecs.end());
    int ret = io_uring_register_buffers(
        &ring_, registeredIovecs_.data(),
        static_cast<unsigned int>(registeredIovecs_.size()));
    if (ret < 0) {
      registeredIovecs_.clear();
      AD_THROW(
          absl::StrCat("io_uring_register_buffers failed (errno: ", -ret, ")"));
    }
    buffersRegistered_ = true;
#else
    registeredIovecs_.assign(iovecs.begin(), iovecs.end());
    buffersRegistered_ = true;
#endif
  }

  void unregisterBuffers() noexcept {
#ifdef QLEVER_HAS_LIBURING
    if (ringInitialized_ && buffersRegistered_) {
      io_uring_unregister_buffers(&ring_);
      buffersRegistered_ = false;
      registeredIovecs_.clear();
    }
#else
    buffersRegistered_ = false;
    registeredIovecs_.clear();
#endif
  }

  // ___________________________________________________________________________
  // Submit a batch of block read requests to the kernel.
  // Supports registered files, registered fixed buffers, and Direct I/O.
  [[nodiscard]] BatchId submitBatch(ql::span<const BlockReadRequest> requests) {
    if (requests.empty()) {
      return 0;
    }

    const BatchId batchId = nextBatchId_++;

#ifdef QLEVER_HAS_LIBURING
    if (!ringInitialized_) {
      // Synchronous fallback if ring is not available
      submitBatchSync(requests);
      return batchId;
    }

    inFlightByBatchId_[batchId] = requests.size();

    for (const auto& req : requests) {
      // If submission queue is saturated, flush and drain completions to free
      // slots
      if (numInFlightRequests_ >= config_.ringEntries) {
        io_uring_submit(&ring_);
        while (numInFlightRequests_ >= config_.ringEntries) {
          drainOneCqe();
        }
      }

      io_uring_sqe* sqe = io_uring_get_sqe(&ring_);
      AD_CORRECTNESS_CHECK(sqe != nullptr);

      int targetFd = static_cast<int>(req.fileIndex);
      if (filesRegistered_ && config_.useRegisteredFiles) {
        AD_CONTRACT_CHECK(req.fileIndex < registeredFds_.size());
        targetFd = static_cast<int>(req.fileIndex);
      }

      if (buffersRegistered_ && config_.useRegisteredBuffers) {
        AD_CONTRACT_CHECK(req.bufferIndex < registeredIovecs_.size());
        // Fixed buffer read with kernel page-pinning
        io_uring_prep_read_fixed(sqe, targetFd, req.destination, req.numBytes,
                                 req.fileOffset, req.bufferIndex);
      } else {
        // Standard unpinned read
        io_uring_prep_read(sqe, targetFd, req.destination, req.numBytes,
                           req.fileOffset);
      }

      if (filesRegistered_ && config_.useRegisteredFiles) {
        sqe->flags |= IOSQE_FIXED_FILE;
      }

      const uint64_t reqId = nextReqId_++;
      inFlightByReqId_[reqId] = InFlightMeta{batchId, req.numBytes};
      // Store the id in the pointer-sized `user_data` field, which every
      // liburing version provides; the `*_data64` helpers need a liburing
      // newer than the distro one in the gcc11 CI image.
      io_uring_sqe_set_data(sqe, reinterpret_cast<void*>(reqId));
      ++numInFlightRequests_;
    }

    io_uring_submit(&ring_);
#else
    submitBatchSync(requests);
#endif

    return batchId;
  }

  // ___________________________________________________________________________
  // Block until all reads belonging to `batchId` have completed.
  BatchResult waitBatch(BatchId batchId) {
    if (batchId == 0) {
      return BatchResult{0, 0, true};
    }

#ifdef QLEVER_HAS_LIBURING
    if (!ringInitialized_) {
      return BatchResult{0, 0, true};
    }

    size_t completed = 0;
    size_t totalBytes = 0;

    while (inFlightByBatchId_.find(batchId) != inFlightByBatchId_.end()) {
      auto [bytesRead, bId] = drainOneCqe();
      if (bId == batchId) {
        ++completed;
        totalBytes += bytesRead;
      }
    }

    return BatchResult{completed, totalBytes, true};
#else
    return BatchResult{0, 0, true};
#endif
  }

  // ___________________________________________________________________________
  // Synchronous pread fallback (supporting both Direct I/O and buffered I/O).
  static void readSync(int fd, uint64_t offset, ql::span<char> dest,
                       bool directIo = true) {
    AD_CONTRACT_CHECK(fd >= 0);
    AD_CONTRACT_CHECK(!dest.empty());
    if (directIo) {
      AD_CONTRACT_CHECK(isBlockAligned(offset));
      AD_CONTRACT_CHECK(isBlockAligned(dest.size()));
      AD_CONTRACT_CHECK(isPointerAligned(dest.data()));
    }

    ssize_t bytesRead =
        ::pread(fd, dest.data(), dest.size(), static_cast<off_t>(offset));
    if (bytesRead < 0) {
      AD_THROW(absl::StrCat("pread failed (errno: ", strerror(errno), ")"));
    }
    if (static_cast<size_t>(bytesRead) != dest.size()) {
      AD_THROW("pread read fewer bytes than requested");
    }
  }

  [[nodiscard]] bool isFilesRegistered() const noexcept {
    return filesRegistered_;
  }
  [[nodiscard]] bool isBuffersRegistered() const noexcept {
    return buffersRegistered_;
  }
  [[nodiscard]] size_t inFlightCount() const noexcept {
    return numInFlightRequests_;
  }

 private:
  void initRing() {
#ifdef QLEVER_HAS_LIBURING
    int ret = io_uring_queue_init(config_.ringEntries, &ring_,
                                  config_.additionalFlags);
    if (ret < 0) {
      ringInitialized_ = false;
      AD_LOG_WARN << "io_uring_queue_init failed: errno " << -ret
                  << ", falling back to synchronous I/O\n";
    } else {
      ringInitialized_ = true;
    }
#endif
  }

  void teardown() noexcept {
#ifdef QLEVER_HAS_LIBURING
    if (ringInitialized_) {
      while (numInFlightRequests_ > 0) {
        io_uring_cqe* cqe = nullptr;
        if (io_uring_wait_cqe(&ring_, &cqe) < 0) {
          break;
        }
        io_uring_cqe_seen(&ring_, cqe);
        --numInFlightRequests_;
      }
      unregisterBuffers();
      unregisterFiles();
      io_uring_queue_exit(&ring_);
      ringInitialized_ = false;
    }
#else
    unregisterBuffers();
    unregisterFiles();
#endif
  }

#ifdef QLEVER_HAS_LIBURING
  std::pair<size_t, BatchId> drainOneCqe() {
    io_uring_cqe* cqe = nullptr;
    int ret = io_uring_wait_cqe(&ring_, &cqe);
    if (ret < 0) {
      AD_THROW(absl::StrCat("io_uring_wait_cqe failed (errno: ", -ret, ")"));
    }

    const int res = cqe->res;
    const uint64_t reqId = cqe->user_data;
    io_uring_cqe_seen(&ring_, cqe);
    --numInFlightRequests_;

    auto it = inFlightByReqId_.find(reqId);
    AD_CORRECTNESS_CHECK(it != inFlightByReqId_.end());
    const InFlightMeta meta = it->second;
    inFlightByReqId_.erase(it);

    if (res < 0) {
      AD_THROW(absl::StrCat("io_uring CQE error (res: ", res, ", errno: ", -res,
                            ")"));
    }
    if (static_cast<size_t>(res) != meta.expectedBytes) {
      AD_THROW(absl::StrCat("io_uring short read: expected ",
                            meta.expectedBytes, " got ", res));
    }

    auto batchIt = inFlightByBatchId_.find(meta.batchId);
    AD_CORRECTNESS_CHECK(batchIt != inFlightByBatchId_.end());
    if (--batchIt->second == 0) {
      inFlightByBatchId_.erase(batchIt);
    }

    return {static_cast<size_t>(res), meta.batchId};
  }
#endif

  void submitBatchSync(ql::span<const BlockReadRequest> requests) {
    for (const auto& req : requests) {
      int targetFd = static_cast<int>(req.fileIndex);
      if (filesRegistered_ && req.fileIndex < registeredFds_.size()) {
        targetFd = registeredFds_[req.fileIndex];
      }
      readSync(targetFd, req.fileOffset, {req.destination, req.numBytes},
               config_.useDirectIo);
    }
  }
};

}  // namespace ad_utility::export_prototypes

#endif  // QLEVER_SRC_UTIL_EXPORT_PROTOTYPES_REGISTEREDIOURINGREADER_H
