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
#include <limits>
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
    // Guard the multiplication below (and the `i * slotSize_` striding in the
    // loop) against `size_t` wraparound on adversarial inputs.
    AD_CONTRACT_CHECK(numSlots <=
                      std::numeric_limits<size_t>::max() / slotSizeBytes);
    totalBytes_ = numSlots * slotSizeBytes;
    // Reserve before allocating the arena: a throwing `reserve` after
    // `posix_memalign` would leak the arena (the destructor does not run for
    // an object whose constructor throws). The `push_back`s below do not throw.
    iovecs_.reserve(numSlots_);

    int ret = posix_memalign(&rawBuffer_, kDirectIoAlignment, totalBytes_);
    if (ret != 0 || rawBuffer_ == nullptr) {
      AD_THROW("posix_memalign failed to allocate pinned buffer arena");
    }

    // Zero out memory to pre-fault pages before kernel DMA registration.
    std::memset(rawBuffer_, 0, totalBytes_);

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
// One read for a `Ring` (see `BasicRegisteredIoUringReader`): read `numBytes`
// at `fileOffset` of `fd` into `destination`. With `fixedFile`, `fd` is an
// index into the registered file table; with `fixedBuffer`, `destination`
// lies in the registered buffer `bufferIndex`. `userData` comes back with
// the completion.
struct RingRead {
  int fd = -1;
  char* destination = nullptr;
  unsigned numBytes = 0;
  uint64_t fileOffset = 0;
  bool fixedFile = false;
  bool fixedBuffer = false;
  int bufferIndex = 0;
  uint64_t userData = 0;
};

// The completion of a `RingRead`: bytes read or `-errno`, and its `userData`.
struct RingCompletion {
  int result = 0;
  uint64_t userData = 0;
};

// _____________________________________________________________________________
// The `Ring` of `RegisteredIoUringReader`: a thin adapter over liburing. All
// functions return `0` or a negative `errno` like liburing. Without liburing
// `init` fails, so the reader uses its synchronous fallback.
class LiburingRing {
#ifdef QLEVER_HAS_LIBURING
  io_uring ring_{};
#endif

 public:
  int init([[maybe_unused]] unsigned entries, [[maybe_unused]] unsigned flags) {
#ifdef QLEVER_HAS_LIBURING
    return io_uring_queue_init(entries, &ring_, flags);
#else
    return -ENOSYS;
#endif
  }
#ifdef QLEVER_HAS_LIBURING
  void exit() noexcept { io_uring_queue_exit(&ring_); }
  int registerFiles(ql::span<const int> fds) {
    return io_uring_register_files(&ring_, fds.data(),
                                   static_cast<unsigned>(fds.size()));
  }
  void unregisterFiles() noexcept { io_uring_unregister_files(&ring_); }
  int registerBuffers(ql::span<const iovec> iovecs) {
    return io_uring_register_buffers(&ring_, iovecs.data(),
                                     static_cast<unsigned>(iovecs.size()));
  }
  void unregisterBuffers() noexcept { io_uring_unregister_buffers(&ring_); }
  // Prepare `read`; return false if the submission queue is full.
  bool prepare(const RingRead& read) {
    io_uring_sqe* sqe = io_uring_get_sqe(&ring_);
    if (sqe == nullptr) {
      return false;
    }
    if (read.fixedBuffer) {
      io_uring_prep_read_fixed(sqe, read.fd, read.destination, read.numBytes,
                               read.fileOffset, read.bufferIndex);
    } else {
      io_uring_prep_read(sqe, read.fd, read.destination, read.numBytes,
                         read.fileOffset);
    }
    sqe->flags |= read.fixedFile ? IOSQE_FIXED_FILE : 0;
    // The pointer-sized `user_data` helper exists in every liburing version.
    io_uring_sqe_set_data(sqe, reinterpret_cast<void*>(read.userData));
    return true;
  }
  int submit() { return io_uring_submit(&ring_); }
  // Wait for and consume one completion.
  int waitCompletion(RingCompletion& completion) {
    io_uring_cqe* cqe = nullptr;
    int ret = io_uring_wait_cqe(&ring_, &cqe);
    if (ret == 0) {
      completion.result = cqe->res;
      completion.userData =
          reinterpret_cast<uint64_t>(io_uring_cqe_get_data(cqe));
      io_uring_cqe_seen(&ring_, cqe);
    }
    return ret;
  }
#else
  // Never called: `init` fails, so the reader never uses the ring.
  void exit() noexcept {}
  int registerFiles(ql::span<const int>) { return -ENOSYS; }
  void unregisterFiles() noexcept {}
  int registerBuffers(ql::span<const iovec>) { return -ENOSYS; }
  void unregisterBuffers() noexcept {}
  bool prepare(const RingRead&) { return false; }
  int submit() { return -ENOSYS; }
  int waitCompletion(RingCompletion&) { return -ENOSYS; }
#endif
};

// _____________________________________________________________________________
// Batch reader over an io_uring `Ring` (`LiburingRing` in production, a fake
// in the tests) with optional registered files (`IORING_REGISTER_FILES`),
// registered fixed buffers (`IORING_REGISTER_BUFFERS` with
// `IORING_OP_READ_FIXED`) and `O_DIRECT` alignment checks. If the ring cannot
// be initialized, every batch is read synchronously with `pread` on the
// calling thread. Not thread-safe.
template <typename Ring>
class BasicRegisteredIoUringReader {
 public:
  using BatchId = uint64_t;

 private:
  RegisteredReaderConfig config_;
  Ring ring_{};
  bool ringInitialized_ = false;
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
  // The (partial) result of every batch that `waitBatch` has not yet handed
  // out, and the first error of a batch. Completions of any batch can be
  // reaped while another batch is submitted or awaited.
  ad_utility::HashMap<BatchId, BatchResult> results_;
  ad_utility::HashMap<BatchId, std::string> errors_;
  uint64_t nextReqId_ = 0;

 public:
  explicit BasicRegisteredIoUringReader(
      RegisteredReaderConfig config = RegisteredReaderConfig{},
      Ring ring = Ring{})
      : config_{config}, ring_{std::move(ring)} {
    AD_CONTRACT_CHECK(config_.ringEntries > 0);
    int ret = ring_.init(config_.ringEntries, config_.additionalFlags);
    ringInitialized_ = ret >= 0;
    if (!ringInitialized_) {
      AD_LOG_WARN << "io_uring_queue_init failed: errno " << -ret
                  << ", falling back to synchronous I/O\n";
    }
  }

  ~BasicRegisteredIoUringReader() { teardown(); }

  BasicRegisteredIoUringReader(const BasicRegisteredIoUringReader&) = delete;
  BasicRegisteredIoUringReader& operator=(const BasicRegisteredIoUringReader&) =
      delete;

  BasicRegisteredIoUringReader(BasicRegisteredIoUringReader&& other) noexcept
      : config_{other.config_},
        ring_{std::move(other.ring_)},
        ringInitialized_{std::exchange(other.ringInitialized_, false)},
        filesRegistered_{std::exchange(other.filesRegistered_, false)},
        buffersRegistered_{std::exchange(other.buffersRegistered_, false)},
        registeredFds_{std::move(other.registeredFds_)},
        registeredIovecs_{std::move(other.registeredIovecs_)},
        numInFlightRequests_{std::exchange(other.numInFlightRequests_, 0)},
        nextBatchId_{other.nextBatchId_},
        inFlightByReqId_{std::move(other.inFlightByReqId_)},
        inFlightByBatchId_{std::move(other.inFlightByBatchId_)},
        results_{std::move(other.results_)},
        errors_{std::move(other.errors_)},
        nextReqId_{other.nextReqId_} {}

  BasicRegisteredIoUringReader& operator=(
      BasicRegisteredIoUringReader&& other) noexcept {
    if (this != &other) {
      teardown();
      config_ = other.config_;
      ring_ = std::move(other.ring_);
      ringInitialized_ = std::exchange(other.ringInitialized_, false);
      filesRegistered_ = std::exchange(other.filesRegistered_, false);
      buffersRegistered_ = std::exchange(other.buffersRegistered_, false);
      registeredFds_ = std::move(other.registeredFds_);
      registeredIovecs_ = std::move(other.registeredIovecs_);
      numInFlightRequests_ = std::exchange(other.numInFlightRequests_, 0);
      nextBatchId_ = other.nextBatchId_;
      inFlightByReqId_ = std::move(other.inFlightByReqId_);
      inFlightByBatchId_ = std::move(other.inFlightByBatchId_);
      results_ = std::move(other.results_);
      errors_ = std::move(other.errors_);
      nextReqId_ = other.nextReqId_;
    }
    return *this;
  }

  // ___________________________________________________________________________
  // IORING_REGISTER_FILES: register `fds` in the ring's fixed file table, so
  // that a read need not look up its descriptor. `BlockReadRequest::fileIndex`
  // then indexes `fds`. Throws without a live ring or if the kernel refuses.
  void registerFiles(ql::span<const int> fds) {
    AD_CONTRACT_CHECK(!fds.empty());
    if (!ringInitialized_) {
      AD_THROW("io_uring is not initialized");
    }
    unregisterFiles();
    registeredFds_.assign(fds.begin(), fds.end());
    int ret = ring_.registerFiles(registeredFds_);
    if (ret < 0) {
      registeredFds_.clear();
      AD_THROW(
          absl::StrCat("io_uring_register_files failed (errno: ", -ret, ")"));
    }
    filesRegistered_ = true;
  }

  void unregisterFiles() noexcept {
    if (filesRegistered_) {
      ring_.unregisterFiles();
      filesRegistered_ = false;
      registeredFds_.clear();
    }
  }

  // ___________________________________________________________________________
  // IORING_REGISTER_BUFFERS: pin `iovecs` once, so that fixed reads into them
  // (`IORING_OP_READ_FIXED`) need no per-read page pinning. Throws without a
  // live ring or if the kernel refuses (e.g. `RLIMIT_MEMLOCK`).
  void registerBuffers(ql::span<const iovec> iovecs) {
    AD_CONTRACT_CHECK(!iovecs.empty());
    if (!ringInitialized_) {
      AD_THROW("io_uring is not initialized");
    }
    unregisterBuffers();
    registeredIovecs_.assign(iovecs.begin(), iovecs.end());
    int ret = ring_.registerBuffers(registeredIovecs_);
    if (ret < 0) {
      registeredIovecs_.clear();
      AD_THROW(
          absl::StrCat("io_uring_register_buffers failed (errno: ", -ret, ")"));
    }
    buffersRegistered_ = true;
  }

  void unregisterBuffers() noexcept {
    if (buffersRegistered_) {
      ring_.unregisterBuffers();
      buffersRegistered_ = false;
      registeredIovecs_.clear();
    }
  }

  // ___________________________________________________________________________
  // Submit a batch of block reads and return its id for `waitBatch` (`0` for
  // an empty batch). Without a ring, the batch is read here synchronously.
  [[nodiscard]] BatchId submitBatch(ql::span<const BlockReadRequest> requests) {
    if (requests.empty()) {
      return 0;
    }
    const BatchId batchId = nextBatchId_++;
    if (!ringInitialized_) {
      results_[batchId] = submitBatchSync(requests);
      return batchId;
    }
    const bool fixedFiles = filesRegistered_ && config_.useRegisteredFiles;
    const bool fixedBuffers =
        buffersRegistered_ && config_.useRegisteredBuffers;
    // Validate the whole batch before anything is submitted.
    for (const auto& req : requests) {
      if (fixedFiles) {
        AD_CONTRACT_CHECK(req.fileIndex < registeredFds_.size());
      }
      if (fixedBuffers) {
        AD_CONTRACT_CHECK(req.bufferIndex < registeredIovecs_.size());
        // The target must be the given range of the registered buffer.
        const iovec& buffer = registeredIovecs_[req.bufferIndex];
        AD_CONTRACT_CHECK(req.destination ==
                          static_cast<char*>(buffer.iov_base) +
                              req.bufferOffset);
        AD_CONTRACT_CHECK(req.bufferOffset + req.numBytes <= buffer.iov_len);
      }
    }
    inFlightByBatchId_[batchId] = requests.size();
    results_[batchId] = BatchResult{};
    for (const auto& req : requests) {
      const uint64_t reqId = nextReqId_++;
      RingRead read{static_cast<int>(req.fileIndex),
                    req.destination,
                    req.numBytes,
                    req.fileOffset,
                    fixedFiles,
                    fixedBuffers,
                    static_cast<int>(req.bufferIndex),
                    reqId};
      // With the ring full, submit and reap completions until an entry frees.
      while (numInFlightRequests_ >= config_.ringEntries) {
        submitRing();
        drainOneCompletion();
      }
      while (!ring_.prepare(read)) {
        submitRing();
        AD_CORRECTNESS_CHECK(numInFlightRequests_ > 0,
                             "io_uring submission queue is full");
        drainOneCompletion();
      }
      inFlightByReqId_[reqId] = InFlightMeta{batchId, req.numBytes};
      ++numInFlightRequests_;
    }
    submitRing();
    return batchId;
  }

  // ___________________________________________________________________________
  // Block until all reads of `batchId` have completed and return their result.
  // An unknown id (or `0`) yields an empty result. Throws on an I/O error or a
  // short read of the batch; all its reads are reaped first, so the reader
  // stays usable.
  BatchResult waitBatch(BatchId batchId) {
    while (inFlightByBatchId_.contains(batchId)) {
      drainOneCompletion();
    }
    auto it = results_.find(batchId);
    if (it == results_.end()) {
      return BatchResult{};
    }
    BatchResult result = it->second;
    results_.erase(it);
    if (auto error = errors_.find(batchId); error != errors_.end()) {
      std::string message = std::move(error->second);
      errors_.erase(error);
      AD_THROW(message);
    }
    return result;
  }

  // ___________________________________________________________________________
  // Synchronous `pread` of `dest.size()` bytes at `offset` (`O_DIRECT`
  // alignment is checked if `directIo`). Throws on an error or short read.
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

  [[nodiscard]] bool isRingInitialized() const noexcept {
    return ringInitialized_;
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
  [[nodiscard]] Ring& ring() noexcept { return ring_; }

 private:
  void submitRing() {
    int ret = ring_.submit();
    if (ret < 0 && ret != -EAGAIN && ret != -EBUSY) {
      AD_THROW(absl::StrCat("io_uring_submit failed (errno: ", -ret, ")"));
    }
  }

  // Reap in-flight reads, unregister, and release the ring.
  void teardown() noexcept {
    if (!ringInitialized_) {
      return;
    }
    RingCompletion completion;
    while (numInFlightRequests_ > 0 && ring_.waitCompletion(completion) == 0) {
      --numInFlightRequests_;
    }
    unregisterBuffers();
    unregisterFiles();
    ring_.exit();
    ringInitialized_ = false;
  }

  // Wait for one completion and account for it in the result (or the error)
  // of its batch.
  void drainOneCompletion() {
    RingCompletion completion;
    int ret = ring_.waitCompletion(completion);
    if (ret < 0) {
      AD_THROW(absl::StrCat("io_uring_wait_cqe failed (errno: ", -ret, ")"));
    }
    --numInFlightRequests_;
    auto it = inFlightByReqId_.find(completion.userData);
    AD_CORRECTNESS_CHECK(it != inFlightByReqId_.end());
    const InFlightMeta meta = it->second;
    inFlightByReqId_.erase(it);
    auto batchIt = inFlightByBatchId_.find(meta.batchId);
    AD_CORRECTNESS_CHECK(batchIt != inFlightByBatchId_.end());
    if (--batchIt->second == 0) {
      inFlightByBatchId_.erase(batchIt);
    }
    const int res = completion.result;
    if (res < 0) {
      errors_.try_emplace(meta.batchId, absl::StrCat("io_uring read failed "
                                                     "(errno: ",
                                                     -res, ")"));
    } else if (static_cast<size_t>(res) != meta.expectedBytes) {
      errors_.try_emplace(meta.batchId,
                          absl::StrCat("io_uring short read: expected ",
                                       meta.expectedBytes, " got ", res));
    } else {
      auto& result = results_[meta.batchId];
      ++result.requestsCompleted;
      result.totalBytesRead += static_cast<size_t>(res);
    }
  }

  // Read all `requests` with `pread` and return their result. `readSync`
  // throws on a failed or short read.
  BatchResult submitBatchSync(ql::span<const BlockReadRequest> requests) {
    BatchResult result;
    for (const auto& req : requests) {
      readSync(static_cast<int>(req.fileIndex), req.fileOffset,
               {req.destination, req.numBytes}, config_.useDirectIo);
      ++result.requestsCompleted;
      result.totalBytesRead += req.numBytes;
    }
    return result;
  }
};

// The production reader.
using RegisteredIoUringReader = BasicRegisteredIoUringReader<LiburingRing>;

}  // namespace ad_utility::export_prototypes

#endif  // QLEVER_SRC_UTIL_EXPORT_PROTOTYPES_REGISTEREDIOURINGREADER_H
