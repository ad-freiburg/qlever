// Copyright 2026, The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "util/IoUringManager.h"

#include <absl/cleanup/cleanup.h>
#include <unistd.h>

#include <cstring>
#include <mutex>
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
void SyncIoPolicy::addBatch(
    int fd, ql::span<const size_t> numBytesToReadPerRequest,
    ql::span<const uint64_t> fileOffsetPerRequest,
    ql::span<char*> targetBufferPerRequest, [[maybe_unused]] BatchHandle handle,
    [[maybe_unused]] const BatchReadOptions& options) const {
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
  // deliberately do not call `drainOneCqe` here: it throws on I/O errors, and a
  // destructor must not throw. We also stop if `io_uring_wait_cqe` fails, to
  // avoid spinning forever (it would not decrement the in-flight count).
  while (numInFlightReadRequests_ > 0) {
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
bool IoUringPolicy::registeredBuffersAvailable() {
  // Registering buffers while reads are in flight would make the kernel
  // quiesce the ring; defer to a batch that starts on an idle ring.
  if (registration_ == Registration::NotTried &&
      numInFlightReadRequests_ == 0) {
    try {
      arena_.emplace(ringSize_, kRegisteredSlotSize);
      auto iovecs = arena_->iovecs();
      int ret = io_uring_register_buffers(&ring_, iovecs.data(),
                                          static_cast<unsigned>(iovecs.size()));
      if (ret < 0) {
        AD_THROW(
            absl::StrCat("io_uring_register_buffers failed: ", strerror(-ret)));
      }
      copiesPerSlot_.resize(ringSize_);
      // Hand out the low slots first (`freeSlots_` is used as a stack).
      freeSlots_.resize(ringSize_);
      for (uint32_t i = 0; i < ringSize_; ++i) {
        freeSlots_[i] = ringSize_ - 1 - i;
      }
      registration_ = Registration::Registered;
      static std::once_flag logOnce;
      std::call_once(logOnce, [this]() {
        AD_LOG_INFO << "io_uring registered buffers are used for vocabulary "
                       "batch reads ("
                    << ringSize_ << " slots of " << kRegisteredSlotSize
                    << " bytes per ring)" << std::endl;
      });
    } catch (const std::exception& e) {
      arena_.reset();
      registration_ = Registration::Failed;
      AD_LOG_WARN << "Could not set up io_uring registered buffers ("
                  << e.what() << "); using plain io_uring reads" << std::endl;
    }
  }
  return registration_ == Registration::Registered;
}

//______________________________________________________________________________
io_uring_sqe* IoUringPolicy::claimSqe() {
  // The ring has no free slot, so make room: submit what we have prepared so
  // far and block until enough completions have been drained.
  if (numInFlightReadRequests_ >= ringSize_) {
    // Flush the SQEs prepared so far to the kernel so the kernel can start
    // servicing them. Their completions will free up submission slots.
    io_uring_submit(&ring_);
    while (numInFlightReadRequests_ >= ringSize_) {
      drainOneCqe();
    }
  }
  // Claim the next free SQE. The check above guarantees a slot is available,
  // so `io_uring_get_sqe` must not return `nullptr` here.
  io_uring_sqe* sqe = io_uring_get_sqe(&ring_);
  AD_CORRECTNESS_CHECK(sqe != nullptr);
  return sqe;
}

//______________________________________________________________________________
void IoUringPolicy::trackSqe(io_uring_sqe* sqe, const InFlightRead& read) {
  // Tag the SQE with a unique request id and record its metadata. io_uring
  // copies the request id (the SQE's `user_data`) verbatim into the matching
  // completion, so `drainOneCqe` can recover it.
  const uint64_t requestId = nextRequestIdToAssign_++;
  inFlightReadsByRequestId_[requestId] = read;
  // Store the id in the pointer-sized `user_data` field, which every
  // liburing version provides. The 64-bit `io_uring_sqe_set_data64` helper
  // requires a very recent liburing that older images (e.g. the gcc11 CI
  // image with its distro liburing) do not have yet.
  io_uring_sqe_set_data(sqe, reinterpret_cast<void*>(requestId));
  numInFlightReadRequests_++;
  // Count the read for its batch. A batch whose earlier reads all completed
  // while this batch was still being submitted was erased by `drainOneCqe`
  // and is added again here.
  numInFlightReadRequestsPerBatch_[read.batchHandle]++;
}

//______________________________________________________________________________
uint32_t IoUringPolicy::acquireSlot() {
  // Every slot that is not free belongs to an in-flight read, whose completion
  // frees it.
  while (freeSlots_.empty()) {
    AD_CORRECTNESS_CHECK(numInFlightReadRequests_ > 0);
    io_uring_submit(&ring_);
    drainOneCqe();
  }
  const uint32_t slot = freeSlots_.back();
  freeSlots_.pop_back();
  return slot;
}

//______________________________________________________________________________
void IoUringPolicy::submitDirectRead(const OpenDirectRead& read, int directIoFd,
                                     BatchHandle handle) {
  io_uring_sqe* sqe = claimSqe();
  io_uring_prep_read_fixed(
      sqe, directIoFd, arena_->getSlotSpan(read.slot).data(),
      static_cast<unsigned>(read.blockEnd - read.blockBegin),
      static_cast<__u64>(read.blockBegin), static_cast<int>(read.slot));
  // At the end of the file, the read returns fewer bytes than the rounded-up
  // size; only the requested bytes must be present.
  trackSqe(sqe, InFlightRead{handle, read.minNumBytes, read.slot});
}

//______________________________________________________________________________
void IoUringPolicy::addBatch(int fd,
                             ql::span<const size_t> numBytesToReadPerRequest,
                             ql::span<const uint64_t> fileOffsetPerRequest,
                             ql::span<char*> targetBufferPerRequest,
                             BatchHandle handle,
                             const BatchReadOptions& options) {
  if (numBytesToReadPerRequest.empty()) {
    return;
  }
  const bool useRegisteredBuffers =
      options.useRegisteredBuffers && registeredBuffersAvailable();
  const bool useDirectIo = useRegisteredBuffers && options.directIoFd >= 0;
  constexpr size_t block = export_prototypes::kDirectIoBlockSize;

  // The `O_DIRECT` read that the current request may still join.
  std::optional<OpenDirectRead> openDirectRead;
  auto submitOpenDirectRead = [&]() {
    if (openDirectRead.has_value()) {
      submitDirectRead(openDirectRead.value(), options.directIoFd, handle);
      openDirectRead.reset();
    }
  };

  // With `O_DIRECT`, read the aligned blocks that enclose the requested bytes
  // into a slot (joining the open read if they fit into its slot), and copy
  // out the requested bytes on completion. Return false if the blocks do not
  // fit into a slot; the request then needs a plain read.
  auto addToDirectRead = [&](size_t numBytesToRead, uint64_t fileOffset,
                             char* targetBuf) {
    const uint64_t blockBegin = fileOffset - fileOffset % block;
    const uint64_t blockEnd =
        (fileOffset + numBytesToRead + block - 1) / block * block;
    auto& open = openDirectRead;
    if (open.has_value() && blockBegin >= open->blockBegin &&
        blockEnd - open->blockBegin <= kRegisteredSlotSize) {
      open->blockEnd = std::max(open->blockEnd, blockEnd);
    } else {
      submitOpenDirectRead();
      if (blockEnd - blockBegin > kRegisteredSlotSize) {
        return false;
      }
      open = OpenDirectRead{acquireSlot(), blockBegin, blockEnd, 0};
    }
    const size_t offsetInSlot = fileOffset - open->blockBegin;
    open->minNumBytes =
        std::max(open->minNumBytes, offsetInSlot + numBytesToRead);
    copiesPerSlot_[open->slot].push_back(
        CopyFromSlot{targetBuf, offsetInSlot, numBytesToRead});
    return true;
  };

  // If draining a completion throws while a read is open, release its slot.
  absl::Cleanup releaseOpenDirectRead{[this, &openDirectRead]() {
    if (openDirectRead.has_value()) {
      copiesPerSlot_[openDirectRead->slot].clear();
      freeSlots_.push_back(openDirectRead->slot);
    }
  }};

  for (const auto& [numBytesToRead, fileOffset, targetBuf] :
       ::ranges::views::zip(numBytesToReadPerRequest, fileOffsetPerRequest,
                            targetBufferPerRequest)) {
    if (useDirectIo && numBytesToRead > 0 &&
        addToDirectRead(numBytesToRead, fileOffset, targetBuf)) {
      continue;
    }
    // A read that fits into a slot of the registered arena reads there and is
    // copied to its target on completion. Acquire the slot before claiming the
    // SQE: `acquireSlot` may submit the prepared SQEs.
    InFlightRead read{handle, numBytesToRead};
    const bool readIntoSlot = useRegisteredBuffers && !useDirectIo &&
                              numBytesToRead > 0 &&
                              numBytesToRead <= kRegisteredSlotSize;
    if (readIntoSlot) {
      read.slot = acquireSlot();
    }
    io_uring_sqe* sqe = claimSqe();
    // Record the read's parameters in the SQE (this only sets the SQE's
    // fields; the request is not handed to the kernel until a later
    // `io_uring_submit`).
    if (readIntoSlot) {
      io_uring_prep_read_fixed(sqe, fd, arena_->getSlotSpan(read.slot).data(),
                               static_cast<unsigned>(numBytesToRead),
                               static_cast<__u64>(fileOffset),
                               static_cast<int>(read.slot));
      copiesPerSlot_[read.slot].push_back(
          CopyFromSlot{targetBuf, 0, numBytesToRead});
    } else {
      io_uring_prep_read(sqe, fd, targetBuf,
                         static_cast<unsigned>(numBytesToRead),
                         static_cast<__u64>(fileOffset));
    }
    trackSqe(sqe, read);
  }
  submitOpenDirectRead();
  // Flush the remaining prepared SQEs to the kernel (`claimSqe` only submits
  // when the submission queue is full, so the last group of SQEs has not yet
  // been submitted).
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
  // Recover the id via the pointer-sized `user_data` field, see `addBatch`.
  const uint64_t requestId =
      reinterpret_cast<uint64_t>(io_uring_cqe_get_data(cqe));
  io_uring_cqe_seen(&ring_, cqe);
  numInFlightReadRequests_--;

  // Every reaped CQE corresponds to exactly one in-flight read whose id we
  // inserted in `addBatch`, so the entry must be present.
  auto reqIt = inFlightReadsByRequestId_.find(requestId);
  AD_CORRECTNESS_CHECK(reqIt != inFlightReadsByRequestId_.end());
  const InFlightRead inFlightRead = reqIt->second;
  inFlightReadsByRequestId_.erase(reqIt);

  // `cqe->res` < 0 is `-errno`. A result smaller than requested (a partial
  // read, or 0 at end of file) means we read fewer bytes than expected, which
  // we treat as an error.
  const bool failed = numBytesRead < 0;
  const bool tooShort =
      !failed && static_cast<size_t>(numBytesRead) < inFlightRead.minNumBytes;
  // A read into a slot of the registered arena: copy the requested bytes to
  // their targets and free the slot (also on error, before throwing).
  if (inFlightRead.slot != kNoSlot) {
    auto& copies = copiesPerSlot_[inFlightRead.slot];
    if (!failed && !tooShort) {
      const char* slotData = arena_->getSlotSpan(inFlightRead.slot).data();
      for (const auto& copy : copies) {
        std::memcpy(copy.target, slotData + copy.offsetInSlot, copy.numBytes);
      }
    }
    copies.clear();
    freeSlots_.push_back(inFlightRead.slot);
  }
  if (failed) {
    AD_THROW("I/O error in IoUringPolicy read operation");
  }
  if (tooShort) {
    AD_THROW("read fewer bytes than requested in IoUringPolicy");
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
