// Copyright 2026, The QLever Authors, in particular:
//
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_TEST_UTIL_FAKEIOURING_H
#define QLEVER_TEST_UTIL_FAKEIOURING_H

#include <algorithm>
#include <cerrno>
#include <cstdint>
#include <cstring>
#include <deque>
#include <random>
#include <string>
#include <utility>
#include <vector>

#include "util/Exception.h"
#include "util/IoUringManager.h"

#ifdef QLEVER_HAS_IO_URING

namespace ad_utility::testing {

// A simulated io_uring for tests of `IoUringPolicy`. The "kernel" reads from
// `file` (the read's `fd` is ignored) and writes into the target buffer only
// when it completes a read. Driven by a seeded random generator, it takes only
// some of the queued SQEs, fails submits with `EINTR` and with bursts of
// `EAGAIN`, interrupts waits, completes requests in any order, and makes reads
// short or fail. It checks the contract of `IoUringInterface`: a free SQE for
// every prepared read, no completion queue overflow, and no wait for more
// completions than requests in the kernel.
class FakeIoUring final : public IoUringInterface {
 public:
  // Probabilities of the faults, per call.
  struct Faults {
    double partialSubmit = 0;
    double submitInterrupted = 0;
    // A burst makes 1 to `maxEagainBurst` submits in a row fail. Bursts longer
    // than `IoUringPolicy`'s retries make it cancel the queued reads.
    double submitOutOfMemory = 0;
    size_t maxEagainBurst = 1;
    double waitInterrupted = 0;
    double shortRead = 0;
    double readError = 0;
    // If nonzero, every submit (or wait) fails with this `errno`.
    int brokenSubmitErrno = 0;
    int brokenWaitErrno = 0;
  };

  FakeIoUring(std::string file, size_t sqCapacity, Faults faults, uint32_t seed)
      : file_{std::move(file)},
        sqCapacity_{sqCapacity},
        faults_{faults},
        random_{seed} {}

  // Whether, since the last `resetObservations`, a read was made short or
  // failed, or `cancelQueued` was called.
  bool injectedReadFault() const { return injectedReadFault_; }

  // How often each fault was injected over the lifetime of the ring, so that
  // tests can check that their faults actually occur.
  struct FaultCounts {
    size_t partialSubmits = 0;
    size_t interruptedSubmits = 0;
    size_t outOfMemorySubmits = 0;
    size_t interruptedWaits = 0;
    size_t faultyReads = 0;
    size_t cancelledReads = 0;
  };
  const FaultCounts& faultCounts() const { return faultCounts_; }
  bool cancelled() const { return cancelled_; }
  void resetObservations() {
    injectedReadFault_ = false;
    cancelled_ = false;
  }

  // The reads (not no-ops) that are queued, in the kernel, or completed but
  // not reaped. Between two `addBatch` calls this must be zero.
  size_t numReadsInRing() const {
    auto isRead = [](const auto& requestOrCompletion) {
      return !requestOrCompletion.isNoOp;
    };
    return static_cast<size_t>(
        std::count_if(queue_.begin(), queue_.end(), isRead) +
        std::count_if(kernel_.begin(), kernel_.end(), isRead) +
        std::count_if(completions_.begin(), completions_.end(), isRead));
  }

  bool prepareRead([[maybe_unused]] int fd, char* buffer, size_t numBytes,
                   uint64_t offset, uint64_t requestId) override {
    if (queue_.size() >= sqCapacity_) {
      return false;
    }
    queue_.push_back(Request{false, buffer, numBytes, offset, requestId});
    return true;
  }

  size_t numQueued() const override { return queue_.size(); }

  int submit() override {
    if (faults_.brokenSubmitErrno != 0) {
      return -faults_.brokenSubmitErrno;
    }
    if (eagainBurstLeft_ > 0) {
      --eagainBurstLeft_;
      ++faultCounts_.outOfMemorySubmits;
      return -EAGAIN;
    }
    if (happens(faults_.submitOutOfMemory)) {
      eagainBurstLeft_ = uniform(1, faults_.maxEagainBurst) - 1;
      ++faultCounts_.outOfMemorySubmits;
      return -EAGAIN;
    }
    if (happens(faults_.submitInterrupted)) {
      ++faultCounts_.interruptedSubmits;
      return -EINTR;
    }
    size_t numTaken = queue_.size();
    if (numTaken > 1 && happens(faults_.partialSubmit)) {
      numTaken = uniform(1, numTaken - 1);
      ++faultCounts_.partialSubmits;
    }
    for (size_t i = 0; i < numTaken; ++i) {
      kernel_.push_back(queue_.front());
      queue_.pop_front();
    }
    AD_CORRECTNESS_CHECK(kernel_.size() + completions_.size() <=
                         2 * sqCapacity_);
    // The kernel may already have completed some of the requests.
    completeRandomRequests(uniform(0, kernel_.size()));
    return static_cast<int>(numTaken);
  }

  int waitForCompletions(unsigned minComplete) override {
    if (faults_.brokenWaitErrno != 0) {
      return -faults_.brokenWaitErrno;
    }
    if (happens(faults_.waitInterrupted)) {
      ++faultCounts_.interruptedWaits;
      return -EINTR;
    }
    // A real wait for more completions than requests in the kernel would
    // block forever.
    AD_CORRECTNESS_CHECK(minComplete <= kernel_.size() + completions_.size());
    while (completions_.size() < minComplete) {
      completeRandomRequests(1);
    }
    return 0;
  }

  size_t popCompletions(ql::span<Completion> completions) override {
    const size_t numPopped = std::min(completions.size(), completions_.size());
    for (size_t i = 0; i < numPopped; ++i) {
      completions[i] = completions_.front().completion;
      completions_.pop_front();
    }
    return numPopped;
  }

  std::vector<uint64_t> cancelQueued(uint64_t noOpRequestId) override {
    cancelled_ = true;
    std::vector<uint64_t> cancelledIds;
    for (Request& request : queue_) {
      if (!request.isNoOp) {
        cancelledIds.push_back(request.requestId);
        ++faultCounts_.cancelledReads;
      }
      request = Request{true, nullptr, 0, 0, noOpRequestId};
    }
    return cancelledIds;
  }

  void backOff() override {}

 private:
  struct Request {
    bool isNoOp;
    char* buffer;
    size_t numBytes;
    uint64_t offset;
    uint64_t requestId;
  };

  struct PendingCompletion {
    Completion completion;
    bool isNoOp;
  };

  bool happens(double probability) {
    return std::bernoulli_distribution{probability}(random_);
  }
  size_t uniform(size_t min, size_t max) {
    return std::uniform_int_distribution<size_t>{min, max}(random_);
  }

  // Complete `numToComplete` random requests of the kernel: write a read's
  // bytes into its buffer and post its completion.
  void completeRandomRequests(size_t numToComplete) {
    for (size_t k = 0; k < numToComplete && !kernel_.empty(); ++k) {
      const size_t i = uniform(0, kernel_.size() - 1);
      const Request request = kernel_[i];
      kernel_.erase(kernel_.begin() + static_cast<ptrdiff_t>(i));
      completions_.push_back(PendingCompletion{
          Completion{request.requestId, perform(request)}, request.isNoOp});
    }
  }

  // The result (`res`) of `request`.
  int perform(const Request& request) {
    if (request.isNoOp) {
      return 0;
    }
    if (happens(faults_.readError)) {
      injectedReadFault_ = true;
      ++faultCounts_.faultyReads;
      return -EIO;
    }
    size_t numBytes =
        std::min(request.numBytes,
                 file_.size() - std::min<size_t>(file_.size(), request.offset));
    if (numBytes > 0 && happens(faults_.shortRead)) {
      numBytes = uniform(0, numBytes - 1);
    }
    if (numBytes != request.numBytes) {
      injectedReadFault_ = true;
      ++faultCounts_.faultyReads;
    }
    if (numBytes > 0) {
      std::memcpy(request.buffer, file_.data() + request.offset, numBytes);
    }
    return static_cast<int>(numBytes);
  }

  std::string file_;
  size_t sqCapacity_;
  Faults faults_;
  std::mt19937 random_;
  std::deque<Request> queue_;
  std::vector<Request> kernel_;
  std::deque<PendingCompletion> completions_;
  size_t eagainBurstLeft_ = 0;
  bool injectedReadFault_ = false;
  bool cancelled_ = false;
  FaultCounts faultCounts_;
};

}  // namespace ad_utility::testing

#endif  // QLEVER_HAS_IO_URING

#endif  // QLEVER_TEST_UTIL_FAKEIOURING_H
