// Copyright 2026, The QLever Authors, in particular:
//
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_UTIL_ADAPTIVEBATCHCONTROLLER_H
#define QLEVER_SRC_UTIL_ADAPTIVEBATCHCONTROLLER_H

#include <algorithm>
#include <cstddef>
#include <cstdint>

#include "util/Exception.h"

namespace ad_utility {

// Ratio controller for the effective io_uring submission batch size: defer
// submissions while many I/Os are in flight (amortization), flush early
// when little work remains (keep the device busy). After Jasny et al.,
// `io_uring for High-Performance DBMSs`
// (https://web.archive.org/web/20260827103943/https://arxiv.org/abs/2512.04859).
// The caller owns submission bookkeeping. `pending`
// counts not-yet-prepared reads of the current batch, not waiting fibers.
struct AdaptiveBatchController {
  // Early-flush floor and tail threshold: smaller groups keep their single
  // end-of-batch submit, and at most this many remaining reads always flush.
  // Normalized to [1, ring size]. Default 16: one submit amortizes the
  // syscall overhead across the group without delaying small batches.
  size_t minBatchSize_ = 16;

  // Incremental-submit ceiling for very large batches. Normalized against
  // the ring size. Defaults to the standard ring window of 256.
  size_t maxBatchSize_ = 256;

  // Defer while outstanding / pending >= deferNumerator_ / deferDenominator_.
  // Default 1/1 (equality defers). Normalized to strictly positive, since
  // zero would silently force flush-everything or defer-everything.
  uint64_t deferNumerator_ = 1;
  uint64_t deferDenominator_ = 1;

  // Enforceable bounds in one place: batch sizes into [1, ringSize] with
  // max >= min, defer ratio strictly positive. `shouldFlush` assumes this.
  [[nodiscard]] AdaptiveBatchController normalized(size_t ringSize) const {
    AdaptiveBatchController result = *this;
    const size_t upperBound = std::max<size_t>(ringSize, 1);
    result.minBatchSize_ =
        std::clamp<size_t>(result.minBatchSize_, 1, upperBound);
    result.maxBatchSize_ =
        std::clamp(result.maxBatchSize_, result.minBatchSize_, upperBound);
    result.deferNumerator_ = std::max<uint64_t>(result.deferNumerator_, 1);
    result.deferDenominator_ = std::max<uint64_t>(result.deferDenominator_, 1);
    return result;
  }

  // Flush on empty, idle, or tail input (`pending <= minBatchSize_`); defer
  // while the in-flight ratio says so. `outstanding` counts submitted but
  // incomplete reads, `pending` the not-yet-prepared ones including the
  // current read. Requires normalized values (checked below).
  [[nodiscard]] bool shouldFlush(size_t outstanding, size_t pending) const {
    AD_CONTRACT_CHECK(minBatchSize_ > 0);
    AD_CONTRACT_CHECK(deferNumerator_ > 0);
    AD_CONTRACT_CHECK(deferDenominator_ > 0);
    if (pending == 0) {
      return true;
    }
    if (outstanding == 0) {
      return true;
    }
    if (pending <= minBatchSize_) {
      return true;
    }
    // Cross-multiplied comparison, exact for all inputs via u128 (no division).
    using U128 = unsigned __int128;
    const U128 lhs = static_cast<U128>(outstanding) * U128{deferDenominator_};
    const U128 rhs = static_cast<U128>(pending) * U128{deferNumerator_};
    return lhs < rhs;
  }
};

}  // namespace ad_utility

#endif  // QLEVER_SRC_UTIL_ADAPTIVEBATCHCONTROLLER_H
