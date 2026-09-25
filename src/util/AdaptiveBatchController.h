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

// Ratio controller that adapts the effective io_uring submission batch size
// to the ratio of outstanding I/Os to still-pending reads. It implements the
// batching half of Jasny et al., `io_uring for High-Performance DBMSs: When
// and How to Use It` (`jasny2026iouring`): defer submissions when many I/Os
// are already in flight (to increase amortization) and flush earlier when
// little work remains (to keep the device busy instead of stalling).
//
// This is a pure value type: `shouldFlush` has no state and no I/O. The
// caller (`IoUringPolicy::addBatch`) owns the submission bookkeeping
// (prepared-since-last-submit counts, the ring-full safety bound, and the
// unconditional end-of-batch flush) and consults `shouldFlush` to decide
// whether the currently prepared group is flushed now or kept open for more
// reads. QLever's export path stays single-threaded; `pending` here means
// the reads of the current batch that are not yet prepared (including the
// one being prepared), not waiting fibers.
struct AdaptiveBatchController {
  // Flush once at least this many reads are prepared since the last submit.
  // Groups below this size are never flushed early by the controller, so
  // tiny batches keep their single end-of-batch submit. The same value is
  // also the tail threshold of `shouldFlush`: once at most this many reads
  // of the batch remain, the controller flushes instead of deferring.
  // Normalized to at least one and at most the ring size by the policy, so a
  // nearly finished batch always flushes instead of waiting for work that will
  // never arrive. Default 16: even modest batch sizes amortize most of the
  // per-operation syscall overhead.
  size_t minBatchSize_ = 16;

  // Flush once this many reads are prepared since the last submit, so a
  // very large batch still submits incrementally. Normalized against the
  // ring size by the policy, so a deferred group never exceeds the ring.
  // Defaults to the standard ring window of 256.
  size_t maxBatchSize_ = 256;

  // Defer threshold as a ratio `deferNumerator_/deferDenominator_`: while
  // `outstanding / pending >= deferNumerator_ / deferDenominator_` (many
  // I/Os in flight relative to the remaining work), defer the submit to
  // increase amortization. Otherwise flush early to keep the device busy.
  // The default of 1/1 defers once there are at least as many outstanding
  // I/Os as still-pending reads (equality defers). Both parts are normalized to
  // at least one (see `normalized`): a zero numerator would always defer and a
  // zero denominator would always flush (outside the tail cases).
  uint64_t deferNumerator_ = 1;
  uint64_t deferDenominator_ = 1;

  // Return a copy with enforceable bounds: the minimum batch size is at
  // least one (a nearly finished batch always flushes instead of waiting
  // for work that will never arrive) and at most `ringSize`, the maximum is
  // at most `ringSize` and at least the minimum (a deferred group never
  // exceeds the ring; a `ringSize` of zero is treated as one),
  // and the defer ratio is strictly positive (a zero denominator or
  // numerator would silently force flush-everything or defer-everything).
  // The struct itself stays a plain aggregate; this is the single
  // normalization boundary, applied by the policy when the controller is
  // installed. `shouldFlush` therefore assumes normalized values.
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

  // Decide whether the currently prepared group should be submitted now.
  // Requires a strictly positive minimum and defer ratio (guaranteed by
  // `normalized`, checked here). Returns true (flush
  // early) when there is nothing left to batch (`pending == 0`), when the
  // device is idle (`outstanding == 0`), or when only a tail of at most
  // `minBatchSize_` reads remains. Returns false (defer, keep preparing)
  // when many I/Os are already in flight relative to the remaining work.
  // Both counts are numbers of reads: `outstanding` counts reads submitted
  // to the kernel and not yet completed, `pending` counts the reads of the
  // batch not yet prepared, including the one currently being prepared.
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
    // Defer while outstanding / pending >= deferNumerator_ / deferDenominator_.
    // The comparison is rearranged to multiplications so no division is
    // needed. The product of two 64-bit values always fits into an unsigned
    // 128-bit integer, so the comparison is exact for all inputs.
    using U128 = unsigned __int128;
    const U128 lhs = static_cast<U128>(outstanding) * U128{deferDenominator_};
    const U128 rhs = static_cast<U128>(pending) * U128{deferNumerator_};
    return lhs < rhs;
  }
};

}  // namespace ad_utility

#endif  // QLEVER_SRC_UTIL_ADAPTIVEBATCHCONTROLLER_H
