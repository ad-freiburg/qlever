// Copyright 2026, The QLever Authors, in particular:
//
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_UTIL_ADAPTIVEBATCHCONTROLLER_H
#define QLEVER_SRC_UTIL_ADAPTIVEBATCHCONTROLLER_H

#include <cstddef>
#include <cstdint>

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
// not-yet-submitted reads of the current batch, not waiting fibers.
struct AdaptiveBatchController {
  // Flush once at least this many reads are prepared since the last submit.
  // Groups below this size are never flushed early, so tiny batches keep
  // their single end-of-batch submit. Normalized to at least one by the
  // policy, so a nearly finished batch always flushes instead of waiting
  // for work that will never arrive. Default 16: even modest batch sizes
  // amortize most of the per-operation syscall overhead.
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
  // The default of 1/1 defers once the outstanding I/Os outnumber the
  // still-pending reads.
  uint64_t deferNumerator_ = 1;
  uint64_t deferDenominator_ = 1;

  // Decide whether the currently prepared group should be submitted now.
  // Returns true (flush early) when there is nothing left to batch
  // (`pending == 0`), when the device is idle (`outstanding == 0`), or
  // when only a tail of at most `minBatchSize_` reads remains. Returns
  // false (defer, keep preparing) when many I/Os are already in flight
  // relative to the remaining work. Both counts are numbers of reads;
  // `pending` includes the read currently being prepared.
  [[nodiscard]] bool shouldFlush(size_t outstanding, size_t pending) const {
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
    // needed; `__int128` keeps it exact for any realistic queue depth.
    const __int128 lhs = static_cast<__int128>(outstanding) * deferDenominator_;
    const __int128 rhs = static_cast<__int128>(pending) * deferNumerator_;
    return lhs < rhs;
  }
};

}  // namespace ad_utility

#endif  // QLEVER_SRC_UTIL_ADAPTIVEBATCHCONTROLLER_H
