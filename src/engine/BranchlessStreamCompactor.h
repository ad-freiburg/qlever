// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of this project.

#pragma once

#include <cstdint>
#include <vector>

#include "backports/span.h"
#include "global/Id.h"
#include "util/Exception.h"

namespace ql::engine::vector {

// _____________________________________________________________________________
// Branchless Stream Compactor:
// Branchless scalar stream compaction for 64-bit ValueId arrays.
// Filters rows and writes matches contiguously to the destination buffer
// without branch mispredictions.
class BranchlessStreamCompactor {
 public:
  // Compact elements matching a predicate into output span, returning count.
  // The unrolled loop writes output[outIdx] unconditionally and only bumps
  // the index on match, so the output must hold at least input.size()
  // elements even when few elements match. Exceptions thrown by the
  // predicate propagate to the caller (compact itself is not noexcept).
  // The predicate runs up to 4 times per unrolled iteration (plus once per
  // epilogue element), so keep it cheap (a simple comparison, not I/O or
  // allocation).
  template <typename Predicate>
  static size_t compact(ql::span<const Id> input, ql::span<Id> output,
                        Predicate&& pred) {
    AD_CORRECTNESS_CHECK(output.size() >= input.size());
    size_t outIdx = 0;
    const size_t n = input.size();

    // Process 4 elements per iteration (scalar unroll, not hardware SIMD).
    // `n - i >= 4` avoids any theoretical overflow of `i + 4` near SIZE_MAX.
    size_t i = 0;
    for (; n - i >= 4; i += 4) {
      bool m0 = pred(input[i]);
      bool m1 = pred(input[i + 1]);
      bool m2 = pred(input[i + 2]);
      bool m3 = pred(input[i + 3]);

      output[outIdx] = input[i];
      outIdx += m0 ? 1 : 0;

      output[outIdx] = input[i + 1];
      outIdx += m1 ? 1 : 0;

      output[outIdx] = input[i + 2];
      outIdx += m2 ? 1 : 0;

      output[outIdx] = input[i + 3];
      outIdx += m3 ? 1 : 0;
    }

    // Scalar epilogue
    for (; i < n; ++i) {
      if (pred(input[i])) {
        output[outIdx++] = input[i];
      }
    }

    return outIdx;
  }

  // Compact the elements `input[i]` with `keep[i] == 1` into `output` and
  // return their number. `keep` holds only `0` and `1`, so one mask can be
  // applied to all columns of a table. `output` must hold at least the kept
  // elements. As long as `output` has room for four more elements, every
  // element is written and the index is advanced only on a match, so this
  // loop has no data-dependent branch; the remaining elements are copied with
  // a branch.
  static size_t compactByMask(ql::span<const Id> input,
                              ql::span<const uint8_t> keep,
                              ql::span<Id> output) {
    AD_CORRECTNESS_CHECK(keep.size() == input.size());
    const size_t n = input.size();
    size_t outIdx = 0;
    size_t i = 0;
    for (; n - i >= 4 && output.size() - outIdx >= 4; i += 4) {
      output[outIdx] = input[i];
      outIdx += keep[i];
      output[outIdx] = input[i + 1];
      outIdx += keep[i + 1];
      output[outIdx] = input[i + 2];
      outIdx += keep[i + 2];
      output[outIdx] = input[i + 3];
      outIdx += keep[i + 3];
    }
    for (; i < n; ++i) {
      if (keep[i]) {
        AD_CORRECTNESS_CHECK(outIdx < output.size());
        output[outIdx++] = input[i];
      }
    }
    return outIdx;
  }
};

}  // namespace ql::engine::vector
