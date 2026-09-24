// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Julian Mundhahs <mundhahj@tf.uni-freiburg.de>, UFR
// 2026 Hannah Bast <bast@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_UTIL_SORTEDSEQUENCE_H
#define QLEVER_SRC_UTIL_SORTEDSEQUENCE_H

#include <functional>

#include "backports/functional.h"
#include "util/SortedSequenceManySortedBlocks.h"
#include "util/SortedSequenceTwoSortedRanges.h"

namespace ad_utility {

// Pick one of the two implementations for our sorted sequence data structure.
//
// `SortedSequenceTwoSortedRanges` from `SortedSequenceTwoSortedRanges.h`
// `SortedSequenceManySortedBlocks` from `SortedSequenceManySortedBlocks.h`
//
// NOTE: The interface is exactly the same, but the performance characteristics
// differ; see the documentation of the two implementations for details.
template <typename ValueType, typename Compare = std::less<>,
          typename Projection = ql::identity>
using SortedSequence =
    SortedSequenceManySortedBlocks<ValueType, Compare, Projection>;

}  // namespace ad_utility

#endif  // QLEVER_SRC_UTIL_SORTEDSEQUENCE_H
