// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_UTIL_PARALLELBLOCKMERGE_PARALLELBLOCKMERGE_H
#define QLEVER_SRC_UTIL_PARALLELBLOCKMERGE_PARALLELBLOCKMERGE_H

#include <algorithm>
#include <cstddef>
#include <memory>
#include <utility>
#include <vector>

#include "backports/concepts.h"
#include "util/CancellationHandle.h"
#include "util/Iterators.h"
#include "util/parallelBlockMerge/MergeHelpers.h"
#include "util/parallelBlockMerge/MergeOptions.h"
#include "util/parallelBlockMerge/ParallelMergeState.h"
#include "util/parallelBlockMerge/RunsInputPolicy.h"
#include "util/parallelBlockMerge/SchedulerPolicy.h"
#include "util/parallelBlockMerge/SerialMergeState.h"

// An STXXL-style parallel k-way merge. The input is a set of presorted runs,
// each of which is split into blocks. The blocks may live compressed on disk;
// only their element count and their first and last key have to be available
// without I/O. This allows to split the global output range into chunks that
// can be merged completely independently and therefore in parallel.
//
// This header contains the merging logic itself; it is the header that users of
// this library include. The policies that the merge is built on (input,
// scheduler, and sink) as well as its options live in the sibling headers of
// this directory.
namespace ad_utility::parallelBlockMerge {

// ___________________________________________________________________________
// The public entry point.
// ___________________________________________________________________________

// Merge the presorted runs of `input` according to `comparator` and return the
// merged elements as a lazy range of blocks in globally sorted order. The
// result is deterministic for a fixed configuration (the same `options` and the
// same `MergeScheduler::maxParallelism()` always yield the same order, also for
// elements that the `comparator` considers equal). Set
// `MergeOptions::stableTieBreaking` to additionally make the order of the tied
// elements independent of the number of chunks; see there for the cost.
//
// The `comparator` has to be able to compare two elements, two keys, as well as
// an element with a key (in both orders). If `moveElements` is `true`, then the
// elements are moved out of the input blocks.
//
// The merge is performed serially in the calling thread if the input is small
// (see `MergeOptions::serialNumRunsThreshold` and
// `MergeOptions::serialNumElementsThreshold`), if the `scheduler` offers no
// parallelism, or if the input cannot be split into more than one chunk.
//
// NOTE: The returned range owns everything that the concurrently running tasks
// refer to, so it is safe (and cheap) to destroy it before it is exhausted.
CPP_template(bool moveElements, typename Input,
             typename Comparator)(requires BlockedRunsInput<Input>) ad_utility::
    InputRangeTypeErased<typename Input::Block> parallelBlockMergeToRange(
        Input input, Comparator comparator, MergeOptions options = {},
        SharedMergeScheduler scheduler = defaultMergeScheduler(),
        ad_utility::SharedCancellationHandle cancellationHandle = nullptr) {
  using Block = typename Input::Block;
  using Key = typename Input::Key;
  using SerialState = detail::SerialMergeState<moveElements, Input, Comparator>;
  using ParallelState =
      detail::ParallelMergeState<moveElements, Input, Comparator>;
  using Result = ad_utility::InputRangeTypeErased<Block>;

  if (scheduler == nullptr) {
    scheduler = defaultMergeScheduler();
  }
  const size_t maxParallelism = scheduler->maxParallelism();

  bool isSerial =
      input.numRuns() <= options.serialNumRunsThreshold || maxParallelism <= 1;
  if (!isSerial) {
    isSerial =
        detail::totalNumElements(input) <= options.serialNumElementsThreshold;
  }

  std::vector<Key> splitters;
  size_t maxInFlight = 0;
  if (!isSerial) {
    splitters = computeSplitters(
        input, comparator, maxParallelism * options.targetChunksPerThread);
    size_t numChunks = splitters.size() + 1;
    size_t requested = options.maxInFlightChunks == 0
                           ? maxParallelism
                           : options.maxInFlightChunks;
    maxInFlight = std::min({requested, maxParallelism, numChunks});
    // A single chunk (or a single in-flight chunk) cannot be parallelized, and
    // would even deadlock, because the producer of the only chunk would block
    // on a full buffer.
    isSerial = maxInFlight <= 1;
  }

  if (isSerial) {
    return Result{std::make_unique<SerialState>(
        std::move(input), std::move(comparator), std::move(options),
        std::move(cancellationHandle))};
  }
  return Result{std::make_unique<ParallelState>(
      std::move(input), std::move(comparator), std::move(options),
      std::move(scheduler), std::move(cancellationHandle), std::move(splitters),
      maxInFlight)};
}

}  // namespace ad_utility::parallelBlockMerge

#endif  // QLEVER_SRC_UTIL_PARALLELBLOCKMERGE_PARALLELBLOCKMERGE_H
