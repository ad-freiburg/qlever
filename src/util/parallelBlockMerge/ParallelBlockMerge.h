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
#include <thread>
#include <utility>

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
// elements that the `comparator` considers equal). The relative order of tied
// elements is however *not* specified and in particular may depend on the
// number of chunks, so a caller that cares about the order of equal elements
// has to make the `comparator` a total order.
//
// The `comparator` has to be able to compare two elements, two keys, as well as
// an element with a key (in both orders). If `moveElements` is `true`, then the
// elements are moved out of the input blocks.
//
// The merge is performed serially in the calling thread if the input is small
// (see `MergeOptions::serialNumElementsThreshold`), if the `scheduler` offers
// no parallelism, or if the input cannot be split into more than one chunk.
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

  bool isSerial = maxParallelism <= 1;
  if (!isSerial) {
    isSerial =
        detail::totalNumElements(input) <= options.serialNumElementsThreshold;
  }

  Splitters<Key> splitters;
  size_t maxInFlight = 0;
  if (!isSerial) {
    splitters = computeSplitters(
        input, comparator, maxParallelism * options.targetChunksPerThread);
    size_t requested = options.maxInFlightChunks == 0
                           ? maxParallelism
                           : options.maxInFlightChunks;
    maxInFlight = std::min({requested, maxParallelism, splitters.numChunks()});
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

#ifndef QLEVER_CPP_17
// ___________________________________________________________________________
// The public entry points of the Boost.Asio based merge.
// ___________________________________________________________________________

// Set up a Boost.Asio based parallel merge of the presorted runs of `input`
// according to `comparator` and start it. All the work is scheduled on the
// `executor`, which somebody else has to run. Return the state of the merge,
// whose `next()` yields the merged elements as an awaitable sequence of blocks
// in globally sorted order; see `AsioParallelMergeState` for the details and in
// particular for its lifetime requirements.
//
// The `parallelismHint` is the number of threads that are expected to run the
// `executor`; it is only used to derive the number of chunks and the number of
// chunks that are in flight, both of which may safely exceed the actual
// parallelism. A value of `0` means "as many threads as the hardware offers".
//
// The requirements on the `comparator` and the meaning of `moveElements` are
// the same as for `parallelBlockMergeToRange` above. In contrast to that
// function, this one has no serial fast path, because a merge with a single
// chunk is already the serial merge, just performed by a single coroutine.
CPP_template(bool moveElements, typename Input,
             typename Comparator)(requires BlockedRunsInput<Input>)
    std::shared_ptr<detail::AsioParallelMergeState<
        moveElements, Input,
        Comparator>> parallelBlockMergeAsio(net::any_io_executor executor,
                                            Input input, Comparator comparator,
                                            MergeOptions options = {},
                                            size_t parallelismHint = 0,
                                            ad_utility::SharedCancellationHandle
                                                cancellationHandle = nullptr) {
  using State = detail::AsioParallelMergeState<moveElements, Input, Comparator>;
  if (parallelismHint == 0) {
    parallelismHint = std::max<size_t>(1, std::thread::hardware_concurrency());
  }
  auto splitters = computeSplitters(
      input, comparator, parallelismHint * options.targetChunksPerThread);
  size_t requested = options.maxInFlightChunks == 0 ? parallelismHint
                                                    : options.maxInFlightChunks;
  // NOTE: In contrast to `parallelBlockMergeToRange` the number of in-flight
  // chunks is deliberately *not* bounded by the available parallelism, because
  // a chunk that has to wait suspends its coroutine instead of blocking a
  // thread. A single in-flight chunk is legal as well.
  size_t maxInFlight = std::min(requested, splitters.numChunks());
  return State::create(std::move(executor), std::move(input),
                       std::move(comparator), std::move(options),
                       std::move(cancellationHandle), std::move(splitters),
                       maxInFlight);
}

// The same as `parallelBlockMergeToRange` above, but schedule all the work on
// the Boost.Asio `executor` instead of on a `MergeScheduler`. See
// `parallelBlockMergeAsio` above for the meaning of the `parallelismHint`.
//
// IMPORTANT: The `executor` has to be run by *other* threads (for example by a
// `boost::asio::thread_pool`), because the thread that iterates over the
// returned range is blocked while it waits for the next block and can therefore
// not run any of the merge's coroutines itself. Consumers that are themselves
// asynchronous should use `parallelBlockMergeAsio` above instead.
CPP_template(bool moveElements, typename Input,
             typename Comparator)(requires BlockedRunsInput<Input>) ad_utility::
    InputRangeTypeErased<typename Input::Block> parallelBlockMergeToRangeAsio(
        net::any_io_executor executor, Input input, Comparator comparator,
        MergeOptions options = {}, size_t parallelismHint = 0,
        ad_utility::SharedCancellationHandle cancellationHandle = nullptr) {
  using Block = typename Input::Block;
  using SerialState = detail::SerialMergeState<moveElements, Input, Comparator>;
  using Range = detail::AsioParallelMergeRange<moveElements, Input, Comparator>;
  using Result = ad_utility::InputRangeTypeErased<Block>;

  // For small inputs the overhead of setting up the merge dominates the actual
  // merging, so merge them directly in the calling thread.
  if (detail::totalNumElements(input) <= options.serialNumElementsThreshold) {
    return Result{std::make_unique<SerialState>(
        std::move(input), std::move(comparator), std::move(options),
        std::move(cancellationHandle))};
  }
  return Result{std::make_unique<Range>(parallelBlockMergeAsio<moveElements>(
      std::move(executor), std::move(input), std::move(comparator),
      std::move(options), parallelismHint, std::move(cancellationHandle)))};
}
#endif  // QLEVER_CPP_17

}  // namespace ad_utility::parallelBlockMerge

#endif  // QLEVER_SRC_UTIL_PARALLELBLOCKMERGE_PARALLELBLOCKMERGE_H
