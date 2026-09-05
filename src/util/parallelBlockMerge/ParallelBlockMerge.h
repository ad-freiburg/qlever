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
#include <boost/asio/any_io_executor.hpp>
#include <cstddef>
#include <memory>
#include <utility>

#include "backports/concepts.h"
#include "util/CancellationHandle.h"
#include "util/Iterators.h"
#include "util/parallelBlockMerge/MergeExecutor.h"
#include "util/parallelBlockMerge/MergeHelpers.h"
#include "util/parallelBlockMerge/MergeOptions.h"
#include "util/parallelBlockMerge/ParallelMergeState.h"
#include "util/parallelBlockMerge/RunsInputPolicy.h"
#include "util/parallelBlockMerge/SerialMergeState.h"

// An STXXL-style parallel k-way merge. The input is a set of presorted runs,
// each of which is split into blocks. The blocks may live compressed on disk;
// only their element count and their first and last key have to be available
// without I/O. This allows to split the global output range into chunks that
// can be merged completely independently and therefore in parallel.
//
// All the work of a merge is scheduled on a Boost.Asio executor, and a chunk
// that currently cannot make progress (because the consumer has not caught up
// yet) suspends instead of occupying a thread.
//
// This header contains the merging logic itself; it is the header that users of
// this library include. The input policy, the sink, the executor, and the
// options live in the sibling headers of this directory.
namespace ad_utility::parallelBlockMerge {

// ___________________________________________________________________________
// The public entry points.
// ___________________________________________________________________________

// Merge the presorted runs of `input` according to `comparator` in the calling
// thread and return the merged elements as a lazy range of blocks in globally
// sorted order.
//
// The `comparator` has to be able to compare two elements, two keys, as well as
// an element with a key (in both orders). If `moveElements` is `true`, then the
// elements are moved out of the input blocks.
CPP_template(bool moveElements, typename Input,
             typename Comparator)(requires BlockedRunsInput<Input>) ad_utility::
    InputRangeTypeErased<typename Input::Block> serialBlockMergeToRange(
        Input input, Comparator comparator, MergeOptions options = {},
        ad_utility::SharedCancellationHandle cancellationHandle = nullptr) {
  using Block = typename Input::Block;
  using SerialState = detail::SerialMergeState<moveElements, Input, Comparator>;
  return ad_utility::InputRangeTypeErased<Block>{std::make_unique<SerialState>(
      std::move(input), std::move(comparator), std::move(options),
      std::move(cancellationHandle))};
}

// Set up a parallel merge of the presorted runs of `input` according to
// `comparator` and start it. All the work is scheduled on the `executor`, which
// somebody else has to run. Return the state of the merge, whose `asyncNext()`
// yields the merged elements as an asynchronous sequence of blocks in globally
// sorted order; see `detail::ParallelMergeState` for the details and in
// particular for its lifetime requirements.
//
// The result is deterministic for a fixed configuration (the same `options` and
// the same `parallelismHint` always yield the same order, also for elements
// that the `comparator` considers equal). The relative order of tied elements
// is however *not* specified and in particular may depend on the number of
// chunks, so a caller that cares about the order of equal elements has to make
// the `comparator` a total order.
//
// The `parallelismHint` is the number of threads that are expected to run the
// `executor`; it is only used to derive the number of chunks and the number of
// chunks that are in flight, both of which may safely exceed the actual
// parallelism. A value of `0` means "as many threads as the hardware offers".
//
// The `blockStorageFactory` decides where the finished output blocks live
// between the producer of a chunk and the consumer, see `BlockStorage`. An
// empty factory (the default) keeps them in memory, which means that a producer
// whose chunk is far ahead of the consumer suspends; a factory that spills to
// disk (see `engine/idTable/CompressedIdTableBlockStorage.h`) lets it run ahead
// instead.
//
// The requirements on the `comparator` and the meaning of `moveElements` are
// the same as for `serialBlockMergeToRange` above. In contrast to
// `parallelBlockMergeToRange` below, this function has no serial fast path,
// because a merge with a single chunk is already the serial merge, just
// performed by a single task on the `executor`.
CPP_template(bool moveElements, typename Input,
             typename Comparator)(requires BlockedRunsInput<Input>)
    std::shared_ptr<detail::ParallelMergeState<
        moveElements, Input,
        Comparator>> parallelBlockMergeAsync(net::any_io_executor executor,
                                             Input input, Comparator comparator,
                                             MergeOptions options = {},
                                             size_t parallelismHint = 0,
                                             ad_utility::
                                                 SharedCancellationHandle
                                                     cancellationHandle =
                                                         nullptr,
                                             BlockStorageFactory<
                                                 typename Input::Block>
                                                 blockStorageFactory = {}) {
  using State = detail::ParallelMergeState<moveElements, Input, Comparator>;
  if (!executor) {
    executor = defaultMergeExecutor();
  }
  if (parallelismHint == 0) {
    parallelismHint = defaultMergeParallelism();
  }
  auto splitters = computeSplitters(
      input, comparator, parallelismHint * options.targetChunksPerThread);
  size_t requested = options.maxInFlightChunks == 0 ? parallelismHint
                                                    : options.maxInFlightChunks;
  // NOTE: The number of in-flight chunks is deliberately *not* bounded by the
  // available parallelism, because a chunk that has to wait suspends instead of
  // blocking a thread. A single in-flight chunk is legal as well.
  size_t maxInFlight = std::min(requested, splitters.numChunks());
  return State::create(std::move(executor), std::move(input),
                       std::move(comparator), std::move(options),
                       std::move(cancellationHandle), std::move(splitters),
                       maxInFlight, std::move(blockStorageFactory));
}

// The same as `parallelBlockMergeAsync` above, but return the merged blocks as
// an ordinary (blocking) lazy range, for consumers that are not themselves
// asynchronous. A default-constructed `executor` means "use
// `defaultMergeExecutor()`".
//
// The merge is performed serially in the calling thread (and the `executor` is
// then never used at all) if the input is small (see
// `MergeOptions::serialNumElementsThreshold`) or if the `parallelismHint`
// resolves to a single thread. On that path there is no sink at all, so the
// `blockStorageFactory` is ignored.
//
// IMPORTANT: Except on that serial path, the `executor` has to be run by
// *other* threads (for example by a `boost::asio::thread_pool`), because the
// thread that iterates over the returned range is blocked while it waits for
// the next block and can therefore not run any of the merge's tasks itself.
//
// NOTE: The returned range keeps everything that the concurrently running tasks
// refer to alive, so it is safe (and cheap) to destroy it before it is
// exhausted.
CPP_template(bool moveElements, typename Input,
             typename Comparator)(requires BlockedRunsInput<Input>) ad_utility::
    InputRangeTypeErased<typename Input::Block> parallelBlockMergeToRange(
        net::any_io_executor executor, Input input, Comparator comparator,
        MergeOptions options = {}, size_t parallelismHint = 0,
        ad_utility::SharedCancellationHandle cancellationHandle = nullptr,
        BlockStorageFactory<typename Input::Block> blockStorageFactory = {}) {
  using Block = typename Input::Block;
  using Range = detail::ParallelMergeRange<moveElements, Input, Comparator>;
  using Result = ad_utility::InputRangeTypeErased<Block>;

  if (parallelismHint == 0) {
    parallelismHint = defaultMergeParallelism();
  }
  // A single thread cannot merge two chunks concurrently, and for small inputs
  // the overhead of setting up the merge dominates the actual merging, so merge
  // directly in the calling thread in both cases.
  if (parallelismHint <= 1 ||
      detail::totalNumElements(input) <= options.serialNumElementsThreshold) {
    return serialBlockMergeToRange<moveElements>(
        std::move(input), std::move(comparator), std::move(options),
        std::move(cancellationHandle));
  }
  return Result{std::make_unique<Range>(parallelBlockMergeAsync<moveElements>(
      std::move(executor), std::move(input), std::move(comparator),
      std::move(options), parallelismHint, std::move(cancellationHandle),
      std::move(blockStorageFactory)))};
}

}  // namespace ad_utility::parallelBlockMerge

#endif  // QLEVER_SRC_UTIL_PARALLELBLOCKMERGE_PARALLELBLOCKMERGE_H
