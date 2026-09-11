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

#include <cstddef>
#include <memory>
#include <utility>
#include <vector>

#include "backports/algorithm.h"
#include "backports/asio.h"
#include "backports/concepts.h"
#include "util/CancellationHandle.h"
#include "util/Exception.h"
#include "util/InputRangeUtils.h"
#include "util/Iterators.h"
#include "util/Views.h"
#include "util/parallelBlockMerge/BlockSinkPolicy.h"
#include "util/parallelBlockMerge/BlockStorage.h"
#include "util/parallelBlockMerge/ChunkMerger.h"
#include "util/parallelBlockMerge/InOrderBlockSink.h"
#include "util/parallelBlockMerge/MergeHelpers.h"
#include "util/parallelBlockMerge/MergeOptions.h"
#include "util/parallelBlockMerge/ParallelMergeRange.h"
#include "util/parallelBlockMerge/ParallelMergeState.h"
#include "util/parallelBlockMerge/RunsInputPolicy.h"

// An STXXL-style k-way merge. This is the header that users of this library
// include; it also is the header to read first, because it fixes the
// terminology that the sibling headers of this directory use throughout.
//
// TERMINOLOGY:
// * A `run` is one of the presorted inputs of the merge.
// * A `block` is a contiguous piece of a run. It is the unit of I/O: the blocks
//   may live compressed on disk, and only their element count and their first
//   and last element have to be available without I/O. The output of the merge
//   consists of blocks as well, the size of which the `MergeOptions` control.
// * A `chunk` is a contiguous piece of the *output*, described by a
//   `ChunkBoundary` (a half-open range of elements). The chunks partition the
//   whole range of elements, so every element belongs to exactly one chunk.
// * A `chunk split point` (or just `split point`) is the element at which a new
//   chunk starts. The chunks of a merge are described by the strictly
//   increasing list of their split points, which never includes the (trivial)
//   split point of the very first chunk; `n` split points therefore describe
//   `n + 1` chunks.
//
// It is exactly the metadata-only interface of the input (see `InputConcept` in
// `RunsInputPolicy.h`) which makes the merge splittable: a weighted quantile
// over the block metadata (see `computeChunkBoundaries` in `MergeHelpers.h`)
// divides the output into chunks of roughly equal size, and a chunk can be
// merged (by a `detail::ChunkMerger`) without looking at any other chunk at
// all. Merging the chunks in the order of their index and concatenating their
// output blocks therefore yields the globally sorted result, no matter how the
// chunks were obtained; that also means that the chunks may be merged
// concurrently, which is what the splitting is for.
//
// That is what `parallelBlockMergeToSink` does: it schedules all of its work on
// a Boost.Asio executor, one coroutine per chunk, and pushes the output blocks
// of every chunk to a sink (see `SinkConcept` in `BlockSinkPolicy.h`). A chunk
// that currently cannot make progress, because the sink has no room for its
// next block, suspends instead of occupying a thread. The corresponding
// back-pressure, as well as the order in which the blocks of the individual
// chunks are handed on to a consumer, live in the sink and not in the merge;
// the merging itself is done by the very same `detail::ChunkMerger` that the
// serial merge uses.
//
// NOTE: The parallel merge is implemented with coroutines and hence only
// available in C++20 mode, that is when `QLEVER_REDUCED_FEATURE_SET_FOR_CPP17`
// is not set. Everything else in this directory, `serialBlockMergeToRange`
// included, is available in both modes.
namespace ad_utility::parallelBlockMerge {

// ___________________________________________________________________________
// The public entry points.
// ___________________________________________________________________________

// Merge the presorted runs of `input` according to `comparator` in the calling
// thread and return the merged elements as a lazy range of blocks in globally
// sorted order.
//
// The `chunkBoundaries` describe the chunks that the merge is split into (see
// `computeChunkBoundaries`); they are merged one after the other. The default
// is a single chunk that covers everything, which is the cheapest way to merge
// serially. Any other boundaries yield exactly the same blocks, so they only
// matter for a caller that wants to observe (or test) the chunking itself.
// Because of that, this function is also the reference implementation for a
// merge that distributes the very same chunks over several threads.
//
// The memory that the merge requires is that of a single chunk, that is one
// input block per run plus a single output block, no matter how many chunks
// there are.
//
// If `moveElements` is `true`, then the elements are moved out of the input
// blocks.
CPP_template(bool moveElements, typename Input,
             typename Comparator)(requires InputConcept<Input>) ad_utility::
    InputRangeTypeErased<typename Input::Block> serialBlockMergeToRange(
        Input input, Comparator comparator, MergeOptions options = {},
        ad_utility::SharedCancellationHandle cancellationHandle =
            std::make_shared<ad_utility::CancellationHandle<>>(),
        std::vector<ChunkBoundary<typename Input::Element>> chunkBoundaries =
            singleChunk<typename Input::Element>()) {
  using Block = typename Input::Block;
  using Merger = detail::ChunkMerger<moveElements, Input, Comparator>;
  auto state = std::make_shared<const typename Merger::State>(
      std::move(input), std::move(comparator), std::move(options),
      std::move(cancellationHandle), std::move(chunkBoundaries));
  size_t numChunks = state->chunkBoundaries_.size();
  // Set up the merger of a chunk only once that chunk is actually reached, and
  // let it hold the shared state alive. The `ChunkMerger` is a `NoCopyNoMove`
  // (and each of them is a lazy range of blocks in its own right), which is why
  // it is type-erased into a movable `InputRangeTypeErased` right away.
  auto chunks = ad_utility::CachingTransformInputRange(
      ad_utility::integerRange(numChunks),
      [state = std::move(state)](size_t chunkIdx) {
        return ad_utility::InputRangeTypeErased<Block>{
            std::make_unique<Merger>(state, chunkIdx)};
      });
  // The chunks partition the range of elements and are merged in the order of
  // their index, so the concatenation of their output blocks is exactly the
  // globally sorted output. A chunk that contains no element at all simply
  // contributes no block, which `join` handles for free.
  return ad_utility::InputRangeTypeErased<Block>{
      ql::views::join(std::move(chunks))};
}

#ifndef QLEVER_REDUCED_FEATURE_SET_FOR_CPP17

// Set up a parallel merge of the presorted runs of `input` according to
// `comparator` and start it. All the work is scheduled on the `executor`, which
// must not be empty and which somebody else has to run. The output blocks of
// every chunk are pushed to the sink that `makeSink` creates, see
// `SinkConcept`. Return the state of the merge, see
// `detail::ParallelMergeState` for the details.
//
// NOTE: There deliberately is no default executor, so that the caller stays in
// control of the threads that its merges run on, and in particular of their
// shutdown. A caller that has no executor of its own has to create a thread
// pool (and to keep it alive for at least as long as the merge, see the
// LIFETIME note below).
//
// `makeSink` is called exactly once, as `makeSink(numChunks)`, and has to
// return a `std::shared_ptr` to a sink that expects that many chunks. It is a
// factory (and not simply a sink) because the number of chunks is only known
// once the chunk boundaries have been computed, which happens inside
// this function. See `SinkFactoryConcept` for the exact requirements.
//
// LIFETIME: The returned `shared_ptr` may be dropped at any time, also while
// the merge is still running: every coroutine of the merge holds the state
// alive, and the state in turn holds the sink alive. The flip side is that
// there is deliberately no destructor that waits, so a consumer that abandons
// the merge (instead of reading it to the end) has to call `stop()` on the
// returned state, which makes the coroutines that are still in flight finish
// instead of waiting for a consumer that is gone. The `input`, the
// `comparator` and the `cancellationHandle` are moved into the state and hence
// share its lifetime.
//
// The result is deterministic for a fixed configuration (the same `options`
// always yield the same blocks in the same chunks, also for elements that the
// `comparator` considers equal). The relative order of tied elements is however
// *not* specified and in particular may depend on the number of chunks, so a
// caller that cares about the order of equal elements has to make the
// `comparator` a total order.
//
// How many chunks the merge creates, and how many of them it merges
// concurrently, is derived from the `options`, see `MergeOptions`; in
// particular its `parallelismHint` is the number of threads that are expected
// to run the `executor`.
//
// The requirements on the `comparator` and the meaning of `moveElements` are
// the same as for `serialBlockMergeToRange` above. Note that a merge with a
// single chunk is already the serial merge, just performed by a single
// coroutine on the `executor`, so there is deliberately no serial fast path
// here.
template <bool moveElements, typename Input, typename Comparator,
          typename SinkFactory>
requires InputConcept<Input> &&
             SinkFactoryConcept<SinkFactory, typename Input::Block>
auto parallelBlockMergeToSink(
    ql::any_io_executor executor, Input input, Comparator comparator,
    SinkFactory makeSink, MergeOptions options = {},
    ad_utility::SharedCancellationHandle cancellationHandle =
        detail::freshCancellationHandle())
    -> std::shared_ptr<detail::ParallelMergeStateFor<moveElements, Input,
                                                     Comparator, SinkFactory>> {
  using Sink = detail::SinkFromFactoryT<SinkFactory>;
  using State =
      detail::ParallelMergeState<moveElements, Input, Comparator, Sink>;
  AD_CONTRACT_CHECK(static_cast<bool>(executor),
                    "The executor of a parallel block merge must not be empty");
  auto chunkBoundaries =
      computeChunkBoundaries(input, comparator, options.targetNumChunks());
  size_t numChunks = chunkBoundaries.size();
  size_t maxNumChunksInFlight = options.numChunksInFlight(numChunks);
  // NOTE: The input, the comparator, the options, the cancellation handle, and
  // the chunk boundaries are shared by the mergers of all chunks, see
  // `detail::MergeState`.
  auto mergeState = std::make_shared<const typename State::SharedMergeState>(
      std::move(input), std::move(comparator), std::move(options),
      std::move(cancellationHandle), std::move(chunkBoundaries));
  return State::create(std::move(executor), std::move(mergeState),
                       makeSink(numChunks), maxNumChunksInFlight);
}

#endif  // QLEVER_REDUCED_FEATURE_SET_FOR_CPP17

// Merge the presorted runs of `input` according to `comparator` and return the
// merged blocks as an ordinary (blocking) lazy range, for consumers that are
// not themselves asynchronous. This is `parallelBlockMergeToSink` plus the
// `InOrderBlockSink` that turns the concurrently produced blocks back into a
// single sequential range, see `InOrderBlockSink.h`.
//
// The merge is performed serially in the calling thread (and the `executor` is
// then never used at all) if the input is small (see
// `MergeOptions::serialNumElementsThreshold`) or if `options.parallelism()` is
// a single thread. On that path there is no sink at all, so the
// `blockStorageFactory` is ignored. Except on that path the `executor` must not
// be empty, see `parallelBlockMergeToSink`.
//
// The `blockStorageFactory` decides where the finished output blocks live
// between the producer of a chunk and the consumer, see `BlockStorage`. An
// empty factory (the default) keeps them in memory, which means that a producer
// whose chunk is far ahead of the consumer suspends (buffering at most
// `MergeOptions::bufferedBlocksPerChunk` blocks per chunk); a factory that
// spills the blocks to disk lets it run ahead instead.
//
// IMPORTANT: Except on the serial path, the `executor` has to be run by *other*
// threads (for example by a `boost::asio::thread_pool`), because the thread
// that iterates over the returned range is blocked while it waits for the next
// block and can therefore not run any of the merge's coroutines itself.
//
// NOTE: The returned range keeps everything that the concurrently running
// coroutines refer to alive, so it is safe (and cheap) to destroy it before it
// is exhausted.
//
// NOTE: The parallel merge is implemented with coroutines and hence only
// available in C++20 mode. When `QLEVER_REDUCED_FEATURE_SET_FOR_CPP17` is set,
// this function always takes the serial path above; it is then a mere
// convenience wrapper around `serialBlockMergeToRange` and in particular
// ignores the `executor` and the `blockStorageFactory`.
CPP_template(bool moveElements, typename Input,
             typename Comparator)(requires InputConcept<Input>) ad_utility::
    InputRangeTypeErased<typename Input::Block> parallelBlockMergeToRange(
        ql::any_io_executor executor, Input input, Comparator comparator,
        MergeOptions options = {},
        ad_utility::SharedCancellationHandle cancellationHandle =
            std::make_shared<ad_utility::CancellationHandle<>>(),
        BlockStorageFactory<typename Input::Block> blockStorageFactory = {}) {
#ifdef QLEVER_REDUCED_FEATURE_SET_FOR_CPP17
  // There is no parallel path at all in this mode, so these two arguments are
  // unused, see the NOTE above.
  static_cast<void>(executor);
  static_cast<void>(blockStorageFactory);
#else
  using Block = typename Input::Block;
  using Result = ad_utility::InputRangeTypeErased<Block>;
  // A single thread cannot merge two chunks concurrently, and for small inputs
  // the overhead of setting up the parallel merge dominates the actual merging,
  // so merge directly in the calling thread in both cases.
  if (options.parallelism() > 1 &&
      detail::totalNumElements(input) > options.serialNumElementsThreshold) {
    using Sink = InOrderBlockSink<Block>;
    // NOTE: An empty `blockStorageFactory` means "keep the blocks in memory",
    // which is what bounds the memory consumption of the merge via
    // back-pressure, see `InMemoryBlockStorage`.
    auto storageFactory =
        blockStorageFactory
            ? std::move(blockStorageFactory)
            : Sink::makeInMemoryStorageFactory(options.bufferedBlocksPerChunk);
    // The sink is created inside `parallelBlockMergeToSink`, because only that
    // function knows the number of chunks; this lambda is what lets us get hold
    // of it afterwards, so that the returned range can read from it.
    std::shared_ptr<Sink> sink;
    auto makeSink = [&sink, &executor,
                     &storageFactory](size_t numChunks) mutable {
      sink = std::make_shared<Sink>(executor, numChunks,
                                    std::move(storageFactory));
      return sink;
    };
    auto state = parallelBlockMergeToSink<moveElements>(
        executor, std::move(input), std::move(comparator), makeSink,
        std::move(options), std::move(cancellationHandle));
    using Range =
        detail::ParallelMergeRange<typename decltype(state)::element_type>;
    return Result{std::make_unique<Range>(std::move(state), std::move(sink))};
  }
#endif  // QLEVER_REDUCED_FEATURE_SET_FOR_CPP17
  return serialBlockMergeToRange<moveElements>(
      std::move(input), std::move(comparator), std::move(options),
      std::move(cancellationHandle));
}

}  // namespace ad_utility::parallelBlockMerge

#endif  // QLEVER_SRC_UTIL_PARALLELBLOCKMERGE_PARALLELBLOCKMERGE_H
