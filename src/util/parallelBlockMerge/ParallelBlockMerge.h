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
#include <vector>

#include "backports/algorithm.h"
#include "backports/concepts.h"
#include "util/CancellationHandle.h"
#include "util/InputRangeUtils.h"
#include "util/Iterators.h"
#include "util/Views.h"
#include "util/parallelBlockMerge/BlockSinkPolicy.h"
#include "util/parallelBlockMerge/ChunkMerger.h"
#include "util/parallelBlockMerge/MergeExecutor.h"
#include "util/parallelBlockMerge/MergeHelpers.h"
#include "util/parallelBlockMerge/MergeOptions.h"
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
// a Boost.Asio executor, one task per chunk, and pushes the output blocks of
// every chunk to a sink (see `SinkConcept` in `BlockSinkPolicy.h`). A chunk
// that currently cannot make progress, because the sink has no room for its
// next block, suspends instead of occupying a thread. The corresponding
// back-pressure, as well as the order in which the blocks of the individual
// chunks are handed on to a consumer, live in the sink and not in the merge;
// the merging itself is done by the very same `detail::ChunkMerger` that the
// serial merge uses.
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

// Set up a parallel merge of the presorted runs of `input` according to
// `comparator` and start it. All the work is scheduled on the `executor`, which
// somebody else has to run; a default-constructed `executor` means "use
// `defaultMergeExecutor()`". The output blocks of every chunk are pushed to the
// sink that `makeSink` creates, see `SinkConcept`. Return the state of the
// merge, see `detail::ParallelMergeState` for the details and in particular for
// its lifetime requirements.
//
// `makeSink` is called exactly once, as `makeSink(numChunks)`, and has to
// return a `std::shared_ptr` to a sink that expects that many chunks. It is a
// factory (and not simply a sink) because the number of chunks is only known
// once the chunk boundaries have been computed, which typically happens inside
// this function.
//
// The result is deterministic for a fixed configuration (the same `options` and
// the same `parallelismHint` always yield the same blocks in the same chunks,
// also for elements that the `comparator` considers equal). The relative order
// of tied elements is however *not* specified and in particular may depend on
// the number of chunks, so a caller that cares about the order of equal
// elements has to make the `comparator` a total order.
//
// The `parallelismHint` is the number of threads that are expected to run the
// `executor`; it is only used to derive the number of chunks and the number of
// chunks that are in flight, both of which may safely exceed the actual
// parallelism. A value of `0` means "as many threads as the hardware offers".
//
// The requirements on the `comparator` and the meaning of `moveElements` are
// the same as for `serialBlockMergeToRange` above. Note that a merge with a
// single chunk is already the serial merge, just performed by a single task on
// the `executor`, so there is deliberately no serial fast path here.
CPP_template(bool moveElements, typename Input, typename Comparator,
             typename SinkFactory)(requires InputConcept<Input>)
    std::shared_ptr<detail::ParallelMergeStateFor<
        moveElements, Input, Comparator,
        SinkFactory>> parallelBlockMergeToSink(net::any_io_executor executor,
                                               Input input,
                                               Comparator comparator,
                                               SinkFactory makeSink,
                                               MergeOptions options = {},
                                               size_t parallelismHint = 0,
                                               ad_utility::SharedCancellationHandle
                                                   cancellationHandle = detail::
                                                       freshCancellationHandle()) {
  using Sink = detail::SinkFromFactoryT<SinkFactory>;
  using State =
      detail::ParallelMergeState<moveElements, Input, Comparator, Sink>;
  if (!executor) {
    executor = defaultMergeExecutor();
  }
  if (parallelismHint == 0) {
    parallelismHint = defaultMergeParallelism();
  }
  auto chunkBoundaries = computeChunkBoundaries(
      input, comparator, parallelismHint * options.targetChunksPerThread);
  size_t numChunks = chunkBoundaries.size();
  size_t requested = options.maxInFlightChunks == 0 ? parallelismHint
                                                    : options.maxInFlightChunks;
  // NOTE: The number of in-flight chunks is deliberately *not* bounded by the
  // available parallelism, because a chunk that has to wait suspends instead of
  // blocking a thread. A single in-flight chunk is legal as well.
  size_t maxInFlight = std::min(requested, numChunks);
  // NOTE: The input, the comparator, the options, the cancellation handle, and
  // the chunk boundaries are shared by the mergers of all chunks, see
  // `detail::MergeState`.
  auto mergeState = std::make_shared<const typename State::SharedMergeState>(
      std::move(input), std::move(comparator), std::move(options),
      std::move(cancellationHandle), std::move(chunkBoundaries));
  return State::create(std::move(executor), std::move(mergeState),
                       makeSink(numChunks), maxInFlight);
}

}  // namespace ad_utility::parallelBlockMerge

#endif  // QLEVER_SRC_UTIL_PARALLELBLOCKMERGE_PARALLELBLOCKMERGE_H
