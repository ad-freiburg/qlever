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
#include "backports/concepts.h"
#include "util/CancellationHandle.h"
#include "util/InputRangeUtils.h"
#include "util/Iterators.h"
#include "util/Views.h"
#include "util/parallelBlockMerge/ChunkMerger.h"
#include "util/parallelBlockMerge/MergeHelpers.h"
#include "util/parallelBlockMerge/MergeOptions.h"
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
        ad_utility::SharedCancellationHandle cancellationHandle = nullptr,
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

}  // namespace ad_utility::parallelBlockMerge

#endif  // QLEVER_SRC_UTIL_PARALLELBLOCKMERGE_PARALLELBLOCKMERGE_H
