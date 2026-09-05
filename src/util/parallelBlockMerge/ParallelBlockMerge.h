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

#include <memory>
#include <utility>

#include "backports/concepts.h"
#include "util/CancellationHandle.h"
#include "util/Iterators.h"
#include "util/parallelBlockMerge/MergeHelpers.h"
#include "util/parallelBlockMerge/MergeOptions.h"
#include "util/parallelBlockMerge/RunsInputPolicy.h"
#include "util/parallelBlockMerge/SerialMergeState.h"

// An STXXL-style k-way merge. The input is a set of presorted runs, each of
// which is split into blocks. The blocks may live compressed on disk; only
// their element count and their first and last key have to be available
// without I/O (see `BlockedRunsInput` in `RunsInputPolicy.h`).
//
// It is exactly that metadata-only interface which makes the merge splittable:
// a weighted quantile over the block metadata (see `computeSplitters` in
// `MergeHelpers.h`) divides the global output range into chunks of roughly
// equal size, and a chunk can be merged (by a `detail::ChunkMerger`) without
// looking at any other chunk at all. Merging the chunks in the order of their
// index and concatenating their output blocks therefore yields the globally
// sorted result, no matter how the chunks were obtained; that also means that
// the chunks may be merged concurrently, which is what the splitting is for.
//
// This header contains the merging logic itself; it is the header that users of
// this library include. The input policy and the options live in the sibling
// headers of this directory.
namespace ad_utility::parallelBlockMerge {

// ___________________________________________________________________________
// The public entry points.
// ___________________________________________________________________________

// Merge the presorted runs of `input` according to `comparator` in the calling
// thread and return the merged elements as a lazy range of blocks in globally
// sorted order.
//
// The `splitters` describe the chunks that the merge is split into (see
// `computeSplitters`); they are merged one after the other. The default is a
// single chunk that covers the whole key range, which is the cheapest way to
// merge serially. Any other splitters yield exactly the same blocks, so they
// only matter for a caller that wants to observe (or test) the chunking itself.
//
// The `comparator` has to be able to compare two elements, two keys, as well as
// an element with a key (in both orders). If `moveElements` is `true`, then the
// elements are moved out of the input blocks.
CPP_template(bool moveElements, typename Input,
             typename Comparator)(requires BlockedRunsInput<Input>) ad_utility::
    InputRangeTypeErased<typename Input::Block> serialBlockMergeToRange(
        Input input, Comparator comparator, MergeOptions options = {},
        ad_utility::SharedCancellationHandle cancellationHandle = nullptr,
        Splitters<typename Input::Key> splitters = {}) {
  using Block = typename Input::Block;
  using SerialState = detail::SerialMergeState<moveElements, Input, Comparator>;
  return ad_utility::InputRangeTypeErased<Block>{std::make_unique<SerialState>(
      std::move(input), std::move(comparator), std::move(options),
      std::move(cancellationHandle), std::move(splitters))};
}

}  // namespace ad_utility::parallelBlockMerge

#endif  // QLEVER_SRC_UTIL_PARALLELBLOCKMERGE_PARALLELBLOCKMERGE_H
