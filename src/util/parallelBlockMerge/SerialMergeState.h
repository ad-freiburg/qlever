// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_UTIL_PARALLELBLOCKMERGE_SERIALMERGESTATE_H
#define QLEVER_SRC_UTIL_PARALLELBLOCKMERGE_SERIALMERGESTATE_H

#include <cstddef>
#include <optional>
#include <utility>

#include "backports/concepts.h"
#include "util/CancellationHandle.h"
#include "util/Iterators.h"
#include "util/NoCopyNoMove.h"
#include "util/parallelBlockMerge/ChunkMerger.h"
#include "util/parallelBlockMerge/MergeHelpers.h"
#include "util/parallelBlockMerge/MergeOptions.h"
#include "util/parallelBlockMerge/RunsInputPolicy.h"

namespace ad_utility::parallelBlockMerge {
namespace detail {

// The state of a serial merge, exposed as a lazy range of blocks: merge the
// chunks that the `Splitters` describe one after the other in the calling
// thread, and yield the concatenation of their output blocks.
//
// Because the chunks partition the global key range, and because they are
// merged in the order of their index, that concatenation is exactly the
// globally sorted output, *independently* of the splitters. This is what makes
// this class the reference implementation of a chunked merge: a merge that
// distributes the very same chunks over several threads has to produce the very
// same blocks.
//
// The memory that a merge requires is that of a single `ChunkMerger`, that is
// one input block per run plus a single output block, no matter how many chunks
// there are.
CPP_template(bool moveElements, typename Input, typename Comparator)(
    requires BlockedRunsInput<Input>) class SerialMergeState
    : public ad_utility::InputRangeFromGet<typename Input::Block>,
      public ad_utility::NoCopyNoMove {
 public:
  using Block = typename Input::Block;
  using Key = typename Input::Key;

 private:
  // NOTE: The `ChunkMerger` refers to the input and to the comparator by
  // pointer, so this class has to own them, which is also the only reason it
  // exists at all.
  Input input_;
  Comparator comparator_;
  MergeOptions options_;
  ad_utility::SharedCancellationHandle cancellationHandle_;
  Splitters<Key> splitters_;
  // The index of the chunk that `merger_` covers, or `splitters_.numChunks()`
  // if all chunks are done.
  size_t chunkIndex_ = 0;
  std::optional<ChunkMerger<moveElements, Input, Comparator>> merger_{};

 public:
  // Construct from the owned `input` and `comparator`, the `options`, an
  // optional `cancellationHandle`, and the `splitters` that describe the chunks
  // (the default is a single chunk that covers the whole key range).
  SerialMergeState(Input input, Comparator comparator, MergeOptions options,
                   ad_utility::SharedCancellationHandle cancellationHandle,
                   Splitters<Key> splitters = {})
      : input_{std::move(input)},
        comparator_{std::move(comparator)},
        options_{std::move(options)},
        cancellationHandle_{std::move(cancellationHandle)},
        splitters_{std::move(splitters)} {}

  // Return the next output block, or `std::nullopt` if all chunks are
  // exhausted. Empty chunks (which are perfectly legal, see `Splitters`) are
  // simply skipped, hence the loop.
  std::optional<Block> get() override {
    while (true) {
      if (!merger_.has_value()) {
        if (chunkIndex_ >= splitters_.numChunks()) {
          return std::nullopt;
        }
        merger_.emplace(&input_, &comparator_, options_,
                        splitters_.getSplittersAt(chunkIndex_),
                        cancellationHandle_);
        ++chunkIndex_;
      }
      if (auto block = merger_->get()) {
        return block;
      }
      merger_.reset();
    }
  }
};
}  // namespace detail
}  // namespace ad_utility::parallelBlockMerge

#endif  // QLEVER_SRC_UTIL_PARALLELBLOCKMERGE_SERIALMERGESTATE_H
