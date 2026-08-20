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

// The state of a purely serial merge, exposed as a lazy range of blocks. The
// merging itself is done by a single `ChunkMerger` that covers the whole key
// range; the only reason this class exists is that the `ChunkMerger` refers to
// the input and the comparator by pointer, so somebody has to own them.
CPP_template(bool moveElements, typename Input, typename Comparator)(
    requires BlockedRunsInput<Input>) class SerialMergeState
    : public ad_utility::InputRangeFromGet<typename Input::Block>,
      public ad_utility::NoCopyNoMove {
 public:
  using Block = typename Input::Block;
  using Key = typename Input::Key;

 private:
  Input input_;
  Comparator comparator_;
  std::optional<ChunkMerger<moveElements, Input, Comparator>> merger_;

 public:
  // Construct the owned input and comparator, and the single merger that covers
  // the whole key range. NOTE: The `merger_` points to the other members, which
  // is why this class is a `NoCopyNoMove`.
  SerialMergeState(Input input, Comparator comparator, MergeOptions options,
                   ad_utility::SharedCancellationHandle cancellationHandle)
      : input_{std::move(input)}, comparator_{std::move(comparator)} {
    merger_.emplace(input_, comparator_, std::move(options), Split<Key>{},
                    std::move(cancellationHandle));
  }

  // ________________________________________________________________________
  std::optional<Block> get() override { return merger_->get(); }
};
}  // namespace detail
}  // namespace ad_utility::parallelBlockMerge

#endif  // QLEVER_SRC_UTIL_PARALLELBLOCKMERGE_SERIALMERGESTATE_H
