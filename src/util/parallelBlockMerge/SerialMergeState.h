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
#include "util/parallelBlockMerge/ChunkMerger.h"
#include "util/parallelBlockMerge/MergeOptions.h"
#include "util/parallelBlockMerge/RunsInputPolicy.h"

namespace ad_utility::parallelBlockMerge {
namespace detail {

// The state of a purely serial merge, exposed as a lazy range of blocks. It
// owns the input and the comparator, because the `ChunkMerger` only refers to
// them by pointer.
CPP_template(bool moveElements, typename Input, typename Comparator)(
    requires BlockedRunsInput<Input>) class SerialMergeState
    : public ad_utility::InputRangeFromGet<typename Input::Block> {
 public:
  using Block = typename Input::Block;
  using Key = typename Input::Key;

 private:
  Input input_;
  Comparator comparator_;
  std::optional<ChunkMerger<moveElements, Input, Comparator>> merger_;

 public:
  // ________________________________________________________________________
  SerialMergeState(Input input, Comparator comparator, MergeOptions options,
                   ad_utility::SharedCancellationHandle cancellationHandle)
      : input_{std::move(input)}, comparator_{std::move(comparator)} {
    merger_.emplace(input_, comparator_, std::move(options), std::nullopt,
                    std::nullopt, std::move(cancellationHandle));
  }

  // The `merger_` points to the other members, so this class must neither be
  // copied nor moved.
  SerialMergeState(const SerialMergeState&) = delete;
  SerialMergeState& operator=(const SerialMergeState&) = delete;
  SerialMergeState(SerialMergeState&&) = delete;
  SerialMergeState& operator=(SerialMergeState&&) = delete;

  // ________________________________________________________________________
  std::optional<Block> get() override { return merger_->nextBlock(); }
};
}  // namespace detail
}  // namespace ad_utility::parallelBlockMerge

#endif  // QLEVER_SRC_UTIL_PARALLELBLOCKMERGE_SERIALMERGESTATE_H
