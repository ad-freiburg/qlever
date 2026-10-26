// Copyright 2023 - 2026 The QLever Authors, in particular:
//
// 2023 - 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
// 2025 Bayerische Motoren Werke Aktiengesellschaft (BMW AG)
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_UTIL_VIEWS_UNIQUEBLOCKVIEW_H
#define QLEVER_SRC_UTIL_VIEWS_UNIQUEBLOCKVIEW_H

#include <algorithm>
#include <memory>
#include <optional>
#include <utility>

#include "backports/algorithm.h"
#include "util/GlobalExecutor.h"
#include "util/InputRangeUtils.h"
#include "util/Iterators.h"
#include "util/Log.h"
#include "util/Views.h"
#include "util/views/AsyncTransformView.h"

namespace ad_utility {

// The default number of blocks that `uniqueBlockView` (see below) hands to the
// global thread pool at the same time. Removing the duplicates from a block is
// much cheaper than producing it (typically the merge phase of an external
// sort) or consuming it (typically writing a permutation), so a few blocks in
// flight suffice to take the deduplication off the critical path of the
// consumer. Every block in flight costs memory that no memory limit accounts
// for, see the NOTE at `uniqueBlockView`.
constexpr inline size_t DEFAULT_UNIQUE_BLOCK_VIEW_NUM_BLOCKS_IN_FLIGHT = 4;

// Takes a view of blocks and yields the elements of the same view, but removes
// consecutive duplicates inside the blocks and across block boundaries.
//
// The duplicates inside a block are removed on the global thread pool, for up
// to `numBlocksInFlight` blocks at a time, because that is by far the most
// expensive part and it is independent for each block. Only the boundary
// between two consecutive blocks needs the previous block: its last element
// (which the deduplication never changes, because it is the last element of a
// sorted block) is recorded before the block is handed to the pool. The blocks
// are yielded in their original order.
//
// NOTE 1: A block that becomes completely empty (all of its elements were
// duplicates of the last element of the previous block) is skipped, so the
// number of yielded blocks may be smaller than the number of input blocks.
//
// NOTE 2: Up to `numBlocksInFlight` input blocks are held in memory at the same
// time (the ones that are being deduplicated and the finished ones that the
// consumer has not pulled yet), in addition to the block that the consumer
// holds. This memory is not covered by the memory limit of the producer of the
// blocks (for the index build, the external sorter), so `numBlocksInFlight`
// times the size of a block has to be small compared to that limit.
//
// IMPORTANT: The consuming thread blocks until the deduplication of the next
// block has finished on the pool, so the view must not be consumed from a
// thread of the global pool itself (if all threads of the pool wait like
// this, the deduplication never runs). This is the same restriction as for
// `parallelBlockMerge::parallelBlockMergeToRange`.
template <typename SortedBlockView>
InputRangeTypeErased<ql::ranges::range_value_t<SortedBlockView>>
uniqueBlockView(
    SortedBlockView view,
    size_t numBlocksInFlight = DEFAULT_UNIQUE_BLOCK_VIEW_NUM_BLOCKS_IN_FLIGHT) {
  using Block = ql::ranges::range_value_t<SortedBlockView>;
  using Value = ql::ranges::range_value_t<Block>;
  auto notEmpty = std::not_fn(ql::ranges::empty);

  // The statistics that are logged at the end. They are shared by all the
  // stages of the pipeline below.
  struct Counts {
    size_t numInputs_ = 0;
    size_t numUnique_ = 0;
  };
  auto counts = std::make_shared<Counts>();

  // The sequential part, which runs on the consuming thread: move each
  // non-empty block out of the `view` and pair it with the last value of the
  // block before it (if any).
  auto withLastOfPrevious =
      [counts, lastOfPrevious = std::optional<Value>{}](Block& block) mutable {
        counts->numInputs_ += block.size();
        auto last = std::exchange(lastOfPrevious, block.back());
        return std::pair{std::move(block), std::move(last)};
      };
  auto blocksWithLastOfPrevious =
      CachingTransformInputRange{std::move(view) | ql::views::filter(notEmpty),
                                 std::move(withLastOfPrevious)};

  // The part that runs on the global thread pool: remove the duplicates of a
  // single block, given the last value of the block before it (if any).
  auto deduplicate = [](std::pair<Block, std::optional<Value>> blockAndLast) {
    auto& [block, lastOfPrevious] = blockAndLast;
    auto beg = lastOfPrevious.has_value()
                   ? ql::ranges::find_if(
                         block, [&p = lastOfPrevious.value()](
                                    const auto& el) { return el != p; })
                   : block.begin();
    block.erase(std::unique(beg, block.end()), block.end());
    block.erase(block.begin(), beg);
    return std::move(block);
  };
  // NOTE: The `AsyncTransformView` can't be moved, so it is stored on the heap.
  InputRangeTypeErased<Block> deduplicated{
      std::make_unique<AsyncTransformView<decltype(blocksWithLastOfPrevious),
                                          decltype(deduplicate)>>(
          std::move(blocksWithLastOfPrevious), deduplicate,
          std::max<size_t>(1, numBlocksInFlight), globalExecutor())};

  // Count the unique elements. A block may become empty (all of its elements
  // were equal to the last one of the previous block); such a block is skipped
  // by the `filter` below.
  auto countUnique = [counts](Block& block) {
    counts->numUnique_ += block.size();
    return std::move(block);
  };
  // NOTE: The callback is `noexcept`, because it may be invoked from the
  // destructor of the `CallbackOnEndView`, which is used via a virtual
  // destructor that must not throw.
  auto logCounts = [counts]() noexcept {
    AD_LOG_INFO << "Number of inputs to `uniqueView`: " << counts->numInputs_
                << '\n';
    AD_LOG_INFO << "Number of unique elements: " << counts->numUnique_
                << std::endl;
  };
  return InputRangeTypeErased{CallbackOnEndView{
      CachingTransformInputRange{
          std::move(deduplicated) | ql::views::filter(notEmpty), countUnique},
      logCounts}};
}

}  // namespace ad_utility

#endif  // QLEVER_SRC_UTIL_VIEWS_UNIQUEBLOCKVIEW_H
