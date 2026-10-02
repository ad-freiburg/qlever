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
#include "util/NoCopyNoMove.h"
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

namespace detail::uniqueBlockView {

// Return an `AsyncTransformView` that yields the blocks of the `view` with all
// their duplicates removed (see `uniqueBlockView` below for details). The
// total number of elements of the input blocks is added to `*numInputs`.
template <typename SortedBlockView>
auto makeDeduplicatedBlocks(SortedBlockView view, size_t* numInputs,
                            size_t numBlocksInFlight) {
  using Block = ql::ranges::range_value_t<SortedBlockView>;
  using Value = ql::ranges::range_value_t<Block>;

  // The sequential part, which runs on the consuming thread: move each
  // non-empty block out of the `view` and pair it with the last value of the
  // block before it (if any).
  auto withLastOfPrevious = [numInputs,
                             lastOfPrevious =
                                 std::optional<Value>{}](Block& block) mutable {
    *numInputs += block.size();
    auto last = std::exchange(lastOfPrevious, block.back());
    return std::pair{std::move(block), std::move(last)};
  };

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

  return AsyncTransformView{
      CachingTransformInputRange{
          std::move(view) | ql::views::filter(std::not_fn(ql::ranges::empty)),
          std::move(withLastOfPrevious)},
      deduplicate, std::max<size_t>(1, numBlocksInFlight), globalExecutor()};
}

// The range that is returned by `uniqueBlockView` below. It skips the blocks
// that became empty during the deduplication and logs some statistics at the
// end.
//
// NOTE: This class is never moved (it is only accessed through a
// `std::unique_ptr`), so its `deduplicated_` blocks can safely point to its
// `numInputs_`.
template <typename SortedBlockView>
class UniqueBlockView
    : public InputRangeFromGet<ql::ranges::range_value_t<SortedBlockView>>,
      public NoCopyNoMove {
  using Block = ql::ranges::range_value_t<SortedBlockView>;
  size_t numInputs_ = 0;
  size_t numUnique_ = 0;
  decltype(makeDeduplicatedBlocks(std::declval<SortedBlockView>(), nullptr,
                                  0)) deduplicated_;

 public:
  UniqueBlockView(SortedBlockView view, size_t numBlocksInFlight)
      : deduplicated_(makeDeduplicatedBlocks(std::move(view), &numInputs_,
                                             numBlocksInFlight)) {}

  std::optional<Block> get() override {
    while (auto block = deduplicated_.get()) {
      // A block may become empty (all of its elements were equal to the last
      // one of the previous block); such a block is skipped.
      if (block->empty()) {
        continue;
      }
      numUnique_ += block->size();
      return block;
    }
    AD_LOG_INFO << "Number of inputs to `uniqueView`: " << numInputs_ << '\n';
    AD_LOG_INFO << "Number of unique elements: " << numUnique_ << std::endl;
    return std::nullopt;
  }
};

}  // namespace detail::uniqueBlockView

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
// NOTE: A block that becomes completely empty (all of its elements were
// duplicates of the last element of the previous block) is skipped, so the
// number of yielded blocks may be smaller than the number of input blocks.
//
// NOTE: Up to `numBlocksInFlight` input blocks are held in memory at the same
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
  return InputRangeTypeErased{std::make_unique<
      detail::uniqueBlockView::UniqueBlockView<SortedBlockView>>(
      std::move(view), numBlocksInFlight)};
}

}  // namespace ad_utility

#endif  // QLEVER_SRC_UTIL_VIEWS_UNIQUEBLOCKVIEW_H
