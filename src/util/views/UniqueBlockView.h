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
#include "util/Iterators.h"
#include "util/Log.h"
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
template <typename SortedBlockView,
          typename BlockType = ql::ranges::range_value_t<SortedBlockView>,
          typename ValueType = ql::ranges::range_value_t<BlockType>>
InputRangeTypeErased<BlockType> uniqueBlockView(
    SortedBlockView view,
    size_t numBlocksInFlight = DEFAULT_UNIQUE_BLOCK_VIEW_NUM_BLOCKS_IN_FLIGHT) {
  // A block together with the last value of the block before it (if any).
  using BlockAndLastOfPrevious = std::pair<BlockType, std::optional<ValueType>>;

  // The sequential part, which runs on the consuming thread: yield the
  // non-empty blocks of the `view` together with the last value of their
  // respective previous block.
  struct BlocksWithLastOfPrevious : InputRangeFromGet<BlockAndLastOfPrevious> {
    // NOTE: The `view` and the counter are owned by the
    // `UniqueBlockViewFromGet` below, which is never moved. This range itself
    // is moved into its `AsyncTransformView` after its construction, which is
    // also why the iterator is only obtained on the first call to `get()`.
    SortedBlockView* view_;
    size_t* numInputs_;
    std::optional<ql::ranges::iterator_t<SortedBlockView>> iter_;
    std::optional<ValueType> lastValueFromPreviousBlock_;

    BlocksWithLastOfPrevious(SortedBlockView* view, size_t* numInputs)
        : view_{view}, numInputs_{numInputs} {}

    std::optional<BlockAndLastOfPrevious> get() override {
      if (!iter_.has_value()) {
        iter_ = ql::ranges::begin(*view_);
      }
      auto& iter = iter_.value();
      for (; iter != ql::ranges::end(*view_); ++iter) {
        if (ql::ranges::empty(*iter)) {
          continue;
        }
        BlockType block = std::move(*iter);
        ++iter;
        *numInputs_ += block.size();
        auto lastOfPrevious =
            std::exchange(lastValueFromPreviousBlock_, block.back());
        return BlockAndLastOfPrevious{std::move(block),
                                      std::move(lastOfPrevious)};
      }
      return std::nullopt;
    }
  };

  // The part that runs on the global thread pool: remove the duplicates of a
  // single block, given the last value of the block before it (if any).
  struct Deduplicate {
    BlockType operator()(BlockAndLastOfPrevious blockAndLastOfPrevious) const {
      auto& [block, lastOfPrevious] = blockAndLastOfPrevious;
      auto beg = lastOfPrevious.has_value()
                     ? ql::ranges::find_if(
                           block, [&p = lastOfPrevious.value()](
                                      const auto& el) { return el != p; })
                     : block.begin();
      auto it = std::unique(beg, block.end());
      block.erase(it, block.end());
      block.erase(block.begin(), beg);
      return std::move(block);
    }
  };

  // NOTE: This object is never moved (it is only accessed through the
  // `std::unique_ptr` below), so the `BlocksWithLastOfPrevious` can safely
  // point to its `view_` and `numInputs_`.
  struct UniqueBlockViewFromGet : InputRangeFromGet<BlockType> {
    SortedBlockView view_;
    size_t numInputs_{0};
    size_t numUnique_{0};
    AsyncTransformView<BlocksWithLastOfPrevious, Deduplicate> deduplicated_;

    explicit UniqueBlockViewFromGet(SortedBlockView view,
                                    size_t numBlocksInFlight)
        : view_{std::move(view)},
          deduplicated_{BlocksWithLastOfPrevious{&view_, &numInputs_},
                        Deduplicate{}, std::max<size_t>(1, numBlocksInFlight),
                        globalExecutor()} {}

    std::optional<BlockType> get() override {
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
  return InputRangeTypeErased{std::make_unique<UniqueBlockViewFromGet>(
      std::move(view), numBlocksInFlight)};
}

}  // namespace ad_utility

#endif  // QLEVER_SRC_UTIL_VIEWS_UNIQUEBLOCKVIEW_H
