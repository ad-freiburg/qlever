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

#include <boost/asio/packaged_task.hpp>
#include <boost/asio/post.hpp>
#include <deque>
#include <future>
#include <memory>
#include <optional>

#include "backports/algorithm.h"
#include "util/GlobalExecutor.h"
#include "util/Iterators.h"
#include "util/Log.h"

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
  struct UniqueBlockViewFromGet : InputRangeFromGet<BlockType> {
    SortedBlockView view_;

    decltype(ql::views::filter(view_,
                               std::not_fn(ql::ranges::empty))) nonEmptyView_;
    decltype(ql::ranges::begin(nonEmptyView_)) iter_;

    std::optional<ValueType> lastValueFromPreviousBlock_{std::nullopt};
    size_t numInputs_{0};
    size_t numUnique_{0};
    size_t numBlocksInFlight_;
    // The blocks that are currently being deduplicated, in their order.
    std::deque<std::future<BlockType>> pending_;

    explicit UniqueBlockViewFromGet(SortedBlockView view,
                                    size_t numBlocksInFlight)
        : view_{std::move(view)},
          nonEmptyView_(
              ql::views::filter(view_, std::not_fn(ql::ranges::empty))),
          iter_{ql::ranges::begin(nonEmptyView_)},
          numBlocksInFlight_{std::max<size_t>(1, numBlocksInFlight)} {}

    // Remove the duplicates of a single `block`, given the last value of the
    // block before it (if any). This is the part that runs on the pool.
    static BlockType deduplicate(BlockType block,
                                 std::optional<ValueType> lastOfPrevious) {
      auto beg = lastOfPrevious.has_value()
                     ? ql::ranges::find_if(
                           block, [&p = lastOfPrevious.value()](
                                      const auto& el) { return el != p; })
                     : block.begin();
      auto it = std::unique(beg, block.end());
      block.erase(it, block.end());
      block.erase(block.begin(), beg);
      return block;
    }

    // Hand blocks to the pool until `numBlocksInFlight_` of them are pending
    // or the input is exhausted.
    void fillPipeline() {
      while (pending_.size() < numBlocksInFlight_ &&
             iter_ != ql::ranges::end(nonEmptyView_)) {
        auto block = std::move(*iter_);
        ++iter_;
        numInputs_ += block.size();
        auto lastOfPrevious = lastValueFromPreviousBlock_;
        lastValueFromPreviousBlock_ = block.back();
        pending_.push_back(boost::asio::post(
            globalExecutor(),
            std::packaged_task<BlockType()>{[block = std::move(block),
                                             lastOfPrevious = std::move(
                                                 lastOfPrevious)]() mutable {
              return deduplicate(std::move(block), std::move(lastOfPrevious));
            }}));
      }
    }

    std::optional<BlockType> get() override {
      for (;;) {
        fillPipeline();
        if (pending_.empty()) {
          AD_LOG_INFO << "Number of inputs to `uniqueView`: " << numInputs_
                      << '\n';
          AD_LOG_INFO << "Number of unique elements: " << numUnique_
                      << std::endl;
          return std::nullopt;
        }
        auto block = pending_.front().get();
        pending_.pop_front();
        // A block may become empty (all of its elements were equal to the last
        // one of the previous block); such a block is skipped.
        if (block.empty()) {
          continue;
        }
        numUnique_ += block.size();
        return block;
      }
    }
  };
  return InputRangeTypeErased{std::make_unique<UniqueBlockViewFromGet>(
      std::move(view), numBlocksInFlight)};
}

}  // namespace ad_utility

#endif  // QLEVER_SRC_UTIL_VIEWS_UNIQUEBLOCKVIEW_H
