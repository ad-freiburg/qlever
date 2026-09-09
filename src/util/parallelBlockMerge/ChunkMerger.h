// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_UTIL_PARALLELBLOCKMERGE_CHUNKMERGER_H
#define QLEVER_SRC_UTIL_PARALLELBLOCKMERGE_CHUNKMERGER_H

#include <cstddef>
#include <memory>
#include <optional>
#include <utility>
#include <vector>

#include "backports/algorithm.h"
#include "backports/concepts.h"
#include "util/CancellationHandle.h"
#include "util/Exception.h"
#include "util/Forward.h"
#include "util/Iterators.h"
#include "util/MemorySize/MemorySize.h"
#include "util/NoCopyNoMove.h"
#include "util/parallelBlockMerge/MergeHelpers.h"
#include "util/parallelBlockMerge/MergeOptions.h"
#include "util/parallelBlockMerge/RunsInputPolicy.h"

// The merger of a single chunk, together with the state that the mergers of all
// chunks of a merge share. For the terminology (runs, blocks, and chunks) see
// `util/parallelBlockMerge/ParallelBlockMerge.h`, which is the header to read
// first.
namespace ad_utility::parallelBlockMerge {
namespace detail {

// Everything that a single merge consists of, and that the mergers of its
// chunks share. It is always held by a `shared_ptr`, so that a `ChunkMerger`
// (of which there is one per chunk, created lazily) can keep it alive, no
// matter in which order and on which thread the chunks are merged. This is also
// the only owner of the `input` and the `comparator`, both of which the
// `ChunkMerger`s only refer to.
template <typename Input, typename Comparator>
struct MergeState {
  using Element = typename Input::Element;

  Input input_;
  Comparator comparator_;
  MergeOptions options_;
  // Never `nullptr`, see the constructor below.
  ad_utility::SharedCancellationHandle cancellationHandle_;
  // The boundaries of the chunks, which partition the whole range of elements.
  // Never empty, see `computeChunkBoundaries`.
  std::vector<ChunkBoundary<Element>> chunkBoundaries_;

  // NOTE: An explicit constructor (instead of aggregate initialization) is
  // needed so that `std::make_shared` can be used.
  MergeState(Input input, Comparator comparator, MergeOptions options,
             ad_utility::SharedCancellationHandle cancellationHandle,
             std::vector<ChunkBoundary<Element>> chunkBoundaries)
      : input_{std::move(input)},
        comparator_{std::move(comparator)},
        options_{std::move(options)},
        cancellationHandle_{std::move(cancellationHandle)},
        chunkBoundaries_{std::move(chunkBoundaries)} {
    AD_CONTRACT_CHECK(cancellationHandle_ != nullptr);
    AD_CONTRACT_CHECK(!chunkBoundaries_.empty());
  }
};

// Merge that part of the runs of a `MergeState` that lies in the range of the
// chunk with the index `chunkIdx` and yield the result as a lazy range of
// output blocks (see `get()`).
//
// The blocks of the input are read lazily and one at a time per run, and the
// last block of a run is released as soon as that run is exhausted, so the
// memory that a single `ChunkMerger` requires is one input block per run that
// still contributes plus a single output block.
//
// The `Comparator` has to be able to compare two elements. It is also applied
// to the bounds of the chunk, which the `InputConcept` requires to be
// equivalent to actual elements.
//
// If `moveElements` is `true`, then the elements are moved out of the input
// blocks into the output blocks.
CPP_template(bool moveElements, typename Input, typename Comparator)(
    requires InputConcept<Input>) class ChunkMerger
    : public ad_utility::InputRangeFromGet<typename Input::Block>,
      public ad_utility::NoCopyNoMove {
 public:
  using Block = typename Input::Block;
  using Element = typename Input::Element;
  using State = MergeState<Input, Comparator>;

 private:
  // The lazy cursor over that part of a single run that lies in the range of
  // the chunk. The current element is `*it_`, and the cursor is exhausted if
  // `it_ == end_` and there is no further block to read.
  //
  // NOTE: `it_` and `end_` are iterators into `block_`, so a `Cursor` must not
  // be relocated once it has been set up. This is guaranteed because
  // `cursors_` is reserved to its final capacity before the first
  // `emplace_back` (the `heap_` relies on the very same property).
  struct Cursor {
    using Iterator = ql::ranges::iterator_t<Block>;

    size_t runIdx_;
    size_t nextBlockIdx_;
    size_t endBlockIdx_;
    Block block_;
    Iterator it_;
    Iterator end_;

    // NOTE: The `block` is only passed in because `Block` need not be default
    // constructible; it is always the result of `makeEmptyBlock()`.
    Cursor(size_t runIdx, size_t firstBlockIdx, size_t endBlockIdx, Block block)
        : runIdx_{runIdx},
          nextBlockIdx_{firstBlockIdx},
          endBlockIdx_{endBlockIdx},
          block_{std::move(block)},
          it_{ql::ranges::begin(block_)},
          end_{it_} {}
  };

  std::shared_ptr<const State> state_;
  size_t chunkIdx_;
  std::vector<Cursor> cursors_;
  // The min-heap over the cursors, see `heapComparator()`.
  std::vector<Cursor*> heap_;
  bool isInitialized_ = false;

 public:
  // Construct from the shared `state` (which must not be `nullptr`) and the
  // index of the chunk to merge.
  //
  // NOTE: The merger holds pointers into itself (the `heap_` points into
  // `cursors_`), which is why it is a `NoCopyNoMove`.
  ChunkMerger(std::shared_ptr<const State> state, size_t chunkIdx)
      : state_{std::move(state)}, chunkIdx_{chunkIdx} {
    AD_CONTRACT_CHECK(state_ != nullptr);
    AD_CONTRACT_CHECK(chunkIdx_ < state_->chunkBoundaries_.size());
  }

  // Return the next output block, or `std::nullopt` if the chunk is exhausted
  // (the `ChunkMerger` is itself a lazy range of the output blocks of its
  // chunk). The returned block is never empty, because an `OutputBlockSize` is
  // never satisfied by an empty block. This is the only function that performs
  // I/O and it must not be called concurrently for the same `ChunkMerger`.
  std::optional<Block> get() override {
    initializeIfNecessary();
    if (heap_.empty()) {
      return std::nullopt;
    }
    const Input& input = state_->input_;
    auto block = input.makeEmptyBlock();
    size_t numElements = 0;
    MemorySize memory = MemorySize::bytes(0);
    auto comparator = heapComparator();
    while (!heap_.empty() &&
           !state_->options_.outputBlockSize.isBlockLargeEnough(numElements,
                                                                memory)) {
      ql::ranges::pop_heap(heap_, comparator);
      Cursor* cursor = heap_.back();
      auto&& element = *cursor->it_;
      memory += input.memorySizeOfElement(element);
      input.appendToBlock(block, ad_utility::moveIf<moveElements>(element));
      ++numElements;
      ++cursor->it_;
      // NOTE: `readNextBlockIfNecessary` may replace `cursor->block_`, which
      // invalidates `element`. This is fine, because the element was already
      // appended.
      if (readNextBlockIfNecessary(*cursor)) {
        ql::ranges::push_heap(heap_, comparator);
      } else {
        heap_.pop_back();
      }
    }
    state_->cancellationHandle_->throwIfCancelled();
    return block;
  }

 private:
  // The boundary of the chunk that this merger covers.
  const ChunkBoundary<Element>& boundary() const {
    return state_->chunkBoundaries_[chunkIdx_];
  }

  // Return the comparator of the `heap_`. Its arguments are reversed, such that
  // the max-heap of the standard library acts as a min-heap.
  auto heapComparator() const {
    return
        [comparator = &state_->comparator_](const Cursor* a, const Cursor* b) {
          return (*comparator)(*b->it_, *a->it_);
        };
  }

  // Set up the cursors of all runs that contribute to this chunk and build the
  // initial heap, unless that was already done.
  void initializeIfNecessary() {
    if (std::exchange(isInitialized_, true)) {
      return;
    }
    const Input& input = state_->input_;
    size_t numRuns = input.numRuns();
    cursors_.reserve(numRuns);
    for (size_t runIdx = 0; runIdx < numRuns; ++runIdx) {
      auto blockRange =
          blockRangeForRun(input, state_->comparator_, boundary(), runIdx);
      if (blockRange.empty()) {
        continue;
      }
      cursors_.emplace_back(runIdx, blockRange.firstBlockIdx_,
                            blockRange.endBlockIdx_, input.makeEmptyBlock());
    }
    heap_.reserve(cursors_.size());
    for (auto& cursor : cursors_) {
      // This is the only place where the *first* block of a cursor is read.
      if (readNextBlockIfNecessary(cursor, true)) {
        heap_.push_back(&cursor);
      }
    }
    ql::ranges::make_heap(heap_, heapComparator());
  }

  // Make sure that the `cursor` points to a valid element, reading further
  // blocks if necessary. Return `false` if the `cursor` is exhausted. Pass
  // `isFirstBlock == true` if the next block that is read is the first block of
  // the cursor, which is the only one that may have to be trimmed at the front;
  // note that this only applies to the first iteration of the loop below,
  // because a first block that is trimmed away completely is directly followed
  // by the second one.
  bool readNextBlockIfNecessary(Cursor& cursor, bool isFirstBlock = false) {
    const Input& input = state_->input_;
    const Comparator& comparator = state_->comparator_;
    while (cursor.it_ == cursor.end_) {
      if (cursor.nextBlockIdx_ == cursor.endBlockIdx_) {
        // The cursor is exhausted. Release its last block right away instead
        // of keeping it alive until the whole chunk is merged, so that a chunk
        // whose runs finish early does not hold one dead block per such run.
        // The iterators are reset as well, so that they never dangle.
        cursor.block_ = input.makeEmptyBlock();
        cursor.it_ = ql::ranges::begin(cursor.block_);
        cursor.end_ = cursor.it_;
        return false;
      }
      size_t blockIdx = cursor.nextBlockIdx_;
      ++cursor.nextBlockIdx_;
      cursor.block_ = input.getBlock(cursor.runIdx_, blockIdx);
      cursor.it_ = ql::ranges::begin(cursor.block_);
      cursor.end_ = cursor.it_ + ql::ranges::size(cursor.block_);
      // Only the very first block of the chunk can contain elements that are
      // smaller than `lo_`, and only the very last one can contain elements
      // that are not smaller than `hi_`.
      if (std::exchange(isFirstBlock, false) && boundary().lo_.has_value()) {
        cursor.it_ = ql::ranges::lower_bound(
            cursor.block_, boundary().lo_.value(), comparator);
      }
      if (blockIdx + 1 == cursor.endBlockIdx_ && boundary().hi_.has_value()) {
        cursor.end_ = ql::ranges::lower_bound(
            cursor.block_, boundary().hi_.value(), comparator);
      }
      AD_CORRECTNESS_CHECK(cursor.it_ <= cursor.end_);
    }
    return true;
  }
};
}  // namespace detail
}  // namespace ad_utility::parallelBlockMerge

#endif  // QLEVER_SRC_UTIL_PARALLELBLOCKMERGE_CHUNKMERGER_H
