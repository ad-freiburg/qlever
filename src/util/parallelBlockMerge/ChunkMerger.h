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

namespace ad_utility::parallelBlockMerge {
namespace detail {

// Merge that part of the runs of a `BlockedRunsInput` that lies in the
// half-open key range of a `Split` and yield the result as a lazy range of
// output blocks (see `get()`).
//
// The blocks of the input are read lazily and one at a time per run, so the
// memory that a single `ChunkMerger` requires is one input block per run plus
// a single output block.
//
// The `Comparator` has to be able to compare two elements, two keys, as well as
// an element with a key (in both orders).
//
// If `moveElements` is `true`, then the elements are moved out of the input
// blocks into the output blocks.
CPP_template(bool moveElements, typename Input, typename Comparator)(
    requires BlockedRunsInput<Input>) class ChunkMerger
    : public ad_utility::InputRangeFromGet<typename Input::Block>,
      public ad_utility::NoCopyNoMove {
 public:
  using Block = typename Input::Block;
  using Key = typename Input::Key;

 private:
  // The lazy cursor over that part of a single run that lies in the key range
  // of the chunk. The current element is `*it_`, and the cursor is exhausted if
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

  const Input* input_;
  const Comparator* comparator_;
  MergeOptions options_;
  Split<Key> split_;
  ad_utility::SharedCancellationHandle cancellationHandle_;
  std::vector<Cursor> cursors_;
  // The min-heap over the cursors, see `heapComparator()`.
  std::vector<Cursor*> heap_;
  bool isInitialized_ = false;

 public:
  // Construct from the `input` and the `comparator` (both of which must not be
  // `nullptr` and have to outlive the `ChunkMerger`), the `options`, the key
  // range of the chunk, and an optional `cancellationHandle`.
  //
  // NOTE: The merger holds pointers into itself (the `heap_` points into
  // `cursors_`) as well as to the `input` and the `comparator`, which is why it
  // is a `NoCopyNoMove`, and why the latter two are passed as pointers.
  ChunkMerger(const Input* input, const Comparator* comparator,
              MergeOptions options, Split<Key> split,
              ad_utility::SharedCancellationHandle cancellationHandle)
      : input_{input},
        comparator_{comparator},
        options_{std::move(options)},
        split_{std::move(split)},
        cancellationHandle_{std::move(cancellationHandle)} {
    AD_CONTRACT_CHECK(input_ != nullptr);
    AD_CONTRACT_CHECK(comparator_ != nullptr);
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
    auto block = input_->makeEmptyBlock();
    size_t numElements = 0;
    MemorySize memory = MemorySize::bytes(0);
    auto comparator = heapComparator();
    while (!heap_.empty() &&
           !options_.outputBlockSize.isBlockLargeEnough(numElements, memory)) {
      ql::ranges::pop_heap(heap_, comparator);
      Cursor* cursor = heap_.back();
      auto&& element = *cursor->it_;
      memory += input_->memorySizeOfElement(element);
      input_->appendToBlock(block, ad_utility::moveIf<moveElements>(element));
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
    if (cancellationHandle_ != nullptr) {
      cancellationHandle_->throwIfCancelled();
    }
    return block;
  }

 private:
  // Return the comparator of the `heap_`. Its arguments are reversed, such that
  // the max-heap of the standard library acts as a min-heap.
  auto heapComparator() const {
    return [comparator = comparator_](const Cursor* a, const Cursor* b) {
      return (*comparator)(*b->it_, *a->it_);
    };
  }

  // Set up the cursors of all runs that contribute to this chunk and build the
  // initial heap, unless that was already done.
  void initializeIfNecessary() {
    if (std::exchange(isInitialized_, true)) {
      return;
    }
    size_t numRuns = input_->numRuns();
    cursors_.reserve(numRuns);
    for (size_t runIdx = 0; runIdx < numRuns; ++runIdx) {
      auto blockRange = blockRangeForRun(*input_, *comparator_, split_, runIdx);
      if (blockRange.empty()) {
        continue;
      }
      cursors_.emplace_back(runIdx, blockRange.firstBlockIdx_,
                            blockRange.endBlockIdx_, input_->makeEmptyBlock());
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
    while (cursor.it_ == cursor.end_) {
      if (cursor.nextBlockIdx_ == cursor.endBlockIdx_) {
        return false;
      }
      size_t blockIdx = cursor.nextBlockIdx_;
      ++cursor.nextBlockIdx_;
      cursor.block_ = input_->readBlock(cursor.runIdx_, blockIdx);
      cursor.it_ = ql::ranges::begin(cursor.block_);
      cursor.end_ = cursor.it_ + blockSize(cursor.block_);
      // Only the very first block of the chunk can contain elements that are
      // smaller than `lo`, and only the very last one can contain elements that
      // are not smaller than `hi`.
      if (std::exchange(isFirstBlock, false) && split_.lo_.has_value()) {
        cursor.it_ = ql::ranges::lower_bound(cursor.block_, split_.lo_.value(),
                                             *comparator_);
      }
      if (blockIdx + 1 == cursor.endBlockIdx_ && split_.hi_.has_value()) {
        cursor.end_ = ql::ranges::lower_bound(cursor.block_, split_.hi_.value(),
                                              *comparator_);
      }
      AD_CORRECTNESS_CHECK(cursor.it_ <= cursor.end_);
    }
    return true;
  }
};
}  // namespace detail
}  // namespace ad_utility::parallelBlockMerge

#endif  // QLEVER_SRC_UTIL_PARALLELBLOCKMERGE_CHUNKMERGER_H
