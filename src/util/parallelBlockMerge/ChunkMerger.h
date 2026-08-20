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
#include "util/MemorySize/MemorySize.h"
#include "util/parallelBlockMerge/MergeHelpers.h"
#include "util/parallelBlockMerge/MergeOptions.h"
#include "util/parallelBlockMerge/RunsInputPolicy.h"

namespace ad_utility::parallelBlockMerge {
namespace detail {

// Merge that part of the runs of a `BlockedRunsInput` that lies in the
// half-open key range `[lo, hi)` and yield the result as a sequence of output
// blocks (see `nextBlock()`). An empty `lo`/`hi` means minus/plus infinity.
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
    requires BlockedRunsInput<Input>) class ChunkMerger {
 public:
  using Block = typename Input::Block;
  using Key = typename Input::Key;

 private:
  // The lazy cursor over that part of a single run that lies in `[lo, hi)`. The
  // current element is `begin(block_)[idx_]`, and the cursor is exhausted if
  // `idx_ == end_` and there is no further block to read.
  struct Cursor {
    size_t run_;
    size_t firstBlock_;
    size_t nextBlock_;
    size_t endBlock_;
    Block block_;
    size_t idx_ = 0;
    size_t end_ = 0;

    // NOTE: The `block` is only passed in because `Block` need not be default
    // constructible; it is always the result of `makeEmptyBlock()`.
    Cursor(size_t run, size_t firstBlock, size_t endBlock, Block block)
        : run_{run},
          firstBlock_{firstBlock},
          nextBlock_{firstBlock},
          endBlock_{endBlock},
          block_{std::move(block)} {}
  };

  const Input* input_;
  const Comparator* comparator_;
  MergeOptions options_;
  std::optional<Key> lo_;
  std::optional<Key> hi_;
  ad_utility::SharedCancellationHandle cancellationHandle_;
  std::vector<Cursor> cursors_;
  // The min-heap over the cursors, see `heapComparator()`.
  std::vector<Cursor*> heap_;
  bool isInitialized_ = false;

 public:
  // Construct from the `input` and the `comparator` (both of which have to
  // outlive the `ChunkMerger`), the `options`, the (inclusive) lower and the
  // (exclusive) upper bound of the chunk, and an optional `cancellationHandle`.
  ChunkMerger(const Input& input, const Comparator& comparator,
              MergeOptions options, std::optional<Key> lo,
              std::optional<Key> hi,
              ad_utility::SharedCancellationHandle cancellationHandle)
      : input_{&input},
        comparator_{&comparator},
        options_{std::move(options)},
        lo_{std::move(lo)},
        hi_{std::move(hi)},
        cancellationHandle_{std::move(cancellationHandle)} {}

  // The merger holds pointers into itself (the `heap_` points into
  // `cursors_`), so it must neither be copied nor moved.
  ChunkMerger(const ChunkMerger&) = delete;
  ChunkMerger& operator=(const ChunkMerger&) = delete;
  ChunkMerger(ChunkMerger&&) = delete;
  ChunkMerger& operator=(ChunkMerger&&) = delete;

  // Return the next output block, or `std::nullopt` if the chunk is exhausted.
  // The returned block is never empty. This is the only function that performs
  // I/O and it must not be called concurrently for the same `ChunkMerger`.
  std::optional<Block> nextBlock() {
    if (!isInitialized_) {
      initialize();
      isInitialized_ = true;
    }
    if (heap_.empty()) {
      return std::nullopt;
    }
    auto block = input_->makeEmptyBlock();
    size_t numElements = 0;
    MemorySize memory = MemorySize::bytes(0);
    auto comparator = heapComparator();
    while (!heap_.empty()) {
      ql::ranges::pop_heap(heap_, comparator);
      Cursor* cursor = heap_.back();
      auto&& element = ql::ranges::begin(cursor->block_)[cursor->idx_];
      memory += input_->memorySizeOfElement(element);
      if constexpr (moveElements) {
        input_->appendToBlock(block, std::move(element));
      } else {
        input_->appendToBlock(block, element);
      }
      ++numElements;
      ++cursor->idx_;
      // NOTE: `advance` may replace `cursor->block_`, which invalidates
      // `element`. This is fine, because the element was already appended.
      if (advance(*cursor)) {
        ql::ranges::push_heap(heap_, comparator);
      } else {
        heap_.pop_back();
      }
      if (numElements >= options_.outputBlockSize ||
          memory >= options_.maxOutputBlockMemory) {
        break;
      }
    }
    if (cancellationHandle_ != nullptr) {
      cancellationHandle_->throwIfCancelled();
    }
    return block;
  }

 private:
  // Return the comparator of the `heap_`. Its arguments are reversed, such that
  // the max-heap of the standard library acts as a min-heap. The result is
  // deterministic for a fixed configuration in any case; only with
  // `MergeOptions::stableTieBreaking` are ties additionally broken by the index
  // of the run, which makes the tie order independent of the number of chunks.
  //
  // NOTE: The default (unstable) path calls the `comparator` exactly once per
  // heap comparison. The stable path needs a second call whenever the first one
  // returns `false`, which for the expensive comparators of the index build
  // (a full ICU collation, for example) is a substantial cost. The flag is
  // therefore captured once here and not read per comparison.
  auto heapComparator() const {
    return [comparator = comparator_, isStable = options_.stableTieBreaking](
               const Cursor* a, const Cursor* b) {
      const auto& elA = ql::ranges::begin(a->block_)[a->idx_];
      const auto& elB = ql::ranges::begin(b->block_)[b->idx_];
      if (!isStable) {
        return (*comparator)(elB, elA);
      }
      if ((*comparator)(elB, elA)) {
        return true;
      }
      if ((*comparator)(elA, elB)) {
        return false;
      }
      return b->run_ < a->run_;
    };
  }

  // Set up the cursors of all runs that contribute to this chunk, and build the
  // initial heap.
  void initialize() {
    size_t numRuns = input_->numRuns();
    cursors_.reserve(numRuns);
    for (size_t run = 0; run < numRuns; ++run) {
      size_t numBlocks = input_->numBlocks(run);
      // The first block that may contain an element that is not smaller than
      // `lo`.
      size_t startBlock = 0;
      if (lo_.has_value()) {
        startBlock = firstIndexWhere(numBlocks, [this, run](size_t block) {
          return !(*comparator_)(input_->lastKey(run, block), lo_.value());
        });
      }
      // The first block all of whose elements are not smaller than `hi`, that
      // is the (exclusive) end of the range of blocks.
      //
      // NOTE: The predicate is deliberately `!comparator(firstKey, hi)` and not
      // `comparator(hi, firstKey)`. The latter would be off by one and would
      // read one superfluous block whenever `firstKey(block) == hi`.
      size_t endBlock = numBlocks;
      if (hi_.has_value()) {
        endBlock = firstIndexWhere(numBlocks, [this, run](size_t block) {
          return !(*comparator_)(input_->firstKey(run, block), hi_.value());
        });
      }
      if (startBlock >= endBlock) {
        continue;
      }
      cursors_.emplace_back(run, startBlock, endBlock,
                            input_->makeEmptyBlock());
    }
    heap_.reserve(cursors_.size());
    for (auto& cursor : cursors_) {
      if (advance(cursor)) {
        heap_.push_back(&cursor);
      }
    }
    ql::ranges::make_heap(heap_, heapComparator());
  }

  // Make sure that the `cursor` points to a valid element, reading further
  // blocks if necessary. Return `false` if the `cursor` is exhausted.
  bool advance(Cursor& cursor) {
    while (cursor.idx_ == cursor.end_) {
      if (cursor.nextBlock_ == cursor.endBlock_) {
        return false;
      }
      size_t block = cursor.nextBlock_;
      ++cursor.nextBlock_;
      cursor.block_ = input_->readBlock(cursor.run_, block);
      cursor.idx_ = 0;
      cursor.end_ = blockSize(cursor.block_);
      // Only the very first block of the chunk can contain elements that are
      // smaller than `lo`, and only the very last one can contain elements that
      // are not smaller than `hi`.
      if (block == cursor.firstBlock_ && lo_.has_value()) {
        cursor.idx_ = static_cast<size_t>(
            ql::ranges::lower_bound(cursor.block_, lo_.value(), *comparator_) -
            ql::ranges::begin(cursor.block_));
      }
      if (block + 1 == cursor.endBlock_ && hi_.has_value()) {
        cursor.end_ = static_cast<size_t>(
            ql::ranges::lower_bound(cursor.block_, hi_.value(), *comparator_) -
            ql::ranges::begin(cursor.block_));
      }
      AD_CORRECTNESS_CHECK(cursor.idx_ <= cursor.end_);
    }
    return true;
  }
};
}  // namespace detail
}  // namespace ad_utility::parallelBlockMerge

#endif  // QLEVER_SRC_UTIL_PARALLELBLOCKMERGE_CHUNKMERGER_H
