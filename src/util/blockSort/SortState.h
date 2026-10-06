// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.
//
// Derived from Boost.Sort, file
// `boost/sort/block_indirect_sort/blk_detail/backbone.hpp`:
// Copyright (c) 2016 Francisco Jose Tapia (fjtapia@gmail.com)
// Distributed under the Boost Software License, Version 1.0. (See the
// accompanying file `LICENSE_1_0.txt` or copy at
// http://www.boost.org/LICENSE_1_0.txt)

#ifndef QLEVER_SRC_UTIL_BLOCKSORT_SORTSTATE_H
#define QLEVER_SRC_UTIL_BLOCKSORT_SORTSTATE_H

#ifndef QLEVER_REDUCED_FEATURE_SET_FOR_CPP17

#include <absl/functional/any_invocable.h>

#include <algorithm>
#include <atomic>
#include <bit>
#include <boost/asio/awaitable.hpp>
#include <boost/sort/block_indirect_sort/blk_detail/block.hpp>
#include <boost/sort/common/range.hpp>
#include <cstddef>
#include <cstdint>
#include <mutex>
#include <optional>
#include <range/v3/range/conversion.hpp>
#include <utility>
#include <vector>

#include "backports/asio.h"
#include "backports/iterator.h"
#include "util/Exception.h"
#include "util/NoCopyNoMove.h"
#include "util/Synchronized.h"
#include "util/Views.h"
#include "util/blockSort/TaskGroup.h"

namespace ad_utility::blockSort::detail {

// The parts of Boost.Sort that are reused as they are: `range` with its merge
// and move primitives, and `block_pos` (a block position plus a side bit). We
// use Boost's `range` instead of `ql::ranges::subrange`, because these
// primitives only accept the former.
//
// The primitives on `bsc::range` that are used, all on sorted ranges:
// * `move_forward(dest, src)`: move the elements of `src` to the beginning of
//   `dest` (note the order of the arguments).
// * `is_mergeable(a, b, cmp)`: whether `a` and `b` overlap, i.e. whether `a`
//   followed by `b` is not sorted.
// * `merge_uncontiguous(a, b, buffer, cmp)`: merge `a` and `b`, which need not
//   be adjacent, such that `a` gets the smallest elements and `b` the rest.
// * `merge_flow(target, buffer, next, cmp)`: one step of merging a run block by
//   block. `target` is a block whose elements have been moved away, `buffer`
//   holds the elements carried over. Move the smallest elements of `buffer` and
//   `next` to `target`, and the rest to `buffer`. All three have the same size.
//
// NOTE: As in Boost, the end of a `bsc::range` is called `last`, although it
// points past the last element, like the `end` of our iterator pairs.
namespace bsc = boost::sort::common;
namespace bsd = boost::sort::blk_detail;

// A pool of scratch buffers of `bufferSize` elements each, borrowed by the
// tasks that merge or move blocks. Replaces the `thread_local` buffer of
// Boost's `backbone`, which doesn't work for coroutines that may change
// threads.
//
// A buffer is allocated when it is first needed and reused afterwards, so
// `acquire()` never waits. A buffer is never held across a suspension point,
// so there are at most as many buffers as threads that run the executor.
//
// Not movable, because the `Lease`s point into this object.
template <typename Value>
class ScratchBuffers : public ad_utility::NoCopyNoMove {
 private:
  using Buffer = std::vector<Value>;
  size_t bufferSize_;
  // The value that new buffers are filled with, a copy of the first element of
  // the input, because not every `Value` is default-constructible (e.g. an
  // `IdTable` row with a dynamic number of columns). It is only a placeholder:
  // the elements of a buffer are always overwritten before they are read, but
  // they have to be live objects, because they are move-assigned to (Boost does
  // the same).
  Value prototype_;
  struct Buffers {
    std::vector<Buffer> unused_;
    size_t numAllocated_ = 0;
  };
  ad_utility::Synchronized<Buffers, std::mutex> buffers_;

 public:
  // A borrowed buffer, returned to the pool on destruction.
  //
  // Not movable, so that it always gives its buffer back exactly once.
  // `acquire()` can still return it by value, because returning a prvalue needs
  // no move since C++17.
  class Lease : public ad_utility::NoCopyNoMove {
   private:
    ScratchBuffers* pool_;
    Buffer buffer_;

   public:
    // Hold `buffer`, which belongs to `pool`.
    Lease(ScratchBuffers* pool, Buffer buffer)
        : pool_{pool}, buffer_{std::move(buffer)} {}
    // Doesn't allocate, see `acquire()`.
    ~Lease() { pool_->buffers_.wlock()->unused_.push_back(std::move(buffer_)); }
    // The elements of the borrowed buffer.
    [[nodiscard]] bsc::range<Value*> range() {
      return {buffer_.data(), buffer_.data() + buffer_.size()};
    }
  };

  // A pool of buffers of `bufferSize` copies of `prototype`.
  ScratchBuffers(size_t bufferSize, Value prototype)
      : bufferSize_{bufferSize}, prototype_{std::move(prototype)} {}

  // Borrow a free buffer, or allocate a new one if all are taken.
  [[nodiscard]] Lease acquire() {
    std::optional<Buffer> unused =
        buffers_.withWriteLock([](Buffers& buffers) -> std::optional<Buffer> {
          if (buffers.unused_.empty()) {
            // Make room for all buffers, so that returning one never
            // allocates.
            buffers.unused_.reserve(++buffers.numAllocated_);
            return std::nullopt;
          }
          Buffer buffer = std::move(buffers.unused_.back());
          buffers.unused_.pop_back();
          return buffer;
        });
    // Allocate a new buffer outside of the lock.
    return Lease{this, unused.has_value() ? std::move(unused).value()
                                          : Buffer(bufferSize_, prototype_)};
  }
};

// The default number of elements that one task sorts on its own (see
// `SortState::maxElementsPerTask_`), like in Boost: bigger elements get smaller
// tasks, so that the work per task stays roughly comparable. It is 2^18 for
// elements of a single byte and halves at 2, 8, 32, 128, and 512 bytes, down to
// 2^13. For example, 4 bytes give 2^17, 8 bytes 2^16, and 64 bytes 2^15.
template <typename Value>
constexpr size_t defaultMaxElementsPerTask() {
  auto bitsOfSize = static_cast<uint32_t>(std::bit_width(sizeof(Value))) / 2;
  return size_t{1} << (18 - std::min(bitsOfSize, uint32_t{5}));
}

// The default number of elements per block (Boost's `block_size`): bigger
// elements get smaller blocks, so that a block stays in cache.
template <typename Value>
[[nodiscard]] constexpr uint32_t blockSizeFor() {
  constexpr size_t numBytes = sizeof(Value);
  constexpr uint32_t sizes[] = {4096, 4096, 4096, 4096, 2048,
                                1024, 768,  512,  256,  128};
  // Indexed by the number of bits of `numBytes - 1`, capped at 256 bytes.
  return sizes[std::bit_width(std::min(numBytes, size_t{257}) - 1)];
}

// The number of blocks that a single task merges or moves.
constexpr size_t BLOCKS_PER_TASK = 64;

// The size of the parts when `numBlocks` blocks are cut into parts of at most
// `BLOCKS_PER_TASK` blocks that are as equal as possible. Unlike Boost, this
// rounds up, so that the last part is the smallest (like the last chunk of
// `ad_utility::chunkedIotaView`) instead of the largest.
constexpr size_t partSize(size_t numBlocks) {
  size_t numParts = (numBlocks + BLOCKS_PER_TASK - 1) / BLOCKS_PER_TASK;
  return (numBlocks + numParts - 1) / numParts;
}

// The tuning parameters of a sort. The public interface derives them from the
// size of the elements, see `blockIndirectSort`; only tests use smaller values,
// to reach all code paths with small inputs.
struct SortParams {
  // The number of elements per block.
  size_t blockSize;
  // The number of elements that a single task sorts on its own. Must be at
  // least 16, because the pivot selection needs nine distinct samples.
  size_t maxElementsPerTask;
};

// The state shared by the tasks of a single sort, similar to
// `boost::sort::blk_detail::backbone`.
template <typename Compare>
class SortState {
 public:
  // The maximal number of elements that a single task sorts on its own (with
  // `pdqsort`); bigger ranges are split further, see `parallelQuicksort`. The
  // default is `defaultMaxElementsPerTask`.
  size_t maxElementsPerTask_;
  // The comparator. It is called concurrently by all tasks of the sort.
  Compare cmp_;
  // The executor on which the tasks run. The `TaskGroup`s of the sort keep a
  // reference to it.
  ql::any_io_executor executor_;
  // Set as soon as any task of this sort has failed, see `TaskGroup`.
  std::atomic<bool> stopped_{false};

  // A sort by `cmp` on `executor` whose tasks sort at most `maxElementsPerTask`
  // elements on their own. `maxElementsPerTask` must be at least 16, because
  // the pivot selection needs nine distinct samples.
  SortState(Compare cmp, size_t maxElementsPerTask,
            ql::any_io_executor executor)
      : maxElementsPerTask_{maxElementsPerTask},
        cmp_{std::move(cmp)},
        executor_{std::move(executor)} {
    AD_CONTRACT_CHECK(maxElementsPerTask_ >= 16);
  }

  // Run `body`, which spawns the children of a task of this sort, and wait for
  // them, see `TaskGroup::withChildren`.
  [[nodiscard]] net::awaitable<void> withChildren(
      absl::AnyInvocable<void(TaskGroup&)> body) {
    return TaskGroup::withChildren(executor_, stopped_, std::move(body));
  }

  // Run `inlined` and `spawned` concurrently and wait for both, see
  // `TaskGroup::runConcurrently`.
  [[nodiscard]] net::awaitable<void> runConcurrently(
      net::awaitable<void> inlined, net::awaitable<void> spawned) {
    return TaskGroup::runConcurrently(executor_, stopped_, std::move(inlined),
                                      std::move(spawned));
  }
};

// The state shared by the tasks of a block indirect sort of `[begin, end)`,
// the rest of Boost's `backbone` without its work stack. The input is divided
// into `numBlocks_` blocks of `blockSize_` elements; only the last one (the
// *tail*) may be shorter.
template <typename Iterator, typename Compare>
class BlockSortState : public SortState<Compare> {
 public:
  using Value = ql::iter_value_t<Iterator>;
  using IteratorRange = bsc::range<Iterator>;
  using RangePos = bsc::range<size_t>;

  // The whole range to sort.
  IteratorRange globalRange_;
  // The number of elements per block, see `SortParams::blockSize`.
  size_t blockSize_;
  // The number of elements of `globalRange_`.
  size_t numElements_;
  // The number of blocks, including the tail.
  size_t numBlocks_;
  // `index_[i]` is the block that ends up at position `i`.
  std::vector<bsd::block_pos> index_;
  // The last block if it is incomplete, empty otherwise. The tail is never
  // moved, so it always stays the last entry of `index_` (see `mergeTail`).
  IteratorRange tailRange_;
  // The scratch buffers of `blockSize_` elements, see `acquireBuffer`.
  ScratchBuffers<Value> buffers_;

  // Sort `[begin, end)`. The range must not be empty, which `sortOnExecutor`
  // makes sure of.
  BlockSortState(Iterator begin, Iterator end, Compare cmp, SortParams params,
                 ql::any_io_executor executor)
      : SortState<Compare>{std::move(cmp), params.maxElementsPerTask,
                           std::move(executor)},
        globalRange_{begin, end},
        blockSize_{params.blockSize},
        numElements_{static_cast<size_t>(end - begin)},
        numBlocks_{(numElements_ + blockSize_ - 1) / blockSize_},
        index_{::ranges::to<std::vector<bsd::block_pos>>(
            ad_utility::integerRange(numBlocks_))},
        tailRange_{numElements_ % blockSize_ == 0
                       ? end
                       : getBlockBegin(numBlocks_ - 1),
                   end},
        buffers_{blockSize_, Value(*begin)} {}

  // The first element of the block at physical position `pos`. This also works
  // for `pos == numBlocks_` (a block past the last one), which gives the end of
  // the range, so that `getBlockBegin(pos)` is always the end of the blocks
  // before `pos`. The clamping handles the tail, which is shorter than
  // `blockSize_`.
  [[nodiscard]] Iterator getBlockBegin(size_t pos) const {
    return globalRange_.first + std::min(pos * blockSize_, numElements_);
  }

  // The elements of the block at physical position `pos`.
  [[nodiscard]] IteratorRange getBlock(size_t pos) const {
    return {getBlockBegin(pos), getBlockBegin(pos + 1)};
  }

  // Whether the block `a` sorts before the block `b` by their first elements.
  [[nodiscard]] bool blockIsLessByFirstElement(bsd::block_pos a,
                                               bsd::block_pos b) const {
    return this->cmp_(*getBlockBegin(a.pos()), *getBlockBegin(b.pos()));
  }

  // Borrow one of the scratch buffers of `blockSize_` elements.
  [[nodiscard]] typename ScratchBuffers<Value>::Lease acquireBuffer() {
    return buffers_.acquire();
  }
};

}  // namespace ad_utility::blockSort::detail

#endif  // QLEVER_REDUCED_FEATURE_SET_FOR_CPP17

#endif  // QLEVER_SRC_UTIL_BLOCKSORT_SORTSTATE_H
