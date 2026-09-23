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

#include <boost/sort/block_indirect_sort/blk_detail/block.hpp>
#include <boost/sort/common/range.hpp>
#include <cstddef>
#include <exception>
#include <iterator>
#include <mutex>
#include <thread>
#include <utility>
#include <vector>

#include "backports/asio.h"
#include "util/Exception.h"
#include "util/NoCopyNoMove.h"
#include "util/blockSort/TaskGroup.h"

namespace ad_utility::blockSort::detail {

// The parts of Boost.Sort that are reused as they are: `range` with its merge
// and move primitives, and `block_pos` (a block position plus a side bit).
namespace bsc = boost::sort::common;
namespace bsd = boost::sort::blk_detail;

// Hands out the numbers `0, ..., numSlots - 1`, each to one holder at a time.
class SlotPool : public ad_utility::NoCopyNoMove {
 private:
  std::mutex mutex_;
  // The slots that are currently not taken.
  std::vector<size_t> unused_;

 public:
  // Create a pool of `numSlots` free slots.
  explicit SlotPool(size_t numSlots) {
    unused_.reserve(numSlots);
    for (size_t i = 0; i < numSlots; ++i) {
      unused_.push_back(i);
    }
  }

  // Take a free slot. Only spins if all slots are taken, which for the scratch
  // buffers only happens if more threads run the executor than there are
  // buffers.
  [[nodiscard]] size_t acquire() {
    for (;;) {
      {
        std::lock_guard lock{mutex_};
        if (!unused_.empty()) {
          size_t slot = unused_.back();
          unused_.pop_back();
          return slot;
        }
      }
      std::this_thread::yield();
    }
  }

  // Give back a slot that was taken with `acquire()`.
  void release(size_t slot) {
    std::lock_guard lock{mutex_};
    unused_.push_back(slot);
  }
};

// A fixed pool of scratch buffers of `bufferSize` elements each, borrowed by
// the tasks that merge or move blocks. Replaces the `thread_local` buffer of
// Boost's `backbone`, which doesn't work for coroutines that may change
// threads.
//
// A buffer is never held across a suspension point, so one buffer per thread
// is enough and `acquire()` doesn't wait in practice.
//
// Not movable, because the `Lease`s point into this object.
template <typename Value>
class ScratchBuffers : public ad_utility::NoCopyNoMove {
 private:
  std::vector<Value> storage_;
  size_t bufferSize_;
  SlotPool slots_;

 public:
  // A borrowed buffer, returned to the pool on destruction.
  //
  // Not movable, so that it always gives its buffer back exactly once.
  // `acquire()` can still return it by value, because returning a prvalue needs
  // no move since C++17.
  class Lease : public ad_utility::NoCopyNoMove {
   private:
    ScratchBuffers* pool_;
    size_t slot_;

   public:
    // Hold the buffer with index `slot` of `pool`.
    Lease(ScratchBuffers* pool, size_t slot) : pool_{pool}, slot_{slot} {}
    ~Lease() { pool_->slots_.release(slot_); }
    // The elements of the borrowed buffer.
    [[nodiscard]] bsc::range<Value*> range() const {
      Value* first = pool_->storage_.data() + slot_ * pool_->bufferSize_;
      return {first, first + pool_->bufferSize_};
    }
  };

  // Allocate `numBuffers` buffers of `bufferSize` copies of `initialValue`
  // (the elements are move-assigned to, so they have to be live objects).
  ScratchBuffers(size_t numBuffers, size_t bufferSize,
                 const Value& initialValue)
      : storage_(numBuffers * bufferSize, initialValue),
        bufferSize_{bufferSize},
        slots_{numBuffers} {}

  // Borrow a buffer, see `SlotPool::acquire`.
  [[nodiscard]] Lease acquire() { return Lease{this, slots_.acquire()}; }
};

// The number of blocks that a single task merges or moves.
constexpr size_t groupSize = 64;

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

// The state shared by the tasks of a single sort, Boost's `backbone` without
// its work stack. The input is divided into `numBlocks_` blocks of
// `blockSize_` elements; only the last one (the *tail*) may be shorter.
template <typename Iterator, typename Compare>
class SortState {
 public:
  using Value = typename std::iterator_traits<Iterator>::value_type;
  using RangeIt = bsc::range<Iterator>;
  using RangePos = bsc::range<size_t>;

  // The whole range to sort.
  RangeIt globalRange_;
  size_t blockSize_;
  size_t maxElementsPerTask_;
  size_t numElements_;
  size_t numBlocks_;
  // `index_[i]` is the block that ends up at position `i`.
  std::vector<bsd::block_pos> index_;
  // The last block if it is incomplete, empty otherwise.
  RangeIt tailRange_;
  Compare cmp_;
  ScratchBuffers<Value> buffers_;
  ql::any_io_executor executor_;
  ErrorSink errors_;

  // Sort `[first, last)` with `numBuffers` scratch buffers. The range must not
  // be empty, which `runSort` makes sure of.
  SortState(Iterator first, Iterator last, Compare cmp, SortParams params,
            size_t numBuffers, ql::any_io_executor executor)
      : globalRange_{first, last},
        blockSize_{params.blockSize},
        maxElementsPerTask_{params.maxElementsPerTask},
        numElements_{static_cast<size_t>(last - first)},
        numBlocks_{(numElements_ + blockSize_ - 1) / blockSize_},
        tailRange_{numElements_ % blockSize_ == 0
                       ? last
                       : getBlockBegin(numBlocks_ - 1),
                   last},
        cmp_{std::move(cmp)},
        buffers_{numBuffers, blockSize_, Value(*first)},
        executor_{std::move(executor)} {
    index_.reserve(numBlocks_);
    for (size_t i = 0; i < numBlocks_; ++i) {
      index_.emplace_back(i);
    }
  }

  // The first element of the block at physical position `pos`.
  [[nodiscard]] Iterator getBlockBegin(size_t pos) const {
    return globalRange_.first + pos * blockSize_;
  }

  // The elements of the block at physical position `pos`.
  [[nodiscard]] RangeIt getRange(size_t pos) const {
    Iterator first = getBlockBegin(pos);
    Iterator last =
        pos == numBlocks_ - 1 ? globalRange_.last : first + blockSize_;
    return {first, last};
  }

  // Whether the block `a` sorts before the block `b` by their first elements.
  [[nodiscard]] bool blockIsLess(bsd::block_pos a, bsd::block_pos b) const {
    return cmp_(*getBlockBegin(a.pos()), *getBlockBegin(b.pos()));
  }

  // Borrow one of the scratch buffers of `blockSize_` elements.
  [[nodiscard]] typename ScratchBuffers<Value>::Lease acquireBuffer() {
    return buffers_.acquire();
  }

  // Whether any task of this sort has failed.
  [[nodiscard]] bool hasError() const noexcept { return errors_.hasError(); }

  // A new group for the children of a task of this sort.
  [[nodiscard]] TaskGroup makeTaskGroup() {
    return TaskGroup{executor_, errors_};
  }
};

}  // namespace ad_utility::blockSort::detail

#endif  // QLEVER_REDUCED_FEATURE_SET_FOR_CPP17

#endif  // QLEVER_SRC_UTIL_BLOCKSORT_SORTSTATE_H
