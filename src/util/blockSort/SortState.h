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

#include <atomic>
#include <boost/asio/awaitable.hpp>
#include <boost/sort/block_indirect_sort/blk_detail/block.hpp>
#include <boost/sort/common/range.hpp>
#include <cstddef>
#include <iterator>
#include <mutex>
#include <range/v3/range/conversion.hpp>
#include <utility>
#include <vector>

#include "backports/asio.h"
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
    {
      auto buffers = buffers_.wlock();
      if (!buffers->unused_.empty()) {
        Buffer buffer = std::move(buffers->unused_.back());
        buffers->unused_.pop_back();
        return Lease{this, std::move(buffer)};
      }
      // Make room for all buffers, so that returning one never allocates.
      buffers->unused_.reserve(++buffers->numAllocated_);
    }
    return Lease{this, Buffer(bufferSize_, prototype_)};
  }
};

// The number of blocks that a single task merges or moves.
constexpr size_t BLOCKS_PER_TASK = 64;

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
  using IteratorRange = bsc::range<Iterator>;
  using RangePos = bsc::range<size_t>;

  // The whole range to sort.
  IteratorRange globalRange_;
  size_t blockSize_;
  size_t maxElementsPerTask_;
  size_t numElements_;
  size_t numBlocks_;
  // `index_[i]` is the block that ends up at position `i`.
  std::vector<bsd::block_pos> index_;
  // The last block if it is incomplete, empty otherwise.
  IteratorRange tailRange_;
  Compare cmp_;
  ScratchBuffers<Value> buffers_;
  ql::any_io_executor executor_;
  // Set as soon as any task of this sort has failed, see `TaskGroup`.
  std::atomic<bool> stopped_{false};

  // Sort `[first, last)`. The range must not be empty, which `runSort` makes
  // sure of.
  SortState(Iterator first, Iterator last, Compare cmp, SortParams params,
            ql::any_io_executor executor)
      : globalRange_{first, last},
        blockSize_{params.blockSize},
        maxElementsPerTask_{params.maxElementsPerTask},
        numElements_{static_cast<size_t>(last - first)},
        numBlocks_{(numElements_ + blockSize_ - 1) / blockSize_},
        index_{::ranges::to<std::vector<bsd::block_pos>>(
            ad_utility::integerRange(numBlocks_))},
        tailRange_{numElements_ % blockSize_ == 0
                       ? last
                       : getBlockBegin(numBlocks_ - 1),
                   last},
        cmp_{std::move(cmp)},
        buffers_{blockSize_, Value(*first)},
        executor_{std::move(executor)} {}

  // The first element of the block at physical position `pos`.
  [[nodiscard]] Iterator getBlockBegin(size_t pos) const {
    return globalRange_.first + pos * blockSize_;
  }

  // The elements of the block at physical position `pos`.
  [[nodiscard]] IteratorRange getBlock(size_t pos) const {
    Iterator first = getBlockBegin(pos);
    Iterator end =
        pos == numBlocks_ - 1 ? globalRange_.last : first + blockSize_;
    return {first, end};
  }

  // Whether the block `a` sorts before the block `b` by their first elements.
  [[nodiscard]] bool blockIsLessByFirstElement(bsd::block_pos a,
                                               bsd::block_pos b) const {
    return cmp_(*getBlockBegin(a.pos()), *getBlockBegin(b.pos()));
  }

  // Borrow one of the scratch buffers of `blockSize_` elements.
  [[nodiscard]] typename ScratchBuffers<Value>::Lease acquireBuffer() {
    return buffers_.acquire();
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

}  // namespace ad_utility::blockSort::detail

#endif  // QLEVER_REDUCED_FEATURE_SET_FOR_CPP17

#endif  // QLEVER_SRC_UTIL_BLOCKSORT_SORTSTATE_H
