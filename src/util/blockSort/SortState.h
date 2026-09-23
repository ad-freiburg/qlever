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
#include <cstdint>
#include <exception>
#include <iterator>
#include <mutex>
#include <thread>
#include <utility>
#include <vector>

#include "backports/asio.h"
#include "util/Exception.h"
#include "util/blockSort/TaskGroup.h"

namespace ad_utility::blockSort::detail {

// The parts of Boost.Sort that are reused as they are: `range` with its merge
// and move primitives, and the index helpers `block_pos` (a block position plus
// a side bit) and `compare_block_pos` (compares blocks by their first element).
namespace bsc = boost::sort::common;
namespace bsd = boost::sort::blk_detail;

// A fixed pool of scratch buffers of `bufferSize` elements each, borrowed by
// the tasks that merge or move blocks. Replaces the `thread_local` buffer of
// Boost's `backbone`, which doesn't work for coroutines that may change
// threads.
//
// A buffer is never held across a suspension point, so one buffer per thread
// is enough and `acquire()` doesn't wait in practice.
template <typename Value>
class ScratchBuffers {
 private:
  std::vector<Value> storage_;
  size_t bufferSize_;
  std::mutex mutex_;
  // The buffers that are currently not borrowed.
  std::vector<Value*> unused_;

 public:
  // A borrowed buffer, returned to the pool on destruction.
  class Lease {
   private:
    ScratchBuffers* pool_;
    Value* buffer_;

   public:
    Lease(ScratchBuffers* pool, Value* buffer) : pool_{pool}, buffer_{buffer} {}
    Lease(Lease&& other) noexcept
        : pool_{std::exchange(other.pool_, nullptr)}, buffer_{other.buffer_} {}
    Lease(const Lease&) = delete;
    Lease& operator=(const Lease&) = delete;
    Lease& operator=(Lease&&) = delete;
    ~Lease() {
      if (pool_ != nullptr) {
        pool_->release(buffer_);
      }
    }
    bsc::range<Value*> range() const {
      return {buffer_, buffer_ + pool_->bufferSize_};
    }
  };

  // Allocate `numBuffers` buffers of `bufferSize` copies of `initialValue`
  // (the elements are move-assigned to, so they have to be live objects).
  ScratchBuffers(size_t numBuffers, size_t bufferSize,
                 const Value& initialValue)
      : storage_(numBuffers * bufferSize, initialValue),
        bufferSize_{bufferSize} {
    unused_.reserve(numBuffers);
    for (size_t i = 0; i < numBuffers; ++i) {
      unused_.push_back(storage_.data() + i * bufferSize_);
    }
  }

  // The `Lease`s point into this object.
  ScratchBuffers(const ScratchBuffers&) = delete;
  ScratchBuffers& operator=(const ScratchBuffers&) = delete;

  // Borrow a buffer. Only spins if more threads run the executor than there
  // are buffers.
  Lease acquire() {
    while (true) {
      {
        std::lock_guard<std::mutex> lock{mutex_};
        if (!unused_.empty()) {
          Value* buffer = unused_.back();
          unused_.pop_back();
          return Lease{this, buffer};
        }
      }
      std::this_thread::yield();
    }
  }

 private:
  void release(Value* buffer) {
    std::lock_guard<std::mutex> lock{mutex_};
    unused_.push_back(buffer);
  }
};

// The number of blocks that a single task merges or moves.
constexpr uint32_t groupSize = 64;

// The state shared by the tasks of a single sort, Boost's `backbone` without
// its work stack. The input is divided into `numBlocks_` blocks of `BlockSize`
// elements; only the last one (the *tail*) may be shorter.
template <uint32_t BlockSize, typename Iterator, typename Compare>
class SortState {
 public:
  using Value = typename std::iterator_traits<Iterator>::value_type;
  using RangeIt = bsc::range<Iterator>;
  using RangePos = bsc::range<size_t>;
  using CompareBlockPos = bsd::compare_block_pos<BlockSize, Iterator, Compare>;
  static constexpr uint32_t blockSize_ = BlockSize;

  // The whole range to sort.
  RangeIt globalRange_;
  // `index_[i]` is the block that ends up at position `i`.
  std::vector<bsd::block_pos> index_;
  size_t numElements_;
  size_t numBlocks_;
  // The last block if it is incomplete, empty otherwise.
  RangeIt tailRange_;
  Compare cmp_;
  ScratchBuffers<Value> buffers_;
  ql::any_io_executor executor_;
  ErrorSink errors_;

  // Sort the non-empty `[first, last)` with `numBuffers` scratch buffers.
  SortState(Iterator first, Iterator last, Compare cmp, size_t numBuffers,
            ql::any_io_executor executor)
      : globalRange_{first, last},
        numElements_{static_cast<size_t>(last - first)},
        numBlocks_{(numElements_ + BlockSize - 1) / BlockSize},
        cmp_{std::move(cmp)},
        buffers_{numBuffers, BlockSize, Value(*first)},
        executor_{std::move(executor)} {
    AD_CORRECTNESS_CHECK(first != last);
    index_.reserve(numBlocks_ + 1);
    for (size_t i = 0; i < numBlocks_; ++i) {
      index_.emplace_back(bsd::block_pos{i});
    }
    size_t numTailElements = numElements_ % BlockSize;
    tailRange_.first =
        numTailElements == 0 ? last : first + (numBlocks_ - 1) * BlockSize;
    tailRange_.last = last;
  }

  // The first element of the block at physical position `pos`.
  Iterator getBlockBegin(size_t pos) const {
    return globalRange_.first + pos * BlockSize;
  }

  // The elements of the block at physical position `pos`.
  RangeIt getRange(size_t pos) const {
    Iterator first = getBlockBegin(pos);
    Iterator last =
        pos == numBlocks_ - 1 ? globalRange_.last : first + BlockSize;
    return {first, last};
  }

  typename ScratchBuffers<Value>::Lease acquireBuffer() {
    return buffers_.acquire();
  }

  bool hasError() const noexcept { return errors_.hasError(); }
  void storeError(std::exception_ptr error) noexcept {
    errors_.store(std::move(error));
  }

  TaskGroup makeTaskGroup() { return TaskGroup{executor_, errors_}; }
};

}  // namespace ad_utility::blockSort::detail

#endif  // QLEVER_REDUCED_FEATURE_SET_FOR_CPP17

#endif  // QLEVER_SRC_UTIL_BLOCKSORT_SORTSTATE_H
