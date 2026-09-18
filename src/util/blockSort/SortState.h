// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_UTIL_BLOCKSORT_SORTSTATE_H
#define QLEVER_SRC_UTIL_BLOCKSORT_SORTSTATE_H

#ifndef QLEVER_REDUCED_FEATURE_SET_FOR_CPP17

#include <boost/sort/block_indirect_sort/blk_detail/block.hpp>
#include <boost/sort/common/range.hpp>
#include <boost/sort/common/util/algorithm.hpp>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <iterator>
#include <memory>
#include <mutex>
#include <new>
#include <thread>
#include <utility>
#include <vector>

#include "backports/asio.h"
#include "util/Exception.h"
#include "util/blockSort/TaskGroup.h"

namespace ad_utility::blockSort::detail {

// The parts of Boost.Sort that we reuse instead of writing them again: the
// `range` type together with its merge and move primitives, the low-level
// algorithms (`nbits64`, `initialize`, `destroy`, ...), and the two index
// helpers `block_pos` (a block position plus a side bit, packed into a
// `size_t`) and `compare_block_pos` (which compares two blocks by their first
// element).
namespace bsc = boost::sort::common;
namespace bscu = boost::sort::common::util;
namespace bsd = boost::sort::blk_detail;

// A fixed set of scratch buffers of `bufferSize` elements each, which the tasks
// of a sort borrow while they merge or move blocks.
//
// This replaces the `static thread_local value_t* buf` of Boost's `backbone`,
// which cannot be carried over to coroutines: a coroutine is not tied to a
// thread, so a buffer that a coroutine picked up from the thread it started on
// may well belong to a different coroutine after the next suspension.
//
// A borrowed buffer is therefore never held across a suspension point — every
// user of one is a stretch of straight-line code — which is also why
// `numBuffers` buffers are always enough for `numBuffers` threads, and why
// `acquire()` never has to wait in practice: a thread that holds a buffer
// always gives it back without waiting for anything.
template <typename Value>
class ScratchBuffers {
 private:
  // The raw storage. `Value` may be over-aligned, so this needs the aligned
  // form of `operator new` rather than plain `malloc`.
  struct Deallocate {
    void operator()(Value* pointer) const noexcept {
      ::operator delete(static_cast<void*>(pointer),
                        std::align_val_t{alignof(Value)});
    }
  };
  std::unique_ptr<Value, Deallocate> storage_;
  size_t bufferSize_;
  size_t numValues_;
  std::mutex mutex_;
  // The buffers that nobody currently holds. Contended `numBlocks / GroupSize`
  // times per sort at most, so a plain mutex is more than good enough.
  std::vector<Value*> unused_;

 public:
  // A borrowed buffer, which is returned to the pool when this object goes out
  // of scope.
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

  // Allocate `numBuffers` buffers of `bufferSize` elements. The elements are
  // *initialized* (the algorithm move-assigns into them, so they have to be
  // live objects), which `initialValue` is the seed for: it is moved into the
  // first slot, that slot into the second, and so on, and the last slot is
  // finally moved back into `initialValue`. This needs nothing but a move
  // constructor and a move assignment of `Value`, see
  // `boost::sort::common::initialize`.
  ScratchBuffers(size_t numBuffers, size_t bufferSize, Value& initialValue)
      : storage_{static_cast<Value*>(
            ::operator new(numBuffers * bufferSize * sizeof(Value),
                           std::align_val_t{alignof(Value)}))},
        bufferSize_{bufferSize},
        numValues_{numBuffers * bufferSize} {
    bsc::initialize(allValues(), initialValue);
    unused_.reserve(numBuffers);
    for (size_t i = 0; i < numBuffers; ++i) {
      unused_.push_back(storage_.get() + i * bufferSize_);
    }
  }

  ScratchBuffers(const ScratchBuffers&) = delete;
  ScratchBuffers& operator=(const ScratchBuffers&) = delete;

  ~ScratchBuffers() { bsc::destroy(allValues()); }

  // Borrow a buffer. There is always one available unless more threads run the
  // executor than the sort was told about; in that case this spins, which
  // terminates because a buffer is never held across a suspension point.
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
  bsc::range<Value*> allValues() const {
    return {storage_.get(), storage_.get() + numValues_};
  }

  void release(Value* buffer) {
    std::lock_guard<std::mutex> lock{mutex_};
    unused_.push_back(buffer);
  }
};

// Everything that the tasks of a single sort share. This is the equivalent of
// Boost's `backbone`, minus its concurrent stack of work items and its
// `exec()` loop (the executor does that job, see `TaskGroup`) and with the
// thread local buffer replaced by the `ScratchBuffers` above.
//
// The elements to sort are divided into `numBlocks_` blocks of `BlockSize`
// elements (the last one, the *tail*, may be shorter). The algorithm first
// sorts the blocks logically, by permuting `index_`, and only afterwards moves
// them to where the index says they belong.
template <uint32_t BlockSize, uint32_t GroupSize, typename Iterator,
          typename Compare>
class SortState {
 public:
  using Value = typename std::iterator_traits<Iterator>::value_type;
  using RangeIt = bsc::range<Iterator>;
  using RangeBuf = bsc::range<Value*>;
  using RangePos = bsc::range<size_t>;
  using CompareBlockPos = bsd::compare_block_pos<BlockSize, Iterator, Compare>;
  static constexpr uint32_t blockSize_ = BlockSize;
  static constexpr uint32_t groupSize_ = GroupSize;

  // The whole range to sort.
  RangeIt globalRange_;
  // The logical order of the blocks: `index_[i]` is the block that ends up at
  // position `i`. Permuted by the merging, applied by the moving.
  std::vector<bsd::block_pos> index_;
  size_t numElements_;
  size_t numBlocks_;
  // The elements of the last block, which is the only one that may be shorter
  // than `BlockSize`. Empty if the last block is full.
  RangeIt tailRange_;
  Compare cmp_;
  ScratchBuffers<Value> buffers_;
  ql::any_io_executor executor_;
  ErrorSink errors_;

  // Set up the state for sorting `[first, last)`, with `numBuffers` scratch
  // buffers (one per thread that is expected to run the `executor`). The range
  // must not be empty.
  SortState(Iterator first, Iterator last, Compare cmp, size_t numBuffers,
            ql::any_io_executor executor)
      : globalRange_{first, last},
        numElements_{static_cast<size_t>(last - first)},
        numBlocks_{(numElements_ + BlockSize - 1) / BlockSize},
        cmp_{std::move(cmp)},
        buffers_{numBuffers, BlockSize, *first},
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

  // The first element of the block at position `pos`. NOTE: For a block that
  // has not been moved yet, the physical position *is* the logical position.
  Iterator getBlockBegin(size_t pos) const {
    return globalRange_.first + pos * BlockSize;
  }

  // The elements of the block at position `pos`, which for the last block are
  // only the `numElements_ % BlockSize` elements of the tail.
  RangeIt getRange(size_t pos) const {
    Iterator first = getBlockBegin(pos);
    Iterator last =
        pos == numBlocks_ - 1 ? globalRange_.last : first + BlockSize;
    return {first, last};
  }

  // Borrow one of the scratch buffers, see `ScratchBuffers`.
  typename ScratchBuffers<Value>::Lease acquireBuffer() {
    return buffers_.acquire();
  }

  bool hasError() const noexcept { return errors_.hasError(); }
  void storeError(std::exception_ptr error) noexcept {
    errors_.store(std::move(error));
  }

  // A fresh group of children for a task that is about to fan out.
  TaskGroup makeTaskGroup() { return TaskGroup{executor_, errors_}; }
};

}  // namespace ad_utility::blockSort::detail

#endif  // QLEVER_REDUCED_FEATURE_SET_FOR_CPP17

#endif  // QLEVER_SRC_UTIL_BLOCKSORT_SORTSTATE_H
