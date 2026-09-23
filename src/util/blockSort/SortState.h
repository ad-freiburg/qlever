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
#include "util/blockSort/BoostSortHeaders.h"
#include "util/blockSort/TaskGroup.h"

namespace ad_utility::blockSort::detail {

// The parts of Boost.Sort that are reused as they are: `range` with its merge
// and move primitives, and the index helpers `block_pos` (a block position plus
// a side bit) and `compare_block_pos` (compares blocks by their first element).
namespace bsc = boost::sort::common;
namespace bscu = boost::sort::common::util;
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
  // Aligned, because `Value` may be over-aligned.
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

  // Allocate `numBuffers` buffers of `bufferSize` elements. The elements have
  // to be live objects (they are move-assigned to), so they are constructed by
  // chaining moves from `initialValue`, which gets its value back at the end,
  // see `boost::sort::common::initialize`.
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
  bsc::range<Value*> allValues() const {
    return {storage_.get(), storage_.get() + numValues_};
  }

  void release(Value* buffer) {
    std::lock_guard<std::mutex> lock{mutex_};
    unused_.push_back(buffer);
  }
};

// The state shared by the tasks of a single sort, Boost's `backbone` without
// its work stack. The input is divided into `numBlocks_` blocks of `BlockSize`
// elements; only the last one (the *tail*) may be shorter.
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
  // `index_[i]` is the block that ends up at position `i`.
  std::vector<bsd::block_pos> index_;
  size_t numElements_;
  size_t numBlocks_;
  // The last block if it is incomplete, empty otherwise.
  RangeIt tailRange_;
  Compare cmp_;
  // A copy of the first element to initialize `buffers_` from (a proxy
  // `*first` can't be passed by reference). Must be declared before `buffers_`.
  Value bufferSeed_;
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
        bufferSeed_{*first},
        buffers_{numBuffers, BlockSize, bufferSeed_},
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
