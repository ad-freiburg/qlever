//  Copyright 2023, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>
//  Copyright 2025, Bayerische Motoren Werke Aktiengesellschaft (BMW AG)

#ifndef QLEVER_PARALLELMULTIWAYMERGE_H
#define QLEVER_PARALLELMULTIWAYMERGE_H

#include <absl/functional/bind_front.h>

#include "util/AsyncStream.h"
#include "util/Generator.h"
#include "util/TypeTraits.h"
#include "util/ValueSizeGetters.h"
#include "util/Views.h"

namespace ad_utility {

namespace detail {

using namespace ad_utility::memory_literals;

// Call `buffer.push_back(el)`. If `moveElements` is `true`, then `el` gets
// moved. It is necessary to explicitly pass the type `T` stored in the vector
// to enable the usage of this lambda in combination with `std::bind_front` and
// `std::ref`.
CPP_template(bool moveElements, typename T, typename SizeGetter)(
    requires ValueSizeGetter<SizeGetter, T>) constexpr auto pushSingleElement =
    [](std::vector<T>& buffer, MemorySize& sz, auto& el) {
      sz += SizeGetter{}(el);
      if constexpr (moveElements) {
        buffer.push_back(std::move(el));
      } else {
        buffer.push_back(el);
      }
    };

// This concept is fulfilled if `Range` is a range that stores values of type
// `T`.
template <typename Range, typename T>
CPP_concept RangeWithValue = ql::ranges::range<Range> &&
                             std::same_as<ql::ranges::range_value_t<Range>, T>;

// Fulfilled if `Range` is a random access range the elements of which are
// ranges of elements of type `T`, e.g. `std::vector<std::generator<T>>`.
template <typename Range, typename T>
CPP_concept RandomAccessRangeOfRanges =
    ql::ranges::random_access_range<Range> &&
    RangeWithValue<ql::ranges::range_value_t<Range>, T>;

// Merge the elements from the presorted ranges `range1` and `range2` according
// to the `comparator`. The result of the merging will be returned in blocks of
// size `blocksize`. If `moveElements` is true, then the elements from the
// ranges will be moved.
// TODO<joka921> Maybe add a `buffering generator` that automatically stores the
// buffers.
CPP_template(typename T, bool moveElements, typename SizeGetter,
             typename Range1, typename Range2, typename ComparisonFuncT)(
    requires ValueSizeGetter<SizeGetter, T> CPP_and RangeWithValue<Range1, T>
        CPP_and RangeWithValue<Range2, T>
            CPP_and ad_utility::InvocableWithExactReturnType<
                ComparisonFuncT, bool, const T&,
                const T&>) class LazyBinaryMerge
    : public ad_utility::InputRangeMixin<LazyBinaryMerge<
          T, moveElements, SizeGetter, Range1, Range2, ComparisonFuncT>> {
 private:
  MemorySize maxMem_;
  size_t maxBlockSize_;
  Range1 range1_;
  Range2 range2_;
  ComparisonFuncT comparison_;
  bool finished_{false};
  std::vector<T> buffer_{};

  std::pair<ql::ranges::iterator_t<Range1>, ql::ranges::sentinel_t<Range1>>
      it1_{};
  std::pair<ql::ranges::iterator_t<Range2>, ql::ranges::sentinel_t<Range2>>
      it2_{};

 public:
  using value_type = std::vector<T>;
  LazyBinaryMerge(MemorySize maxMem, size_t maxBlockSize, Range1 range1,
                  Range2 range2, ComparisonFuncT comparison)
      : maxMem_{maxMem},
        maxBlockSize_{maxBlockSize},
        range1_{std::move(range1)},
        range2_{std::move(range2)},
        comparison_{comparison} {}

  void getNextBlock() {
    MemorySize sizeOfCurrentBlock = 0_B;

    buffer_.clear();
    buffer_.reserve(maxBlockSize_);

    // Helper lambda to check if we are at the end of a range.
    auto exhausted = [](const auto& itPair) {
      return itPair.first == itPair.second;
    };

    auto isBufferLargeEnough = [this, &sizeOfCurrentBlock] {
      return buffer_.size() >= maxBlockSize_ || sizeOfCurrentBlock >= maxMem_;
    };

    // Push the next element from the range denoted by `itPair` to the `buffer`,
    // and advance the iterator. Return true if the range then is exhausted.
    auto push = [this, &sizeOfCurrentBlock, &exhausted](auto& itPair) {
      auto& it = itPair.first;
      detail::pushSingleElement<moveElements, T, SizeGetter>(
          buffer_, sizeOfCurrentBlock, *it);
      ++it;
      return exhausted(itPair);
    };

    // Push the smaller element one of `*it1` and `*it2` to the `buffer` and
    // advance the corresponding iterator. Return true iff that iterator
    // reaches the end after the increment.
    auto pushSmaller = [this, &push]() {
      if (comparison_(*it1_.first, *it2_.first)) {
        return push(it1_);
      } else {
        return push(it2_);
      }
    };

    if (!exhausted(it1_) && !exhausted(it2_)) {
      while (!pushSmaller() && !isBufferLargeEnough()) {
        // The work is done inside the pushSmaller() function
      }
    }

    auto pushRemainder = [&isBufferLargeEnough, &push,
                          &exhausted](auto& itPair) {
      if (!isBufferLargeEnough() && !exhausted(itPair)) {
        while (!push(itPair) && !isBufferLargeEnough()) {
          // The work is done inside the push() function
        }
      }
    };

    pushRemainder(it1_);
    pushRemainder(it2_);

    finished_ = buffer_.empty();
  }

  void start() {
    // Turn the ranges into `(iterator, end)` pairs.
    auto makeItPair = [](auto& range) {
      return std::pair{ql::ranges::begin(range), ql::ranges::end(range)};
    };

    it1_ = makeItPair(range1_);
    it2_ = makeItPair(range2_);
    getNextBlock();
  }
  bool isFinished() { return finished_; }
  auto& get() { return buffer_; }
  const auto& get() const { return buffer_; }

  void next() { getNextBlock(); }
};

// Return the elements of the `range` in blocks of the given `blocksize`.
// TODO<joka921> This gets much simpler with the buffering generator.
CPP_template(typename T, bool moveElements, typename SizeGetter,
             typename R)(requires ValueSizeGetter<SizeGetter, T> CPP_and
                             RangeWithValue<R, T>) class BatchToVector
    : public ad_utility::InputRangeMixin<
          BatchToVector<T, moveElements, SizeGetter, R>> {
 private:
  MemorySize maxMem_;
  size_t blocksize_;
  R range_;
  ql::ranges::iterator_t<R> it_{};
  mutable bool finished_{false};
  std::vector<T> buffer_{};

 public:
  BatchToVector(MemorySize maxMem, size_t blocksize, R range)
      : maxMem_{maxMem}, blocksize_{blocksize}, range_{std::move(range)} {}

  void getNextBlock() {
    MemorySize curMem = 0_B;

    buffer_.clear();
    buffer_.reserve(blocksize_);

    while (it_ != range_.end()) {
      detail::pushSingleElement<moveElements, T, SizeGetter>(buffer_, curMem,
                                                             *it_);
      ++it_;
      if (buffer_.size() >= blocksize_ || curMem >= maxMem_) {
        break;
      }
    }

    finished_ = buffer_.empty();
  }

  void start() {
    it_ = ql::ranges::begin(range_);
    getNextBlock();
  }
  bool isFinished() { return finished_; }
  auto& get() { return buffer_; }
  const auto& get() const { return buffer_; }

  void next() { getNextBlock(); }
};

// The recursive implementation of `parallelMultiwayMerge` (see below). The
// difference is, that the memory limit in this function is per node in the
// recursion tree.
CPP_template(typename T, bool moveElements, typename SizeGetter, typename R,
             typename ComparisonFuncT)(
    requires detail::RandomAccessRangeOfRanges<R, T> CPP_and
        ValueSizeGetter<SizeGetter, T>
            CPP_and InvocableWithExactReturnType<ComparisonFuncT, bool,
                                                 const T&, const T&>)
    ad_utility::InputRangeTypeErased<std::vector<T>> parallelMultiwayMergeImpl(
        MemorySize maxMemPerNode, size_t blocksize, R&& rangeOfRanges,
        ComparisonFuncT comparison) {
  AD_CORRECTNESS_CHECK(!rangeOfRanges.empty());
  auto moveIf = [](auto& range) -> decltype(auto) {
    if constexpr (moveElements) {
      return std::move(range);
    } else {
      return range;
    }
  };

  using ResultT = InputRangeTypeErased<std::vector<T>>;

  if (rangeOfRanges.size() == 1) {
    return ResultT{detail::BatchToVector<T, moveElements, SizeGetter,
                                         ql::ranges::range_value_t<R>>(
        maxMemPerNode, blocksize, moveIf(rangeOfRanges.front()))};
  } else if (rangeOfRanges.size() == 2) {
    return ResultT{
        detail::LazyBinaryMerge<T, moveElements, SizeGetter,
                                ql::ranges::range_value_t<R>,
                                ql::ranges::range_value_t<R>, ComparisonFuncT>(
            maxMemPerNode, blocksize, moveIf(rangeOfRanges[0]),
            moveIf(rangeOfRanges[1]), comparison)};
  } else {
    size_t size = ql::ranges::size(rangeOfRanges);
    size_t split = size / 2;
    auto beg = rangeOfRanges.begin();
    auto splitIt = beg + split;
    auto end = rangeOfRanges.end();
    auto join = [](auto&& view) {
      return ql::views::join(ad_utility::OwningView{AD_FWD(view)});
    };

    auto parallelMerge = [join, blocksize, comparison, maxMemPerNode](
                             auto it, auto end) {
      auto subRange{ql::ranges::subrange{it, end}};
      return join(parallelMultiwayMergeImpl<T, moveElements, SizeGetter>(
          maxMemPerNode, blocksize, std::move(subRange), comparison));
    };

    auto mergeRange1 = parallelMerge(beg, splitIt);
    auto mergeRange2 = parallelMerge(splitIt, end);

    return ResultT{ad_utility::streams::runStreamAsync(
        detail::LazyBinaryMerge<T, moveElements, SizeGetter,
                                decltype(mergeRange1), decltype(mergeRange2),
                                ComparisonFuncT>(
            maxMemPerNode, blocksize, std::move(mergeRange1),
            std::move(mergeRange2), comparison),
        2)};
  }
}
}  // namespace detail

// Merge the sorted ranges contained in the `rangeOfRanges` according to the
// `comparison`. The merge will respect the `memoryLimit`. The `blocksize` is
// used in addition to limit the size of intermediate blocks in the recursive
// implementation. It can be tweaked for maximum performance, currently values
// of at least `50-100` seem to work well.
CPP_template(typename T, bool moveElements,
             typename SizeGetter)(requires ValueSizeGetter<SizeGetter,
                                                           T>)  //
    struct ParallelMultiwayMergeStruct {
  CPP_template_2(typename R, typename Comp)(
      requires detail::RandomAccessRangeOfRanges<R, T> CPP_and
          ValueSizeGetter<SizeGetter, T>
              CPP_and
                  InvocableWithExactReturnType<Comp, bool, const T&, const T&>)
      ad_utility::InputRangeTypeErased<std::vector<T>>
      operator()(MemorySize memoryLimit, R&& rangeOfRanges, Comp comparison,
                 size_t blocksize = 100) const {
    // There is one suboperation per input in the recursion tree, so we have to
    // divide the memory limit.
    auto maxMemPerNode = memoryLimit / ql::ranges::size(rangeOfRanges);
    return detail::parallelMultiwayMergeImpl<T, moveElements, SizeGetter>(
        maxMemPerNode, blocksize, AD_FWD(rangeOfRanges), std::move(comparison));
  }
};
// A variable, s.t. we don't have to initiate a struct each time.
template <typename T, bool moveElements,
          typename SizeGetter = DefaultValueSizeGetter<T>>
constexpr ParallelMultiwayMergeStruct<T, moveElements, SizeGetter>
    parallelMultiwayMerge;
}  // namespace ad_utility

#endif  // QLEVER_PARALLELMULTIWAYMERGE_H
