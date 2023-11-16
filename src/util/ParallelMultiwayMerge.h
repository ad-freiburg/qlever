//  Copyright 2023, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#ifndef QLEVER_PARALLELMULTIWAYMERGE_H
#define QLEVER_PARALLELMULTIWAYMERGE_H

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
template <bool moveElements, typename T, ValueSizeGetter<T> SizeGetter>
constexpr auto pushSingleElement =
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
concept RangeWithValue = std::ranges::range<Range> &&
                         std::same_as<std::ranges::range_value_t<Range>, T>;

// Fulfilled if `Range` is a random access range the elements of which are
// ranges of elements of type `T`, e.g. `std::vector<std::generator<T>>`.
template <typename Range, typename T>
concept RandomAccessRangeOfRanges =
    std::ranges::random_access_range<Range> &&
    RangeWithValue<std::ranges::range_value_t<Range>, T>;

// Merge the elements from the presorted ranges `range1` and `range2` according
// to the `comparator`. The result of the merging will be yielded in blocks of
// size `blocksize`. If `moveElements` is true, then the elements from the
// ranges will be moved.
// TODO<joka921> Maybe add a `buffering generator` that automatically stores the
// buffers.
template <typename T, bool moveElements, ValueSizeGetter<T> SizeGetter>
cppcoro::generator<std::vector<T>> lazyBinaryMerge(
    MemorySize maxMem, size_t maxBlockSize, RangeWithValue<T> auto range1,
    RangeWithValue<T> auto range2,
    ad_utility::InvocableWithExactReturnType<bool, const T&, const T&> auto
        comparison) {
  // Set up the buffer as well as a lambda to clear and reserve it.
  std::vector<T> buffer;
  MemorySize sizeOfCurrentBlock{};

  auto clearBuffer = [&buffer, maxBlockSize, &sizeOfCurrentBlock]() {
    buffer.clear();
    buffer.reserve(maxBlockSize);
    sizeOfCurrentBlock = 0_B;
  };

  clearBuffer();

  // Turn the ranges into `(iterator, end)` pairs.
  auto makeItPair = [](auto& range) {
    return std::pair{std::ranges::begin(range), std::ranges::end(range)};
  };

  auto it1 = makeItPair(range1);
  auto it2 = makeItPair(range2);

  // Helper lambda to check if we are at the end of a range.
  auto exhausted = [](const auto& itPair) {
    return itPair.first == itPair.second;
  };

  auto pushToBuffer =
      std::bind_front(detail::pushSingleElement<moveElements, T, SizeGetter>,
                      std::ref(buffer), std::ref(sizeOfCurrentBlock));

  auto isBufferLargeEnough = [&] {
    return buffer.size() >= maxBlockSize || sizeOfCurrentBlock >= maxMem;
  };

  // Push the next element from the range denoted by `itPair` to the `buffer`,
  // and advance the iterator. Return true if the range then is exhausted.
  auto push = [&pushToBuffer, &exhausted](auto& itPair) {
    auto& it = itPair.first;
    pushToBuffer(*it);
    ++it;
    return exhausted(itPair);
  };

  // Push the smaller element one of `*it1` and `*it2` to the `buffer` and
  // advance the corresponding iterator. Return true iff that iterator reaches
  // the end after the increment.
  auto pushSmaller = [&comparison, &push, &it1, &it2]() {
    if (comparison(*it1.first, *it2.first)) {
      return push(it1);
    } else {
      return push(it2);
    }
  };

  if (!exhausted(it1) && !exhausted(it2)) {
    while (true) {
      if (pushSmaller()) {
        break;
      }
      if (isBufferLargeEnough()) {
        co_yield buffer;
        clearBuffer();
      }
    }
  }

  // One of the buffers might still have unmerged contents, simply append them.
  auto yieldRemainder =
      [&buffer, &isBufferLargeEnough, &clearBuffer,
       &pushToBuffer](auto& itPair) -> cppcoro::generator<std::vector<T>> {
    for (auto& el : std::ranges::subrange(itPair.first, itPair.second)) {
      pushToBuffer(el);
      if (isBufferLargeEnough()) {
        co_yield buffer;
        clearBuffer();
      }
    }
  };

  for (auto& block : yieldRemainder(it1)) {
    co_yield block;
  }
  for (auto& block : yieldRemainder(it2)) {
    co_yield block;
  }
  if (!buffer.empty()) {
    co_yield buffer;
  }
}

// Yield the elements of the `range` in blocks of the given `blocksize`.
// TODO<joka921> This gets much simpler with the buffering generator.
template <typename T, bool moveElements, ValueSizeGetter<T> SizeGetter>
cppcoro::generator<std::vector<T>> batchToVector(MemorySize maxMem,
                                                 size_t blocksize,
                                                 RangeWithValue<T> auto range) {
  std::vector<T> buffer;
  buffer.reserve(blocksize);
  MemorySize curMem = 0_B;
  for (auto& el : range) {
    detail::pushSingleElement<moveElements, T, SizeGetter>(buffer, curMem, el);
    if (buffer.size() >= blocksize || curMem >= maxMem) {
      co_yield buffer;
      buffer.clear();
      buffer.reserve(blocksize);
      curMem = 0_B;
    }
  }
  if (!buffer.empty()) {
    co_yield buffer;
  }
}

// The recursive implementation of `parallelMultiwayMerge` (see below). The
// difference is, that the memory limit in this function is per node in the
// recursion tree.
template <typename T, bool moveElements,
          ValueSizeGetter<T> SizeGetter = DefaultValueSizeGetter<T>>
cppcoro::generator<std::vector<T>> parallelMultiwayMergeImpl(
    MemorySize maxMemPerNode, size_t blocksize,
    detail::RandomAccessRangeOfRanges<T> auto&& rangeOfRanges,
    InvocableWithExactReturnType<bool, const T&, const T&> auto comparison) {
  AD_CORRECTNESS_CHECK(!rangeOfRanges.empty());
  auto moveIf = [](auto& range) -> decltype(auto) {
    if constexpr (moveElements) {
      return std::move(range);
    } else {
      return range;
    }
  };
  if (rangeOfRanges.size() == 1) {
    return detail::batchToVector<T, moveElements, SizeGetter>(
        maxMemPerNode, blocksize, moveIf(rangeOfRanges.front()));
  } else if (rangeOfRanges.size() == 2) {
    return detail::lazyBinaryMerge<T, moveElements, SizeGetter>(
        maxMemPerNode, blocksize, moveIf(rangeOfRanges[0]),
        moveIf(rangeOfRanges[1]), comparison);
  } else {
    size_t size = std::ranges::size(rangeOfRanges);
    size_t split = size / 2;
    auto beg = rangeOfRanges.begin();
    auto splitIt = beg + split;
    auto end = rangeOfRanges.end();
    auto join = [](auto&& view) {
      return std::views::join(ad_utility::OwningView{AD_FWD(view)});
    };

    auto parallelMerge = [join, blocksize, comparison, maxMemPerNode](
                             auto it, auto end) {
      return join(parallelMultiwayMergeImpl<T, moveElements, SizeGetter>(
          maxMemPerNode, blocksize, std::ranges::subrange{it, end},
          comparison));
    };

    return ad_utility::streams::runStreamAsync(
        detail::lazyBinaryMerge<T, moveElements, SizeGetter>(
            maxMemPerNode, blocksize, parallelMerge(beg, splitIt),
            parallelMerge(splitIt, end), comparison),
        2);
  }
}
}  // namespace detail

// Merge the sorted ranges contained in the `rangeOfRanges` according to the
// `comparison`. The merge will respect the `memoryLimit`. The `blocksize` is
// used in addition to limit the size of intermediate blocks in the recursive
// implementation. It can be tweaked for maximum performance, currently values
// of at least `50-100` seem to work well.
template <typename T, bool moveElements,
          ValueSizeGetter<T> SizeGetter = DefaultValueSizeGetter<T>>
cppcoro::generator<std::vector<T>> parallelMultiwayMerge(
    MemorySize memoryLimit,
    detail::RandomAccessRangeOfRanges<T> auto&& rangeOfRanges,
    InvocableWithExactReturnType<bool, const T&, const T&> auto comparison,
    size_t blocksize = 100) {
  // There is one suboperation per input in the recursion tree, so we have to
  // divide the memory limit.
  auto maxMemPerNode = memoryLimit / std::ranges::size(rangeOfRanges);
  return detail::parallelMultiwayMergeImpl<T, moveElements, SizeGetter>(
      maxMemPerNode, blocksize, AD_FWD(rangeOfRanges), std::move(comparison));
}
}  // namespace ad_utility

#endif  // QLEVER_PARALLELMULTIWAYMERGE_H
