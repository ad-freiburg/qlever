//  Copyright 2023, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#ifndef QLEVER_PARALLELMULTIWAYMERGE_H
#define QLEVER_PARALLELMULTIWAYMERGE_H

#include "util/AsyncStream.h"
#include "util/Generator.h"
#include "util/TypeTraits.h"
#include "util/Views.h"

namespace ad_utility {

namespace detail {

// Call `buffer.push_back(el)`. If `moveElements` is `true`, then `el` gets
// moved. It is necessary to explicitly pass the type `T` stored in the vector
// to enable the usage of this lambda in combination with `std::bind_front` and
// `std::ref`.
template <bool moveElements, typename T>
constexpr auto pushSingleElement = [](std::vector<T>& buffer, auto& el) {
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
template <typename T, bool moveElements>
cppcoro::generator<std::vector<T>> lazyBinaryMerge(
    size_t blocksize, RangeWithValue<T> auto range1,
    RangeWithValue<T> auto range2,
    ad_utility::InvocableWithExactReturnType<bool, const T&, const T&> auto
        comparison) {
  // Set up the buffer as well as a lambda to clear and reserve it.
  std::vector<T> buffer;

  auto clearBuffer = [&buffer, blocksize]() {
    buffer.clear();
    buffer.reserve(blocksize);
  };

  clearBuffer();

  // Turn the ranges into `(iterator, end)` pairs.
  auto makeItPair = [](auto& range) {
    return std::pair{std::ranges::begin(range), std::ranges::end(range)};
  };

  auto it1 = makeItPair(range1);
  auto it2 = makeItPair(range2);

  // Helper lambda to check if we are at the end of a range
  auto exhausted = [](const auto& itPair) {
    return itPair.first == itPair.second;
  };

  auto pushToBuffer = std::bind_front(
      detail::pushSingleElement<moveElements, T>, std::ref(buffer));

  // Push the next element from the range denoted by `itPair` to the `buffer`,
  // and advance the iterator. Return true if the range then is exhausted.
  auto push = [&pushToBuffer, &exhausted](auto& itPair) {
    auto& it = itPair.first;
    pushToBuffer(*it);
    ++it;
    return exhausted(itPair);
  };

  auto pushSmaller = [&comparison, &push, &it1, &it2]() {
    if (comparison(*it1.first, *it2.first)) {
      return push(it1);
    } else {
      return (push(it2));
    }
  };

  if (!exhausted(it1) && !exhausted(it2)) {
    while (true) {
      if (pushSmaller()) {
        break;
      }
      if (buffer.size() >= blocksize) {
        co_yield buffer;
        clearBuffer();
      }
    }
  }

  // One of the buffers might still have unmerged contents, simply append them.
  auto yieldRemainder =
      [&buffer, blocksize, &clearBuffer,
       &pushToBuffer](auto& itPair) -> cppcoro::generator<std::vector<T>> {
    for (auto& el : std::ranges::subrange(itPair.first, itPair.second)) {
      pushToBuffer(el);
      if (buffer.size() >= blocksize) {
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
template <typename T, bool moveElements>
cppcoro::generator<std::vector<T>> batchToVector(size_t blocksize,
                                                 RangeWithValue<T> auto range) {
  std::vector<T> buffer;
  buffer.reserve(blocksize);
  for (auto& el : range) {
    detail::pushSingleElement<moveElements, T>(buffer, el);
    if (buffer.size() >= blocksize) {
      co_yield buffer;
      buffer.clear();
      buffer.reserve(blocksize);
    }
  }
  if (!buffer.empty()) {
    co_yield buffer;
  }
}

}  // namespace detail

// Merge the sorted ranges contained in the `rangeOfRanges` according to the
// `comparison`. The parameter `blocksize` can be used to balance the
// performance and memory consumption. A higher value will increase the memory
// consumption while a too low value will hurt the performance.
// TODO<joka921> Implement a more elegant mechanism to balance the memory
// consumption and the number of used threads.
template <typename T, bool moveElements>
cppcoro::generator<std::vector<T>> parallelMultiwayMerge(
    size_t blocksize, detail::RandomAccessRangeOfRanges<T> auto&& rangeOfRanges,
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
    return detail::batchToVector<T, moveElements>(
        blocksize, moveIf(rangeOfRanges.front()));
  } else if (rangeOfRanges.size() == 2) {
    return detail::lazyBinaryMerge<T, moveElements>(
        blocksize, moveIf(rangeOfRanges[0]), moveIf(rangeOfRanges[1]),
        comparison);
  } else {
    size_t size = std::ranges::size(rangeOfRanges);
    size_t split = size / 2;
    auto beg = rangeOfRanges.begin();
    auto splitIt = beg + split;
    auto end = rangeOfRanges.end();
    auto join = [](auto&& view) {
      return std::views::join(ad_utility::OwningView{AD_FWD(view)});
    };

    auto parallelMerge = [join, blocksize, comparison](auto it, auto end) {
      return join(parallelMultiwayMerge<T, moveElements>(
          blocksize, std::ranges::subrange{it, end}, comparison));
    };

    return ad_utility::streams::runStreamAsync(
        detail::lazyBinaryMerge<T, moveElements>(
            blocksize, parallelMerge(beg, splitIt), parallelMerge(splitIt, end),
            comparison),
        2);
  }
}
}  // namespace ad_utility

#endif  // QLEVER_PARALLELMULTIWAYMERGE_H
