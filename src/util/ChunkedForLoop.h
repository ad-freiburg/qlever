//   Copyright 2024, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#ifndef QLEVER_CHUNKEDFORLOOP_H
#define QLEVER_CHUNKEDFORLOOP_H

#include <concepts>
#include <cstdint>

#include "backports/algorithm.h"

namespace ad_utility {

namespace detail {

/// Helper function that sets the current counter to end, forcing the loop to
/// end in the next iteration, without having to check a boolean value on each
/// iteration. In a classic for-loop this would be the break keyword, but this
/// doesn't work here because the code is inside a nested function.
constexpr auto getSetIndexToEndAction(std::size_t& current, std::size_t end) {
  return [&current, end]() { current = end; };
}

/// Helper type that represents the type of the lambda returned by
/// `getSetIndexToEndAction`.
using SetIndexToEndAction =
    std::invoke_result_t<decltype(getSetIndexToEndAction), size_t&, size_t>;

/// True if `Func` has a signature of `* func(size_t, const auto&)`
/// where `*` can be any type. False otherwise.
template <typename Func>
concept IteratorWithBreak = std::invocable<Func, size_t, SetIndexToEndAction>;

/// Helper concept that allows `chunkedForLoop` to offer an action with an
/// optional second argument in `action`.
template <typename Func>
concept IteratorAction =
    std::is_invocable_v<Func, std::size_t> || IteratorWithBreak<Func>;
}  // namespace detail

/// Helper function to run a classic for-loop from `start` to `end`, where
/// `action` is called with the current index in the range and an optional
/// invocable second argument that when called will cause the loop to exit in
/// the next iteration, similar to the break keyword. `chunkOperation` is called
/// every `CHUNK_SIZE` iteration steps, and at least a single time at the end if
/// the range is not empty.
template <std::size_t CHUNK_SIZE, detail::IteratorAction Action>
inline void chunkedForLoop(std::size_t start, std::size_t end,
                           const Action& action,
                           const std::invocable auto& chunkOperation) {
  static_assert(CHUNK_SIZE != 0, "Chunk size must be non-zero");
  using std::size_t;
  while (start < end) {
    size_t chunkEnd = std::min(end, start + CHUNK_SIZE);
    while (start < chunkEnd) {
      if constexpr (detail::IteratorWithBreak<Action>) {
        std::invoke(action, start, detail::getSetIndexToEndAction(start, end));
      } else {
        std::invoke(action, start);
      }
      ++start;
    }
    std::invoke(chunkOperation);
  }
}

// Helper concept that combines the sized range and input range concepts.
template <typename R>
concept SizedInputRange =
    std::ranges::sized_range<R> && std::ranges::input_range<R>;

// Similar to `ql::ranges::copy`, but invokes `chunkOperation` every
// `chunkSize` elements. (Round up to the next chunk size if the range size is
// not a multiple of `chunkSize`.)
template <SizedInputRange R, std::weakly_incrementable O>
inline void chunkedCopy(R&& inputRange, O result,
                        std::ranges::range_difference_t<R> chunkSize,
                        const std::invocable auto& chunkOperation)
    requires std::indirectly_copyable<std::ranges::iterator_t<R>, O> {
  auto begin = ql::ranges::begin(inputRange);
  auto end = std::ranges::end(inputRange);
  auto target = result;
  while (std::ranges::distance(begin, end) >= chunkSize) {
    auto start = begin;
    std::ranges::advance(begin, chunkSize);
    target = ql::ranges::copy(start, begin, target).out;
    chunkOperation();
  }
  ql::ranges::copy(begin, end, target);
  chunkOperation();
}

// Helper concept that combines the sized range and output range concepts.
template <typename R, typename T>
concept SizedOutputRange =
    std::ranges::sized_range<R> && std::ranges::output_range<R, T>;

// Similar to `ql::ranges::fill`, but invokes `chunkOperation` every
// `chunkSize` elements. (Round up to the next chunk size if the range size is
// not a multiple of `chunkSize`.)
template <typename T, SizedOutputRange<T> R>
inline void chunkedFill(R&& outputRange, const T& value,
                        std::ranges::range_difference_t<R> chunkSize,
                        const std::invocable auto& chunkOperation) {
  auto begin = ql::ranges::begin(outputRange);
  auto end = std::ranges::end(outputRange);
  while (std::ranges::distance(begin, end) >= chunkSize) {
    auto start = begin;
    std::ranges::advance(begin, chunkSize);
    ql::ranges::fill(start, begin, value);
    chunkOperation();
  }
  ql::ranges::fill(begin, end, value);
  chunkOperation();
}
}  // namespace ad_utility

#endif  // QLEVER_CHUNKEDFORLOOP_H
