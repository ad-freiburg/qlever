//   Copyright 2024, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#ifndef QLEVER_CHUNKEDFORLOOP_H
#define QLEVER_CHUNKEDFORLOOP_H

#include <algorithm>
#include <concepts>
#include <cstdint>

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
}  // namespace ad_utility

#endif  // QLEVER_CHUNKEDFORLOOP_H
