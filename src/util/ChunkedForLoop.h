//   Copyright 2024, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#ifndef QLEVER_CHUNKEDFOREACH_H
#define QLEVER_CHUNKEDFOREACH_H

#include <algorithm>
#include <concepts>
#include <cstdint>

namespace ad_utility {

namespace detail {
inline auto getSetIndexToEndAction(std::size_t& current, std::size_t end) {
  return [&current, end]() { current = end; };
}

template <typename Func>
constexpr bool isIteratorWithBreak =
    std::is_invocable_v<Func, std::size_t,
                        const decltype(getSetIndexToEndAction(
                            std::declval<std::size_t&>(), 0))&>;

template <typename Func>
concept IteratorAction =
    std::is_invocable_v<Func, std::size_t> || isIteratorWithBreak<Func>;
;
}  // namespace detail

template <std::size_t CHUNK_SIZE>
inline void chunkedForLoop(std::size_t start, std::size_t end,
                           const detail::IteratorAction auto& action,
                           const std::invocable auto& chunkOperation) {
  static_assert(CHUNK_SIZE != 0, "Chunk size must be non-zero");
  using std::size_t;
  while (start < end) {
    size_t chunkEnd = std::min(end, start + CHUNK_SIZE);
    while (start < chunkEnd) {
      if constexpr (detail::isIteratorWithBreak<decltype(action)>) {
        std::invoke(action, start, detail::getSetIndexToEndAction(start, end));
      } else {
        std::invoke(action, start);
      }
      start++;
    }
    std::invoke(chunkOperation);
  }
}
}  // namespace ad_utility

#endif  // QLEVER_CHUNKEDFOREACH_H
