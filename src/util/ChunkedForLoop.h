//   Copyright 2024, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#ifndef QLEVER_CHUNKEDFOREACH_H
#define QLEVER_CHUNKEDFOREACH_H

#include <algorithm>
#include <concepts>
#include <cstdint>

namespace ad_utility {
template <std::size_t CHUNK_SIZE>
inline void chunkedForLoop(std::size_t start, std::size_t end,
                           const std::invocable<std::size_t> auto& action,
                           const std::invocable auto& chunkOperation) {
  static_assert(CHUNK_SIZE != 0, "Chunk size must be non-zero");
  using std::size_t;
  while (start < end) {
    size_t chunkEnd = std::min(end, start + CHUNK_SIZE);
    while (start < chunkEnd) {
      std::invoke(action, start);
      start++;
    }
    std::invoke(chunkOperation);
  }
}
}  // namespace ad_utility

#endif  // QLEVER_CHUNKEDFOREACH_H
