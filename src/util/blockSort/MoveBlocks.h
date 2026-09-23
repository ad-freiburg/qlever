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
// `boost/sort/block_indirect_sort/blk_detail/move_blocks.hpp`:
// Copyright (c) 2016 Francisco Jose Tapia (fjtapia@gmail.com)
// Distributed under the Boost Software License, Version 1.0. (See the
// accompanying file `LICENSE_1_0.txt` or copy at
// http://www.boost.org/LICENSE_1_0.txt)

#ifndef QLEVER_SRC_UTIL_BLOCKSORT_MOVEBLOCKS_H
#define QLEVER_SRC_UTIL_BLOCKSORT_MOVEBLOCKS_H

#ifndef QLEVER_REDUCED_FEATURE_SET_FOR_CPP17

#include <boost/asio/awaitable.hpp>
#include <boost/sort/common/range.hpp>
#include <cstddef>
#include <utility>
#include <vector>

#include "util/blockSort/SortState.h"
#include "util/blockSort/TaskGroup.h"

// Move the blocks to where the index says, by rotating the independent cycles
// of the permutation in parallel, using a scratch buffer as the spare block. A
// port of `boost::sort::blk_detail::move_blocks`.
namespace ad_utility::blockSort::detail {

namespace net = boost::asio;

// Rotate the blocks along `cycle`: block `cycle[i + 1]` moves to `cycle[i]`,
// and block `cycle[0]` moves to `cycle.back()`.
template <typename State>
void moveSequence(State& state, const std::vector<size_t>& cycle) {
  auto lease = state.acquireBuffer();
  auto buffer = lease.range();

  auto target = state.getRange(cycle[0]);
  bsc::move_forward(buffer, target);
  for (size_t i = 1; i < cycle.size(); ++i) {
    auto source = target;
    target = state.getRange(cycle[i]);
    bsc::move_forward(source, target);
  }
  bsc::move_forward(target, buffer);
}

// Rotate a long cycle (at least `groupSize` blocks) by rotating its parts in
// parallel, and then the cycle of the last blocks of the parts, which are
// still in the wrong place.
template <typename State>
net::awaitable<void> moveLongSequence(State& state, std::vector<size_t> cycle) {
  size_t numParts = (cycle.size() + groupSize - 1) / groupSize;
  size_t sizePart = cycle.size() / numParts;
  std::vector<size_t> remainder;
  co_await state.withChildren([&state, &cycle, &remainder, numParts,
                               sizePart](TaskGroup& group) {
    remainder.reserve(numParts);
    for (size_t i = 0; i < numParts; ++i) {
      auto begin = cycle.begin() + i * sizePart;
      auto end = i + 1 == numParts ? cycle.end() : begin + sizePart;
      remainder.push_back(*(end - 1));
      group.spawnFunction([&state, part = std::vector<size_t>(begin, end)]() {
        moveSequence(state, part);
      });
    }
  });
  // One block per part, so this is short enough for a single task.
  moveSequence(state, remainder);
}

// Spawn the rotation of `cycle`, cut into parts if it is long.
template <typename State>
void spawnCycle(State& state, TaskGroup& group, std::vector<size_t> cycle) {
  if (cycle.size() < groupSize) {
    group.spawnFunction(
        [&state, cycle = std::move(cycle)]() { moveSequence(state, cycle); });
  } else {
    group.spawn(moveLongSequence(state, std::move(cycle)));
  }
}

// Apply the permutation `state.index_` to the blocks.
template <typename State>
net::awaitable<void> moveBlocks(State& state) {
  co_await state.withChildren([&state](TaskGroup& group) {
    for (size_t cycleStart = 0; cycleStart < state.index_.size();
         ++cycleStart) {
      if (state.index_[cycleStart].pos() == cycleStart) {
        continue;
      }
      // Collect the cycle and mark its blocks as in place on the way.
      std::vector<size_t> cycle{cycleStart};
      size_t destination = cycleStart;
      while (state.index_[destination].pos() != cycleStart) {
        size_t source = state.index_[destination].pos();
        cycle.push_back(source);
        state.index_[destination].set_pos(destination);
        destination = source;
      }
      state.index_[destination].set_pos(destination);
      spawnCycle(state, group, std::move(cycle));
    }
  });
}

}  // namespace ad_utility::blockSort::detail

#endif  // QLEVER_REDUCED_FEATURE_SET_FOR_CPP17

#endif  // QLEVER_SRC_UTIL_BLOCKSORT_MOVEBLOCKS_H
