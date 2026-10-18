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
#include <cstddef>
#include <utility>
#include <vector>

#include "util/blockSort/BoostSortHeaders.h"
#include "util/blockSort/SortState.h"
#include "util/blockSort/TaskGroup.h"
#include "util/views/ChunkedIotaView.h"

// Move the blocks to where the index says, by rotating the independent cycles
// of the permutation in parallel, using a scratch buffer as the spare block. A
// port of `boost::sort::blk_detail::move_blocks`.
namespace ad_utility::blockSort::detail {

namespace net = boost::asio;

// Rotate the blocks along `cycle`: block `cycle[i + 1]` moves to `cycle[i]`,
// and block `cycle[0]` moves to `cycle.back()`. The scratch buffer is the one
// spare place: block `cycle[0]` is moved into it first, then each block moves
// into the place that was freed last.
template <typename State>
void moveSequence(State& state, const std::vector<size_t>& cycle) {
  auto lease = state.acquireBuffer();
  auto buffer = lease.range();

  auto freePlace = state.getBlock(cycle[0]);
  bsc::move_forward(buffer, freePlace);
  for (size_t i = 1; i < cycle.size(); ++i) {
    auto block = state.getBlock(cycle[i]);
    bsc::move_forward(freePlace, block);
    freePlace = block;
  }
  bsc::move_forward(freePlace, buffer);
}

// Rotate a long cycle (at least `BLOCKS_PER_TASK` blocks) by rotating its parts
// in parallel, and then the cycle of the last blocks of the parts, which are
// still in the wrong place.
template <typename State>
net::awaitable<void> moveLongSequence(State& state, std::vector<size_t> cycle) {
  std::vector<size_t> remainder;
  co_await state.withChildren([&state, &cycle, &remainder](TaskGroup& group) {
    for (auto [begin, end] : ad_utility::chunkedIotaView(
             size_t{0}, cycle.size(), partSize(cycle.size()))) {
      remainder.push_back(cycle[end - 1]);
      group.spawnFunction(
          [&state, part = std::vector<size_t>(cycle.begin() + begin,
                                              cycle.begin() + end)]() {
            moveSequence(state, part);
          });
    }
  });
  // One block per part. Unlike Boost, this isn't cut into parts again if it
  // has more than `BLOCKS_PER_TASK` blocks (which needs a cycle of more than
  // 64 * 64 blocks), because moving a few hundred blocks in one task is cheap.
  moveSequence(state, remainder);
}

// Spawn the rotation of `cycle`, cut into parts if it is long. A long cycle is
// a coroutine, because the rotation of its remainder has to wait for its parts.
template <typename State>
void spawnCycle(State& state, TaskGroup& group, std::vector<size_t> cycle) {
  if (cycle.size() < BLOCKS_PER_TASK) {
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
