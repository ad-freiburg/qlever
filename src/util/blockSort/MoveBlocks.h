// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_UTIL_BLOCKSORT_MOVEBLOCKS_H
#define QLEVER_SRC_UTIL_BLOCKSORT_MOVEBLOCKS_H

#ifndef QLEVER_REDUCED_FEATURE_SET_FOR_CPP17

#include <boost/asio/awaitable.hpp>
#include <boost/sort/common/range.hpp>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <utility>
#include <vector>

#include "util/Exception.h"
#include "util/blockSort/SortState.h"
#include "util/blockSort/TaskGroup.h"

// The last phase of the block indirect sort: the index says where every block
// belongs, and here the blocks are finally moved there.
//
// The index is a permutation, so it decomposes into cycles, and the cycles are
// independent of each other and can be applied concurrently. Rotating a cycle
// needs one spare block, which is what the scratch buffer is for.
//
// This is a port of `boost::sort::blk_detail::move_blocks`.
namespace ad_utility::blockSort::detail {

namespace net = boost::asio;

// Rotate the blocks along `cycle`: the first block goes into the scratch
// buffer, every following block is moved into the place of its predecessor,
// and the buffer finally goes into the place that is left over.
//
// A leaf of the algorithm: it never suspends, and is therefore an ordinary
// function rather than a coroutine.
template <typename State>
void moveSequence(State& state, const std::vector<size_t>& cycle) {
  AD_CORRECTNESS_CHECK(!cycle.empty());
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

// Rotate the blocks along a cycle that is too long to be moved by a single
// task. The cycle is cut into parts which are rotated concurrently; rotating
// the parts leaves the *last* block of every part in the wrong place, so a
// final rotation of exactly those blocks — again along a cycle, and hence again
// by this function — completes the move.
template <typename State>
net::awaitable<void> moveLongSequence(State& state, std::vector<size_t> cycle) {
  constexpr uint32_t groupSize = State::groupSize_;
  if (cycle.size() < groupSize) {
    moveSequence(state, cycle);
    co_return;
  }

  size_t numParts = (cycle.size() + groupSize - 1) / groupSize;
  size_t sizePart = cycle.size() / numParts;
  // The last block of every part, which is what remains to be rotated.
  std::vector<size_t> remainder;

  TaskGroup group = state.makeTaskGroup();
  // The part that this thread moves itself, see the NOTE in `cutRange`.
  std::vector<size_t> ownPart;
  // NOTE: Nothing in here suspends, see the note in `cutRange`.
  try {
    remainder.reserve(numParts);
    auto it = cycle.begin();
    for (size_t i = 0; i < numParts - 1; ++i, it += sizePart) {
      remainder.push_back(*(it + sizePart - 1));
      group.spawnFunction(
          [&state, part = std::vector<size_t>(it, it + sizePart)]() {
            moveSequence(state, part);
          });
    }
    remainder.push_back(cycle.back());
    ownPart.assign(it, cycle.end());
  } catch (...) {
    state.storeError(std::current_exception());
    ownPart.clear();
  }
  if (!ownPart.empty()) {
    group.runInlineFunction(
        [&state, &ownPart]() { moveSequence(state, ownPart); });
  }
  co_await group.join();
  if (state.hasError()) {
    co_return;
  }
  co_await moveLongSequence(state, std::move(remainder));
}

// Hand a cycle to the `group`: a long one is cut into parts first (and hence is
// a coroutine), a short one is rotated as it is.
template <typename State>
void spawnCycle(State& state, TaskGroup& group, std::vector<size_t> cycle) {
  if (cycle.size() < State::groupSize_) {
    group.spawnFunction(
        [&state, cycle = std::move(cycle)]() { moveSequence(state, cycle); });
  } else {
    group.spawn(moveLongSequence(state, std::move(cycle)));
  }
}

// Apply the permutation in `state.index_` to the blocks themselves, by
// rotating each of its cycles.
template <typename State>
net::awaitable<void> moveBlocks(State& state) {
  TaskGroup group = state.makeTaskGroup();
  // The cycle that this thread rotates itself, see the NOTE in `cutRange`.
  std::vector<size_t> ownCycle;
  try {
    size_t cycleStart = 0;
    while (cycleStart < state.index_.size()) {
      // Skip the blocks that are already where they belong (the cycles of
      // length one).
      while (cycleStart < state.index_.size() &&
             state.index_[cycleStart].pos() == cycleStart) {
        ++cycleStart;
      }
      if (cycleStart == state.index_.size()) {
        break;
      }
      // Collect the cycle that starts here, and mark its blocks as final on
      // the way, so that the loop above skips them afterwards.
      std::vector<size_t> cycle;
      cycle.push_back(cycleStart);
      size_t destination = cycleStart;
      while (state.index_[destination].pos() != cycleStart) {
        size_t source = state.index_[destination].pos();
        cycle.push_back(source);
        state.index_[destination].set_pos(destination);
        destination = source;
      }
      state.index_[destination].set_pos(destination);

      // Hand the previous cycle over and keep this one, so that the cycle that
      // stays here is the last one.
      if (!ownCycle.empty()) {
        spawnCycle(state, group, std::move(ownCycle));
      }
      ownCycle = std::move(cycle);
    }
  } catch (...) {
    state.storeError(std::current_exception());
    ownCycle.clear();
  }
  if (!ownCycle.empty()) {
    if (ownCycle.size() < State::groupSize_) {
      group.runInlineFunction(
          [&state, &ownCycle]() { moveSequence(state, ownCycle); });
    } else {
      co_await group.runInline(moveLongSequence(state, std::move(ownCycle)));
    }
  }
  co_await group.join();
}

}  // namespace ad_utility::blockSort::detail

#endif  // QLEVER_REDUCED_FEATURE_SET_FOR_CPP17

#endif  // QLEVER_SRC_UTIL_BLOCKSORT_MOVEBLOCKS_H
