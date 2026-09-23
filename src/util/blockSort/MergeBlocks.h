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
// `boost/sort/block_indirect_sort/blk_detail/merge_blocks.hpp`:
// Copyright (c) 2016 Francisco Jose Tapia (fjtapia@gmail.com)
// Distributed under the Boost Software License, Version 1.0. (See the
// accompanying file `LICENSE_1_0.txt` or copy at
// http://www.boost.org/LICENSE_1_0.txt)

#ifndef QLEVER_SRC_UTIL_BLOCKSORT_MERGEBLOCKS_H
#define QLEVER_SRC_UTIL_BLOCKSORT_MERGEBLOCKS_H

#ifndef QLEVER_REDUCED_FEATURE_SET_FOR_CPP17

#include <boost/asio/awaitable.hpp>
#include <boost/sort/common/range.hpp>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <optional>
#include <vector>

#include "backports/algorithm.h"
#include "util/blockSort/SortState.h"
#include "util/blockSort/TaskGroup.h"

// Merge two adjacent sorted ranges of blocks by permuting the index. Elements
// are only exchanged between neighbouring blocks that overlap. A port of
// `boost::sort::blk_detail::merge_blocks`.
namespace ad_utility::blockSort::detail {

namespace net = boost::asio;

// Merge the elements of the blocks at `positions`, which are already in the
// right order (by their first element), using one scratch buffer.
template <typename State>
void mergeRangePos(State& state, typename State::RangePos positions) {
  if (positions.size() < 2) {
    return;
  }
  auto lease = state.acquireBuffer();
  auto buffer = lease.range();

  auto previous = state.getRange(state.index_[positions.first].pos());
  bsc::move_forward(buffer, previous);
  auto current = previous;
  for (size_t pos = positions.first + 1; pos != positions.last; ++pos) {
    current = state.getRange(state.index_[pos].pos());
    bsc::merge_flow(previous, buffer, current, state.cmp_);
    previous = current;
  }
  bsc::move_forward(current, buffer);
}

// Merge the incomplete last block (the *tail*) into the last block of
// `positions1` and drop it from `positions2`. If that block now overlaps with
// its predecessor, move it to `positions2`.
template <typename State>
void tailProcess(State& state, std::vector<bsd::block_pos>& positions1,
                 std::vector<bsd::block_pos>& positions2) {
  if (positions1.empty() || positions2.empty()) {
    return;
  }
  positions2.pop_back();

  size_t posBack1 = positions1.back().pos();
  auto rangeBack1 = state.getRange(posBack1);
  if (!bsc::is_mergeable(rangeBack1, state.tailRange_, state.cmp_)) {
    return;
  }
  {
    auto lease = state.acquireBuffer();
    bsc::merge_uncontiguous(rangeBack1, state.tailRange_, lease.range(),
                            state.cmp_);
  }
  if (positions1.size() > 1) {
    size_t posBefore = positions1[positions1.size() - 2].pos();
    if (bsc::is_mergeable(state.getRange(posBefore), rangeBack1, state.cmp_)) {
      positions2.emplace_back(posBack1, false);
      positions1.pop_back();
    }
  }
}

// Merge `positions` in parallel by cutting it into parts. Merging the two
// blocks at a cut makes the parts independent.
template <typename State>
net::awaitable<void> cutRange(State& state,
                              typename State::RangePos positions) {
  if (positions.size() < groupSize) {
    mergeRangePos(state, positions);
    co_return;
  }

  TaskGroup group = state.makeTaskGroup();
  // The part that this thread merges itself, see below.
  std::optional<typename State::RangePos> ownPart;
  // `join()` is awaited below, see LIFETIME at `TaskGroup`.
  try {
    size_t numParts = (positions.size() + groupSize - 1) / groupSize;
    size_t sizePart = positions.size() / numParts;
    size_t posIni = positions.first;
    size_t posLast = positions.last;
    // Release the buffer before merging `ownPart`, which needs one itself.
    {
      auto lease = state.acquireBuffer();
      while (posIni < posLast) {
        // Only cut between blocks from different sides of the merge.
        size_t pos = posIni + sizePart;
        while (pos < posLast &&
               state.index_[pos - 1].side() == state.index_[pos].side()) {
          ++pos;
        }
        if (pos < posLast) {
          bsc::merge_uncontiguous(state.getRange(state.index_[pos - 1].pos()),
                                  state.getRange(state.index_[pos].pos()),
                                  lease.range(), state.cmp_);
        } else {
          pos = posLast;
        }
        if (pos - posIni > 1) {
          // Keep the last part for this thread.
          if (ownPart.has_value()) {
            group.spawnFunction([&state, part = ownPart.value()]() {
              mergeRangePos(state, part);
            });
          }
          ownPart = typename State::RangePos{posIni, pos};
        }
        posIni = pos;
      }
    }
  } catch (...) {
    state.storeError(std::current_exception());
    ownPart.reset();
  }
  // Merge the last part in this thread instead of suspending right away (Boost
  // executes work items while waiting).
  if (ownPart.has_value()) {
    group.runInlineFunction(
        [&state, part = ownPart.value()]() { mergeRangePos(state, part); });
  }
  co_await group.join();
}

// Spawn the merge of `run`, cut into parts if it is big.
template <typename State>
void spawnRun(State& state, TaskGroup& group, typename State::RangePos run) {
  if (run.size() > groupSize) {
    group.spawn(cutRange(state, run));
  } else {
    group.spawnFunction([&state, run]() { mergeRangePos(state, run); });
  }
}

// Find the maximal runs of overlapping blocks in `positions` and merge each of
// them. Blocks that don't overlap with their neighbours are skipped.
template <typename State>
net::awaitable<void> extractRanges(State& state,
                                   typename State::RangePos positions) {
  if (positions.size() < 2) {
    co_return;
  }

  TaskGroup group = state.makeTaskGroup();
  // The run that this thread merges itself, see `cutRange`.
  std::optional<typename State::RangePos> ownRun;
  try {
    size_t runBegin = positions.first;
    bsd::block_pos blockAtBegin = state.index_[runBegin];
    // The block of the current run with the greatest last element, and its
    // side. Only blocks from the other side can overlap with it.
    auto rangeMax = state.getRange(blockAtBegin.pos());
    bool sideMax = blockAtBegin.side();
    auto rangeCurrent = rangeMax;
    bool sideCurrent = sideMax;

    for (size_t pos = runBegin + 1; pos <= positions.last; ++pos) {
      bool isEnd = pos == positions.last;
      bool isMergeable = false;
      if (!isEnd) {
        bsd::block_pos blockAtPos = state.index_[pos];
        rangeCurrent = state.getRange(blockAtPos.pos());
        sideCurrent = blockAtPos.side();
        isMergeable = sideMax != sideCurrent &&
                      bsc::is_mergeable(rangeMax, rangeCurrent, state.cmp_);
      }
      if (state.hasError()) {
        break;
      }
      if (isEnd || !isMergeable) {
        typename State::RangePos run{runBegin, pos};
        if (run.size() > 1) {
          // Keep the last run for this thread.
          if (ownRun.has_value()) {
            spawnRun(state, group, ownRun.value());
          }
          ownRun = run;
        }
        runBegin = pos;
        if (!isEnd) {
          rangeMax = rangeCurrent;
          sideMax = sideCurrent;
        }
      } else if (state.cmp_(*rangeMax.back(), *rangeCurrent.back())) {
        rangeMax = rangeCurrent;
        sideMax = sideCurrent;
      }
    }
  } catch (...) {
    state.storeError(std::current_exception());
    ownRun.reset();
  }
  if (ownRun.has_value()) {
    if (ownRun->size() > groupSize) {
      co_await group.runInline(cutRange(state, ownRun.value()));
    } else {
      group.runInlineFunction(
          [&state, run = ownRun.value()]() { mergeRangePos(state, run); });
    }
  }
  co_await group.join();
}

// Merge the sorted index ranges `[posIndex1, posIndex2)` and
// `[posIndex2, posIndex3)`: first merge the blocks by their first element in
// the index, tagged with their side, then merge the elements of overlapping
// blocks (`extractRanges`).
template <typename State>
net::awaitable<void> mergeBlocks(State& state, size_t posIndex1,
                                 size_t posIndex2, size_t posIndex3) {
  size_t numBlocks1 = posIndex2 - posIndex1;
  size_t numBlocks2 = posIndex3 - posIndex2;
  if (numBlocks1 == 0 || numBlocks2 == 0) {
    co_return;
  }

  std::vector<bsd::block_pos> positions1;
  std::vector<bsd::block_pos> positions2;
  positions1.reserve(numBlocks1 + 1);
  positions2.reserve(numBlocks2 + 1);
  for (size_t i = posIndex1; i < posIndex2; ++i) {
    positions1.emplace_back(state.index_[i].pos(), true);
  }
  for (size_t i = posIndex2; i < posIndex3; ++i) {
    positions2.emplace_back(state.index_[i].pos(), false);
  }

  if (positions2.back().pos() == state.numBlocks_ - 1 &&
      state.tailRange_.not_empty()) {
    tailProcess(state, positions1, positions2);
    numBlocks1 = positions1.size();
    numBlocks2 = positions2.size();
  }
  if (state.hasError()) {
    co_return;
  }

  typename State::CompareBlockPos compareBlocks{state.globalRange_.first,
                                                state.cmp_};
  ql::ranges::merge(positions1, positions2, state.index_.begin() + posIndex1,
                    compareBlocks);
  if (state.hasError()) {
    co_return;
  }
  co_await extractRanges(
      state,
      typename State::RangePos{posIndex1, posIndex1 + numBlocks1 + numBlocks2});
}

}  // namespace ad_utility::blockSort::detail

#endif  // QLEVER_REDUCED_FEATURE_SET_FOR_CPP17

#endif  // QLEVER_SRC_UTIL_BLOCKSORT_MERGEBLOCKS_H
