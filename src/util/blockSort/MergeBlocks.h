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
#include <vector>

#include "backports/algorithm.h"
#include "util/Exception.h"
#include "util/blockSort/SortState.h"
#include "util/blockSort/TaskGroup.h"

// Merge two adjacent sorted ranges of blocks by permuting the index. Elements
// are only exchanged between neighbouring blocks that overlap. A port of
// `boost::sort::blk_detail::merge_blocks`.
//
// Terminology: The two *sides* of a merge are the two sorted halves. The
// blocks of both sides are first merged in the index by their first element,
// so that only blocks whose value ranges *overlap* are still out of order. A
// *run* is a maximal sequence of consecutive blocks (in index order) that
// overlap; merging the elements of each run sorts the whole range.
namespace ad_utility::blockSort::detail {

namespace net = boost::asio;

// Merge the elements of the blocks at `positions`, which are already in the
// right order (by their first element), using one scratch buffer: move the
// first block into the buffer, then let `merge_flow` fill each block with its
// final elements from the buffer and the next block, and finally move the
// buffer into the last block.
template <typename State>
void mergeRun(State& state, typename State::RangePos positions) {
  auto lease = state.acquireBuffer();
  auto buffer = lease.range();

  auto previous = state.getBlock(state.index_[positions.first].pos());
  bsc::move_forward(buffer, previous);
  auto current = previous;
  for (size_t pos = positions.first + 1; pos != positions.last; ++pos) {
    current = state.getBlock(state.index_[pos].pos());
    bsc::merge_flow(previous, buffer, current, state.cmp_);
    previous = current;
  }
  bsc::move_forward(current, buffer);
}

// Handle the incomplete last block (the *tail*) before `mergeSortedHalves`
// merges `positions1` and `positions2`, the sorted blocks of the two sides, of
// which `positions2` ends with the tail: the tail starts as the last entry of
// the index, and stays there, because this function removes it from
// `positions2`, so that the merge of the index leaves its entry untouched.
//
// The tail is shorter than the other blocks, so it can't be part of a run
// (`merge_flow` needs blocks of equal size) and can't be moved (`moveBlocks`
// moves whole blocks). It therefore has to stay the last block, and hence get
// the largest elements of both sides. These are in the last blocks of the two
// sides, so it suffices to merge the tail with the last block of `positions1`.
// The tail is then dropped from `positions2`.
template <typename State>
void mergeTail(State& state, std::vector<bsd::block_pos>& positions1,
               std::vector<bsd::block_pos>& positions2) {
  // `sortRecursively` makes sure that each side has at least
  // `BLOCKS_PER_TASK` blocks.
  AD_CORRECTNESS_CHECK(positions1.size() >= 2);
  positions2.pop_back();

  size_t posBack1 = positions1.back().pos();
  auto rangeBack1 = state.getBlock(posBack1);
  // If the tail doesn't overlap the last block of `positions1`, it already
  // holds the largest elements.
  if (!bsc::is_mergeable(rangeBack1, state.tailRange_, state.cmp_)) {
    return;
  }
  // Give the largest elements of both blocks to the tail.
  {
    auto lease = state.acquireBuffer();
    bsc::merge_uncontiguous(rangeBack1, state.tailRange_, lease.range(),
                            state.cmp_);
  }
  // The last block of `positions1` may now start with elements of the tail
  // that are smaller than the end of its predecessor. Then move it to the end
  // of `positions2`, which it still extends in sorted order, because its first
  // element came from the tail.
  size_t posBefore = positions1[positions1.size() - 2].pos();
  if (bsc::is_mergeable(state.getBlock(posBefore), rangeBack1, state.cmp_)) {
    positions2.emplace_back(posBack1, false);
    positions1.pop_back();
  }
}

// Cut `run` (more than `BLOCKS_PER_TASK` blocks, see `findRuns`) into parts
// of at least `sizePart` blocks that can be merged independently. A cut is only
// made between two blocks from different sides, which are first merged with
// each other, so that every element before the cut is at most every element
// after it.
//
// NOTE: Two neighbours from the same side don't overlap each other, but a
// block of the other side before them could still overlap the second one, so a
// cut between them wouldn't separate the elements.
template <typename State>
std::vector<typename State::RangePos> cutRun(State& state,
                                             typename State::RangePos run) {
  AD_CORRECTNESS_CHECK(run.size() > BLOCKS_PER_TASK);
  size_t sizePart = partSize(run.size());
  std::vector<typename State::RangePos> parts;
  auto lease = state.acquireBuffer();
  size_t partBegin = run.first;
  while (partBegin < run.last) {
    size_t pos = partBegin + sizePart;
    while (pos < run.last &&
           state.index_[pos - 1].side() == state.index_[pos].side()) {
      ++pos;
    }
    // Merge the two blocks at the cut, see above.
    if (pos < run.last) {
      bsc::merge_uncontiguous(state.getBlock(state.index_[pos - 1].pos()),
                              state.getBlock(state.index_[pos].pos()),
                              lease.range(), state.cmp_);
    } else {
      pos = run.last;
    }
    parts.emplace_back(partBegin, pos);
    partBegin = pos;
  }
  return parts;
}

// Whether the blocks of `run` are sorted by their first element in the index.
template <typename State>
bool isSortedByFirstElement(const State& state, typename State::RangePos run) {
  return ql::ranges::is_sorted(state.index_.begin() + run.first,
                               state.index_.begin() + run.last,
                               [&state](bsd::block_pos a, bsd::block_pos b) {
                                 return state.blockIsLessByFirstElement(a, b);
                               });
}

// Whether each block of `run` is sorted.
template <typename State>
bool allBlocksAreSorted(const State& state, typename State::RangePos run) {
  return ql::ranges::all_of(
      state.index_.begin() + run.first, state.index_.begin() + run.last,
      [&state](bsd::block_pos b) {
        auto block = state.getBlock(b.pos());
        return ql::ranges::is_sorted(block.first, block.last, state.cmp_);
      });
}

// Spawn the merge of `run`, see the glossary at the top: at least two blocks
// in index order, sorted by their first element (each block overlaps the
// largest block before it), and without the tail (which `mergeTail` handles),
// so that all blocks have the same size. A long run is first cut into parts by
// a child of its own (so that several long runs are cut in parallel), which
// then spawns the merges of the parts into the same group.
template <typename State>
void spawnMergeOfRun(State& state, TaskGroup& group,
                     typename State::RangePos run) {
  AD_CORRECTNESS_CHECK(run.size() >= 2);
  // The tail is always the last block in the index, see `mergeTail`.
  AD_CORRECTNESS_CHECK(state.tailRange_.empty() || run.last < state.numBlocks_);
  // The order of the elements is only checked while the sort is running: after
  // an exception, children are skipped (see `TaskGroup`), so the blocks may
  // never have been sorted.
  AD_EXPENSIVE_CHECK(state.stopped_.load(std::memory_order_acquire) ||
                     isSortedByFirstElement(state, run));
  AD_EXPENSIVE_CHECK(state.stopped_.load(std::memory_order_acquire) ||
                     allBlocksAreSorted(state, run));
  if (run.size() <= BLOCKS_PER_TASK) {
    group.spawnFunction([&state, run] { mergeRun(state, run); });
    return;
  }
  group.spawnFunction([&state, &group, run] {
    for (auto part : cutRun(state, run)) {
      group.spawnFunction([&state, part] { mergeRun(state, part); });
    }
  });
}

// Find the runs in `positions` (see the glossary at the top). A new run
// begins at each block that doesn't overlap any of the blocks before it.
// Blocks that don't overlap with their neighbours are skipped.
//
// NOTE: Unlike Boost's `extract_ranges`, this doesn't track the sides of the
// blocks. The result is the same, because two blocks of the same side never
// overlap (each side is sorted, and the merge of the index keeps the order of
// the blocks of each side, also for the block that `mergeTail` moves to the
// other side, whose first element came from the tail).
template <typename State>
std::vector<typename State::RangePos> findRuns(
    State& state, typename State::RangePos positions) {
  auto blockAt = [&state](size_t pos) {
    return state.getBlock(state.index_[pos].pos());
  };
  std::vector<typename State::RangePos> runs;
  // Close the current run at `runEnd` (exclusive). A run of a single block
  // doesn't need to be merged.
  auto endRun = [&runs, runBegin = positions.first](size_t runEnd) mutable {
    if (runEnd - runBegin > 1) {
      runs.emplace_back(runBegin, runEnd);
    }
    runBegin = runEnd;
  };
  // The maximum of the elements so far.
  auto maxBack = blockAt(positions.first).back();
  for (size_t pos = positions.first + 1; pos < positions.last; ++pos) {
    auto block = blockAt(pos);
    // The first element of `block` is the minimum of all remaining elements
    // (each block is sorted, and the blocks are sorted by their first element).
    // If it is not smaller than the maximum so far, no remaining element is
    // smaller than any element before it, so a new run begins here.
    if (!state.cmp_(*block.first, *maxBack)) {
      endRun(pos);
    }
    // Keep track of the maximum so far, which isn't necessarily in the last
    // block.
    if (state.cmp_(*maxBack, *block.back())) {
      maxBack = block.back();
    }
  }
  endRun(positions.last);
  return runs;
}

// Merge the sorted index ranges `[posIndex1, posIndex2)` and
// `[posIndex2, posIndex3)`: first merge the blocks by their first element in
// the index, tagged with their side, then merge the elements of each run (see
// the glossary at the top).
template <typename State>
net::awaitable<void> mergeSortedHalves(State& state, size_t posIndex1,
                                       size_t posIndex2, size_t posIndex3) {
  std::vector<bsd::block_pos> positions1;
  std::vector<bsd::block_pos> positions2;
  positions1.reserve(posIndex2 - posIndex1);
  positions2.reserve(posIndex3 - posIndex2);
  for (size_t i = posIndex1; i < posIndex2; ++i) {
    positions1.emplace_back(state.index_[i].pos(), true);
  }
  for (size_t i = posIndex2; i < posIndex3; ++i) {
    positions2.emplace_back(state.index_[i].pos(), false);
  }

  // Only the last range of the index contains the tail, as its last entry, see
  // `mergeTail`.
  if (posIndex3 == state.numBlocks_ && state.tailRange_.not_empty()) {
    AD_CORRECTNESS_CHECK(positions2.back().pos() == state.numBlocks_ - 1);
    mergeTail(state, positions1, positions2);
  }

  ql::ranges::merge(positions1, positions2, state.index_.begin() + posIndex1,
                    [&state](bsd::block_pos a, bsd::block_pos b) {
                      return state.blockIsLessByFirstElement(a, b);
                    });
  auto runs = findRuns(
      state, typename State::RangePos{
                 posIndex1, posIndex1 + positions1.size() + positions2.size()});
  co_await state.withChildren([&state, &runs](TaskGroup& group) {
    for (auto run : runs) {
      spawnMergeOfRun(state, group, run);
    }
  });
}

}  // namespace ad_utility::blockSort::detail

#endif  // QLEVER_REDUCED_FEATURE_SET_FOR_CPP17

#endif  // QLEVER_SRC_UTIL_BLOCKSORT_MERGEBLOCKS_H
