// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_UTIL_BLOCKSORT_MERGEBLOCKS_H
#define QLEVER_SRC_UTIL_BLOCKSORT_MERGEBLOCKS_H

#ifndef QLEVER_REDUCED_FEATURE_SET_FOR_CPP17

#include <boost/asio/awaitable.hpp>
#include <boost/sort/common/range.hpp>
#include <boost/sort/common/util/merge.hpp>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <optional>
#include <vector>

#include "util/blockSort/SortState.h"
#include "util/blockSort/TaskGroup.h"

// The merging half of the block indirect sort: given two adjacent ranges of the
// index whose blocks are each already in order, bring the blocks of both into a
// single order. The blocks themselves are only permuted in the index here; the
// elements are moved around only as far as two *neighbouring* blocks have to
// exchange elements, which is what a single scratch buffer is enough for.
//
// This is a port of `boost::sort::blk_detail::merge_blocks`.
namespace ad_utility::blockSort::detail {

namespace net = boost::asio;

// Merge the blocks of `positions` into one sorted run of elements. The blocks
// are already in the right order, so this only has to push the elements that
// are out of place from one block into the next: the scratch buffer holds the
// elements of the block that is currently being filled, and `merge_flow` moves
// the smallest elements of the buffer and of the next block into it.
//
// A leaf of the algorithm: it never suspends, and is therefore an ordinary
// function rather than a coroutine.
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

// Handle the last block if it is the incomplete one (the *tail*). A tail is
// merged into the block before it right away, and is then either dropped from
// the merge or, if that block has become mergeable with its own predecessor,
// takes that block's place in the second half.
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

// Merge a range of the index that is too big for a single `mergeRangePos` by
// cutting it into parts that can be merged independently. Two adjacent parts
// are made independent by merging the two blocks at the cut, after which no
// element has to cross it any more.
template <typename State>
net::awaitable<void> cutRange(State& state,
                              typename State::RangePos positions) {
  constexpr uint32_t groupSize = State::groupSize_;
  if (positions.size() < groupSize) {
    mergeRangePos(state, positions);
    co_return;
  }

  TaskGroup group = state.makeTaskGroup();
  // The part that this thread merges itself, see the NOTE below.
  std::optional<typename State::RangePos> ownPart;
  // NOTE: Nothing in here suspends, so the `catch` may not be turned into a
  // `co_await` of the `group` — that has to happen below, see the LIFETIME
  // note at `TaskGroup`.
  try {
    size_t numParts = (positions.size() + groupSize - 1) / groupSize;
    size_t sizePart = positions.size() / numParts;
    size_t posIni = positions.first;
    size_t posLast = positions.last;
    // The lease ends with this scope, so that this thread does not hold two
    // buffers at once when it merges `ownPart` below.
    {
      auto lease = state.acquireBuffer();
      while (posIni < posLast) {
        // A cut is only possible between two blocks that come from different
        // halves of the merge, because only those can have elements to
        // exchange.
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
          // Hand the previous part over and keep this one, so that the part
          // that stays here is the last one, see the NOTE below.
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
  // NOTE: The last part is merged by this very thread instead of being handed
  // to the executor. Boost's equivalent of `join()` executes work items while
  // it waits, so the thread of a task that fans out never idles; here the
  // thread would instead suspend and look for other work, of which there is
  // none once the last tasks of a phase are running.
  if (ownPart.has_value()) {
    group.runInlineFunction(
        [&state, part = ownPart.value()]() { mergeRangePos(state, part); });
  }
  co_await group.join();
}

// Hand a run of mergeable blocks to the `group`: a big one is cut into parts
// first (and hence is a coroutine), a small one is merged as it is.
template <typename State>
void spawnRun(State& state, TaskGroup& group, typename State::RangePos run) {
  if (run.size() > State::groupSize_) {
    group.spawn(cutRange(state, run));
  } else {
    group.spawnFunction([&state, run]() { mergeRangePos(state, run); });
  }
}

// Split the freshly merged index range into the maximal runs of blocks that
// still have elements to exchange, and merge each of those runs. Blocks that
// are already in their final shape are simply skipped, which is where most of
// the work of a merge is saved.
template <typename State>
net::awaitable<void> extractRanges(State& state,
                                   typename State::RangePos positions) {
  constexpr uint32_t groupSize = State::groupSize_;
  if (positions.size() < 2) {
    co_return;
  }

  TaskGroup group = state.makeTaskGroup();
  // The run that this thread merges itself, see the NOTE in `cutRange`.
  std::optional<typename State::RangePos> ownRun;
  try {
    size_t runBegin = positions.first;
    bsd::block_pos blockAtBegin = state.index_[runBegin];
    // The block of the current run whose *last* element is the greatest, and
    // the half of the merge that it came from. A block can only be mergeable
    // with the blocks of the other half that follow it.
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
          // Hand the previous run over and keep this one, so that the run that
          // stays here is the last one.
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

// Merge the two adjacent index ranges `[posIndex1, posIndex2)` and
// `[posIndex2, posIndex3)`, whose blocks are each already sorted.
//
// The blocks are first merged *logically*, by their first element, which only
// permutes the index. Every block then carries the side of the merge it came
// from, and `extractRanges` uses those sides to find the blocks that still have
// elements to exchange.
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
  bscu::merge(positions1.begin(), positions1.end(), positions2.begin(),
              positions2.end(), state.index_.begin() + posIndex1,
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
