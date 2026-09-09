// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_UTIL_PARALLELBLOCKMERGE_MERGEHELPERS_H
#define QLEVER_SRC_UTIL_PARALLELBLOCKMERGE_MERGEHELPERS_H

#include <cstddef>
#include <optional>
#include <utility>
#include <vector>

#include "backports/algorithm.h"
#include "backports/concepts.h"
#include "util/Exception.h"
#include "util/Views.h"
#include "util/parallelBlockMerge/MergeHelpersImpl.h"
#include "util/parallelBlockMerge/RunsInputPolicy.h"

// The chunk boundaries of the block merge and their computation. For the
// terminology (runs, blocks, chunks, and split points) see
// `util/parallelBlockMerge/ParallelBlockMerge.h`, which is the header to read
// first. The internals of the computation live in `MergeHelpersImpl.h`.
namespace ad_utility::parallelBlockMerge {

// ___________________________________________________________________________
// The chunk boundaries.
// ___________________________________________________________________________

// The half-open range `[lo_, hi_)` of elements that a single chunk of the merge
// covers. An empty `lo_`/`hi_` means minus/plus infinity, so the single chunk
// of a merge that is not split at all has neither of the two bounds set.
template <typename Element>
struct ChunkBoundary {
  std::optional<Element> lo_{};
  std::optional<Element> hi_{};
};

// The chunk boundaries of a merge that is not split at all: a single chunk that
// covers the whole range of elements.
template <typename Element>
std::vector<ChunkBoundary<Element>> singleChunk() {
  return {ChunkBoundary<Element>{}};
}

// The sizes of the chunks that the input of a merge is split into: the first
// `firstChunkSizes_.size()` chunks get the corresponding size from that vector,
// and all the remaining chunks get the size `remainingChunkSize_`. All the
// sizes have to be strictly positive.
//
// NOTE: Smaller leading chunks reduce the latency at the start of a merge. The
// consumer has to drain the chunks in the order of their index, so it reaches
// the output blocks that the producers of the later chunks have already
// buffered sooner.
//
// NOTE: The sizes are targets and not guarantees. The precision of the chunk
// sizes is limited by the block size of the input of the merge, see
// `computeChunkBoundaries`.
struct ChunkSizes {
  std::vector<size_t> firstChunkSizes_;
  size_t remainingChunkSize_;
};

namespace detail {

// The half-open range of block indices `[firstBlockIdx_, endBlockIdx_)` of a
// single run that a chunk has to look at.
struct BlockRange {
  size_t firstBlockIdx_;
  size_t endBlockIdx_;

  // Return `true` if the range contains no block at all, in which case the run
  // does not contribute to the chunk.
  bool empty() const { return firstBlockIdx_ >= endBlockIdx_; }
};

// Return the range of blocks of the run with the index `runIdx` that can
// contain elements in the range of the `boundary`. This only looks at the
// (I/O-free) metadata of the blocks, so a returned block may still turn out to
// contain no matching element at all.
CPP_template(typename Input,
             typename Comparator)(requires InputConcept<Input>) BlockRange
    blockRangeForRun(const Input& input, const Comparator& comparator,
                     const ChunkBoundary<typename Input::Element>& boundary,
                     size_t runIdx) {
  auto blocks = ad_utility::integerRange(input.numBlocks(runIdx));
  // Return the number of blocks in the prefix for which the `predicate` holds.
  // The predicate is monotone (once it is `false` it stays `false`), so this is
  // a binary search.
  auto lengthOfPrefixWhere = [&blocks](const auto& predicate) {
    return static_cast<size_t>(ql::ranges::partition_point(blocks, predicate) -
                               ql::ranges::begin(blocks));
  };

  // The first block that may contain an element that is not smaller than `lo_`,
  // that is the first block the last element of which is not smaller than
  // `lo_`.
  size_t firstBlockIdx = 0;
  if (boundary.lo_.has_value()) {
    firstBlockIdx = lengthOfPrefixWhere([&](size_t blockIdx) {
      return comparator(input.lastElement(runIdx, blockIdx),
                        boundary.lo_.value());
    });
  }
  // The first block all of whose elements are not smaller than `hi_`, that is
  // the (exclusive) end of the range of blocks.
  //
  // NOTE: The predicate is deliberately `comparator(firstElement, hi_)` and not
  // `!comparator(hi_, firstElement)`. The latter would be off by one and would
  // read one superfluous block whenever `firstElement(block) == hi_`.
  size_t endBlockIdx = input.numBlocks(runIdx);
  if (boundary.hi_.has_value()) {
    endBlockIdx = lengthOfPrefixWhere([&](size_t blockIdx) {
      return comparator(input.firstElement(runIdx, blockIdx),
                        boundary.hi_.value());
    });
  }
  return {firstBlockIdx, endBlockIdx};
}

// Convert the `splitPoints` into the boundaries of the `splitPoints.size() + 1`
// chunks that they describe: the `i`-th of them separates chunk `i` from chunk
// `i + 1`. The lower bound of the first and the upper bound of the last chunk
// are empty, that is minus and plus infinity.
template <typename Element>
std::vector<ChunkBoundary<Element>> chunkBoundariesFromSplitPoints(
    const std::vector<Element>& splitPoints) {
  std::vector<ChunkBoundary<Element>> result;
  result.reserve(splitPoints.size() + 1);
  for (size_t chunkIdx = 0; chunkIdx <= splitPoints.size(); ++chunkIdx) {
    ChunkBoundary<Element> boundary;
    if (chunkIdx > 0) {
      boundary.lo_ = splitPoints.at(chunkIdx - 1);
    }
    if (chunkIdx < splitPoints.size()) {
      boundary.hi_ = splitPoints.at(chunkIdx);
    }
    result.push_back(std::move(boundary));
  }
  return result;
}

// The common part of the two overloads of `computeChunkBoundaries` below: run
// the steps from `MergeHelpersImpl.h`, where `makeTargets` turns the total
// number of elements into the target quantiles.
CPP_template(typename Input, typename Comparator,
             typename MakeTargets)(requires InputConcept<Input>)
    std::vector<ChunkBoundary<typename Input::Element>> chunkBoundariesImpl(
        const Input& input, const Comparator& comparator,
        const MakeTargets& makeTargets) {
  using Element = typename Input::Element;
  auto elementsAndWeights =
      sortAndAccumulateWeights(collectElementsAndWeights(input), comparator);
  if (elementsAndWeights.empty()) {
    return singleChunk<Element>();
  }
  // The accumulated weight of the last entry is the total number of elements.
  auto targets = makeTargets(elementsAndWeights.back().second);
  return chunkBoundariesFromSplitPoints(
      pickChunkSplitPoints(elementsAndWeights, targets));
}

}  // namespace detail

// Compute the chunk boundaries by a weighted quantile over the block metadata
// only (no I/O at all): every block contributes its last element, weighted by
// its number of elements. Return the boundaries of at most `numChunks` chunks,
// see `ChunkBoundary`. The result is never empty, and the chunks partition the
// whole range of elements.
//
// NOTE: The result may well describe fewer than `numChunks` chunks, because all
// equal elements have to end up in the same chunk. If all the elements of the
// input are equal, for example, then there is a single non-empty chunk that
// holds all of them, and there is nothing that could be done about it.
//
// TODO<joka921> A block whose first and last element are equal could be put
// into a chunk of its own that consists of exactly that element. Such a chunk
// needs no merge at all, its blocks can simply be passed through, which
// requires an additional flag in the `ChunkBoundary` as well as the (currently
// not required) guarantee that the comparator considers all elements of such a
// chunk equivalent. This only pays off for the parallel merge, so it is
// deferred until then.
CPP_template(typename Input, typename Comparator)(requires InputConcept<Input>)
    std::vector<ChunkBoundary<typename Input::Element>> computeChunkBoundaries(
        const Input& input, const Comparator& comparator, size_t numChunks) {
  using Element = typename Input::Element;
  // A single chunk needs no boundaries at all, and in that case even the scan
  // of the block metadata can be skipped.
  if (numChunks <= 1) {
    return singleChunk<Element>();
  }
  return detail::chunkBoundariesImpl(
      input, comparator, [numChunks](size_t totalNumElements) {
        return detail::uniformTargets(totalNumElements, numChunks);
      });
}

// The same as above, but with explicit `chunkSizes` (see `ChunkSizes`) instead
// of a fixed number of equally sized chunks. Use this to make the first chunks
// smaller than the remaining ones, which reduces the latency at the start of
// the merge.
//
// NOTE: All the guarantees of the overload above still hold, in particular the
// result may well describe fewer chunks than the `chunkSizes` ask for, and it
// describes a single chunk if the input has at most `firstChunkSizes_.front()`
// (respectively `remainingChunkSize_`) elements.
CPP_template(typename Input, typename Comparator)(requires InputConcept<Input>)
    std::vector<ChunkBoundary<typename Input::Element>> computeChunkBoundaries(
        const Input& input, const Comparator& comparator,
        ChunkSizes chunkSizes) {
  AD_CONTRACT_CHECK(chunkSizes.remainingChunkSize_ > 0);
  AD_CONTRACT_CHECK(ql::ranges::all_of(chunkSizes.firstChunkSizes_,
                                       [](size_t size) { return size > 0; }));
  return detail::chunkBoundariesImpl(
      input, comparator, [&chunkSizes](size_t totalNumElements) {
        return detail::targetsFromChunkSizes(totalNumElements,
                                             chunkSizes.firstChunkSizes_,
                                             chunkSizes.remainingChunkSize_);
      });
}

}  // namespace ad_utility::parallelBlockMerge

#endif  // QLEVER_SRC_UTIL_PARALLELBLOCKMERGE_MERGEHELPERS_H
