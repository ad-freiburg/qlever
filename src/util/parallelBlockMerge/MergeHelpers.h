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

#include <algorithm>
#include <cstddef>
#include <optional>
#include <range/v3/numeric/accumulate.hpp>
#include <range/v3/numeric/partial_sum.hpp>
#include <utility>
#include <vector>

#include "backports/algorithm.h"
#include "backports/concepts.h"
#include "util/Exception.h"
#include "util/TransparentFunctors.h"
#include "util/Views.h"
#include "util/parallelBlockMerge/RunsInputPolicy.h"

// The chunk boundaries of the block merge and their computation. For the
// terminology (runs, blocks, and chunks) see
// `util/parallelBlockMerge/ParallelBlockMerge.h`, which is the header to read
// first.
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

// ___________________________________________________________________________
// Metadata-only helpers. None of these performs any I/O.
// ___________________________________________________________________________

// Return a view of all `[runIdx, blockIdx]` pairs of the `input`, in the order
// of the runs and, within a run, in the order of the blocks.
//
// NOTE: The returned view refers to the `input`, which therefore has to outlive
// it.
CPP_template(typename Input)(
    requires InputConcept<Input>) auto allBlocksInAllRuns(const Input& input) {
  return ::ranges::views::for_each(
      ad_utility::integerRange(input.numRuns()), [&input](size_t runIdx) {
        return ::ranges::views::transform(
            ad_utility::integerRange(input.numBlocks(runIdx)),
            [runIdx](size_t blockIdx) {
              return std::pair<size_t, size_t>{runIdx, blockIdx};
            });
      });
}

// Return the total number of elements of all runs of the `input`.
CPP_template(typename Input)(requires InputConcept<Input>) size_t
    totalNumElements(const Input& input) {
  return ::ranges::accumulate(
      allBlocksInAllRuns(input) |
          ::ranges::views::transform([&input](const auto& runAndBlock) {
            return input.numElementsInBlock(runAndBlock.first,
                                            runAndBlock.second);
          }),
      size_t{0});
}

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

// ___________________________________________________________________________
// The computation of the chunk boundaries.
// ___________________________________________________________________________
//
// The computation consists of four independent steps (see
// `computeChunkBoundaries` below for the interface and for the guarantees),
// each of which is a pure function: collect the elements and their weights,
// accumulate those weights, compute the target quantiles, and pick the
// chunk starts at those quantiles.

// The last element of a single block together with the number of elements in
// that block, which is the weight of that element.
template <typename Element>
using ElementAndWeight = std::pair<Element, size_t>;

// Step 1: Collect the last element of every block of the `input` together with
// its weight.
//
// PRECONDITION: No block of the `input` is empty, see `InputConcept`.
CPP_template(typename Input)(requires InputConcept<Input>) std::
    vector<ElementAndWeight<typename Input::Element>> collectElementsAndWeights(
        const Input& input) {
  std::vector<ElementAndWeight<typename Input::Element>> result;
  for (auto [runIdx, blockIdx] : allBlocksInAllRuns(input)) {
    size_t numElements = input.numElementsInBlock(runIdx, blockIdx);
    AD_CORRECTNESS_CHECK(numElements > 0);
    result.emplace_back(input.lastElement(runIdx, blockIdx), numElements);
  }
  return result;
}

// Step 2: Sort the `elementsAndWeights` by their element, merge the entries of
// equal elements into a single one, and replace the weights by their prefix
// sums. In the result, `result[i].second` is the number of elements of the
// whole input that are (approximately) not greater than `result[i].first`, and
// the elements are strictly increasing.
template <typename Element, typename Comparator>
std::vector<ElementAndWeight<Element>> sortAndAccumulateWeights(
    std::vector<ElementAndWeight<Element>> elementsAndWeights,
    const Comparator& comparator) {
  ql::ranges::sort(elementsAndWeights, comparator, ad_utility::first);
  std::vector<ElementAndWeight<Element>> result;
  result.reserve(elementsAndWeights.size());
  for (auto& elementAndWeight : elementsAndWeights) {
    // Merging equal elements is what makes the quantiles below exact: a target
    // then always identifies a single entry, and picking that entry for two
    // different targets can be avoided by simply moving on to the next one.
    if (!result.empty() &&
        !comparator(result.back().first, elementAndWeight.first)) {
      result.back().second += elementAndWeight.second;
    } else {
      result.push_back(std::move(elementAndWeight));
    }
  }
  auto weights = result | ql::views::transform(ad_utility::second);
  ::ranges::partial_sum(weights, ql::ranges::begin(weights));
  return result;
}

// Step 3a: The strictly increasing numbers of elements at which a new chunk
// starts, one per chunk boundary, for `numChunks` equally sized chunks.
inline std::vector<size_t> uniformTargets(size_t totalNumElements,
                                          size_t numChunks) {
  std::vector<size_t> targets;
  for (size_t i = 1; i < numChunks; ++i) {
    // NOTE: The target is at least `1`, because a target of `0` would always
    // pick the smallest element and thereby waste a chunk on the empty range
    // in front of it.
    targets.push_back(std::max<size_t>(1, totalNumElements * i / numChunks));
  }
  return targets;
}

// Step 3b: The same, but for explicitly given `chunkSizes`: the `i`-th target
// is the total size of the first `i` chunks. Stop as soon as a target has
// reached the total number of elements, because all the chunks after that one
// would be empty. This is what makes a `remainingChunkSize_` that is smaller
// than the input terminate, and it also handles leading sizes that already
// exceed the input.
inline std::vector<size_t> targetsFromChunkSizes(size_t totalNumElements,
                                                 const ChunkSizes& chunkSizes) {
  std::vector<size_t> targets;
  size_t sizeOfPreviousChunks = 0;
  // Return `false` if the chunk of the given `chunkSize` is the last one.
  auto addTarget = [&sizeOfPreviousChunks, &targets,
                    totalNumElements](size_t chunkSize) {
    sizeOfPreviousChunks += chunkSize;
    if (sizeOfPreviousChunks >= totalNumElements) {
      return false;
    }
    targets.push_back(sizeOfPreviousChunks);
    return true;
  };
  for (size_t chunkSize : chunkSizes.firstChunkSizes_) {
    if (!addTarget(chunkSize)) {
      return targets;
    }
  }
  while (addTarget(chunkSizes.remainingChunkSize_)) {
  }
  return targets;
}

// Step 4: Walk the target quantiles and pick the elements at which a new chunk
// starts. As the `targets` as well as the accumulated weights are increasing, a
// single scan that never goes back suffices. The result is strictly increasing,
// because the `elementsAndWeights` are (see `sortAndAccumulateWeights`) and
// because an entry that was picked is never looked at again.
//
// NOTE: The first chunk may well start at the smallest element of the whole
// input, in which case the chunk before it is empty. This happens whenever the
// blocks that end with that element already hold enough elements to reach the
// first target, in particular if all the elements of the input are equal. An
// empty chunk is perfectly legal: it simply yields no output block at all, see
// `ChunkMerger`.
template <typename Element>
std::vector<Element> pickChunkStarts(
    const std::vector<ElementAndWeight<Element>>& elementsAndWeights,
    const std::vector<size_t>& targets) {
  std::vector<Element> result;
  auto it = elementsAndWeights.begin();
  for (size_t target : targets) {
    it = ql::ranges::lower_bound(it, elementsAndWeights.end(), target,
                                 std::less<>{}, ad_utility::second);
    if (it == elementsAndWeights.end()) {
      break;
    }
    result.push_back(it->first);
    ++it;
  }
  return result;
}

// Convert the `chunkStarts` into the boundaries of the `chunkStarts.size() + 1`
// chunks that they describe: the `i`-th of them separates chunk `i` from chunk
// `i + 1`. The lower bound of the first and the upper bound of the last chunk
// are empty, that is minus and plus infinity.
template <typename Element>
std::vector<ChunkBoundary<Element>> chunkBoundariesFromChunkStarts(
    const std::vector<Element>& chunkStarts) {
  std::vector<ChunkBoundary<Element>> result;
  result.reserve(chunkStarts.size() + 1);
  for (size_t chunkIdx = 0; chunkIdx <= chunkStarts.size(); ++chunkIdx) {
    ChunkBoundary<Element> boundary;
    if (chunkIdx > 0) {
      boundary.lo_ = chunkStarts.at(chunkIdx - 1);
    }
    if (chunkIdx < chunkStarts.size()) {
      boundary.hi_ = chunkStarts.at(chunkIdx);
    }
    result.push_back(std::move(boundary));
  }
  return result;
}

// The common part of the two overloads of `computeChunkBoundaries` below: run
// the steps above, where `makeTargets` turns the total number of elements into
// the target quantiles.
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
  return chunkBoundariesFromChunkStarts(
      pickChunkStarts(elementsAndWeights, targets));
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
        return detail::targetsFromChunkSizes(totalNumElements, chunkSizes);
      });
}

}  // namespace ad_utility::parallelBlockMerge

#endif  // QLEVER_SRC_UTIL_PARALLELBLOCKMERGE_MERGEHELPERS_H
