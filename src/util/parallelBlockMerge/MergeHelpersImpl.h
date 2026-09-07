// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_UTIL_PARALLELBLOCKMERGE_MERGEHELPERSIMPL_H
#define QLEVER_SRC_UTIL_PARALLELBLOCKMERGE_MERGEHELPERSIMPL_H

#include <algorithm>
#include <cstddef>
#include <range/v3/numeric/accumulate.hpp>
#include <range/v3/numeric/partial_sum.hpp>
#include <range/v3/range/conversion.hpp>
#include <range/v3/range/operations.hpp>
#include <range/v3/view/chunk_by.hpp>
#include <utility>
#include <vector>

#include "backports/algorithm.h"
#include "backports/concepts.h"
#include "util/Exception.h"
#include "util/TransparentFunctors.h"
#include "util/Views.h"
#include "util/parallelBlockMerge/RunsInputPolicy.h"

// The internals of the computation of the chunk split points. This is the
// implementation of `util/parallelBlockMerge/MergeHelpers.h`, which is the
// header to read first; for the terminology (runs, blocks, chunks, and split
// points) see `util/parallelBlockMerge/ParallelBlockMerge.h`.
namespace ad_utility::parallelBlockMerge::detail {

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

// ___________________________________________________________________________
// The computation of the chunk split points.
// ___________________________________________________________________________
//
// The computation consists of four independent steps, each of which is a pure
// function: collect the elements and their weights, accumulate those weights,
// compute the target quantiles, and pick the split points at those quantiles.
// The `ChunkBoundary`s are then assembled from the split points; see
// `computeChunkBoundaries` in `MergeHelpers.h` for the interface and for the
// guarantees.
//
// NOTE: Neither the targets of step 3 nor the split points of step 4 ever
// include the trivial first one, which is `0` (that is, no element at all) and
// "the smallest occurring element" respectively.

// The last element of a single block together with the weight of that element.
// The weight is initially the number of elements in that block; in
// `sortAndAccumulateWeights` it becomes the prefix sum of those numbers, that
// is the number of elements in that block as well as in all previous blocks.
template <typename Element>
using ElementAndWeight = std::pair<Element, size_t>;

// Step 1: Collect the last element of every block of the `input` together with
// its weight.
//
// PRECONDITION: No block of the `input` is empty, see `InputConcept`.
CPP_template(typename Input)(requires InputConcept<Input>) std::
    vector<ElementAndWeight<typename Input::Element>> collectElementsAndWeights(
        const Input& input) {
  using Element = typename Input::Element;
  return ::ranges::to_vector(
      allBlocksInAllRuns(input) |
      ::ranges::views::transform([&input](const auto& runAndBlock) {
        auto [runIdx, blockIdx] = runAndBlock;
        size_t numElements = input.numElementsInBlock(runIdx, blockIdx);
        AD_CORRECTNESS_CHECK(numElements > 0);
        return ElementAndWeight<Element>{input.lastElement(runIdx, blockIdx),
                                         numElements};
      }));
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
  // Merging equal elements is what makes the quantiles below exact: a target
  // then always identifies a single entry, and picking that entry for two
  // different targets can be avoided by simply moving on to the next one.
  auto isEquivalent = [&comparator](const ElementAndWeight<Element>& a,
                                    const ElementAndWeight<Element>& b) {
    return !comparator(a.first, b.first);
  };
  // Turn a group of entries with equal elements into a single entry, the weight
  // of which is the sum of their weights.
  auto mergeGroup = [](auto&& group) {
    return ElementAndWeight<Element>{
        ::ranges::front(group).first,
        ::ranges::accumulate(
            group | ::ranges::views::transform(ad_utility::second), size_t{0})};
  };
  auto result = ::ranges::to_vector(
      ::ranges::views::chunk_by(elementsAndWeights, isEquivalent) |
      ::ranges::views::transform(mergeGroup));
  auto weights = result | ql::views::transform(ad_utility::second);
  ::ranges::partial_sum(weights, ql::ranges::begin(weights));
  return result;
}

// Step 3a: The numbers of elements at which a new chunk starts, one per split
// point, for `numChunks` equally sized chunks.
//
// NOTE: The targets are increasing, but not necessarily *strictly*, because a
// target is always at least `1`: a target of `0` would pick the smallest
// element and thereby waste a chunk on the empty range in front of it. Asking
// for more chunks than the input has elements therefore yields the target `1`
// several times, which is fine: `pickChunkSplitPoints` picks a different (and
// hence greater) element for each of them.
inline std::vector<size_t> uniformTargets(size_t totalNumElements,
                                          size_t numChunks) {
  std::vector<size_t> targets;
  for (size_t i = 1; i < numChunks; ++i) {
    targets.push_back(std::max<size_t>(1, totalNumElements * i / numChunks));
  }
  return targets;
}

// Step 3b: The same, but for explicitly given chunk sizes (the components of a
// `ChunkSizes`, see `MergeHelpers.h`): the `i`-th target is the total size of
// the first `i` chunks. Stop as soon as a target has reached the total number
// of elements, because all the chunks after that one would be empty. This is
// what makes a `remainingChunkSize` that is smaller than the input terminate,
// and it also handles leading sizes that already exceed the input.
inline std::vector<size_t> targetsFromChunkSizes(
    size_t totalNumElements, const std::vector<size_t>& firstChunkSizes,
    size_t remainingChunkSize) {
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
  for (size_t chunkSize : firstChunkSizes) {
    if (!addTarget(chunkSize)) {
      return targets;
    }
  }
  while (addTarget(remainingChunkSize)) {
  }
  return targets;
}

// Step 4: Walk the target quantiles and pick the split points, that is the
// elements at which a new chunk starts. As the `targets` as well as the
// accumulated weights are increasing, a single scan that never goes back
// suffices. The result is strictly increasing, because the
// `elementsAndWeights` are (see `sortAndAccumulateWeights`) and because an
// entry that was picked is never looked at again.
//
// NOTE: The first split point may well be the smallest element of the whole
// input, in which case the chunk in front of it is empty. This happens whenever
// the blocks that end with that element already hold enough elements to reach
// the first target, in particular if all the elements of the input are equal.
// An empty chunk is perfectly legal: it simply yields no output block at all,
// see `ChunkMerger`.
template <typename Element>
std::vector<Element> pickChunkSplitPoints(
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

}  // namespace ad_utility::parallelBlockMerge::detail

#endif  // QLEVER_SRC_UTIL_PARALLELBLOCKMERGE_MERGEHELPERSIMPL_H
