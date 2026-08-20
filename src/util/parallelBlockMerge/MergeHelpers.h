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
#include <utility>
#include <vector>

#include "backports/algorithm.h"
#include "backports/concepts.h"
#include "util/parallelBlockMerge/RunsInputPolicy.h"

namespace ad_utility::parallelBlockMerge {

// ___________________________________________________________________________
// The splitters.
// ___________________________________________________________________________

// Compute the chunk boundaries by a weighted quantile over the block metadata
// only (no I/O at all): every block contributes its `lastKey`, weighted by its
// number of elements. Return a strictly increasing (wrt `comparator`) vector of
// at most `numChunks - 1` keys. Chunk `i` then covers the half-open value range
// `[result[i - 1], result[i])`, where `result[-1]` is minus infinity and
// `result[numChunks - 1]` is plus infinity.
//
// NOTE: The result may well be shorter than `numChunks - 1`, in particular it
// is empty if all keys are equal. In that case the merge simply consists of
// fewer chunks, and there is nothing that could be done about it, because all
// elements with the same key have to end up in the same chunk.
CPP_template(typename Input,
             typename Comparator)(requires BlockedRunsInput<Input>)
    std::vector<typename Input::Key> computeSplitters(
        const Input& input, const Comparator& comparator, size_t numChunks) {
  using Key = typename Input::Key;
  std::vector<Key> result;
  if (numChunks <= 1) {
    return result;
  }

  // Collect the `lastKey` of every (non-empty) block together with the number
  // of elements in that block, which is the weight of the key. Also compute the
  // smallest key of the whole input, which is needed below.
  std::vector<std::pair<Key, size_t>> keysAndWeights;
  std::optional<Key> smallestKey;
  size_t totalNumElements = 0;
  size_t numRuns = input.numRuns();
  for (size_t run = 0; run < numRuns; ++run) {
    size_t numBlocks = input.numBlocks(run);
    for (size_t block = 0; block < numBlocks; ++block) {
      size_t numElements = input.numElementsInBlock(run, block);
      if (numElements == 0) {
        continue;
      }
      const Key& firstKey = input.firstKey(run, block);
      if (!smallestKey.has_value() || comparator(firstKey, *smallestKey)) {
        smallestKey = firstKey;
      }
      keysAndWeights.emplace_back(input.lastKey(run, block), numElements);
      totalNumElements += numElements;
    }
  }
  if (keysAndWeights.empty()) {
    return result;
  }

  ql::ranges::sort(keysAndWeights,
                   [&comparator](const std::pair<Key, size_t>& a,
                                 const std::pair<Key, size_t>& b) {
                     return comparator(a.first, b.first);
                   });

  // Replace the weights by their prefix sums, such that `keysAndWeights[i]`
  // holds the number of elements that are (approximately) not greater than
  // `keysAndWeights[i].first`.
  size_t prefixSum = 0;
  for (auto& keyAndWeight : keysAndWeights) {
    prefixSum += keyAndWeight.second;
    keyAndWeight.second = prefixSum;
  }

  // Walk the target quantiles. As the targets are increasing, a single linear
  // scan over the (sorted) keys suffices.
  size_t idx = 0;
  for (size_t i = 1; i < numChunks; ++i) {
    // NOTE: The target is at least `1`, because a target of `0` would always
    // yield the smallest key and therefore an empty first chunk.
    size_t target = std::max<size_t>(1, totalNumElements * i / numChunks);
    while (idx < keysAndWeights.size() && keysAndWeights[idx].second < target) {
      ++idx;
    }
    if (idx == keysAndWeights.size()) {
      break;
    }
    const Key& candidate = keysAndWeights[idx].first;
    // Only keep the candidate if it makes the result strictly increasing. The
    // smallest key of the input acts as the (virtual) predecessor of the first
    // splitter, because a splitter that is not greater than that key would
    // yield an empty first chunk.
    const Key& previous = result.empty() ? *smallestKey : result.back();
    if (comparator(previous, candidate)) {
      result.push_back(candidate);
    }
  }
  return result;
}

namespace detail {

// Return the total number of elements of all runs of the `input`. This only
// uses the metadata and therefore performs no I/O.
CPP_template(typename Input)(requires BlockedRunsInput<Input>) size_t
    totalNumElements(const Input& input) {
  size_t result = 0;
  size_t numRuns = input.numRuns();
  for (size_t run = 0; run < numRuns; ++run) {
    size_t numBlocks = input.numBlocks(run);
    for (size_t block = 0; block < numBlocks; ++block) {
      result += input.numElementsInBlock(run, block);
    }
  }
  return result;
}

// Return the first index in `[0, numIndices)` for which the monotone
// `predicate` (that is, once it is `true` it stays `true`) holds, or
// `numIndices` if there is no such index.
template <typename Predicate>
size_t firstIndexWhere(size_t numIndices, const Predicate& predicate) {
  size_t low = 0;
  size_t high = numIndices;
  while (low < high) {
    size_t mid = low + (high - low) / 2;
    if (predicate(mid)) {
      high = mid;
    } else {
      low = mid + 1;
    }
  }
  return low;
}

// Return the number of elements of the `block`.
template <typename Block>
size_t blockSize(const Block& block) {
  return static_cast<size_t>(ql::ranges::distance(block));
}
}  // namespace detail

}  // namespace ad_utility::parallelBlockMerge

#endif  // QLEVER_SRC_UTIL_PARALLELBLOCKMERGE_MERGEHELPERS_H
