// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_TEST_UTIL_PARALLELBLOCKMERGETESTHELPERS_H
#define QLEVER_TEST_UTIL_PARALLELBLOCKMERGETESTHELPERS_H

#include <cstddef>
#include <cstdint>
#include <utility>
#include <vector>

#include "backports/algorithm.h"
#include "util/Random.h"
#include "util/parallelBlockMerge/MergeHelpers.h"
#include "util/parallelBlockMerge/MergeOptions.h"
#include "util/parallelBlockMerge/RunsInputPolicy.h"

// Helpers that both `ParallelBlockMergeTest.cpp` (which tests the merge itself)
// and `MergeHelpersTest.cpp` (which tests the helpers from `MergeHelpers.h`)
// need.
namespace parallelBlockMergeTestHelpers {

using SizeVec = std::vector<size_t>;
using SizeInput = ad_utility::parallelBlockMerge::VectorInput<size_t>;
using Pair = std::pair<size_t, size_t>;

// A comparator that only looks at the first component of a pair, so that ties
// are visible in the second component.
struct ComparePairs {
  bool operator()(const Pair& a, const Pair& b) const {
    return a.first < b.first;
  }
};

// Return the split points of the `boundaries`, that is the upper bounds of all
// chunks but the last one.
template <typename Element>
std::vector<Element> chunkSplitPoints(
    const std::vector<ad_utility::parallelBlockMerge::ChunkBoundary<Element>>&
        boundaries) {
  std::vector<Element> result;
  for (const auto& boundary : boundaries) {
    if (boundary.hi_.has_value()) {
      result.push_back(boundary.hi_.value());
    }
  }
  return result;
}

// Return `MergeOptions` with the given number of elements per output block.
// That number is deliberately small in the tests, such that even a single chunk
// yields several blocks.
inline ad_utility::parallelBlockMerge::MergeOptions optionsWithBlockSize(
    size_t outputBlockSize = 7) {
  ad_utility::parallelBlockMerge::MergeOptions options;
  options.outputBlockSize =
      ad_utility::parallelBlockMerge::OutputBlockSize::numElements(
          outputBlockSize);
  return options;
}

// Return `numRuns` sorted vectors of random numbers, the sizes of which are
// uniformly distributed in `[minSize, maxSize]`.
inline std::vector<SizeVec> makeRandomRuns(size_t numRuns, size_t minSize,
                                           size_t maxSize) {
  ad_utility::FastRandomIntGenerator<uint64_t> valueGenerator;
  ad_utility::SlowRandomIntGenerator<size_t> sizeGenerator{minSize, maxSize};
  std::vector<SizeVec> runs;
  for (size_t i = 0; i < numRuns; ++i) {
    SizeVec run(sizeGenerator());
    ql::ranges::generate(run, valueGenerator);
    ql::ranges::sort(run);
    runs.push_back(std::move(run));
  }
  return runs;
}

// Return the concatenation of all `runs`.
template <typename T>
std::vector<T> concatenation(const std::vector<std::vector<T>>& runs) {
  std::vector<T> result;
  for (const auto& run : runs) {
    result.insert(result.end(), run.begin(), run.end());
  }
  return result;
}

// Return the sorted concatenation of all `runs`.
inline SizeVec sortedConcatenation(const std::vector<SizeVec>& runs) {
  SizeVec result = concatenation(runs);
  ql::ranges::sort(result);
  return result;
}

// Return four identical runs, each of which consists of ten copies of each of
// the elements `0`, `1`, `2` and `3`. With a block size of ten, every one of
// the four distinct elements is the last element of four different blocks.
inline std::vector<SizeVec> runsWithEqualLastElements() {
  std::vector<SizeVec> runs;
  for (size_t run = 0; run < 4; ++run) {
    SizeVec elements;
    for (size_t value = 0; value < 4; ++value) {
      elements.insert(elements.end(), 10, value);
    }
    runs.push_back(std::move(elements));
  }
  return runs;
}

// Return two runs, all elements of which are equal, so that there is no way to
// actually split the input.
inline std::vector<SizeVec> runsWithEqualElements() {
  return {SizeVec(100, 42u), SizeVec(100, 42u)};
}

// Return one run with the 10000 elements `0 ... 9999` plus 50 runs with two
// elements each, all of which lie at the very beginning of the huge run.
inline std::vector<SizeVec> oneHugeAndManyTinyRuns() {
  std::vector<SizeVec> runs;
  SizeVec huge(10000);
  ql::ranges::generate(huge, [i = size_t{0}]() mutable { return i++; });
  runs.push_back(std::move(huge));
  for (size_t i = 0; i < 50; ++i) {
    runs.push_back(SizeVec{i, i + 1});
  }
  return runs;
}

}  // namespace parallelBlockMergeTestHelpers

#endif  // QLEVER_TEST_UTIL_PARALLELBLOCKMERGETESTHELPERS_H
