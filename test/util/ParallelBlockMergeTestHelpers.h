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

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <range/v3/range/conversion.hpp>
#include <utility>
#include <vector>

#include "backports/algorithm.h"
#include "util/Exception.h"
#include "util/Forward.h"
#include "util/MemorySize/MemorySize.h"
#include "util/Random.h"
#include "util/parallelBlockMerge/MergeHelpers.h"
#include "util/parallelBlockMerge/MergeOptions.h"
#include "util/parallelBlockMerge/RunsInputPolicy.h"

// Helpers that both `ParallelBlockMergeTest.cpp` (which tests the merge itself)
// and `MergeHelpersTest.cpp` (which tests the helpers from `MergeHelpers.h`)
// need, in particular the in-memory `VectorInput` policy.
namespace parallelBlockMergeTestHelpers {

// ___________________________________________________________________________
// An in-memory input policy.
// ___________________________________________________________________________

// Expose a set of runs, each of which is a `std::vector` of blocks, as an
// `ad_utility::parallelBlockMerge::InputConcept`. Every run (that is, the
// concatenation of its blocks) has to be sorted, and no block may be empty.
//
// NOTE: This lives in the test directory on purpose. In production the merge is
// only ever used on external (that is, on-disk) data, so an in-memory input is
// only ever needed by the tests.
//
// NOTE: `getBlock` returns a *copy* of the block, which is of course not
// efficient, but perfectly fine for the tests. The copy is also what makes the
// class correct for a merge with `moveElements == true`. The very same block
// may be read by two different chunks (namely by the two chunks whose boundary
// lies inside that block), so a chunk that moves the elements out of a block
// must not be able to affect the other one.
template <typename T>
class VectorInput {
 public:
  using value_type = T;
  using Element = T;
  using Block = std::vector<T>;

 private:
  std::vector<std::vector<Block>> runs_;

 public:
  // Construct from the blocks of every run.
  explicit VectorInput(std::vector<std::vector<Block>> runs)
      : runs_{std::move(runs)} {
    for (const auto& run : runs_) {
      AD_CONTRACT_CHECK(ql::ranges::none_of(
          run, [](const Block& block) { return block.empty(); }));
    }
  }

  // ________________________________________________________________________
  size_t numRuns() const { return runs_.size(); }

  // ________________________________________________________________________
  size_t numBlocks(size_t runIdx) const { return runs_.at(runIdx).size(); }

  // ________________________________________________________________________
  size_t numElementsInBlock(size_t runIdx, size_t blockIdx) const {
    return block(runIdx, blockIdx).size();
  }

  // ________________________________________________________________________
  const Element& firstElement(size_t runIdx, size_t blockIdx) const {
    return block(runIdx, blockIdx).front();
  }

  // ________________________________________________________________________
  const Element& lastElement(size_t runIdx, size_t blockIdx) const {
    return block(runIdx, blockIdx).back();
  }

  // Return a copy of the block, see the note at the top of this class.
  Block getBlock(size_t runIdx, size_t blockIdx) const {
    return block(runIdx, blockIdx);
  }

  // ________________________________________________________________________
  Block makeEmptyBlock() const { return Block{}; }

  // ________________________________________________________________________
  template <typename U>
  void appendToBlock(Block& block, U&& element) const {
    block.push_back(AD_FWD(element));
  }

  // ________________________________________________________________________
  ad_utility::MemorySize memorySizeOfElement(
      [[maybe_unused]] const value_type& element) const {
    return ad_utility::MemorySize::bytes(sizeof(value_type));
  }

 private:
  // Return the block with the given index of the run with the given index.
  const Block& block(size_t runIdx, size_t blockIdx) const {
    return runs_.at(runIdx).at(blockIdx);
  }
};

// Split each of the `runs` (each of which has to be sorted) into blocks of
// `blockSize` elements, where the last block of a run may be smaller, and
// return the corresponding `VectorInput`. This is the convenient way to obtain
// a `VectorInput` from flat vectors.
template <typename T>
VectorInput<T> makeVectorInput(const std::vector<std::vector<T>>& runs,
                               size_t blockSize) {
  AD_CONTRACT_CHECK(blockSize > 0);
  std::vector<std::vector<std::vector<T>>> blockedRuns;
  blockedRuns.reserve(runs.size());
  for (const auto& run : runs) {
    auto& blocks = blockedRuns.emplace_back();
    for (size_t begin = 0; begin < run.size(); begin += blockSize) {
      size_t end = std::min(begin + blockSize, run.size());
      blocks.emplace_back(run.begin() + begin, run.begin() + end);
    }
  }
  return VectorInput<T>{std::move(blockedRuns)};
}

// ___________________________________________________________________________
// Inputs and other helpers for the individual tests.
// ___________________________________________________________________________

using SizeVec = std::vector<size_t>;
using SizeInput = VectorInput<size_t>;
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
  // Two generators are needed, because only the `Slow` one can be restricted to
  // a range: the `valueGenerator` yields the elements (from the whole range of
  // `uint64_t`), and the `sizeGenerator` the size of a single run.
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
  return ::ranges::to_vector(runs | ql::views::join);
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
