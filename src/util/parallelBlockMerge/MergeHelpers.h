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
#include <utility>
#include <vector>

#include "backports/algorithm.h"
#include "backports/concepts.h"
#include "util/Exception.h"
#include "util/Views.h"
#include "util/parallelBlockMerge/RunsInputPolicy.h"

namespace ad_utility::parallelBlockMerge {

// ___________________________________________________________________________
// The chunk boundaries.
// ___________________________________________________________________________

// The half-open key range `[lo_, hi_)` that a single chunk of the merge covers.
// An empty `lo_`/`hi_` means minus/plus infinity, so the `Split` of a merge
// that consists of a single chunk has neither of the two bounds set.
template <typename Key>
struct Split {
  std::optional<Key> lo_{};
  std::optional<Key> hi_{};
};

// The chunk boundaries of a whole merge: a strictly increasing (with respect to
// the comparator of the merge) sequence of keys that splits the global output
// range into `numChunks()` chunks, see `computeSplitters` below for how they
// are obtained.
template <typename Key>
class Splitters {
 private:
  // The `i`-th splitter separates chunk `i` from chunk `i + 1`, so there is
  // exactly one splitter less than there are chunks.
  std::vector<Key> splitters_;

 public:
  Splitters() = default;

  // ________________________________________________________________________
  explicit Splitters(std::vector<Key> splitters)
      : splitters_{std::move(splitters)} {}

  // The number of chunks that these splitters describe. Note that this is at
  // least one, also for an empty set of splitters (which means that the whole
  // merge is a single chunk).
  size_t numChunks() const { return splitters_.size() + 1; }

  // Return the key range that the chunk with the given index covers. The lower
  // bound of the first and the upper bound of the last chunk are empty, that is
  // minus and plus infinity.
  Split<Key> getSplittersAt(size_t chunkIndex) const {
    AD_CONTRACT_CHECK(chunkIndex < numChunks());
    Split<Key> result;
    if (chunkIndex > 0) {
      result.lo_ = splitters_.at(chunkIndex - 1);
    }
    if (chunkIndex + 1 < numChunks()) {
      result.hi_ = splitters_.at(chunkIndex);
    }
    return result;
  }

  // The splitter keys themselves. Only needed for testing.
  const std::vector<Key>& keys() const { return splitters_; }
};

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
// NOTE: The sizes are targets and not guarantees. They are only honored up to
// the granularity of the input blocks, and a chunk that is supposed to start
// after `n` elements actually starts at the key of the `n`-th element, which is
// the same off-by-one that a uniform number of chunks has, see
// `computeSplitters`.
struct ChunkSizes {
  std::vector<size_t> firstChunkSizes_;
  size_t remainingChunkSize_;
};

namespace detail {

// ___________________________________________________________________________
// Metadata-only helpers. None of these performs any I/O.
// ___________________________________________________________________________

// Return a view of all `[runIndex, blockIndex]` pairs of the `input`, in the
// order of the runs and, within a run, in the order of the blocks.
//
// NOTE: The returned view refers to the `input`, which therefore has to outlive
// it.
CPP_template(typename Input)(
    requires BlockedRunsInput<Input>) auto allBlocksInAllRuns(const Input&
                                                                  input) {
  return ::ranges::views::for_each(
      ad_utility::integerRange(input.numRuns()), [&input](size_t run) {
        return ::ranges::views::transform(
            ad_utility::integerRange(input.numBlocks(run)),
            [run](size_t block) {
              return std::pair<size_t, size_t>{run, block};
            });
      });
}

// Return the total number of elements of all runs of the `input`.
CPP_template(typename Input)(requires BlockedRunsInput<Input>) size_t
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

// Return the range of blocks of the given `run` that can contain elements in
// the key range of the `split`. This only looks at the (I/O-free) metadata of
// the blocks, so a returned block may still turn out to contain no matching
// element at all.
CPP_template(typename Input,
             typename Comparator)(requires BlockedRunsInput<Input>) BlockRange
    blockRangeForRun(const Input& input, const Comparator& comparator,
                     const Split<typename Input::Key>& split, size_t run) {
  auto blocks = ad_utility::integerRange(input.numBlocks(run));
  // Return the number of blocks in the prefix for which the `predicate` holds.
  // The predicate is monotone (once it is `false` it stays `false`), so this is
  // a binary search.
  auto lengthOfPrefixWhere = [&blocks](const auto& predicate) {
    return static_cast<size_t>(ql::ranges::partition_point(blocks, predicate) -
                               ql::ranges::begin(blocks));
  };

  // The first block that may contain an element that is not smaller than `lo`,
  // that is the first block the last key of which is not smaller than `lo`.
  size_t firstBlockIdx = 0;
  if (split.lo_.has_value()) {
    firstBlockIdx = lengthOfPrefixWhere([&](size_t block) {
      return comparator(input.lastKey(run, block), split.lo_.value());
    });
  }
  // The first block all of whose elements are not smaller than `hi`, that is
  // the (exclusive) end of the range of blocks.
  //
  // NOTE: The predicate is deliberately `comparator(firstKey, hi)` and not
  // `!comparator(hi, firstKey)`. The latter would be off by one and would read
  // one superfluous block whenever `firstKey(block) == hi`.
  size_t endBlockIdx = input.numBlocks(run);
  if (split.hi_.has_value()) {
    endBlockIdx = lengthOfPrefixWhere([&](size_t block) {
      return comparator(input.firstKey(run, block), split.hi_.value());
    });
  }
  return {firstBlockIdx, endBlockIdx};
}

// Return the number of elements of the `block`.
template <typename Block>
size_t blockSize(const Block& block) {
  return static_cast<size_t>(ql::ranges::distance(block));
}

// ___________________________________________________________________________
// The computation of the chunk boundaries.
// ___________________________________________________________________________

// The computation of the chunk boundaries of a merge, see `computeSplitters`
// below for the interface and for the guarantees. This is a class and not a
// single function only so that the three steps of the computation (collect the
// keys, accumulate their weights, walk the quantiles) can be read one at a
// time.
//
// NOTE: `chunkSizes_` is set if and only if the caller has requested explicit
// chunk sizes instead of a fixed number of equally sized chunks.
CPP_template(typename Input, typename Comparator)(
    requires BlockedRunsInput<Input>) class SplitterComputation {
 public:
  using Key = typename Input::Key;

 private:
  const Input& input_;
  const Comparator& comparator_;
  size_t numChunks_ = 0;
  std::optional<ChunkSizes> chunkSizes_{};
  // The `lastKey` of every non-empty block together with the number of elements
  // in that block, which is the weight of that key. After
  // `sortAndAccumulateWeights` the keys are sorted and the weights are replaced
  // by their prefix sums.
  std::vector<std::pair<Key, size_t>> keysAndWeights_{};
  // The smallest key of the whole input, which acts as the (virtual)
  // predecessor of the first splitter.
  std::optional<Key> smallestKey_{};
  size_t totalNumElements_ = 0;

 public:
  // Split the input into (at most) `numChunks` equally sized chunks.
  SplitterComputation(const Input& input, const Comparator& comparator,
                      size_t numChunks)
      : input_{input}, comparator_{comparator}, numChunks_{numChunks} {}

  // Split the input into chunks of the given `chunkSizes`.
  SplitterComputation(const Input& input, const Comparator& comparator,
                      ChunkSizes chunkSizes)
      : input_{input},
        comparator_{comparator},
        chunkSizes_{std::move(chunkSizes)} {}

  // Run all steps of the computation and return the resulting splitters.
  Splitters<Key> compute() {
    // A single chunk needs no splitters at all, and in that case even the scan
    // of the block metadata can be skipped. NOTE: For explicit chunk sizes the
    // number of chunks is only known once the total number of elements is, so
    // there is no such shortcut.
    if (!chunkSizes_.has_value() && numChunks_ <= 1) {
      return Splitters<Key>{};
    }
    collectKeysAndWeights();
    if (keysAndWeights_.empty()) {
      return Splitters<Key>{};
    }
    sortAndAccumulateWeights();
    return Splitters<Key>{walkQuantiles(computeTargets())};
  }

 private:
  // Collect the `lastKey` of every non-empty block of the input together with
  // its weight, as well as the smallest key and the total number of elements.
  void collectKeysAndWeights() {
    for (auto [run, block] : allBlocksInAllRuns(input_)) {
      size_t numElements = input_.numElementsInBlock(run, block);
      if (numElements == 0) {
        continue;
      }
      const Key& firstKey = input_.firstKey(run, block);
      if (!smallestKey_.has_value() || comparator_(firstKey, *smallestKey_)) {
        smallestKey_ = firstKey;
      }
      keysAndWeights_.emplace_back(input_.lastKey(run, block), numElements);
      totalNumElements_ += numElements;
    }
  }

  // Sort the keys and replace the weights by their prefix sums, such that
  // `keysAndWeights_[i].second` is the number of elements that are
  // (approximately) not greater than `keysAndWeights_[i].first`.
  void sortAndAccumulateWeights() {
    ql::ranges::sort(keysAndWeights_, [this](const std::pair<Key, size_t>& a,
                                             const std::pair<Key, size_t>& b) {
      return comparator_(a.first, b.first);
    });
    size_t prefixSum = 0;
    for (auto& keyAndWeight : keysAndWeights_) {
      prefixSum += keyAndWeight.second;
      keyAndWeight.second = prefixSum;
    }
  }

  // The strictly increasing numbers of elements at which a new chunk starts,
  // one per chunk boundary. PRECONDITION: `collectKeysAndWeights` has run, so
  // that `totalNumElements_` is known.
  std::vector<size_t> computeTargets() const {
    return chunkSizes_.has_value() ? targetsFromChunkSizes() : uniformTargets();
  }

  // The targets for `numChunks_` equally sized chunks.
  std::vector<size_t> uniformTargets() const {
    std::vector<size_t> targets;
    for (size_t i = 1; i < numChunks_; ++i) {
      // NOTE: The target is at least `1`, because a target of `0` would always
      // yield the smallest key and therefore an empty first chunk.
      targets.push_back(
          std::max<size_t>(1, totalNumElements_ * i / numChunks_));
    }
    return targets;
  }

  // The targets for explicitly given `chunkSizes_`: the `i`-th target is the
  // total size of the first `i` chunks. Stop as soon as a target has reached
  // the total number of elements, because all the chunks after that one would
  // be empty. This is what makes a `remainingChunkSize_` that is smaller than
  // the input terminate, and it also handles leading sizes that already exceed
  // the input.
  std::vector<size_t> targetsFromChunkSizes() const {
    std::vector<size_t> targets;
    size_t sizeOfPreviousChunks = 0;
    // Return `false` if the chunk of the given `chunkSize` is the last one.
    auto addTarget = [&sizeOfPreviousChunks, &targets, this](size_t chunkSize) {
      sizeOfPreviousChunks += chunkSize;
      if (sizeOfPreviousChunks >= totalNumElements_) {
        return false;
      }
      targets.push_back(sizeOfPreviousChunks);
      return true;
    };
    for (size_t chunkSize : chunkSizes_.value().firstChunkSizes_) {
      if (!addTarget(chunkSize)) {
        return targets;
      }
    }
    while (addTarget(chunkSizes_.value().remainingChunkSize_)) {
    }
    return targets;
  }

  // Walk the target quantiles and pick the splitter keys. As the `targets` are
  // increasing, a single linear scan over the (sorted) keys suffices.
  std::vector<Key> walkQuantiles(const std::vector<size_t>& targets) const {
    std::vector<Key> result;
    size_t idx = 0;
    for (size_t target : targets) {
      while (idx < keysAndWeights_.size() &&
             keysAndWeights_[idx].second < target) {
        ++idx;
      }
      if (idx == keysAndWeights_.size()) {
        break;
      }
      const Key& candidate = keysAndWeights_[idx].first;
      // Only keep the candidate if it makes the result strictly increasing. The
      // smallest key of the input acts as the (virtual) predecessor of the
      // first splitter, because a splitter that is not greater than that key
      // would yield an empty first chunk.
      const Key& previous = result.empty() ? *smallestKey_ : result.back();
      if (comparator_(previous, candidate)) {
        result.push_back(candidate);
      }
    }
    return result;
  }
};

}  // namespace detail

// Compute the chunk boundaries by a weighted quantile over the block metadata
// only (no I/O at all): every block contributes its `lastKey`, weighted by its
// number of elements. Return a strictly increasing (wrt `comparator`) sequence
// of at most `numChunks - 1` keys, see `Splitters`.
//
// NOTE: The result may well describe fewer than `numChunks` chunks, in
// particular it is a single chunk if all keys are equal. In that case the merge
// simply consists of fewer chunks, and there is nothing that could be done
// about it, because all elements with the same key have to end up in the same
// chunk.
CPP_template(typename Input,
             typename Comparator)(requires BlockedRunsInput<Input>)
    Splitters<typename Input::Key> computeSplitters(
        const Input& input, const Comparator& comparator, size_t numChunks) {
  return detail::SplitterComputation<Input, Comparator>{input, comparator,
                                                        numChunks}
      .compute();
}

// The same as above, but with explicit `chunkSizes` (see `ChunkSizes`)
// instead of a fixed number of equally sized chunks. Use this to make the first
// chunks smaller than the remaining ones, which reduces the latency at the
// start of the merge.
//
// NOTE: All the guarantees of the overload above still hold, in particular the
// result may well describe fewer chunks than the `chunkSizes` ask for, and it
// describes a single chunk if the input has at most `firstChunkSizes_.front()`
// (respectively `remainingChunkSize_`) elements.
CPP_template(typename Input,
             typename Comparator)(requires BlockedRunsInput<Input>)
    Splitters<typename Input::Key> computeSplitters(
        const Input& input, const Comparator& comparator,
        ChunkSizes chunkSizes) {
  AD_CONTRACT_CHECK(chunkSizes.remainingChunkSize_ > 0);
  AD_CONTRACT_CHECK(ql::ranges::all_of(chunkSizes.firstChunkSizes_,
                                       [](size_t size) { return size > 0; }));
  return detail::SplitterComputation<Input, Comparator>{input, comparator,
                                                        std::move(chunkSizes)}
      .compute();
}

}  // namespace ad_utility::parallelBlockMerge

#endif  // QLEVER_SRC_UTIL_PARALLELBLOCKMERGE_MERGEHELPERS_H
