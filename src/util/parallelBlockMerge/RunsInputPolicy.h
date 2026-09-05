// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_UTIL_PARALLELBLOCKMERGE_RUNSINPUTPOLICY_H
#define QLEVER_SRC_UTIL_PARALLELBLOCKMERGE_RUNSINPUTPOLICY_H

#include <algorithm>
#include <cstddef>
#include <type_traits>
#include <utility>
#include <vector>

#include "backports/algorithm.h"
#include "backports/concepts.h"
#include "util/Exception.h"
#include "util/MemorySize/MemorySize.h"

// The input policy of the block merge (see
// `util/parallelBlockMerge/ParallelBlockMerge.h`): the `BlockedRunsInput`
// concept, and the `VectorRunsInput` adapter for in-memory data.
namespace ad_utility::parallelBlockMerge {

// The requirements of the `BlockedRunsInput` concept below, see there for the
// documentation.
template <typename T>
CPP_requires(
    BlockedRunsInput_,
    requires(const T& t, size_t run, size_t block, typename T::Block& out,
             ql::ranges::range_reference_t<typename T::Block> el)(
        // The number of presorted runs.
        ql::concepts::convertible_to<decltype(t.numRuns()), size_t>,
        // The number of blocks of a single run.
        ql::concepts::convertible_to<decltype(t.numBlocks(run)), size_t>,
        // The number of elements in a single block, available without I/O.
        ql::concepts::convertible_to<decltype(t.numElementsInBlock(run, block)),
                                     size_t>,
        // The first and the last key of a single block, available without I/O.
        ql::concepts::same_as<decltype(t.firstKey(run, block)),
                              const typename T::Key&>,
        ql::concepts::same_as<decltype(t.lastKey(run, block)),
                              const typename T::Key&>,
        // Materialize a single block. This is the only operation that performs
        // I/O, and it has to be thread-safe.
        ql::concepts::same_as<decltype(t.readBlock(run, block)),
                              typename T::Block>,
        // Create an empty block, and append a single element to a block.
        ql::concepts::same_as<decltype(t.makeEmptyBlock()), typename T::Block>,
        t.appendToBlock(out, el),
        // The memory that a single element occupies.
        ql::concepts::convertible_to<decltype(t.memorySizeOfElement(el)),
                                     MemorySize>));

namespace detail {
// Extract `T::Block` if it exists, and `void` otherwise. This is needed so that
// the `BlockedRunsInput` concept below is a hard `false` (instead of a
// compilation error) for types without a nested `Block` type. Note that this
// requires a class template (and not simply a `CPP_requires` clause), because
// in C++17 mode the concepts are emulated via variable templates, for which
// SFINAE does not apply to the template arguments.
template <typename T, typename = void>
struct BlockTypeOrVoid {
  using type = void;
};

// ___________________________________________________________________________
template <typename T>
struct BlockTypeOrVoid<T, std::void_t<typename T::Block>> {
  using type = typename T::Block;
};

// ___________________________________________________________________________
template <typename T>
using BlockTypeOrVoidT = typename BlockTypeOrVoid<T>::type;
}  // namespace detail

// The input policy of the merge. It abstracts a set of presorted runs
// (`numRuns()` many), each of which is split into blocks (`numBlocks(run)` many
// for the run with the given index). The elements of the blocks are ordered
// according to a key of type `T::Key`, and the concatenation of all blocks of a
// single run is sorted with respect to that key.
//
// The crucial property of this policy is that the number of elements
// (`numElementsInBlock`) as well as the first and the last key
// (`firstKey`/`lastKey`) of every block are available *without* performing any
// I/O. They typically come from cheap in-memory metadata. It is exactly this
// property that allows the blocks themselves to live compressed on disk: the
// merge can compute the boundaries of the independent chunks from the metadata
// alone and only then read (via the thread-safe `readBlock`) those blocks that
// a given chunk actually needs.
//
// The remaining member functions describe how the *output* blocks are built:
// `makeEmptyBlock()` creates a fresh (empty) block, `appendToBlock(block, el)`
// appends a single element to it, and `memorySizeOfElement(el)` reports how
// much memory that element occupies, so that the memory limit of an output
// block can be honored.
//
// All member functions must be `const` and thread-safe. This is what makes the
// chunks of a merge independent of each other not only in principle, but also
// in practice: a merge that distributes them over several threads calls these
// functions from all of those threads at the same time.
template <typename T>
CPP_concept BlockedRunsInput =
    ql::ranges::random_access_range<detail::BlockTypeOrVoidT<T>> &&
    CPP_requires_ref(BlockedRunsInput_, T);

// ___________________________________________________________________________
// An in-memory input policy.
// ___________________________________________________________________________

// Expose a set of sorted random-access ranges as runs of fixed-size virtual
// blocks. The first and last key of every virtual block are directly available
// from the underlying range, so no I/O and no stored metadata are needed. Use
// this to run the merge on in-memory data, and in tests.
CPP_template(typename Range)(
    requires ql::ranges::random_access_range<Range>) class VectorRunsInput {
 public:
  using value_type = ql::ranges::range_value_t<Range>;
  using Key = value_type;
  using Block = std::vector<value_type>;

 private:
  std::vector<Range> runs_;
  size_t virtualBlockSize_;
  // If `true`, then `readBlock` moves the elements out of the underlying
  // ranges. This only has an effect if the elements of the ranges are mutable
  // (for example if `Range` is a `subrange` or a `span`), and it has to match
  // the `moveElements` argument of `serialBlockMergeToRange`.
  bool moveElements_;

 public:
  // Construct from the `runs` (each of which has to be sorted) and the number
  // of elements in a single virtual block.
  explicit VectorRunsInput(std::vector<Range> runs, size_t virtualBlockSize,
                           bool moveElements = false)
      : runs_{std::move(runs)},
        virtualBlockSize_{virtualBlockSize},
        moveElements_{moveElements} {
    AD_CONTRACT_CHECK(virtualBlockSize > 0);
  }

  // ________________________________________________________________________
  size_t numRuns() const { return runs_.size(); }

  // ________________________________________________________________________
  size_t numBlocks(size_t run) const {
    size_t numElements = runSize(run);
    return (numElements + virtualBlockSize_ - 1) / virtualBlockSize_;
  }

  // ________________________________________________________________________
  size_t numElementsInBlock(size_t run, size_t block) const {
    size_t begin = block * virtualBlockSize_;
    return std::min(virtualBlockSize_, runSize(run) - begin);
  }

  // ________________________________________________________________________
  const Key& firstKey(size_t run, size_t block) const {
    return ql::ranges::begin(runs_[run])[block * virtualBlockSize_];
  }

  // ________________________________________________________________________
  const Key& lastKey(size_t run, size_t block) const {
    size_t begin = block * virtualBlockSize_;
    return ql::ranges::begin(
        runs_[run])[begin + numElementsInBlock(run, block) - 1];
  }

  // ________________________________________________________________________
  Block readBlock(size_t run, size_t block) const {
    size_t begin = block * virtualBlockSize_;
    size_t numElements = numElementsInBlock(run, block);
    Block result;
    result.reserve(numElements);
    auto it = ql::ranges::begin(runs_[run]) + begin;
    for (size_t i = 0; i < numElements; ++i, ++it) {
      if (moveElements_) {
        result.push_back(std::move(*it));
      } else {
        result.push_back(*it);
      }
    }
    return result;
  }

  // ________________________________________________________________________
  Block makeEmptyBlock() const { return Block{}; }

  // ________________________________________________________________________
  template <typename T>
  void appendToBlock(Block& block, T&& element) const {
    block.push_back(std::forward<T>(element));
  }

  // ________________________________________________________________________
  MemorySize memorySizeOfElement(
      [[maybe_unused]] const value_type& element) const {
    return MemorySize::bytes(sizeof(value_type));
  }

 private:
  // Return the number of elements of the run with the given index.
  size_t runSize(size_t run) const {
    return static_cast<size_t>(ql::ranges::distance(runs_[run]));
  }
};

}  // namespace ad_utility::parallelBlockMerge

#endif  // QLEVER_SRC_UTIL_PARALLELBLOCKMERGE_RUNSINPUTPOLICY_H
