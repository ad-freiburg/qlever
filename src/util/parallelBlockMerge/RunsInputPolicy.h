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

// The input policy of the block merge: the `InputConcept`, and the
// `VectorInput` adapter for in-memory data. For the terminology (runs, blocks,
// and chunks) see `util/parallelBlockMerge/ParallelBlockMerge.h`, which is the
// header to read first.
namespace ad_utility::parallelBlockMerge {

// The requirements of the `InputConcept` below, see there for the
// documentation.
template <typename T>
CPP_requires(
    InputConcept_,
    requires(const T& t, size_t runIdx, size_t blockIdx, typename T::Block& out,
             ql::ranges::range_reference_t<typename T::Block> el)(
        // The number of presorted runs.
        ql::concepts::convertible_to<decltype(t.numRuns()), size_t>,
        // The number of blocks of a single run.
        ql::concepts::convertible_to<decltype(t.numBlocks(runIdx)), size_t>,
        // The number of elements in a single block, available without I/O.
        ql::concepts::convertible_to<
            decltype(t.numElementsInBlock(runIdx, blockIdx)), size_t>,
        // The first and the last element of a single block, available without
        // I/O.
        ql::concepts::convertible_to<decltype(t.firstElement(runIdx, blockIdx)),
                                     typename T::Element>,
        ql::concepts::convertible_to<decltype(t.lastElement(runIdx, blockIdx)),
                                     typename T::Element>,
        // Materialize a single block. This is the only operation that performs
        // I/O, and it has to be thread-safe.
        ql::concepts::convertible_to<decltype(t.readBlock(runIdx, blockIdx)),
                                     typename T::Block>,
        // Create an empty block, and append a single element to a block.
        ql::concepts::same_as<decltype(t.makeEmptyBlock()), typename T::Block>,
        t.appendToBlock(out, el),
        // The memory that a single element occupies.
        ql::concepts::convertible_to<decltype(t.memorySizeOfElement(el)),
                                     MemorySize>));

namespace detail {
// Extract `T::Block` if it exists, and `void` otherwise. This is needed so that
// the `InputConcept` below is a hard `false` (instead of a compilation error)
// for types without a nested `Block` type. Note that this requires a class
// template (and not simply a `CPP_requires` clause), because in C++17 mode the
// concepts are emulated via variable templates, for which SFINAE does not apply
// to the template arguments.
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
// (`numRuns()` many), each of which is split into blocks (`numBlocks(runIdx)`
// many for the run with the given index). The concatenation of all blocks of a
// single run is sorted with respect to the comparator of the merge, and no
// block is empty.
//
// The crucial property of this policy is that the number of elements
// (`numElementsInBlock`) as well as the first and the last element
// (`firstElement`/`lastElement`) of every block are available *without*
// performing any I/O. They typically come from cheap in-memory metadata. It is
// exactly this property that allows the blocks themselves to live compressed on
// disk: the merge can compute the boundaries of the independent chunks from the
// metadata alone and only then read (via the thread-safe `readBlock`) those
// blocks that a given chunk actually needs.
//
// IMPORTANT: There is deliberately no separate notion of a *key*. The merge
// splits on whole elements and applies the `Comparator` to them and only to
// them, so `firstElement(runIdx, blockIdx)` and `lastElement(runIdx, blockIdx)`
// have to be equivalent to the first and the last element of
// `readBlock(runIdx, blockIdx)`: the `Comparator` must not be able to
// distinguish them. `Element` need not be the value type of `Block` (it may for
// example be an owning type that corresponds to a proxy reference, or a type
// that only stores those parts of an element that the comparator looks at), it
// only has to be morally the same value. Anything weaker silently breaks the
// merge, because a chunk trims its first and its last input block by exactly
// these bounds, see `ChunkMerger`.
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
CPP_concept InputConcept =
    ql::ranges::random_access_range<detail::BlockTypeOrVoidT<T>> &&
    ql::ranges::sized_range<detail::BlockTypeOrVoidT<T>> &&
    CPP_requires_ref(InputConcept_, T);

// ___________________________________________________________________________
// An in-memory input policy.
// ___________________________________________________________________________

// Expose a set of runs, each of which is a `std::vector` of blocks, as an
// `InputConcept`. Every run (that is, the concatenation of its blocks) has to
// be sorted, and no block may be empty.
//
// NOTE: `readBlock` returns a *copy* of the block, which is of course not
// efficient. This is deliberate and perfectly fine: in production the merge is
// only ever used on external (that is, on-disk) data, where a block has to be
// materialized anyway, so this class only serves the tests. The copy is also
// what makes the class correct for a merge with `moveElements == true`. The
// very same block may be read by two different chunks (namely by the two chunks
// whose boundary lies inside that block), so a chunk that moves the elements
// out of a block must not be able to affect the other one.
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
  Block readBlock(size_t runIdx, size_t blockIdx) const {
    return block(runIdx, blockIdx);
  }

  // ________________________________________________________________________
  Block makeEmptyBlock() const { return Block{}; }

  // ________________________________________________________________________
  template <typename U>
  void appendToBlock(Block& block, U&& element) const {
    block.push_back(std::forward<U>(element));
  }

  // ________________________________________________________________________
  MemorySize memorySizeOfElement(
      [[maybe_unused]] const value_type& element) const {
    return MemorySize::bytes(sizeof(value_type));
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

}  // namespace ad_utility::parallelBlockMerge

#endif  // QLEVER_SRC_UTIL_PARALLELBLOCKMERGE_RUNSINPUTPOLICY_H
