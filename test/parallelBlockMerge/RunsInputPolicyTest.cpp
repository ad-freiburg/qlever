// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <cstddef>
#include <range/v3/range/conversion.hpp>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include "../util/ParallelBlockMergeTestHelpers.h"
#include "util/Iterators.h"
#include "util/MemorySize/MemorySize.h"
#include "util/parallelBlockMerge/RunsInputPolicy.h"

using namespace ad_utility::parallelBlockMerge;
using namespace parallelBlockMergeTestHelpers;

namespace {
// A minimal type that fulfills the `InputConcept`. It is only used to check
// that the concept is satisfiable, the actual data are irrelevant.
struct DummyInput {
  using Element = int;
  using Block = std::vector<int>;

  size_t numRuns() const { return 0; }
  size_t numBlocks([[maybe_unused]] size_t runIdx) const { return 0; }
  size_t numElementsInBlock([[maybe_unused]] size_t runIdx,
                            [[maybe_unused]] size_t blockIdx) const {
    return 0;
  }
  const Element& firstElement([[maybe_unused]] size_t runIdx,
                              [[maybe_unused]] size_t blockIdx) const {
    return element_;
  }
  const Element& lastElement([[maybe_unused]] size_t runIdx,
                             [[maybe_unused]] size_t blockIdx) const {
    return element_;
  }
  Block getBlock([[maybe_unused]] size_t runIdx,
                 [[maybe_unused]] size_t blockIdx) const {
    return {};
  }
  Block makeEmptyBlock() const { return {}; }
  void appendToBlock(Block& block, int el) const { block.push_back(el); }
  ad_utility::MemorySize memorySizeOfElement([[maybe_unused]] int el) const {
    return ad_utility::MemorySize::bytes(sizeof(int));
  }

 private:
  Element element_ = 0;
};

// The same, but `getBlock` returns a reference instead of a value, which the
// concept explicitly allows.
struct DummyInputWithReferenceToBlock : public DummyInput {
  const Block& getBlock([[maybe_unused]] size_t runIdx,
                        [[maybe_unused]] size_t blockIdx) const {
    return block_;
  }

 private:
  Block block_{};
};

// An input with lazy blocks and a separate `OutputBlock` type, which the
// concept explicitly allows, see `LazyVectorInput` for a complete example.
struct DummyLazyInput : public DummyInput {
  using Block = ad_utility::InputRangeTypeErased<int>;
  using OutputBlock = std::vector<int>;
  Block getBlock([[maybe_unused]] size_t runIdx,
                 [[maybe_unused]] size_t blockIdx) const {
    return Block{std::vector<int>{}};
  }
};

// An input that declares an `OutputBlock` type which its `makeEmptyBlock` and
// its `appendToBlock` do not use.
struct InputWithInconsistentOutputBlock : public DummyInput {
  using OutputBlock = std::vector<long>;
};

// An input whose `Block` is not a range at all.
struct InputWithNonRangeBlock : public DummyInput {
  using Block = int;
  Block getBlock([[maybe_unused]] size_t runIdx,
                 [[maybe_unused]] size_t blockIdx) const {
    return 0;
  }
};

// A type that is missing several of the required member functions.
struct NotAnInput {};

// A type that has the required nested types, but not the member functions.
struct AlmostAnInput {
  using Element = int;
  using Block = std::vector<int>;
};

static_assert(InputConcept<DummyInput>);
static_assert(InputConcept<DummyInputWithReferenceToBlock>);
static_assert(InputConcept<VectorInput<int>>);
static_assert(InputConcept<DummyLazyInput>);
static_assert(InputConcept<LazyVectorInput<int>>);
static_assert(!InputConcept<InputWithInconsistentOutputBlock>);
static_assert(!InputConcept<InputWithNonRangeBlock>);
static_assert(!InputConcept<NotAnInput>);
static_assert(!InputConcept<AlmostAnInput>);

// The `OutputBlockT` is the `OutputBlock` if there is one, and the `Block`
// otherwise.
static_assert(std::is_same_v<OutputBlockT<DummyInput>, std::vector<int>>);
static_assert(std::is_same_v<OutputBlockT<DummyLazyInput>, std::vector<int>>);
static_assert(std::is_same_v<OutputBlockT<InputWithInconsistentOutputBlock>,
                             std::vector<long>>);
}  // namespace

// _____________________________________________________________________________
TEST(RunsInputPolicy, VectorInputMetadata) {
  std::vector<std::vector<int>> runs{{1, 3, 5, 7, 9}, {2, 4}};
  auto input = makeVectorInput(runs, 2);
  EXPECT_EQ(input.numRuns(), 2u);
  // The last block of a run may be shorter than the others.
  EXPECT_EQ(input.numBlocks(0), 3u);
  EXPECT_EQ(input.numBlocks(1), 1u);
  EXPECT_EQ(input.numElementsInBlock(0, 0), 2u);
  EXPECT_EQ(input.numElementsInBlock(0, 2), 1u);
  EXPECT_EQ(input.firstElement(0, 1), 5);
  EXPECT_EQ(input.lastElement(0, 1), 7);
  EXPECT_EQ(input.lastElement(0, 2), 9);
  EXPECT_THAT(input.getBlock(0, 0), ::testing::ElementsAre(1, 3));
  EXPECT_THAT(input.getBlock(0, 2), ::testing::ElementsAre(9));
  EXPECT_THAT(input.getBlock(1, 0), ::testing::ElementsAre(2, 4));
  auto block = input.makeEmptyBlock();
  EXPECT_THAT(block, ::testing::IsEmpty());
  input.appendToBlock(block, 42);
  EXPECT_THAT(block, ::testing::ElementsAre(42));
  EXPECT_EQ(input.memorySizeOfElement(42),
            ad_utility::MemorySize::bytes(sizeof(int)));
}

// _____________________________________________________________________________
TEST(RunsInputPolicy, VectorInputEdgeCases) {
  // A run without any element simply has no block at all.
  std::vector<std::vector<int>> runs{{}, {1, 2}, {}};
  auto input = makeVectorInput(runs, 4);
  EXPECT_EQ(input.numRuns(), 3u);
  EXPECT_EQ(input.numBlocks(0), 0u);
  EXPECT_EQ(input.numBlocks(1), 1u);
  EXPECT_EQ(input.numBlocks(2), 0u);

  // The block size has to be positive.
  EXPECT_ANY_THROW(makeVectorInput(runs, 0));
  // No block may be empty.
  EXPECT_ANY_THROW((VectorInput<int>{std::vector<std::vector<std::vector<int>>>{
      {std::vector<int>{1, 2}, std::vector<int>{}}}}));
}

// _____________________________________________________________________________
TEST(RunsInputPolicy, VectorInputReadBlockReturnsACopy) {
  // `getBlock` deliberately hands out a copy, so that a chunk which moves the
  // elements out of a block cannot affect another chunk that reads the very
  // same block.
  std::vector<std::vector<std::string>> runs{{"alphaalpha", "betabeta"}};
  auto input = makeVectorInput(runs, 2);
  auto block = input.getBlock(0, 0);
  for (auto& element : block) {
    [[maybe_unused]] std::string moved = std::move(element);
  }
  EXPECT_THAT(input.getBlock(0, 0),
              ::testing::ElementsAre("alphaalpha", "betabeta"));
}

// _____________________________________________________________________________
TEST(RunsInputPolicy, LazyVectorInput) {
  std::vector<std::vector<int>> runs{{1, 3, 5, 7, 9}, {2, 4}};
  auto input = makeLazyVectorInput(runs, 2);
  // The metadata are exactly those of the underlying `VectorInput`.
  EXPECT_EQ(input.numRuns(), 2u);
  EXPECT_EQ(input.numBlocks(0), 3u);
  EXPECT_EQ(input.numElementsInBlock(0, 2), 1u);
  EXPECT_EQ(input.firstElement(0, 1), 5);
  EXPECT_EQ(input.lastElement(0, 1), 7);
  // A lazy block yields the elements of the block, and the output blocks are
  // ordinary vectors.
  EXPECT_THAT(::ranges::to_vector(input.getBlock(0, 1)),
              ::testing::ElementsAre(5, 7));
  std::vector<int> outputBlock = input.makeEmptyBlock();
  input.appendToBlock(outputBlock, 42);
  EXPECT_THAT(outputBlock, ::testing::ElementsAre(42));

  // A lazy block may be moved before its iteration has started, but not
  // afterwards, because its iterators point into the block itself.
  auto block = input.getBlock(1, 0);
  auto movedBlock = std::move(block);
  auto it = movedBlock.begin();
  EXPECT_EQ(*it, 2);
  EXPECT_ANY_THROW(auto movedAgain = std::move(movedBlock));
}
