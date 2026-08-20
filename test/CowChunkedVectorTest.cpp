// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Hannah Bast <bast@cs.uni-freiburg.de>, UFR

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <numeric>

#include "util/CowChunkedVector.h"

using ad_utility::CowChunkedVector;

namespace {

// Materialize the contents of a `CowChunkedVector` via its chunk spans.
template <typename T, size_t ChunkSize>
std::vector<T> materialize(const CowChunkedVector<T, ChunkSize>& vec) {
  std::vector<T> result;
  for (const auto& chunk : vec.chunkSpans()) {
    result.insert(result.end(), chunk.begin(), chunk.end());
  }
  return result;
}

// Return the vector `{0, 1, ..., numElements - 1}`.
std::vector<int> iota(int numElements) {
  std::vector<int> result(numElements);
  std::iota(result.begin(), result.end(), 0);
  return result;
}

// _____________________________________________________________________________
TEST(CowChunkedVector, constructionAndAccess) {
  CowChunkedVector<int, 4> empty;
  EXPECT_EQ(empty.size(), 0u);
  EXPECT_TRUE(empty.empty());
  EXPECT_TRUE(empty.chunkSpans().empty());

  // Sizes around the chunk boundary.
  for (int numElements : {1, 3, 4, 5, 8, 9}) {
    auto input = iota(numElements);
    CowChunkedVector<int, 4> vec{input};
    EXPECT_EQ(vec.size(), input.size());
    EXPECT_EQ(materialize(vec), input);
    for (size_t i = 0; i < input.size(); ++i) {
      EXPECT_EQ(vec[i], input[i]);
    }
    EXPECT_EQ(vec.chunkSpans().size(), (numElements + 3) / 4);
  }
}

// _____________________________________________________________________________
TEST(CowChunkedVector, pushBackAndPopBack) {
  CowChunkedVector<int, 4> vec;
  for (int i = 0; i < 9; ++i) {
    vec.push_back(i);
    EXPECT_EQ(vec.size(), static_cast<size_t>(i + 1));
  }
  EXPECT_EQ(materialize(vec), iota(9));

  for (int i = 8; i >= 0; --i) {
    vec.pop_back();
    EXPECT_EQ(materialize(vec), iota(i));
  }
  EXPECT_TRUE(vec.empty());
  EXPECT_TRUE(vec.chunkSpans().empty());
}

// _____________________________________________________________________________
TEST(CowChunkedVector, copiesAreUnaffectedByMutations) {
  CowChunkedVector<int, 4> vec{iota(10)};
  auto copy = vec;

  vec.mutableAt(2) = 42;
  vec.mutableAt(9) = 43;
  vec.push_back(44);

  EXPECT_EQ(materialize(copy), iota(10));
  EXPECT_EQ(vec.size(), 11u);
  EXPECT_EQ(vec[2], 42);
  EXPECT_EQ(vec[9], 43);
  EXPECT_EQ(vec[10], 44);

  // Mutations of the copy do not affect the original either.
  copy.mutableAt(0) = 7;
  copy.pop_back();
  EXPECT_EQ(vec[0], 0);
  EXPECT_EQ(vec.size(), 11u);
  EXPECT_EQ(copy.size(), 9u);
}

// _____________________________________________________________________________
TEST(CowChunkedVector, chunksAreSharedBetweenCopies) {
  CowChunkedVector<int, 4> vec{iota(12)};
  auto copy = vec;

  // The chunk spans of an untouched copy alias the original storage.
  EXPECT_EQ(vec.chunkSpans()[0].data(), copy.chunkSpans()[0].data());

  // A mutation clones only the touched chunk.
  vec.mutableAt(5) = 42;
  EXPECT_EQ(vec.chunkSpans()[0].data(), copy.chunkSpans()[0].data());
  EXPECT_NE(vec.chunkSpans()[1].data(), copy.chunkSpans()[1].data());
  EXPECT_EQ(vec.chunkSpans()[2].data(), copy.chunkSpans()[2].data());

  // Mutating via the sole owner does not clone.
  auto* dataBefore = vec.chunkSpans()[1].data();
  vec.mutableAt(6) = 43;
  EXPECT_EQ(vec.chunkSpans()[1].data(), dataBefore);
}

// _____________________________________________________________________________
TEST(CowChunkedVector, contractChecks) {
  CowChunkedVector<int, 4> vec{iota(4)};
  EXPECT_THROW(vec.mutableAt(4), ad_utility::Exception);
  CowChunkedVector<int, 4> empty;
  EXPECT_THROW(empty.pop_back(), ad_utility::Exception);
}

}  // namespace
