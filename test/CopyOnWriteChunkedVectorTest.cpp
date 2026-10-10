// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Hannah Bast <bast@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gmock/gmock.h>

#include <numeric>
#include <utility>
#include <vector>

#include "util/CopyOnWriteChunkedVector.h"

using ad_utility::CopyOnWriteChunkedVector;
using Vec = CopyOnWriteChunkedVector<int, 4>;

namespace {
// Return the vector `{0, 1, ..., n - 1}`.
std::vector<int> iota(int n) {
  std::vector<int> result(n);
  std::iota(result.begin(), result.end(), 0);
  return result;
}

// Return the elements of `vec`, obtained via its chunk spans.
std::vector<int> elements(const Vec& vec) {
  std::vector<int> result;
  for (auto chunk : vec.chunkSpans()) {
    result.insert(result.end(), chunk.begin(), chunk.end());
  }
  return result;
}

// Return the sizes of the chunks of `vec`.
std::vector<size_t> chunkSizes(const Vec& vec) {
  std::vector<size_t> result;
  for (auto chunk : vec.chunkSpans()) {
    result.push_back(chunk.size());
  }
  return result;
}
}  // namespace

// Test the construction from a span of elements and the read access.
TEST(CopyOnWriteChunkedVector, constructAndRead) {
  // An empty vector has no chunks.
  Vec empty;
  EXPECT_TRUE(empty.empty());
  EXPECT_TRUE(empty.chunkSpans().empty());

  // Nine elements make two full chunks and one chunk with one element.
  auto input = iota(9);
  Vec vec{input};
  EXPECT_EQ(vec.size(), 9u);
  EXPECT_THAT(chunkSizes(vec), ::testing::ElementsAre(4, 4, 1));
  EXPECT_EQ(elements(vec), input);
  EXPECT_EQ(vec[0], 0);
  EXPECT_EQ(vec[4], 4);
  EXPECT_EQ(vec[8], 8);

  // Eight elements make exactly two full chunks.
  EXPECT_THAT(chunkSizes(Vec{iota(8)}), ::testing::ElementsAre(4, 4));
}

// Test that `push_back` and `pop_back` add and remove chunks as needed.
TEST(CopyOnWriteChunkedVector, pushBackAndPopBack) {
  // Pushing a fifth element starts a second chunk.
  Vec vec{iota(4)};
  vec.push_back(4);
  EXPECT_EQ(elements(vec), iota(5));
  EXPECT_THAT(chunkSizes(vec), ::testing::ElementsAre(4, 1));

  // Popping it again removes the second chunk, and popping everything removes
  // all chunks.
  vec.pop_back();
  EXPECT_THAT(chunkSizes(vec), ::testing::ElementsAre(4));
  for (int i = 0; i < 4; ++i) {
    vec.pop_back();
  }
  EXPECT_TRUE(vec.empty());
  EXPECT_TRUE(vec.chunkSpans().empty());

  // Popping from an empty vector is a contract violation, and so is writing
  // beyond the end.
  EXPECT_THROW(vec.pop_back(), ad_utility::Exception);
  EXPECT_THROW(vec.mutableAt(0), ad_utility::Exception);
}

// Test that a move transfers the elements and leaves an empty vector behind.
TEST(CopyOnWriteChunkedVector, move) {
  // Five elements in two chunks, moved into a new vector.
  Vec vec{iota(5)};
  Vec moved = std::move(vec);
  EXPECT_EQ(elements(moved), iota(5));
  EXPECT_TRUE(vec.empty());
  EXPECT_EQ(vec.size(), 0u);
}

// Test that a copy shares all chunks, that a mutation clones only the chunk it
// touches, and that the copy is not affected by mutations of the original.
TEST(CopyOnWriteChunkedVector, copyOnWrite) {
  // Ten elements in three chunks, and a copy of them.
  Vec original{iota(10)};
  Vec copy = original;
  auto dataOf = [](const Vec& vec, size_t chunk) {
    return vec.chunkSpans()[chunk].data();
  };
  EXPECT_EQ(dataOf(original, 0), dataOf(copy, 0));
  EXPECT_EQ(dataOf(original, 2), dataOf(copy, 2));

  // Writing an element of the second chunk clones only that chunk.
  original.mutableAt(5) = 42;
  EXPECT_EQ(dataOf(original, 0), dataOf(copy, 0));
  EXPECT_NE(dataOf(original, 1), dataOf(copy, 1));
  EXPECT_EQ(dataOf(original, 2), dataOf(copy, 2));

  // A second write to the now unshared chunk does not clone it again.
  const int* dataBefore = dataOf(original, 1);
  original.mutableAt(6) = 43;
  EXPECT_EQ(dataOf(original, 1), dataBefore);

  // Appending to and removing from the original clones the shared last chunk.
  original.push_back(10);
  original.pop_back();
  original.pop_back();
  EXPECT_NE(dataOf(original, 2), dataOf(copy, 2));

  // The copy still has the original elements.
  EXPECT_EQ(elements(copy), iota(10));
  EXPECT_THAT(elements(original),
              ::testing::ElementsAre(0, 1, 2, 3, 4, 42, 43, 7, 8));
}
