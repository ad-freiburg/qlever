// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <array>
#include <cstdint>
#include <limits>
#include <utility>
#include <vector>

#include "engine/export_prototypes/AlignedBatchBuffer.h"

using qlever::export_pipeline::AlignedBatchBuffer;

namespace {

// Returns true iff `ptr` is aligned to `alignment` bytes.
bool isAligned(const void* ptr, size_t alignment) {
  return reinterpret_cast<std::uintptr_t>(ptr) % alignment == 0;
}

// Copies the contents of `buffer` into a vector for comparison.
template <typename T>
std::vector<T> toVector(const AlignedBatchBuffer<T>& buffer) {
  auto span = buffer.span();
  return std::vector<T>(span.begin(), span.end());
}

// A 12-byte element: `sizeof` does not divide the 64-byte alignment.
struct TwelveBytes {
  std::array<uint32_t, 3> values;
};

}  // namespace

// _____________________________________________________________________________
TEST(AlignedBatchBuffer, DefaultConstructedIsEmpty) {
  AlignedBatchBuffer<uint64_t> buffer;
  EXPECT_EQ(buffer.size(), 0);
  EXPECT_EQ(buffer.capacity(), 0);
  EXPECT_EQ(buffer.data(), nullptr);
  EXPECT_TRUE(buffer.span().empty());
}

// _____________________________________________________________________________
TEST(AlignedBatchBuffer, CapacityIsRoundedUpToWholeCacheLines) {
  // 8 `uint64_t` fit into one 64-byte line.
  AlignedBatchBuffer<uint64_t> buffer{1};
  EXPECT_EQ(buffer.capacity(), 8);
  buffer.reserve(9);
  EXPECT_EQ(buffer.capacity(), 16);
  // Reserving less than the current capacity is a no-op.
  buffer.reserve(3);
  EXPECT_EQ(buffer.capacity(), 16);
  EXPECT_TRUE(
      isAligned(buffer.data(), AlignedBatchBuffer<uint64_t>::kAlignment));
}

// _____________________________________________________________________________
TEST(AlignedBatchBuffer, RoundingWorksWhenElementSizeDoesNotDivideAlignment) {
  // 64 / 12 = 5 elements per line; the capacity must be a multiple of 5 and
  // never smaller than requested.
  for (size_t requested : {1, 4, 5, 6, 11, 13}) {
    AlignedBatchBuffer<TwelveBytes> buffer{requested};
    EXPECT_GE(buffer.capacity(), requested);
    EXPECT_EQ(buffer.capacity() % 5, 0) << requested;
    EXPECT_LT(buffer.capacity() - requested, 5) << requested;
  }
}

// _____________________________________________________________________________
TEST(AlignedBatchBuffer, PushBackAndReservePreserveContents) {
  AlignedBatchBuffer<uint64_t> buffer{4};
  for (uint64_t value : {3u, 1u, 4u, 1u, 5u, 9u, 2u, 6u}) {
    buffer.push_back(value);
  }
  EXPECT_THAT(toVector(buffer), ::testing::ElementsAre(3, 1, 4, 1, 5, 9, 2, 6));

  // Growing relocates the elements into new aligned storage.
  buffer.reserve(100);
  EXPECT_TRUE(isAligned(buffer.data(), 64));
  EXPECT_THAT(toVector(buffer), ::testing::ElementsAre(3, 1, 4, 1, 5, 9, 2, 6));

  buffer.mutableSpan()[0] = 42;
  buffer[1] = 43;
  EXPECT_EQ(buffer[0], 42);
  EXPECT_EQ(std::as_const(buffer)[1], 43);

  buffer.clear();
  EXPECT_EQ(buffer.size(), 0);
  EXPECT_GE(buffer.capacity(), 100);
}

// _____________________________________________________________________________
TEST(AlignedBatchBuffer, PushBackBeyondCapacityThrows) {
  AlignedBatchBuffer<uint64_t> buffer{1};
  while (buffer.size() < buffer.capacity()) {
    buffer.push_back(0);
  }
  EXPECT_ANY_THROW(buffer.push_back(0));
}

// _____________________________________________________________________________
TEST(AlignedBatchBuffer, ReserveRejectsSizesThatWouldOverflow) {
  AlignedBatchBuffer<uint64_t> buffer;
  EXPECT_ANY_THROW(buffer.reserve(std::numeric_limits<size_t>::max()));
  EXPECT_ANY_THROW(
      buffer.reserve(std::numeric_limits<size_t>::max() / sizeof(uint64_t)));
  EXPECT_EQ(buffer.capacity(), 0);
}
