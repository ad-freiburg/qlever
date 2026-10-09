// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <cstdint>
#include <vector>

#include "util/AlignedAllocator.h"
#include "util/GTestHelpers.h"
#include "util/Serializer/ByteBufferSerializer.h"

using ad_utility::AlignedAllocator;
using ad_utility::serialization::AlignedByteBufferReadSerializer;
using ad_utility::serialization::AlignedByteBufferWriteSerializer;

// Helper to check that a pointer is aligned to `alignment`.
static bool isAligned(const void* ptr, size_t alignment) {
  return reinterpret_cast<std::uintptr_t>(ptr) % alignment == 0;
}

// Allocating via `AlignedAllocator` with default `MinAlignment` returns a
// pointer aligned to at least `alignof(std::max_align_t)`.
TEST(AlignedAllocator, BasicAlignment) {
  AlignedAllocator<char> alloc;
  char* ptr = alloc.allocate(1);
  EXPECT_TRUE(isAligned(ptr, alignof(std::max_align_t)));
  alloc.deallocate(ptr, 1);

  char* ptr2 = alloc.allocate(100);
  EXPECT_TRUE(isAligned(ptr2, alignof(std::max_align_t)));
  alloc.deallocate(ptr2, 100);
}

// A custom `MinAlignment` larger than `alignof(std::max_align_t)` is also
// respected by allocators (such as `std::allocator`) that route through
// `::operator new` with an alignment parameter.
TEST(AlignedAllocator, CustomMinAlignment) {
  AlignedAllocator<char, std::allocator<char>, 64> alloc;
  char* ptr = alloc.allocate(128);
  EXPECT_TRUE(isAligned(ptr, 64));
  alloc.deallocate(ptr, 128);
}

// `std::vector<char, AlignedAllocator<char>>` stores data at a
// `max_align_t`-aligned address even after multiple reallocations.
TEST(AlignedAllocator, VectorUsage) {
  std::vector<char, AlignedAllocator<char>> v;
  for (char c = 'a'; c <= 'z'; ++c) {
    v.push_back(c);
  }
  EXPECT_TRUE(isAligned(v.data(), alignof(std::max_align_t)));
  EXPECT_EQ(v.size(), 26u);
  EXPECT_EQ(v[0], 'a');
  EXPECT_EQ(v[25], 'z');
}

// After multiple reallocations triggered by `push_back`, the data pointer
// continues to satisfy the alignment requirement.
TEST(AlignedAllocator, RebindWorks) {
  std::vector<char, AlignedAllocator<char>> v;
  // Force several reallocations.
  for (int i = 0; i < 1024; ++i) {
    v.push_back(static_cast<char>(i & 0xFF));
    EXPECT_TRUE(isAligned(v.data(), alignof(std::max_align_t)));
  }
}

// Two default-constructed `AlignedAllocator` instances compare equal; a copy
// also compares equal to the original.
TEST(AlignedAllocator, EqualityOperators) {
  AlignedAllocator<char> a1;
  AlignedAllocator<char> a2;
  EXPECT_EQ(a1, a2);
  EXPECT_FALSE(a1 != a2);

  auto a3 = a1;
  EXPECT_EQ(a1, a3);
}

// The `AlignedByteBufferWriteSerializer`'s internal buffer starts at an
// address aligned to `alignof(std::max_align_t)`.
TEST(AlignedAllocator, WriteSerializerStorageIsAligned) {
  AlignedByteBufferWriteSerializer writer;
  // Write some data to ensure the vector allocates its buffer.
  for (int i = 0; i < 64; ++i) {
    char c = static_cast<char>(i);
    writer.serializeBytes(&c, 1);
  }
  EXPECT_TRUE(isAligned(writer.data().data(), alignof(std::max_align_t)));
}

// The `AlignedByteBufferReadSerializer` constructor accepts a properly aligned
// buffer and throws when the buffer is not aligned to
// `alignof(std::max_align_t)`.
TEST(AlignedAllocator, ReadSerializerContractCheck) {
  // A vector produced by the aligned write serializer is always aligned —
  // the constructor must not throw.
  AlignedByteBufferWriteSerializer writer;
  char c = 'Q';
  writer.serializeBytes(&c, 1);
  auto alignedData = std::move(writer).data();
  EXPECT_NO_THROW(AlignedByteBufferReadSerializer{std::move(alignedData)});

  // Construct a span that starts one byte into a properly allocated buffer so
  // that it is guaranteed to be misaligned (alignment > 1). We use a span as
  // `Storage` to avoid copying.
  std::vector<char, AlignedAllocator<char>> buf(64, 'x');
  // If `alignof(std::max_align_t)` is 1 the misalignment test is vacuous, but
  // that never happens on real hardware.
  if constexpr (alignof(std::max_align_t) > 1) {
    ql::span<const char> misaligned{buf.data() + 1, buf.size() - 1};
    EXPECT_THROW((ad_utility::serialization::ByteBufferReadSerializerT<
                     true, ql::span<const char>>{misaligned}),
                 ad_utility::Exception);
  }
}
