// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gmock/gmock.h>

#include <stdexcept>
#include <type_traits>
#include <vector>

#include "util/GTestHelpers.h"
#include "util/OwnedOrViewedVector.h"
#include "util/Serializer/ByteBufferSerializer.h"

namespace {
using ad_utility::OwnedOrViewedVector;
using V = OwnedOrViewedVector<int>;
using ::testing::ElementsAre;
using ::testing::IsEmpty;

static_assert(!std::is_copy_constructible_v<V>);
static_assert(!std::is_copy_assignable_v<V>);
static_assert(std::is_nothrow_move_constructible_v<V>);
static_assert(std::is_nothrow_move_assignable_v<V>);

// Return `true` iff the `view()` of `v` points into `expected`.
bool viewsInto(const V& v, const std::vector<int>& expected) {
  return v.view().data() == expected.data() &&
         v.view().size() == expected.size();
}
}  // namespace

// _____________________________________________________________________________
TEST(OwnedOrViewedVector, DefaultConstructed) {
  V v;
  EXPECT_TRUE(v.isOwned());
  EXPECT_TRUE(v.empty());
  EXPECT_EQ(v.size(), 0);
  EXPECT_THAT(v.view(), IsEmpty());
}

// _____________________________________________________________________________
TEST(OwnedOrViewedVector, OwnedAndViewedAccess) {
  V owned{std::vector{1, 2, 3}};
  EXPECT_TRUE(owned.isOwned());
  EXPECT_THAT(owned, ElementsAre(1, 2, 3));
  EXPECT_EQ(owned.size(), 3);
  EXPECT_FALSE(owned.empty());
  EXPECT_EQ(owned[1], 2);
  EXPECT_EQ(owned.back(), 3);

  std::vector<int> external{4, 5};
  V viewed{ql::span<const int>{external}};
  EXPECT_FALSE(viewed.isOwned());
  EXPECT_TRUE(viewsInto(viewed, external));
  EXPECT_THAT(viewed, ElementsAre(4, 5));
}

// _____________________________________________________________________________
TEST(OwnedOrViewedVector, Modify) {
  V v{std::vector{1, 2}};
  // The return value of the function is passed through.
  auto result = v.modify([](std::vector<int>& elements) {
    elements.push_back(3);
    // Force a reallocation, so that a stale span would be detected.
    elements.reserve(1000);
    return 42;
  });
  EXPECT_EQ(result, 42);
  EXPECT_THAT(v, ElementsAre(1, 2, 3));

  // The span is also updated when the function throws.
  EXPECT_ANY_THROW(v.modify([](std::vector<int>& elements) {
    elements.push_back(4);
    elements.reserve(10000);
    throw std::runtime_error{"failure"};
  }));
  EXPECT_THAT(v, ElementsAre(1, 2, 3, 4));

  // A view cannot be modified.
  std::vector<int> external{4, 5};
  V viewed{ql::span<const int>{external}};
  AD_EXPECT_THROW_WITH_MESSAGE(
      viewed.modify([](std::vector<int>&) {}),
      ::testing::HasSubstr("non-owning view cannot be modified"));
  EXPECT_THAT(viewed, ElementsAre(4, 5));
}

// _____________________________________________________________________________
TEST(OwnedOrViewedVector, Move) {
  // Moving an owning object keeps the elements and leaves an empty, owning
  // object behind.
  V a{std::vector{1, 2, 3}};
  V b{std::move(a)};
  EXPECT_TRUE(b.isOwned());
  EXPECT_THAT(b, ElementsAre(1, 2, 3));
  EXPECT_TRUE(a.isOwned());
  EXPECT_THAT(a, IsEmpty());

  V c;
  c = std::move(b);
  EXPECT_THAT(c, ElementsAre(1, 2, 3));
  EXPECT_THAT(b, IsEmpty());

  // Self-move-assignment is a no-op.
  auto& cRef = c;
  c = std::move(cRef);
  EXPECT_THAT(c, ElementsAre(1, 2, 3));

  // Moving a view keeps viewing the same memory.
  std::vector<int> external{4, 5};
  V viewed{ql::span<const int>{external}};
  V movedView{std::move(viewed)};
  EXPECT_FALSE(movedView.isOwned());
  EXPECT_TRUE(viewsInto(movedView, external));
  EXPECT_TRUE(viewed.isOwned());
  EXPECT_THAT(viewed, IsEmpty());
}

// _____________________________________________________________________________
TEST(OwnedOrViewedVector, Clone) {
  // A clone of an owning object owns a copy of the elements.
  V owned{std::vector{1, 2, 3}};
  V ownedClone = owned.clone();
  EXPECT_TRUE(ownedClone.isOwned());
  EXPECT_THAT(ownedClone, ElementsAre(1, 2, 3));
  EXPECT_NE(ownedClone.view().data(), owned.view().data());

  // A clone of a view owns its elements, which are independent of the viewed
  // memory.
  std::vector<int> external{4, 5};
  V viewed{ql::span<const int>{external}};
  V viewedClone = viewed.clone();
  EXPECT_TRUE(viewedClone.isOwned());
  EXPECT_FALSE(viewsInto(viewedClone, external));
  external[0] = 42;
  EXPECT_THAT(viewedClone, ElementsAre(4, 5));

  // A clone of an empty object is empty.
  EXPECT_THAT(V{}.clone(), IsEmpty());
}

// _____________________________________________________________________________
TEST(OwnedOrViewedVector, Serialization) {
  using namespace ad_utility::serialization;
  // Regular serialization, the result owns its elements, also if a view was
  // written.
  std::vector<int> external{4, 5, 6};
  {
    ByteBufferWriteSerializer writer;
    writer << V{ql::span<const int>{external}};
    ByteBufferReadSerializer reader{std::move(writer).data()};
    V v;
    reader >> v;
    EXPECT_TRUE(v.isOwned());
    EXPECT_THAT(v, ElementsAre(4, 5, 6));
  }

  // The format is the same as the one of a `std::vector`.
  {
    ByteBufferWriteSerializer writer;
    writer << V{std::vector{1, 2}};
    ByteBufferReadSerializer reader{std::move(writer).data()};
    std::vector<int> v;
    reader >> v;
    EXPECT_THAT(v, ElementsAre(1, 2));
  }

  // The serialized bytes are exactly those of a `std::vector`, also with the
  // padding of the aligned serialization, no matter whether the elements are
  // owned or viewed. Write a leading `char`, so that padding is required.
  {
    auto serialize = [](const auto& elements) {
      AlignedByteBufferWriteSerializer writer;
      writer << char{'x'};
      writer << elements;
      return std::move(writer).data();
    };
    std::vector<int> elements{1, 2, 3};
    auto expected = serialize(elements);
    EXPECT_EQ(serialize(V{std::vector{1, 2, 3}}), expected);
    EXPECT_EQ(serialize(V{ql::span<const int>{elements}}), expected);
  }

  // Zero-copy deserialization yields a view into the buffer of the
  // serializer.
  {
    AlignedByteBufferWriteSerializer writer;
    writer << V{std::vector{7, 8}};
    AlignedByteBufferReadSerializer reader{std::move(writer).data()};
    auto v = V::fromZeroCopyDeserializer(reader);
    EXPECT_FALSE(v.isOwned());
    EXPECT_THAT(v, ElementsAre(7, 8));
  }
}
