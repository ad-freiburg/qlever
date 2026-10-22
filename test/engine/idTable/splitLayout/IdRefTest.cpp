// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Pascal Keßler <kesslerp@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gtest/gtest.h>

#include <sstream>

#include "IdColumnTestHelpers.h"
#include "engine/idTable/splitLayout/IdRef.h"

using namespace columnBasedIdTable::splitLayout;
using testHelpers::getBitsCompat;
using testHelpers::sampleIds;

// Test that putting the datatype and the payload of an `Id` together again
// yields the same `Id`.
TEST(IdRefTest, datatypeAndPayloadRoundtrip) {
  for (const Id id : sampleIds()) {
    auto [datatype, payload] = getBitsCompat(id);
    EXPECT_EQ(Id::fromBits(datatype, payload), id);
  }
}

// Test that a `ConstIdRef` converts to the referenced `Id` and mirrors its
// read API, and that an `IdRef` additionally writes through.
TEST(IdRefTest, conversionAndReadApi) {
  for (const Id id : sampleIds()) {
    auto [datatype, payload] = getBitsCompat(id);

    // The const proxy reads the referenced id.
    const ConstIdRef constRef{&payload, &datatype};
    EXPECT_EQ(static_cast<Id>(constRef), id);
    EXPECT_EQ(constRef.getDatatype(), id.getDatatype());
    EXPECT_EQ(constRef.getBits(), id.getBits());
    EXPECT_EQ(constRef.getPayloadBits(), payload);
    EXPECT_EQ(constRef.isUndefined(), id.isUndefined());
    EXPECT_TRUE(constRef == id);
    EXPECT_TRUE(id == constRef);

    // Assigning to the mutable proxy changes the referenced slot.
    const IdRef mutableRef{&payload, &datatype};
    EXPECT_EQ(static_cast<Id>(mutableRef), id);
    const auto other = Id::makeFromInt(999);
    mutableRef = other;
    EXPECT_EQ(static_cast<Id>(mutableRef), other);
    EXPECT_EQ(payload, other.getPayloadBits());
    EXPECT_EQ(datatype, other.getDatatypeBits());
  }
}

// Test that assigning an `Id` or another `IdRef` writes through to the
// referenced slot.
TEST(IdRefTest, assignmentWritesThroughToTheReferencedSlot) {
  auto [datatype, payload] = getBitsCompat(Id::makeFromInt(1));
  const IdRef ref{&payload, &datatype};

  // Assign an `Id`.
  ref = Id::makeFromInt(2);
  EXPECT_EQ(payload, Id::makeFromInt(2).getPayloadBits());
  EXPECT_EQ(datatype, Id::makeFromInt(2).getDatatypeBits());

  // Assign another `IdRef`, which copies the referenced value.
  auto [datatype2, payload2] = getBitsCompat(Id::makeFromInt(3));
  const IdRef other{&payload2, &datatype2};
  ref = other;
  EXPECT_EQ(static_cast<Id>(ref), Id::makeFromInt(3));
  EXPECT_EQ(payload, Id::makeFromInt(3).getPayloadBits());
}

// Test that `swap` exchanges the referenced values, not the pointers.
TEST(IdRefTest, swapExchangesTheReferencedValues) {
  auto [datatypeA, payloadA] = getBitsCompat(Id::makeFromInt(10));
  auto [datatypeB, payloadB] = getBitsCompat(Id::makeFromInt(20));
  const IdRef a{&payloadA, &datatypeA};
  const IdRef b{&payloadB, &datatypeB};
  swap(a, b);
  EXPECT_EQ(static_cast<Id>(a), Id::makeFromInt(20));
  EXPECT_EQ(static_cast<Id>(b), Id::makeFromInt(10));
  EXPECT_EQ(payloadA, Id::makeFromInt(20).getPayloadBits());
  EXPECT_EQ(payloadB, Id::makeFromInt(10).getPayloadBits());
}

// Test the comparison operators between proxies and `Id`s, between two
// proxies, and that the ordering is datatype-major like for `Id`.
TEST(IdRefTest, comparisonOperators) {
  const auto idSmall = Id::makeFromInt(1);
  auto [datatypeSmall, payloadSmall] = getBitsCompat(idSmall);
  auto [datatypeLarge, payloadLarge] = getBitsCompat(Id::makeFromInt(2));
  const ConstIdRef small{&payloadSmall, &datatypeSmall};
  const ConstIdRef large{&payloadLarge, &datatypeLarge};

  // Proxy versus `Id`, in both orders.
  EXPECT_TRUE(small < Id::makeFromInt(2));
  EXPECT_TRUE(Id::makeFromInt(1) < large);
  EXPECT_TRUE(small <= Id::makeFromInt(1));
  EXPECT_TRUE(large > Id::makeFromInt(1));
  EXPECT_TRUE(large >= Id::makeFromInt(2));
  EXPECT_TRUE(small != Id::makeFromInt(2));
  EXPECT_FALSE(Id::makeFromInt(2) == small);

  // Proxy versus proxy, also a const versus a mutable one.
  EXPECT_TRUE(small < large);
  EXPECT_TRUE(small <= large);
  EXPECT_TRUE(large > small);
  EXPECT_TRUE(large >= small);
  EXPECT_TRUE(small != large);
  EXPECT_FALSE(small == large);
  auto [datatypeSmallCopy, payloadSmallCopy] = getBitsCompat(idSmall);
  const IdRef mutableSmall{&payloadSmallCopy, &datatypeSmallCopy};
  EXPECT_TRUE(small == mutableSmall);
  EXPECT_TRUE(mutableSmall == small);

  // A `Bool` is smaller than any `Int`, like for `Id`.
  auto [datatypeBool, payloadBool] = getBitsCompat(Id::makeFromBool(true));
  auto [datatypeInt, payloadInt] = getBitsCompat(Id::makeFromInt(-1000));
  const ConstIdRef boolRef{&payloadBool, &datatypeBool};
  const ConstIdRef intRef{&payloadInt, &datatypeInt};
  EXPECT_TRUE(Id::makeFromBool(true) < Id::makeFromInt(-1000));
  EXPECT_TRUE(boolRef < intRef);
}

// Test that streaming a proxy prints the same as streaming the `Id`.
TEST(IdRefTest, streamingMatchesId) {
  for (const Id id : sampleIds()) {
    auto [datatype, payload] = getBitsCompat(id);
    const ConstIdRef ref{&payload, &datatype};
    std::ostringstream refStream;
    refStream << ref;
    std::ostringstream idStream;
    idStream << id;
    EXPECT_EQ(refStream.str(), idStream.str());
  }
}
