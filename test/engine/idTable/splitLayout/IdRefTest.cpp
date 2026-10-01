// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Pascal Keßler <kesslerp@informatik.uni-freiburg.de>, UFR
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
using testHelpers::sampleIds;

// _____________________________________________________________________________
TEST(IdRefTest, roundtripsAndMirrorsApi) {
  for (Id id : sampleIds()) {
    auto [datatype_, payload_] = getBitsCompat(id);

    ConstIdRef constRef{&payload_, &datatype_};
    EXPECT_EQ(static_cast<Id>(constRef), id);
    EXPECT_EQ(constRef.getDatatype(), id.getDatatype());
    EXPECT_EQ(constRef.getBits(), getBitsCompat(id));
    EXPECT_EQ(constRef.getPayloadBits(), payload_);
    EXPECT_EQ(constRef.isUndefined(), id.isUndefined());
    EXPECT_EQ(constRef == id, true);
    EXPECT_EQ(id == constRef, true);

    IdRef mutableRef{&payload_, &datatype_};
    EXPECT_EQ(static_cast<Id>(mutableRef), id);
    auto other = Id::makeFromInt(999);
    mutableRef = other;
    EXPECT_EQ(static_cast<Id>(mutableRef), other);
    EXPECT_EQ(payload_, getBitsCompat(other).payload_);
    EXPECT_EQ(datatype_, getBitsCompat(other).datatype_);
  }
}

// _____________________________________________________________________________
TEST(IdRefTest, assignmentWritesThroughToTheReferencedSlot) {
  auto [datatype_, payload_] = getBitsCompat(Id::makeFromInt(1));
  const IdRef ref{&payload_, &datatype_};

  ref = Id::makeFromInt(2);
  EXPECT_EQ(payload_, getBitsCompat(Id::makeFromInt(2)).payload_);
  EXPECT_EQ(datatype_, getBitsCompat(Id::makeFromInt(2)).datatype_);

  auto [datatype_2, payload_2] = getBitsCompat(Id::makeFromInt(3));
  const IdRef other{&payload_2, &datatype_2};
  ref = other;
  EXPECT_EQ(static_cast<Id>(ref), Id::makeFromInt(3));
  EXPECT_EQ(payload_, getBitsCompat(Id::makeFromInt(3)).payload_);
}

// _____________________________________________________________________________
TEST(IdRefTest, swapExchangesTheReferencedValuesNotThePointers) {
  auto [datatype_A, payload_A] = getBitsCompat(Id::makeFromInt(10));
  auto [datatype_B, payload_B] = getBitsCompat(Id::makeFromInt(20));
  const IdRef a{&payload_A, &datatype_A};
  const IdRef b{&payload_B, &datatype_B};

  swap(a, b);
  EXPECT_EQ(static_cast<Id>(a), Id::makeFromInt(20));
  EXPECT_EQ(static_cast<Id>(b), Id::makeFromInt(10));
  EXPECT_EQ(payload_A, getBitsCompat(Id::makeFromInt(20)).payload_);
  EXPECT_EQ(payload_B, getBitsCompat(Id::makeFromInt(10)).payload_);
}

// _____________________________________________________________________________
TEST(IdRefTest, comparisonOperatorsAgreeWithId) {
  auto bitsSmall = getBitsCompat(Id::makeFromInt(1));
  auto [datatypeLarge, payloadLarge] = getBitsCompat(Id::makeFromInt(2));
  const ConstIdRef small{&bitsSmall.payload_, &bitsSmall.datatype_};
  const ConstIdRef large{&payloadLarge, &datatypeLarge};

  EXPECT_TRUE(small < Id::makeFromInt(2));
  EXPECT_TRUE(Id::makeFromInt(1) < large);
  EXPECT_TRUE(small <= Id::makeFromInt(1));
  EXPECT_TRUE(large > Id::makeFromInt(1));
  EXPECT_TRUE(large >= Id::makeFromInt(2));
  EXPECT_TRUE(small != large);
  EXPECT_FALSE(small == large);

  EXPECT_TRUE(small < large);
  EXPECT_TRUE(small <= large);
  EXPECT_TRUE(large > small);
  EXPECT_TRUE(large >= small);
  EXPECT_TRUE(small != large);
  EXPECT_FALSE(small == large);

  auto bitsSmallCopy = bitsSmall;
  const IdRef mutableSmall{&bitsSmallCopy.payload_, &bitsSmallCopy.datatype_};
  EXPECT_TRUE(small == mutableSmall);
  EXPECT_TRUE(mutableSmall == small);
}

// _____________________________________________________________________________
TEST(IdRefTest, orderingIsDatatypeMajorLikeId) {
  auto [datatypeBool, payloadBool] = getBitsCompat(Id::makeFromBool(true));
  auto [datatypeInt, payloadInt] = getBitsCompat(Id::makeFromInt(-1000));
  const ConstIdRef boolRef{&payloadBool, &datatypeBool};
  const ConstIdRef intRef{&payloadInt, &datatypeInt};

  EXPECT_EQ(Id::makeFromBool(true) < Id::makeFromInt(-1000), boolRef < intRef);
}

// _____________________________________________________________________________
TEST(IdRefTest, streamingMatchesId) {
  for (Id id : sampleIds()) {
    auto [datatype_, payload_] = getBitsCompat(id);
    const ConstIdRef ref{&payload_, &datatype_};

    std::ostringstream refStream;
    refStream << ref;
    std::ostringstream idStream;
    idStream << id;
    EXPECT_EQ(refStream.str(), idStream.str());
  }
}

// _____________________________________________________________________________
TEST(IdRefTest, getBitsCompatAndIdFromBitsCompatRoundtrip) {
  for (Id id : sampleIds()) {
    EXPECT_EQ(idFromBitsCompat(getBitsCompat(id)), id);
  }
}
