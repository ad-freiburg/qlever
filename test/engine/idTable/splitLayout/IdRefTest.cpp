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
#include <type_traits>
#include <utility>
#include <vector>

#include "IdColumnTestHelpers.h"
#include "backports/algorithm.h"
#include "backports/concepts.h"
#include "backports/type_traits.h"
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
    IdRef mutableRef{&payload, &datatype};
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
  IdRef ref{&payload, &datatype};

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

// Test the restricted reference (what `auto x = column[i];` deduces): It has
// the read API, can be assigned to only if it is an rvalue, and converts to the
// full `IdRef`, which writes through for lvalues, too.
TEST(IdRefTest, restrictedReference) {
  using Restricted = IdRef::Base;
  using ConstRestricted = ConstIdRef::Base;

  // Writing is only possible for rvalues.
  static_assert(std::is_assignable_v<Restricted, Id>);
  static_assert(!std::is_assignable_v<Restricted&, Id>);
  static_assert(!std::is_assignable_v<const Restricted&, Id>);
  static_assert(std::is_assignable_v<Restricted, Restricted>);
  static_assert(!std::is_assignable_v<Restricted&, Restricted>);
  static_assert(!std::is_assignable_v<Restricted&, Restricted&>);
  static_assert(!std::is_assignable_v<ConstRestricted, Id>);

  // `IdRef` is the restricted reference plus lvalue assignment.
  static_assert(std::is_base_of_v<Restricted, IdRef>);
  static_assert(std::is_base_of_v<ConstRestricted, ConstIdRef>);
  static_assert(std::is_assignable_v<IdRef&, Id>);
  static_assert(!std::is_assignable_v<ConstIdRef&, Id>);
  static_assert(sizeof(IdRef) == sizeof(Restricted));

  for (const Id id : sampleIds()) {
    auto [datatype, payload] = getBitsCompat(id);
    const Restricted restricted{&payload, &datatype};

    // The read API is the same as for the full reference, and the restricted
    // and the full reference can be compared with each other.
    EXPECT_EQ(static_cast<Id>(restricted), id);
    EXPECT_EQ(restricted.getDatatype(), id.getDatatype());
    EXPECT_EQ(restricted.getBits(), id.getBits());
    EXPECT_EQ(restricted.getPayloadBits(), payload);
    EXPECT_EQ(restricted.isUndefined(), id.isUndefined());
    EXPECT_TRUE(restricted == id);
    EXPECT_TRUE(id == restricted);
    const IdRef full{&payload, &datatype};
    const ConstIdRef constFull{&payload, &datatype};
    EXPECT_TRUE(restricted == full);
    EXPECT_TRUE(restricted == constFull);
    EXPECT_FALSE(restricted < full);
    EXPECT_TRUE(restricted <= constFull);

    // Assigning to an rvalue writes through to the referenced slot.
    const Id second = Id::makeFromInt(7);
    Restricted{&payload, &datatype} = second;
    EXPECT_EQ(static_cast<Id>(restricted), second);
    auto [otherDatatype, otherPayload] = getBitsCompat(Id::makeFromInt(8));
    Restricted{&payload, &datatype} = Restricted{&otherPayload, &otherDatatype};
    EXPECT_EQ(static_cast<Id>(restricted), Id::makeFromInt(8));
    // Copying the referenced value does not rebind the pointers.
    otherPayload = Id::makeFromInt(9).getPayloadBits();
    EXPECT_EQ(static_cast<Id>(restricted), Id::makeFromInt(8));

    // The full reference can be created from the restricted one and writes
    // through to the same slot, also when assigned to as an lvalue.
    IdRef fromRestricted = restricted;
    const Id third = Id::makeFromInt(10);
    fromRestricted = third;
    EXPECT_EQ(static_cast<Id>(restricted), third);
    EXPECT_EQ(payload, third.getPayloadBits());
    EXPECT_EQ(datatype, third.getDatatypeBits());
  }
}

// Test that the mutable restricted reference converts to the const one, like
// `Id&` to `const Id&`, but not the other way round. Then the two have a common
// reference and are totally ordered with each other, which for example a sort
// with a const pivot row needs.
TEST(IdRefTest, mutableRestrictedReferenceConvertsToConst) {
  using Restricted = IdRef::Base;
  using ConstRestricted = ConstIdRef::Base;
  static_assert(std::is_convertible_v<Restricted, ConstRestricted>);
  static_assert(!std::is_convertible_v<ConstRestricted, Restricted>);
  static_assert(std::is_same_v<ql::remove_cvref_t<ql::common_reference_t<
                                   const Restricted&, const ConstRestricted&>>,
                               ConstRestricted>);
  static_assert(
      ql::concepts::totally_ordered_with<Restricted, ConstRestricted>);

  for (const Id id : sampleIds()) {
    auto [datatype, payload] = getBitsCompat(id);
    const Restricted restricted{&payload, &datatype};
    const ConstRestricted converted = restricted;
    EXPECT_EQ(static_cast<Id>(converted), id);

    // The converted reference refers to the same slot, it sees a later write.
    const Id other = Id::makeFromInt(7);
    Restricted{&payload, &datatype} = other;
    EXPECT_EQ(static_cast<Id>(converted), other);

    // The mutable and the const reference can be compared with each other.
    EXPECT_TRUE(restricted == converted);
    EXPECT_FALSE(ql::ranges::less{}(restricted, converted));
    EXPECT_FALSE(ql::ranges::less{}(converted, restricted));
  }
}

// Test that an `IdRef` can be built from a `SplitLayoutId&` and a `ConstIdRef`
// also from a `const SplitLayoutId&`, but neither from a temporary, which would
// dangle. This holds in both builds, because the constructor is declared in
// both.
TEST(IdRefTest, conversionFromSplitLayoutId) {
  static_assert(std::is_convertible_v<SplitLayoutId&, IdRef>);
  static_assert(std::is_convertible_v<SplitLayoutId&, ConstIdRef>);
  static_assert(std::is_convertible_v<const SplitLayoutId&, ConstIdRef>);
  static_assert(!std::is_constructible_v<IdRef, const SplitLayoutId&>);
  static_assert(!std::is_constructible_v<IdRef, SplitLayoutId&&>);
  static_assert(!std::is_constructible_v<ConstIdRef, SplitLayoutId&&>);
  static_assert(!std::is_constructible_v<ConstIdRef, const SplitLayoutId&&>);

  // This is the requirement of `std::indirectly_unary_invocable` that needs the
  // conversion: a range algorithm like `ql::ranges::for_each` only accepts a
  // function with an `IdRef` parameter if it can also be called with an lvalue
  // of the value type of the range.
  auto setToTrue = [](IdRef id) { id = Id::makeFromBool(true); };
  static_assert(ql::concepts::invocable<decltype(setToTrue)&, SplitLayoutId&>);
}

// The following tests read and write through an `IdRef` that refers to an `Id`
// itself. An `IdRef` always reads and writes the global `Id`, which is only the
// `SplitLayoutId` with `USE_SPLIT_LAYOUT`.
#ifdef QLEVER_USE_SPLIT_LAYOUT
// Test that an `IdRef` that refers to an `Id` reads the current value of the
// `Id`, and that assigning to it changes exactly this `Id`, with all its bits.
TEST(IdRefTest, referenceToAnId) {
  const auto ids = sampleIds();
  for (const Id original : ids) {
    for (const Id other : ids) {
      Id id = original;
      Id neighbour = Id::makeFromInt(-1);
      IdRef ref = id;
      const ConstIdRef constRef = std::as_const(id);
      EXPECT_EQ(getBitsCompat(ref), getBitsCompat(original));
      EXPECT_EQ(getBitsCompat(constRef), getBitsCompat(original));

      // Assigning to the reference changes the `Id`, and only this `Id`.
      ref = other;
      EXPECT_EQ(getBitsCompat(id), getBitsCompat(other));
      EXPECT_EQ(neighbour, Id::makeFromInt(-1));

      // The references see a later change of the `Id` (they are no copies).
      id = original;
      EXPECT_EQ(getBitsCompat(ref), getBitsCompat(original));
      EXPECT_EQ(getBitsCompat(constRef), getBitsCompat(original));
    }
  }
}

// Test that a range algorithm accepts a function with an `IdRef` parameter for
// a `std::vector<Id>` and changes its `Id`s in place.
TEST(IdRefTest, rangeAlgorithmOnAVectorWithIdRef) {
  std::vector<Id> vector = sampleIds();
  ql::ranges::for_each(vector, [](IdRef id) { id = Id::makeFromBool(true); });
  EXPECT_EQ(vector, std::vector<Id>(vector.size(), Id::makeFromBool(true)));
}
#endif
