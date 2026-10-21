// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Pascal Keßler <kesslerp@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gtest/gtest.h>

#include <array>
#include <cstddef>
#include <string_view>
#include <utility>

#include "global/Datatype.h"

namespace {
using enum Datatype;

// All datatypes, sorted into the ones that encode their value directly in an
// `Id` (the trivial ones) and the ones that point to an external resource.
constexpr std::array trivialDatatypes{Undefined, Bool, Int,
                                      Double,    Date, GeoPoint};
constexpr std::array nonTrivialDatatypes{
    VocabIndex,     LocalVocabIndex, SecondaryVocabIndex, TextRecordIndex,
    WordVocabIndex, BlankNodeIndex,  EncodedVal};
}  // namespace

// Test that exactly the datatypes that encode their value directly are
// trivial, and that both lists above cover all datatypes.
TEST(DatatypeTest, isDatatypeTrivial) {
  EXPECT_EQ(trivialDatatypes.size() + nonTrivialDatatypes.size(),
            static_cast<size_t>(MaxValue) + 1);
  for (const Datatype type : trivialDatatypes) {
    EXPECT_TRUE(isDatatypeTrivial(type));
  }
  for (const Datatype type : nonTrivialDatatypes) {
    EXPECT_FALSE(isDatatypeTrivial(type));
  }
}

// Test the string of each datatype. Note that `EncodedVal` is printed as
// `EncodedIri`.
TEST(DatatypeTest, toString) {
  constexpr std::array<std::pair<Datatype, std::string_view>, 13> expected{{
      {Undefined, "Undefined"},
      {Bool, "Bool"},
      {Int, "Int"},
      {Double, "Double"},
      {VocabIndex, "VocabIndex"},
      {LocalVocabIndex, "LocalVocabIndex"},
      {SecondaryVocabIndex, "SecondaryVocabIndex"},
      {TextRecordIndex, "TextRecordIndex"},
      {Date, "Date"},
      {GeoPoint, "GeoPoint"},
      {WordVocabIndex, "WordVocabIndex"},
      {BlankNodeIndex, "BlankNodeIndex"},
      {EncodedVal, "EncodedIri"},
  }};
  EXPECT_EQ(expected.size(), static_cast<size_t>(MaxValue) + 1);
  for (const auto& [type, name] : expected) {
    EXPECT_EQ(toString(type), name);
  }
}

// Test that `toString` throws for a value that is not a datatype.
TEST(DatatypeTest, invalidDatatypeEnumValue) {
  EXPECT_ANY_THROW(toString(static_cast<Datatype>(2345)));
}
