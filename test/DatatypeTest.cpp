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
    WordVocabIndex, BlankNodeIndex,  EncodedVal,          TripleTermIndex};
}  // namespace

// Test that exactly the datatypes that encode their value directly are
// trivial, and that both lists above cover all datatypes.
TEST(DatatypeTest, isDatatypeTrivial) {
  EXPECT_EQ(trivialDatatypes.size() + nonTrivialDatatypes.size(),
            qlever::enumSize<Datatype>);
  for (const Datatype type : trivialDatatypes) {
    EXPECT_TRUE(isDatatypeTrivial(type));
  }
  for (const Datatype type : nonTrivialDatatypes) {
    EXPECT_FALSE(isDatatypeTrivial(type));
  }
}

// Test that `isListOfAllDatatypesInOrder` accepts exactly the complete list of
// datatypes in the order of their numeric values.
TEST(DatatypeTest, isListOfAllDatatypesInOrder) {
  constexpr auto allDatatypes = std::to_array(
      {Undefined, Bool, Int, Double, VocabIndex, LocalVocabIndex,
       SecondaryVocabIndex, TextRecordIndex, Date, GeoPoint, WordVocabIndex,
       BlankNodeIndex, EncodedVal, TripleTermIndex});
  static_assert(isListOfAllDatatypesInOrder(allDatatypes));
  EXPECT_TRUE(isListOfAllDatatypesInOrder(allDatatypes));

  // A datatype is missing.
  constexpr std::array tooShort{Undefined, Bool, Int};
  static_assert(!isListOfAllDatatypesInOrder(tooShort));
  EXPECT_FALSE(isListOfAllDatatypesInOrder(tooShort));

  // Two datatypes are swapped.
  auto swapped = allDatatypes;
  std::swap(swapped[1], swapped[2]);
  EXPECT_FALSE(isListOfAllDatatypesInOrder(swapped));
}

// Test the string of each datatype. Note that `EncodedVal` is printed as
// `EncodedIri`.
TEST(DatatypeTest, toString) {
  constexpr std::array<std::pair<Datatype, std::string_view>,
                       qlever::enumSize<Datatype>>
      expected{{
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
          {TripleTermIndex, "TripleTermIndex"},
      }};
  for (const auto& [type, name] : expected) {
    EXPECT_EQ(toString(type), name);
  }
}

// Test that `toString` throws for a value that is not a datatype.
TEST(DatatypeTest, invalidDatatypeEnumValue) {
  EXPECT_ANY_THROW(toString(static_cast<Datatype>(qlever::enumSize<Datatype>)));
}

// Test that appending a datatype preserves the numeric tag assignments used by
// existing index files and binary query results.
TEST(DatatypeTest, ExistingDatatypeTagsRemainStable) {
  constexpr auto existing = std::to_array<Datatype>(
      {Datatype::Undefined, Datatype::Bool, Datatype::Int, Datatype::Double,
       Datatype::VocabIndex, Datatype::LocalVocabIndex,
       Datatype::SecondaryVocabIndex, Datatype::TextRecordIndex, Datatype::Date,
       Datatype::GeoPoint, Datatype::WordVocabIndex, Datatype::BlankNodeIndex,
       Datatype::EncodedVal});
  for (size_t tag = 0; tag < existing.size(); ++tag) {
    EXPECT_EQ(static_cast<size_t>(existing[tag]), tag);
  }
  EXPECT_EQ(static_cast<size_t>(Datatype::TripleTermIndex), existing.size());
}
