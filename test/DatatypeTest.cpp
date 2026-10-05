// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Pascal Keßler <kesslerp@informatik.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "global/DataType.h"
#include "gtest/gtest.h"

TEST(DatatypeTest, isDatatypeTrivial) {
  using enum Datatype;
  constexpr std::array trivialDatatypes{Undefined, Bool, Int,
                                        Double,    Date, GeoPoint};
  for (size_t i = 0; i <= static_cast<size_t>(MaxValue); ++i) {
    const auto type = static_cast<Datatype>(i);
    ASSERT_EQ(ad_utility::contains(trivialDatatypes, type),
              isDatatypeTrivial(type));
  }
}

TEST(DatatypeTest, toString) {
  using enum Datatype;
  for (size_t i = 0; i <= static_cast<size_t>(MaxValue); ++i) {
    const auto type = static_cast<Datatype>(i);
    ASSERT_NO_THROW(toString(type));
  }
}

TEST(DatatypeTest, InvalidDatatypeEnumValue) {
  ASSERT_ANY_THROW(toString(static_cast<Datatype>(2345)));
}
