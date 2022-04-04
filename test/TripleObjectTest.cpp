// Copyright 2018, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach(joka921) <johannes.kalmbach@gmail.com>
//

#include "../src/parser/TripleObject.h"
#include <gtest/gtest.h>


TEST(TripleObject, setAndGet) {
  const char* s = "someString\"%%\\";
  auto testString = [](auto input) {
    TripleObject t{input};
    ASSERT_TRUE(t.isString());
    ASSERT_FALSE(t.isDouble());
    ASSERT_FALSE(t.isInt());
  };
}