// Copyright 2018, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach(joka921) <johannes.kalmbach@gmail.com>
//

#include <gtest/gtest.h>

#include "../src/parser/TripleObject.h"

using namespace std::literals;

TEST(TripleObject, SetAndGetString) {
  const char* s = "someString\"%%\\";
  auto testString = [](auto input) {
    TripleObject t{input};
    ASSERT_TRUE(t.isString());
    ASSERT_FALSE(t.isDouble());
    ASSERT_FALSE(t.isInt());
    ASSERT_EQ(t, input);
    ASSERT_EQ(t.getString(), input);
  };
  testString(s);
  testString(std::string_view{s});
  testString(std::string{s});
}

TEST(TripleObject, SetAndGetDouble) {
  TripleObject object{83.12};
  ASSERT_FALSE(object.isString());
  ASSERT_TRUE(object.isDouble());
  ASSERT_FALSE(object.isInt());
  ASSERT_EQ(object, 83.12);
  ASSERT_EQ(object.getDouble(), 83.12);
}

TEST(TripleObject, SetAndGetInt) {
  TripleObject object{-42};
  ASSERT_FALSE(object.isString());
  ASSERT_FALSE(object.isDouble());
  ASSERT_TRUE(object.isInt());
  ASSERT_EQ(object, -42);
  ASSERT_EQ(object.getInt(), -42);
}

TEST(TripleObject, AssignmentOperator) {
  TripleObject object;
  object = -12.435;
  ASSERT_TRUE(object.isDouble());
  ASSERT_EQ(object, -12.435);
  object = 483;
  ASSERT_TRUE(object.isInt());
  ASSERT_EQ(object, 483);

  auto testString = [&object](auto input) {
    object = input;
    ASSERT_TRUE(object.isString());
    ASSERT_EQ(input, object);
  };
  testString("<someIri>");
  testString(std::string_view{R"("aLiteral")"});
  testString("aPlainString"s);
}

TEST(TripleObject, ToRdf) {
  std::vector<std::string> strings{"plainString", "<IRI>",
                                   R"("aTypedLiteral"^^xsd::integer)"};
  for (const auto& s : strings) {
    ASSERT_EQ(s, TripleObject{s}.toRdf());
  }

  TripleObject object{42};
  ASSERT_EQ(object.toRdf(),
            R"("42"^^<http://www.w3.org/2001/XMLSchema#integer>)");

  object = -43.3;
  ASSERT_EQ(object.toRdf(),
            R"("-43.3"^^<http://www.w3.org/2001/XMLSchema#double>)");
}
