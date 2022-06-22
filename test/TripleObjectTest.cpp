// Copyright 2018, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach(joka921) <johannes.kalmbach@gmail.com>
//

#include <gtest/gtest.h>

#include "../src/parser/TripleComponent.h"

using namespace std::literals;

TEST(TripleObject, SetAndGetString) {
  const char* s = "someString\"%%\\";
  auto testString = [](auto input) {
    TripleComponent t{input};
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
  double value = 83.12;
  TripleComponent object{value};
  ASSERT_FALSE(object.isString());
  ASSERT_TRUE(object.isDouble());
  ASSERT_FALSE(object.isInt());
  ASSERT_EQ(object, value);
  ASSERT_EQ(object.getDouble(), value);
}

TEST(TripleObject, SetAndGetInt) {
  int value = -42;
  TripleComponent object{value};
  ASSERT_FALSE(object.isString());
  ASSERT_FALSE(object.isDouble());
  ASSERT_TRUE(object.isInt());
  ASSERT_EQ(object, value);
  ASSERT_EQ(object.getInt(), value);
}

TEST(TripleObject, AssignmentOperator) {
  TripleComponent object;
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

TEST(TripleObject, ToRdfLiteral) {
  std::vector<std::string> strings{"plainString", "<IRI>",
                                   R"("aTypedLiteral"^^xsd::integer)"};
  for (const auto& s : strings) {
    ASSERT_EQ(s, TripleComponent{s}.toRdfLiteral());
  }

  TripleComponent object{42};
  ASSERT_EQ(object.toRdfLiteral(),
            R"("42"^^<http://www.w3.org/2001/XMLSchema#integer>)");

  object = -43.3;
  ASSERT_EQ(object.toRdfLiteral(),
            R"("-43.3"^^<http://www.w3.org/2001/XMLSchema#double>)");
}
