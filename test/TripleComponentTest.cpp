// Copyright 2018, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach(joka921) <johannes.kalmbach@gmail.com>
//

#include <gtest/gtest.h>

#include "./IndexTestHelpers.h"
#include "./util/IdTestHelpers.h"
#include "./util/TripleComponentTestHelpers.h"
#include "parser/TripleComponent.h"

using namespace std::literals;
using namespace ad_utility::testing;
namespace {
auto I = IntId;
auto D = DoubleId;
auto lit = tripleComponentLiteral;
}  // namespace

TEST(TripleComponent, setAndGetString) {
  const char* s = "someString\"%%\\";
  auto testString = [](auto input) {
    TripleComponent t{input};
    ASSERT_TRUE(t.isString());
    ASSERT_FALSE(t.isDouble());
    ASSERT_FALSE(t.isInt());
    ASSERT_FALSE(t.isVariable());
    ASSERT_EQ(t, input);
    ASSERT_EQ(t.getString(), input);
  };
  testString(s);
  testString(std::string_view{s});
  testString(std::string{s});
}

TEST(TripleComponent, setAndGetDouble) {
  double value = 83.12;
  TripleComponent object{value};
  ASSERT_FALSE(object.isString());
  ASSERT_FALSE(object.isVariable());
  ASSERT_FALSE(object.isInt());
  ASSERT_TRUE(object.isDouble());
  ASSERT_EQ(object, value);
  ASSERT_EQ(object.getDouble(), value);
}

TEST(TripleComponent, setAndGetInt) {
  int value = -42;
  TripleComponent object{value};
  ASSERT_FALSE(object.isString());
  ASSERT_FALSE(object.isDouble());
  ASSERT_FALSE(object.isVariable());
  ASSERT_TRUE(object.isInt());
  ASSERT_EQ(object, value);
  ASSERT_EQ(object.getInt(), value);
}

TEST(TripleComponent, setAndGetVariable) {
  TripleComponent tc{Variable{"?x"}};
  ASSERT_TRUE(tc.isVariable());
  ASSERT_FALSE(tc.isString());
  ASSERT_FALSE(tc.isDouble());
  ASSERT_FALSE(tc.isInt());
  ASSERT_EQ(tc, Variable{"?x"});
  ASSERT_EQ(tc.getVariable(), Variable{"?x"});
}

TEST(TripleComponent, assignmentOperator) {
  TripleComponent object;
  object = -12.435;
  ASSERT_TRUE(object.isDouble());
  ASSERT_EQ(object, -12.435);
  object = 483;
  ASSERT_TRUE(object.isInt());
  ASSERT_EQ(object, 483);

  {
    auto literal = lit("\"a\"", "@en");
    object = literal;
    EXPECT_EQ(object, literal);
    EXPECT_TRUE(object.isLiteral());
    EXPECT_EQ(object.getLiteral(), literal);
    EXPECT_EQ(std::as_const(object).getLiteral(), literal);
  }

  auto testString = [&object](auto input) {
    object = input;
    ASSERT_TRUE(object.isString());
    ASSERT_EQ(input, object);
  };
  testString("<someIri>");
  testString("aPlainString"s);

  object = Variable{"?alpha"};
  ASSERT_TRUE(object.isVariable());
  ASSERT_EQ(object, Variable{"?alpha"});
}

TEST(TripleComponent, toRdfLiteral) {
  TripleComponent literal{lit("\"aTypedLiteral\"", "^^<someType>")};
  EXPECT_EQ(literal.toRdfLiteral(), "\"aTypedLiteral\"^^<someType>");
  std::vector<std::string> strings{"plainString", "<IRI>"};
  for (const auto& s : strings) {
    ASSERT_EQ(s, TripleComponent{s}.toRdfLiteral());
  }

  TripleComponent object{42};
  ASSERT_EQ(object.toRdfLiteral(),
            R"("42"^^<http://www.w3.org/2001/XMLSchema#int>)");

  object = -43.3;
  ASSERT_EQ(object.toRdfLiteral(),
            R"("-43.3"^^<http://www.w3.org/2001/XMLSchema#decimal>)");
  object = DateOrLargeYear{123456, DateOrLargeYear::Type::Year};
  ASSERT_EQ(object.toRdfLiteral(),
            R"("123456"^^<http://www.w3.org/2001/XMLSchema#gYear>)");
}

TEST(TripleComponent, toValueIdIfNotString) {
  TripleComponent tc{42};
  ASSERT_EQ(tc.toValueIdIfNotString().value(), I(42));
  tc = 131.4;
  ASSERT_EQ(tc.toValueIdIfNotString().value(), D(131.4));

  DateOrLargeYear date{123456, DateOrLargeYear::Type::Year};
  tc = date;
  ASSERT_EQ(tc.toValueIdIfNotString().value(), Id::makeFromDate(date));
  tc = "<x>";
  ASSERT_FALSE(tc.toValueIdIfNotString().has_value());
  tc = lit("\"a\"");
  ASSERT_FALSE(tc.toValueIdIfNotString().has_value());

  tc = Variable{"?x"};
  // Note: we cannot simply write `ASSERT_THROW(tc.toValueIdIfNotString(),
  // Exception)` because `toValueIdIfNotString` is marked `nodiscard` and we
  // would get a compiler warning.
  auto f = [&]() { [[maybe_unused]] auto x = tc.toValueIdIfNotString(); };
  ASSERT_THROW(f(), ad_utility::Exception);
}

TEST(TripleComponent, toValueId) {
  auto qec = getQec("<x> <y> <z>. <x> <y> \"alpha\".");
  const auto& vocab = qec->getIndex().getVocab();

  TripleComponent tc = "<x>";
  Id id = I(10293478);
  qec->getIndex().getId("<x>", &id);
  ASSERT_EQ(tc.toValueId(vocab).value(), id);

  tc = lit("\"alpha\"");
  qec->getIndex().getId("\"alpha\"", &id);
  EXPECT_EQ(tc.toValueId(vocab).value(), id);

  tc = "<notexisting>";
  ASSERT_FALSE(tc.toValueId(vocab).has_value());
  tc = 42;

  ASSERT_EQ(tc.toValueIdIfNotString().value(), I(42));

  tc = HAS_PATTERN_PREDICATE;
  ASSERT_EQ(tc.toValueId(vocab).value(),
            qlever::specialIds.at(HAS_PATTERN_PREDICATE));
}

TEST(TripleComponent, settingVariablesAsStringsIsIllegal) {
  ASSERT_THROW(TripleComponent("?x"sv), ad_utility::Exception);
  ASSERT_THROW(TripleComponent("?x"s), ad_utility::Exception);
  TripleComponent tc{42};
  auto assignString = [&]() { tc = "?y"s; };
  auto assignStringView = [&]() { tc = "?y"sv; };

  ASSERT_THROW(assignString(), ad_utility::Exception);
  ASSERT_THROW(assignStringView(), ad_utility::Exception);
}

TEST(TripleComponent, settingLiteralsAsStringsIsIllegal) {
  ASSERT_THROW(TripleComponent("\"x\""sv), ad_utility::Exception);
  ASSERT_THROW(TripleComponent("\'x\'"s), ad_utility::Exception);
  TripleComponent tc{42};
  auto assignString = [&]() { tc = "'y'"s; };
  auto assignStringView = [&]() { tc = "\"y\""sv; };

  ASSERT_THROW(assignString(), ad_utility::Exception);
  ASSERT_THROW(assignStringView(), ad_utility::Exception);
}

TEST(TripleComponent, invalidDatatypeForLiteral) {
  // No datatype.
  ASSERT_NO_THROW(lit("\"alpha\""));
  // A datatype.
  ASSERT_NO_THROW(lit("\"alpha\"", "^^<someType>"));
  // A language tag.
  ASSERT_NO_THROW(lit("\"alpha\"", "@fr-ca"));

  // Something that is invalid because it is none of the above
  ASSERT_ANY_THROW(lit("\"alpha\"", "fr-ca"));
}
