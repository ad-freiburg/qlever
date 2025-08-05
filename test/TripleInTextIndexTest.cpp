// Copyright 2025, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Felix Meisen (fesemeisen@outlook.de)

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "index/TextIndexLiteralFilter.h"
#include "parser/RdfParser.h"
#include "util/GTestHelpers.h"

namespace {

using EqualityVector = std::vector<std::tuple<bool, bool, bool>>;

auto testMultipleTriples = [](TextIndexLiteralFilter& filter,
                              const std::vector<TurtleTriple>& triplesToTest,
                              const EqualityVector& equality) {
  ASSERT_EQ(triplesToTest.size(), equality.size());
  for (size_t i = 0; i < triplesToTest.size(); ++i) {
    auto [sInTextIndex, pInTextIndex, oInTextIndex] =
        filter.computeInTextIndexMap(triplesToTest[i].subject_,
                                     triplesToTest[i].predicate_,
                                     triplesToTest[i].object_);
    ASSERT_EQ(sInTextIndex, std::get<0>(equality[i]));
    ASSERT_EQ(pInTextIndex, std::get<1>(equality[i]));
    ASSERT_EQ(oInTextIndex, std::get<2>(equality[i]));
  }
};

auto iri = [](std::string_view s) {
  return TripleComponent::Iri::fromIriref(s);
};

auto literal = [](std::string_view s) {
  return TripleComponent::Literal::fromStringRepresentation(std::string{s});
};

std::vector<TurtleTriple> testVector = {
    TurtleTriple{iri("<Scientist>"), iri("<has-description>"),
                 literal("\"Test\"")},
    TurtleTriple{iri("<Book>"), iri("<describes>"),
                 literal("\"Stack of paper\"")},
    TurtleTriple{iri("<Rope>"), iri("<descending>"), literal("\"R P O E\"")},
    TurtleTriple{iri("<Uppercase>"), iri("<Describes>"),
                 literal("\"Big letter\"")}};

TEST(TripleInTextIndex, FaultyRegex) {
  AD_EXPECT_THROW_WITH_MESSAGE_AND_TYPE(
      TextIndexLiteralFilter("(abc", false, false),
      ::testing::HasSubstr(
          R"(The regex supposed to filter predicates for which the objects are stored in the text index was "(abc". This is not supported by QLever (which uses Google's RE2 library); the error from RE2 is:)"),
      std::runtime_error);
}

TEST(TripleInTextIndex, NoLiteralObject) {
  TextIndexLiteralFilter filter{"(?s).*", true, false};
  TurtleTriple triple{iri("<Scientist>"), iri("<has-description>"),
                      TripleComponent{4}};
  testMultipleTriples(filter, {triple}, {std::make_tuple(false, false, false)});
}

TEST(TripleInTextIndex, PartialMatch) {
  TextIndexLiteralFilter filter{"descri", true, false};
  EqualityVector equality{{false, false, true},
                          {false, false, true},
                          {false, false, false},
                          {false, false, false}};
  testMultipleTriples(filter, testVector, equality);
}

TEST(TripleInTextIndex, Blacklist) {
  TextIndexLiteralFilter filter{"descri", false, false};
  EqualityVector equality{{false, false, false},
                          {false, false, false},
                          {false, false, true},
                          {false, false, true}};
  testMultipleTriples(filter, testVector, equality);
}

TEST(TripleInTextIndex, NoCaseSensitivity) {
  TextIndexLiteralFilter filter{"(?i)descri", true, false};
  EqualityVector equality{{false, false, true},
                          {false, false, true},
                          {false, false, false},
                          {false, false, true}};
  testMultipleTriples(filter, testVector, equality);
}

}  // namespace
