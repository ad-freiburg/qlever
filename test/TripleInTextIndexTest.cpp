// Copyright 2025, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Felix Meisen (fesemeisen@outlook.de)

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "index/TripleInTextIndexFilter.h"
#include "parser/RdfParser.h"
#include "util/GTestHelpers.h"

namespace {

auto makeTurtleTripleFromStrings = [](std::string_view s, std::string_view p,
                                      std::string_view o) {
  TurtleTriple triple;
  triple.subject_ = TripleComponent{s};
  triple.predicate_ = TripleComponent{p}.getIri();
  triple.object_ = TripleComponent{o};
  return triple;
};

auto testMultipleTriples = [](const TripleInTextIndexFilter& filter,
                              const std::vector<TurtleTriple>& triplesToTest,
                              const std::vector<bool>& equality) {
  ASSERT_EQ(triplesToTest.size(), equality.size());
  for (size_t i = 0; i < triplesToTest.size(); ++i) {
    ASSERT_EQ(filter(TripleComponent{triplesToTest[i].predicate_},
                     triplesToTest[i].object_),
              equality[i]);
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
      TripleInTextIndexFilter("(abc"),
      ::testing::HasSubstr(
          R"(The regex "(abc" is not supported by QLever (which uses Google's RE2 library); the error from RE2 is:)"),
      std::runtime_error);
}

TEST(TripleInTextIndex, NoLiteralObject) {
  TripleInTextIndexFilter filter{"(?s).*"};
  TurtleTriple triple{iri("<Scientist>"), iri("<has-description>"),
                      TripleComponent{4}};
  ASSERT_FALSE(filter(TripleComponent{triple.predicate_}, triple.object_));
}

TEST(TripleInTextIndex, PartialMatch) {
  TripleInTextIndexFilter filter{"descri"};
  std::vector<bool> equality{true, true, false, false};
  testMultipleTriples(filter, testVector, equality);
}

TEST(TripleInTextIndex, Blacklist) {
  TripleInTextIndexFilter filter{"descri", false};
  std::vector<bool> equality{false, false, true, true};
  testMultipleTriples(filter, testVector, equality);
}

TEST(TripleInTextIndex, NoCaseSensitivity) {
  TripleInTextIndexFilter filter{"(?i)descri"};
  std::vector<bool> equality{true, true, false, true};
  testMultipleTriples(filter, testVector, equality);
}

}  // namespace
