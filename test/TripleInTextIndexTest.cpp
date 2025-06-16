// Copyright 2025, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Felix Meisen (fesemeisen@outlook.de)

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "index/TripleInTextIndex.h"
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

auto testMultipleTriples = [](const TripleInTextIndex& filter,
                              const std::vector<TurtleTriple>& triplesToTest,
                              const std::vector<bool>& equality) {
  ASSERT_EQ(triplesToTest.size(), equality.size());
  for (size_t i = 0; i < triplesToTest.size(); ++i) {
    ASSERT_EQ(filter(triplesToTest[i]), equality[i]);
  }
};

std::vector<TurtleTriple> testVector = {
    makeTurtleTripleFromStrings("<Scientist>", "<has-description>", "\"Test\""),
    makeTurtleTripleFromStrings("<Book>", "<describes>", "\"Stack of paper\""),
    makeTurtleTripleFromStrings("<Rope>", "<descending>", "\"R P O E\""),
    makeTurtleTripleFromStrings("<Uppercase>", "<Describes>",
                                "\"Big letter\"")};

TEST(TripleInTextIndex, FaultyRegex) {
  AD_EXPECT_THROW_WITH_MESSAGE_AND_TYPE(
      TripleInTextIndex("(abc"),
      ::testing::HasSubstr(
          R"(The regex "(abc" is not supported by QLever (which uses Google's RE2 library); the error from RE2 is:)"),
      std::runtime_error);
}

TEST(TripleInTextIndex, NoLiteralObject) {
  TripleInTextIndex filter{"*"};
  TurtleTriple triple =
      makeTurtleTripleFromStrings("<Scientist>", "<is-a>", "<Job>");
  ASSERT_FALSE(filter(triple));
}

TEST(TripleInTextIndex, PartialMatch) {
  TripleInTextIndex filter{"descri"};
  std::vector<bool> equality{true, true, false, false};
  testMultipleTriples(filter, testVector, equality);
}

TEST(TripleInTextIndex, Blacklist) {
  TripleInTextIndex filter{"descri", false};
  std::vector<bool> equality{false, false, true, true};
  testMultipleTriples(filter, testVector, equality);
}

TEST(TripleInTextIndex, NoCaseSensitivity) {
  TripleInTextIndex filter{"(?i)descri"};
  std::vector<bool> equality{true, true, false, true};
  testMultipleTriples(filter, testVector, equality);
}

}  // namespace
