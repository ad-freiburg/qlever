// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gmock/gmock.h>

#include "./util/GTestHelpers.h"
#include "util/RegexSet.h"

using ad_utility::RegexSet;
using ::testing::ElementsAre;
using ::testing::HasSubstr;

// _____________________________________________________________________________
// A default-constructed `RegexSet` contains no regexes and never matches.
TEST(RegexSet, emptySetMatchesNothing) {
  RegexSet regexes;
  EXPECT_TRUE(regexes.empty());
  EXPECT_EQ(regexes.size(), 0);
  EXPECT_THAT(regexes.regexesAsStrings(), ElementsAre());
  EXPECT_FALSE(regexes.matchesAny(""));
  EXPECT_FALSE(regexes.matchesAny("anything"));

  // The same holds for an explicitly empty list of regexes.
  RegexSet explicitlyEmpty{{}, "for the test"};
  EXPECT_TRUE(explicitlyEmpty.empty());
  EXPECT_FALSE(explicitlyEmpty.matchesAny("anything"));
}

// _____________________________________________________________________________
// The regexes are matched as a *full* match, and a word matches if at least one
// of them matches it.
TEST(RegexSet, matchesAnyIsAFullMatchOfAnyOfTheRegexes) {
  RegexSet regexes{{"<http://ex/bn_.*>", "\"lit.*\""}, "for the test"};
  EXPECT_FALSE(regexes.empty());
  EXPECT_EQ(regexes.size(), 2);
  EXPECT_THAT(regexes.regexesAsStrings(),
              ElementsAre("<http://ex/bn_.*>", "\"lit.*\""));

  // Each of the regexes is applied.
  EXPECT_TRUE(regexes.matchesAny("<http://ex/bn_1>"));
  EXPECT_TRUE(regexes.matchesAny("\"literal\""));

  // Words that none of the regexes matches completely.
  EXPECT_FALSE(regexes.matchesAny("<http://ex/other>"));
  EXPECT_FALSE(regexes.matchesAny(""));
  // A match of only a prefix or only a suffix is not enough.
  EXPECT_FALSE(regexes.matchesAny("<http://ex/bn_1>trailing"));
  EXPECT_FALSE(regexes.matchesAny("leading<http://ex/bn_1>"));
}

// _____________________________________________________________________________
// A `RegexSet` can be copied and assigned; the copy behaves like the original.
TEST(RegexSet, isCopyable) {
  RegexSet regexes{{"a+"}, "for the test"};
  RegexSet copy = regexes;
  EXPECT_THAT(copy.regexesAsStrings(), ElementsAre("a+"));
  EXPECT_TRUE(copy.matchesAny("aaa"));
  EXPECT_FALSE(copy.matchesAny("aab"));

  RegexSet assigned;
  assigned = copy;
  EXPECT_TRUE(assigned.matchesAny("aaa"));
}

// _____________________________________________________________________________
// A regex that is not a valid regular expression is reported with a
// user-readable message that names the offending regex, the given
// `description`, and the error of the RE2 library.
TEST(RegexSet, invalidRegexIsReported) {
  AD_EXPECT_THROW_WITH_MESSAGE_AND_TYPE(
      RegexSet({"valid", "invalid("}, "for excluding vocabulary entries"),
      ::testing::AllOf(HasSubstr("The regex \"invalid(\""),
                       HasSubstr("for excluding vocabulary entries"),
                       HasSubstr("is not a valid regular expression")),
      std::runtime_error);
}
