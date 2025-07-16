//  Copyright 2022, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include <gmock/gmock.h>

#include "../util/GTestHelpers.h"
#include "rdfTypes/Variable.h"
#include "util/HashSet.h"

// _____________________________________________________________________________
TEST(Variable, legalAndIllegalNames) {
  if constexpr (!ad_utility::areExpensiveChecksEnabled) {
    GTEST_SKIP()
        << "legality of variable names is only checked with expensive checks";
  }
  EXPECT_NO_THROW(Variable("?x", true));
  EXPECT_NO_THROW(Variable("$x", true));
  EXPECT_NO_THROW(Variable("?ql_matching_word_thür", true));

  // No leading ? or $
  auto matcher = ::testing::HasSubstr("not a valid SPARQL variable");
  AD_EXPECT_THROW_WITH_MESSAGE(Variable("x", true), matcher);
  AD_EXPECT_THROW_WITH_MESSAGE(Variable("?x spaceInVar", true), matcher);
}

// _____________________________________________________________________________
TEST(Variable, DollarToQuestionMark) {
  Variable x{"?x"};
  Variable x2{"$x"};
  EXPECT_EQ(x.name(), "?x");
  EXPECT_EQ(x2.name(), "?x");
}

// _____________________________________________________________________________
TEST(Variable, ScoreAndMatchVariablesUnicode) {
  ad_utility::HashSet<Variable> vars;
  auto f = [&](Variable var) { vars.insert(std::move(var)); };

  // Test that the following variables all have valid names (otherwise the
  // constructor of `Variable` would throw), and that they are all unique.
  EXPECT_NO_THROW(f(Variable("?x").getWordScoreVariable("\U0001F600", false)));
  EXPECT_NO_THROW(f(Variable("?x").getWordScoreVariable("\U0001F600*", true)));
  EXPECT_NO_THROW(f(Variable("?x").getWordScoreVariable("äpfel", false)));
  EXPECT_NO_THROW(f(Variable("?x").getEntityScoreVariable("\U0001F600")));
  EXPECT_NO_THROW(f(Variable("?x").getEntityScoreVariable("äpfel")));
  EXPECT_NO_THROW(f(Variable("?x").getMatchingWordVariable("äpfel")));
  EXPECT_NO_THROW(f(Variable("?x").getMatchingWordVariable("\U0001F600")));

  // Expect that all the created variables are unique.
  EXPECT_EQ(vars.size(), 7);

  // Invalid UTF-8 will throw an exception.
  AD_EXPECT_THROW_WITH_MESSAGE(Variable("?x").getMatchingWordVariable("\255"),
                               ::testing::HasSubstr("Invalid UTF-8"));
}
