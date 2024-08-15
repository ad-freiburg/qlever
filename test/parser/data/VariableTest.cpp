//  Copyright 2022, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include <gmock/gmock.h>

#include "../../util/GTestHelpers.h"
#include "parser/data/Variable.h"

// _____________________________________________________________________________
TEST(Variable, legalAndIllegalNames) {
  EXPECT_NO_THROW(Variable("?x"));
  EXPECT_NO_THROW(Variable("$x"));
  EXPECT_NO_THROW(Variable("?ql_matching_word_thür"));

  // No leading ? or $
  auto matcher = ::testing::HasSubstr("not a valid SPARQL variable");
  AD_EXPECT_THROW_WITH_MESSAGE(Variable("x"), matcher);
  AD_EXPECT_THROW_WITH_MESSAGE(Variable("?x spaceInVar"), matcher);
}

// _____________________________________________________________________________
TEST(Variable, DollarToQuestionMark) {
  Variable x{"?x"};
  Variable x2{"$x"};
  EXPECT_EQ(x.name(), "?x");
  EXPECT_EQ(x2.name(), "?x");
}
