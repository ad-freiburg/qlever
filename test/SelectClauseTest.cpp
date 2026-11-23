// Copyright 2022, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author:
//   2022 -     Johannes Kalmbach (kalmbach@informatik.uni-freiburg.de)

#include "./util/AllocatorTestHelpers.h"
#include "engine/sparqlExpressions/LiteralExpression.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "parser/SelectClause.h"
#include "util/ContainersWithAllocator.h"

using parsedQuery::SelectClause;

using namespace ::testing;

TEST(SelectClause, Asterisk) {
  SelectClause clause{ad_utility::testing::makeAllocator()};
  clause.addVisibleVariable(Variable{"?x"});
  clause.setAsterisk();
  clause.addVisibleVariable(Variable{"?y"});
  EXPECT_THAT(clause.getSelectedVariables(),
              ElementsAre(Variable{"?x"}, Variable{"?y"}));
  EXPECT_TRUE(clause.isAsterisk());
  EXPECT_TRUE(clause.getAliases().empty());
}

TEST(SelectClause, Variables) {
  auto allocator = ad_utility::testing::makeAllocator();
  SelectClause clause{allocator};
  clause.setSelected(
      qlm::vector<Variable>{{Variable{"?x"}, Variable{"?y"}}, allocator});
  EXPECT_THAT(clause.getSelectedVariables(),
              ElementsAre(Variable{"?x"}, Variable{"?y"}));
  EXPECT_FALSE(clause.isAsterisk());
  EXPECT_TRUE(clause.getAliases().empty());
}

TEST(SelectClause, VariablesAndAliases) {
  auto allocator = ad_utility::testing::makeAllocator();
  SelectClause clause{allocator};
  qlm::vector<SelectClause::VarOrAlias> v{
      {Variable{"?x"},
       Alias{{std::make_unique<sparqlExpression::IdExpression>(
                  Id::makeFromBool(false)),
              "false"},
             Variable{"?y"}},
       Variable{"?z"}},
      allocator};
  clause.setSelected(v);
  EXPECT_THAT(clause.getSelectedVariables(),
              ElementsAre(Variable{"?x"}, Variable{"?y"}, Variable{"?z"}));
  EXPECT_FALSE(clause.isAsterisk());
  ASSERT_EQ(clause.getAliases().size(), 1u);
  EXPECT_EQ(clause.getAliases()[0], std::get<Alias>(v[1]));
}
