// Copyright 2022, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Autor:
//   2022 -     Johannes Kalmbach (kalmbach@informatik.uni-freiburg.de)

#include "engine/sparqlExpressions/LiteralExpression.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "parser/SelectClause.h"

using parsedQuery::SelectClause;

using namespace ::testing;

TEST(SelectClause, Asterisk) {
  SelectClause clause;
  clause.addVisibleVariable(Variable{"?x"});
  clause.setAsterisk();
  clause.addVisibleVariable(Variable{"?y"});
  EXPECT_THAT(clause.getSelectedVariables(),
              ElementsAre(Variable{"?x"}, Variable{"?y"}));
  EXPECT_TRUE(clause.isAsterisk());
  EXPECT_THAT(clause.getAliases(), IsEmpty());
}

TEST(SelectClause, Variables) {
  SelectClause clause;
  clause.setSelected(std::vector{Variable{"?x"}, Variable{"?y"}});
  EXPECT_THAT(clause.getSelectedVariables(),
              ElementsAre(Variable{"?x"}, Variable{"?y"}));
  EXPECT_FALSE(clause.isAsterisk());
  EXPECT_THAT(clause.getAliases(), IsEmpty());
}

TEST(SelectClause, VariablesAndAliases) {
  SelectClause clause;
  std::vector<SelectClause::VarOrAlias> v{
      Variable{"?x"},
      Alias{
          {std::make_unique<sparqlExpression::BoolExpression>(false), "false"},
          Variable{"?y"}},
      Variable{"?z"}};
  clause.setSelected(v);
  EXPECT_THAT(clause.getSelectedVariables(),
              ElementsAre(Variable{"?x"}, Variable{"?y"}, Variable{"?z"}));
  EXPECT_FALSE(clause.isAsterisk());
  EXPECT_THAT(clause.getAliases(), ElementsAre(std::get<Alias>(v[1])));
}
