// Copyright 2022, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author:
//   2022 -     Johannes Kalmbach (kalmbach@informatik.uni-freiburg.de)

#include "engine/sparqlExpressions/LiteralExpression.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "parser/SelectClause.h"

using qlever::Alias;
using qlever::parsedQuery::SelectClause;

using namespace ::testing;

TEST(SelectClause, Asterisk) {
  SelectClause clause;
  clause.addVisibleVariable(qlever::Variable{"?x"});
  clause.setAsterisk();
  clause.addVisibleVariable(qlever::Variable{"?y"});
  EXPECT_THAT(clause.getSelectedVariables(),
              ElementsAre(qlever::Variable{"?x"}, qlever::Variable{"?y"}));
  EXPECT_TRUE(clause.isAsterisk());
  EXPECT_THAT(clause.getAliases(), IsEmpty());
}

TEST(SelectClause, Variables) {
  SelectClause clause;
  clause.setSelected(
      std::vector{qlever::Variable{"?x"}, qlever::Variable{"?y"}});
  EXPECT_THAT(clause.getSelectedVariables(),
              ElementsAre(qlever::Variable{"?x"}, qlever::Variable{"?y"}));
  EXPECT_FALSE(clause.isAsterisk());
  EXPECT_THAT(clause.getAliases(), IsEmpty());
}

TEST(SelectClause, VariablesAndAliases) {
  SelectClause clause;
  std::vector<SelectClause::VarOrAlias> v{
      qlever::Variable{"?x"},
      Alias{{std::make_unique<sparqlExpression::IdExpression>(
                 qlever::Id::makeFromBool(false)),
             "false"},
            qlever::Variable{"?y"}},
      qlever::Variable{"?z"}};
  clause.setSelected(v);
  EXPECT_THAT(clause.getSelectedVariables(),
              ElementsAre(qlever::Variable{"?x"}, qlever::Variable{"?y"},
                          qlever::Variable{"?z"}));
  EXPECT_FALSE(clause.isAsterisk());
  EXPECT_THAT(clause.getAliases(), ElementsAre(std::get<Alias>(v[1])));
}
