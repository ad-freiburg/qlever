// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR

// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.
#include <gtest/gtest.h>

#include "./SparqlExpressionTestHelpers.h"
#include "./util/TripleComponentTestHelpers.h"
#include "engine/sparqlExpressions/AggregateExpression.h"
#include "engine/sparqlExpressions/BlankNodeExpression.h"
#include "engine/sparqlExpressions/CountStarExpression.h"
#include "engine/sparqlExpressions/ExistsExpression.h"
#include "engine/sparqlExpressions/GroupConcatExpression.h"
#include "engine/sparqlExpressions/LiteralExpression.h"
#include "engine/sparqlExpressions/NaryExpression.h"
#include "engine/sparqlExpressions/NowDatetimeExpression.h"
#include "engine/sparqlExpressions/RandomExpression.h"
#include "engine/sparqlExpressions/RegexExpression.h"
#include "engine/sparqlExpressions/RelationalExpressions.h"
#include "engine/sparqlExpressions/SampleExpression.h"
#include "engine/sparqlExpressions/SparqlExpression.h"
#include "engine/sparqlExpressions/SparqlExpressionPimpl.h"
#include "engine/sparqlExpressions/StdevExpression.h"
#include "engine/sparqlExpressions/UuidExpressions.h"

namespace {

using namespace sparqlExpression;
using ad_utility::testing::iri;

}  // namespace

// _____________________________________________________________________________
TEST(SparqlExpressionMemberFunctions, isResultAlwaysDefined) {
  using namespace ::testing;

  // Setup VariableToColumnMap with different variable statuses
  VariableToColumnMap varColMap;
  Variable alwaysDefinedVar{"?alwaysDefined"};
  Variable possiblyUndefVar{"?possiblyUndef"};
  Variable unboundVar{"?unbound"};

  varColMap[alwaysDefinedVar] = makeAlwaysDefinedColumn(0);
  varColMap[possiblyUndefVar] = makePossiblyUndefinedColumn(1);
  // unboundVar is intentionally not in the map

  // Test LiteralExpression with variables
  {
    auto alwaysDefExpr = std::make_unique<VariableExpression>(alwaysDefinedVar);
    EXPECT_TRUE(alwaysDefExpr->isResultAlwaysDefined(varColMap));

    auto possiblyUndefExpr =
        std::make_unique<VariableExpression>(possiblyUndefVar);
    EXPECT_FALSE(possiblyUndefExpr->isResultAlwaysDefined(varColMap));

    auto unboundExpr = std::make_unique<VariableExpression>(unboundVar);
    EXPECT_FALSE(unboundExpr->isResultAlwaysDefined(varColMap));
  }

  // Test LiteralExpression with ValueId
  {
    auto definedIdExpr = std::make_unique<IdExpression>(Id::makeFromInt(42));
    EXPECT_TRUE(definedIdExpr->isResultAlwaysDefined(varColMap));

    auto undefinedIdExpr = std::make_unique<IdExpression>(Id::makeUndefined());
    EXPECT_FALSE(undefinedIdExpr->isResultAlwaysDefined(varColMap));
  }

  // Test LiteralExpression with Literal and Iri (always defined)
  {
    auto literalExpr = std::make_unique<StringLiteralExpression>(
        ad_utility::testing::tripleComponentLiteral("test"));
    EXPECT_TRUE(literalExpr->isResultAlwaysDefined(varColMap));

    auto iriExpr = std::make_unique<IriExpression>(iri("<http://example.org>"));
    EXPECT_TRUE(iriExpr->isResultAlwaysDefined(varColMap));
  }

  // Test LiteralExpression with vector of IDs (currently always undefined
  // pessimistically).
  {
    auto vecExpr =
        std::make_unique<VectorIdExpression>(VectorWithMemoryLimit<ValueId>(
            0U, ad_utility::testing::makeAllocator()));
    EXPECT_FALSE(vecExpr->isResultAlwaysDefined(varColMap));
  }

  // Test CoalesceExpression
  {
    // COALESCE with one always-defined child -> always defined
    auto coalesce1 = makeCoalesceExpressionVariadic(
        std::make_unique<VariableExpression>(possiblyUndefVar),
        std::make_unique<VariableExpression>(alwaysDefinedVar),
        std::make_unique<IdExpression>(Id::makeUndefined()));
    EXPECT_TRUE(coalesce1->isResultAlwaysDefined(varColMap));

    // COALESCE with no always-defined children -> not always defined
    auto coalesce2 = makeCoalesceExpressionVariadic(
        std::make_unique<VariableExpression>(possiblyUndefVar),
        std::make_unique<VariableExpression>(unboundVar),
        std::make_unique<IdExpression>(Id::makeUndefined()));
    EXPECT_FALSE(coalesce2->isResultAlwaysDefined(varColMap));

    // COALESCE with all always-defined children -> always defined
    auto coalesce3 = makeCoalesceExpressionVariadic(
        std::make_unique<IdExpression>(Id::makeFromInt(1)),
        std::make_unique<IdExpression>(Id::makeFromInt(2)),
        std::make_unique<VariableExpression>(alwaysDefinedVar));
    EXPECT_TRUE(coalesce3->isResultAlwaysDefined(varColMap));
  }

  // Test IfExpression - general case
  {
    // Both branches always defined -> always defined
    auto ifExpr1 =
        makeIfExpression(std::make_unique<IdExpression>(Id::makeFromBool(true)),
                         std::make_unique<IdExpression>(Id::makeFromInt(1)),
                         std::make_unique<IdExpression>(Id::makeFromInt(2)));
    EXPECT_TRUE(ifExpr1->isResultAlwaysDefined(varColMap));

    // Then-branch possibly undefined -> not always defined
    auto ifExpr2 =
        makeIfExpression(std::make_unique<IdExpression>(Id::makeFromBool(true)),
                         std::make_unique<VariableExpression>(possiblyUndefVar),
                         std::make_unique<IdExpression>(Id::makeFromInt(2)));
    EXPECT_FALSE(ifExpr2->isResultAlwaysDefined(varColMap));

    // Else-branch possibly undefined -> not always defined
    auto ifExpr3 = makeIfExpression(
        std::make_unique<IdExpression>(Id::makeFromBool(true)),
        std::make_unique<IdExpression>(Id::makeFromInt(1)),
        std::make_unique<VariableExpression>(possiblyUndefVar));
    EXPECT_FALSE(ifExpr3->isResultAlwaysDefined(varColMap));
  }

  // Test IfExpression - special case: IF(BOUND(?x), ?x, elseExpr)
  {
    // IF(BOUND(?possiblyUndef), ?possiblyUndef, ?alwaysDefined)
    // Result is always defined iff else-branch (?alwaysDefined) is always
    // defined
    auto ifExprSpecial1 = makeIfExpression(
        makeBoundExpression(
            std::make_unique<VariableExpression>(possiblyUndefVar)),
        std::make_unique<VariableExpression>(possiblyUndefVar),
        std::make_unique<VariableExpression>(alwaysDefinedVar));
    EXPECT_TRUE(ifExprSpecial1->isResultAlwaysDefined(varColMap));

    // IF(BOUND(?possiblyUndef), ?possiblyUndef, ?possiblyUndef2)
    // Result depends on else-branch which is possibly undefined
    auto ifExprSpecial2 = makeIfExpression(
        makeBoundExpression(
            std::make_unique<VariableExpression>(possiblyUndefVar)),
        std::make_unique<VariableExpression>(possiblyUndefVar),
        std::make_unique<VariableExpression>(unboundVar));
    EXPECT_FALSE(ifExprSpecial2->isResultAlwaysDefined(varColMap));

    // IF(BOUND(?x), ?x, literal) - else is always defined
    auto ifExprSpecial3 = makeIfExpression(
        makeBoundExpression(
            std::make_unique<VariableExpression>(possiblyUndefVar)),
        std::make_unique<VariableExpression>(possiblyUndefVar),
        std::make_unique<IdExpression>(Id::makeFromInt(42)));
    EXPECT_TRUE(ifExprSpecial3->isResultAlwaysDefined(varColMap));

    // Pattern doesn't match: IF(BOUND(?x), ?y, ?z) where ?x != ?y
    // Falls back to general case: both branches must be always defined
    auto ifExprNotSpecial = makeIfExpression(
        makeBoundExpression(
            std::make_unique<VariableExpression>(possiblyUndefVar)),
        std::make_unique<VariableExpression>(
            alwaysDefinedVar),  // Different variable!
        std::make_unique<IdExpression>(Id::makeFromInt(42)));
    EXPECT_TRUE(ifExprNotSpecial->isResultAlwaysDefined(varColMap));

    // Pattern doesn't match, and one branch is not always defined
    auto ifExprNotSpecial2 = makeIfExpression(
        makeBoundExpression(
            std::make_unique<VariableExpression>(possiblyUndefVar)),
        std::make_unique<VariableExpression>(
            alwaysDefinedVar),  // Different variable!
        std::make_unique<VariableExpression>(unboundVar));
    EXPECT_FALSE(ifExprNotSpecial2->isResultAlwaysDefined(varColMap));
  }
}

// _____________________________________________________________________________
TEST(SparqlExpressionMemberFunctions, isDeterministic) {
  using namespace sparqlExpression;
  using namespace sparqlExpression::detail;

  auto makeVar = []() -> SparqlExpression::Ptr {
    return std::make_unique<VariableExpression>(Variable{"?x"});
  };
  auto makeSharedVar = []() {
    return std::make_shared<VariableExpression>(Variable{"?x"});
  };
  auto makeRand = []() -> SparqlExpression::Ptr {
    return std::make_unique<RandomExpression>();
  };

  EXPECT_TRUE(VariableExpression{Variable{"?x"}}.isDeterministic());
  EXPECT_TRUE(IdExpression{Id::makeFromInt(1)}.isDeterministic());

  EXPECT_TRUE(makeCountStarExpression(false)->isDeterministic());

  EXPECT_TRUE(NowDatetimeExpression{"2024-06-18T12:00:00"}.isDeterministic());

  ParsedQuery pq;
  EXPECT_TRUE(ExistsExpression{pq}.isDeterministic());

  EXPECT_FALSE(makeUniqueBlankNodeExpression()->isDeterministic());
  EXPECT_FALSE(makeBlankNodeExpression(makeVar())->isDeterministic());
  EXPECT_FALSE(RandomExpression{}.isDeterministic());
  EXPECT_FALSE(UuidExpression{}.isDeterministic());
  EXPECT_FALSE(StrUuidExpression{}.isDeterministic());

  EXPECT_TRUE(makeAbsExpression(makeVar())->isDeterministic());
  EXPECT_FALSE(makeAbsExpression(makeRand())->isDeterministic());

  EXPECT_TRUE(makeStrlenExpression(makeVar())->isDeterministic());
  EXPECT_FALSE(makeStrlenExpression(makeRand())->isDeterministic());

  std::vector<SparqlExpression::Ptr> dets;
  dets.push_back(makeVar());
  dets.push_back(makeVar());
  EXPECT_TRUE(makeCoalesceExpression(std::move(dets))->isDeterministic());
  std::vector<SparqlExpression::Ptr> withRand;
  withRand.push_back(makeVar());
  withRand.push_back(makeRand());
  EXPECT_FALSE(makeCoalesceExpression(std::move(withRand))->isDeterministic());

  EXPECT_TRUE(SumExpression(false, makeVar()).isDeterministic());
  EXPECT_FALSE(SumExpression(false, makeRand()).isDeterministic());

  EXPECT_TRUE(SampleExpression(false, makeVar()).isDeterministic());
  EXPECT_FALSE(SampleExpression(false, makeRand()).isDeterministic());

  EXPECT_TRUE(GroupConcatExpression(false, makeVar(), ",").isDeterministic());
  EXPECT_FALSE(GroupConcatExpression(false, makeRand(), ",").isDeterministic());

  EXPECT_TRUE(DeviationExpression(makeVar()).isDeterministic());
  EXPECT_FALSE(DeviationExpression(makeRand()).isDeterministic());

  using E = EqualExpression;
  EXPECT_TRUE(E(E::Children{makeVar(), makeVar()}).isDeterministic());
  EXPECT_FALSE(E(E::Children{makeVar(), makeRand()}).isDeterministic());

  std::vector<SparqlExpression::Ptr> args;
  args.push_back(makeVar());
  EXPECT_TRUE(InExpression(makeVar(), std::move(args)).isDeterministic());
  args.clear();
  args.push_back(makeRand());
  EXPECT_FALSE(InExpression(makeVar(), std::move(args)).isDeterministic());

  SparqlExpressionPimpl detPimpl{makeSharedVar(), "?x"};
  EXPECT_TRUE(detPimpl.isDeterministic());
  SparqlExpressionPimpl nonDetPimpl{std::make_shared<RandomExpression>(),
                                    "RAND()"};
  EXPECT_FALSE(nonDetPimpl.isDeterministic());
}
