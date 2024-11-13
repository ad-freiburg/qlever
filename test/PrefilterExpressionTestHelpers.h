//  Copyright 2024, University of Freiburg,
//                  Chair of Algorithms and Data Structures
//  Author: Hannes Baumann <baumannh@informatik.uni-freiburg.de>

#pragma once

#include <memory>

#include "./engine/sparqlExpressions/LiteralExpression.h"
#include "./engine/sparqlExpressions/NaryExpression.h"
#include "./engine/sparqlExpressions/PrefilterExpressionIndex.h"
#include "./engine/sparqlExpressions/RelationalExpressions.h"
#include "./engine/sparqlExpressions/SparqlExpression.h"
#include "util/DateYearDuration.h"
#include "util/IdTestHelpers.h"

using ad_utility::testing::DateId;

constexpr auto DateParser = &DateYearOrDuration::parseXsdDate;

namespace makeFilterExpression {
using namespace prefilterExpressions;

namespace {
//______________________________________________________________________________
// Make RelationalExpression
template <typename RelExpr>
auto relExpr =
    [](const ValueId& referenceId) -> std::unique_ptr<PrefilterExpression> {
  return std::make_unique<RelExpr>(referenceId);
};

// Make AndExpression or OrExpression
template <typename LogExpr>
auto logExpr = [](std::unique_ptr<PrefilterExpression> child1,
                  std::unique_ptr<PrefilterExpression> child2)
    -> std::unique_ptr<PrefilterExpression> {
  return std::make_unique<LogExpr>(std::move(child1), std::move(child2));
};

// Make NotExpression
auto notPrefilterExpression = [](std::unique_ptr<PrefilterExpression> child)
    -> std::unique_ptr<PrefilterExpression> {
  return std::make_unique<NotExpression>(std::move(child));
};
}  // namespace

// Make PrefilterExpression
//______________________________________________________________________________
// instantiation relational
// LESS THAN (`<`)
constexpr auto lt = relExpr<LessThanExpression>;
// LESS EQUAL (`<=`)
constexpr auto le = relExpr<LessEqualExpression>;
// GREATER EQUAL (`>=`)
constexpr auto ge = relExpr<GreaterEqualExpression>;
// GREATER THAN (`>`)
constexpr auto gt = relExpr<GreaterThanExpression>;
// EQUAL (`==`)
constexpr auto eq = relExpr<EqualExpression>;
// NOT EQUAL (`!=`)
constexpr auto neq = relExpr<NotEqualExpression>;
// AND (`&&`)
constexpr auto andExpr = logExpr<AndExpression>;
// OR (`||`)
constexpr auto orExpr = logExpr<OrExpression>;
// NOT (`!`)
constexpr auto notExpr = notPrefilterExpression;

}  // namespace makeFilterExpression
