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
auto relExpr = [](const IdOrLocalVocabEntry& referenceId)
    -> std::unique_ptr<PrefilterExpression> {
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

namespace filterHelper {

//______________________________________________________________________________
// Create `LocalVocabEntry` / `LiteralOrIri`.
// Note: `Iri` string value must start and end with `<`/`>` and the `Literal`
// value with `'`/`'`.
const auto LVE = [](const std::string& litOrIri) -> LocalVocabEntry {
  return LocalVocabEntry::fromStringRepresentation(litOrIri);
};

//______________________________________________________________________________
// Construct a `PAIR` with the given `PrefilterExpression` and `Variable` value.
auto pr =
    [](std::unique_ptr<prefilterExpressions::PrefilterExpression> expr,
       const Variable& var) -> sparqlExpression::PrefilterExprVariablePair {
  return {std::move(expr), var};
};

//______________________________________________________________________________
// Create a vector containing the provided `<PrefilterExpression, Variable>`
// pairs.
auto makePrefilterVec =
    [](QL_CONCEPT_OR_NOTHING(
        std::convertible_to<
            sparqlExpression::
                PrefilterExprVariablePair>) auto&&... prefilterArgs) {
      std::vector<sparqlExpression::PrefilterExprVariablePair>
          prefilterVarPairs = {};
      if constexpr (sizeof...(prefilterArgs) > 0) {
        (prefilterVarPairs.emplace_back(
             std::forward<sparqlExpression::PrefilterExprVariablePair>(
                 prefilterArgs)),
         ...);
      }
      return prefilterVarPairs;
    };

}  // namespace filterHelper

}  // namespace makeFilterExpression

namespace makeSparqlExpression {
using namespace sparqlExpression;

namespace {
using Literal = ad_utility::triple_component::Literal;
using Iri = ad_utility::triple_component::Iri;
using RelValues = std::variant<Variable, ValueId, Iri, Literal>;

//______________________________________________________________________________
const auto makeLiteralSparqlExpr =
    [](const auto& child) -> std::unique_ptr<SparqlExpression> {
  using T = std::decay_t<decltype(child)>;
  if constexpr (std::is_same_v<T, ValueId>) {
    return std::make_unique<IdExpression>(child);
  } else if constexpr (std::is_same_v<T, Variable>) {
    return std::make_unique<VariableExpression>(child);
  } else if constexpr (std::is_same_v<T, Literal>) {
    return std::make_unique<StringLiteralExpression>(child);
  } else if constexpr (std::is_same_v<T, Iri>) {
    return std::make_unique<IriExpression>(child);
  } else {
    throw std::runtime_error(
        "Can't create a LiteralExpression from provided (input) type.");
  }
};

//______________________________________________________________________________
auto getExpr = [](const auto& variantVal) -> std::unique_ptr<SparqlExpression> {
  return makeLiteralSparqlExpr(variantVal);
};

//______________________________________________________________________________
template <typename RelExpr>
std::unique_ptr<SparqlExpression> makeRelationalSparqlExprImpl(
    const RelValues& child0, const RelValues& child1) {
  return std::make_unique<RelExpr>(std::array<SparqlExpression::Ptr, 2>{
      std::visit(getExpr, child0), std::visit(getExpr, child1)});
};

//______________________________________________________________________________
std::unique_ptr<SparqlExpression> makeStringStartsWithSparqlExpression(
    const RelValues& child0, const RelValues& child1) {
  return makeStrStartsExpression(std::visit(getExpr, child0),
                                 std::visit(getExpr, child1));
};
}  // namespace

//______________________________________________________________________________
// LESS THAN (`<`, `SparqlExpression`)
constexpr auto ltSprql =
    &makeRelationalSparqlExprImpl<sparqlExpression::LessThanExpression>;
// LESS EQUAL (`<=`, `SparqlExpression`)
constexpr auto leSprql =
    &makeRelationalSparqlExprImpl<sparqlExpression::LessEqualExpression>;
// EQUAL (`==`, `SparqlExpression`)
constexpr auto eqSprql =
    &makeRelationalSparqlExprImpl<sparqlExpression::EqualExpression>;
// NOT EQUAL (`!=`, `SparqlExpression`)
constexpr auto neqSprql =
    &makeRelationalSparqlExprImpl<sparqlExpression::NotEqualExpression>;
// GREATER EQUAL (`>=`, `SparqlExpression`)
constexpr auto geSprql =
    &makeRelationalSparqlExprImpl<sparqlExpression::GreaterEqualExpression>;
// GREATER THAN (`>`, `SparqlExpression`)
constexpr auto gtSprql =
    &makeRelationalSparqlExprImpl<sparqlExpression::GreaterThanExpression>;
// AND (`&&`, `SparqlExpression`)
constexpr auto andSprqlExpr = &makeAndExpression;
// OR (`||`, `SparqlExpression`)
constexpr auto orSprqlExpr = &makeOrExpression;
// NOT (`!`, `SparqlExpression`)
constexpr auto notSprqlExpr = &makeUnaryNegateExpression;

//______________________________________________________________________________
// Create SparqlExpression `STRSTARTS`.
constexpr auto strStartsSprql = &makeStringStartsWithSparqlExpression;

}  // namespace makeSparqlExpression
