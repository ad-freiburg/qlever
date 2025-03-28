//  Copyright 2024, University of Freiburg,
//                  Chair of Algorithms and Data Structures
//  Author: Hannes Baumann <baumannh@informatik.uni-freiburg.de>

#ifndef QLEVER_TEST_PREFILTEREXPRESSIONTESTHELPERS_H
#define QLEVER_TEST_PREFILTEREXPRESSIONTESTHELPERS_H

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

// Make IsDatatypeExpression
template <typename IsDtypeExpr>
auto isDtypeExpr =
    [](bool isNegated = false) -> std::unique_ptr<PrefilterExpression> {
  return std::make_unique<IsDtypeExpr>(isNegated);
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
// IS IRI
constexpr auto isIri = isDtypeExpr<IsIriExpression>;
// IS LITERAL
constexpr auto isLit = isDtypeExpr<IsLiteralExpression>;
// IS NUMERIC
constexpr auto isNum = isDtypeExpr<IsNumericExpression>;
// IS BLANK
constexpr auto isBlank = isDtypeExpr<IsBlankExpression>;
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
constexpr auto makePrefilterVec =
    []<QL_CONCEPT_OR_TYPENAME(
        std::convertible_to<
            sparqlExpression::PrefilterExprVariablePair>)... Args>(
        Args&&... prefilterArgs) {
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
using SparqlPtr = std::unique_ptr<SparqlExpression>;
using VariantArgs = std::variant<Variable, ValueId, Iri, Literal, SparqlPtr>;

//______________________________________________________________________________
// If value `child` is not already a `SparqlExpression` pointer, try to create
// the respective leaf `SparqlExpression` (`LiteralExpression`) for the given
// value.
const auto makeOptLiteralSparqlExpr =
    [](auto child) -> std::unique_ptr<SparqlExpression> {
  using T = std::decay_t<decltype(child)>;
  if constexpr (std::is_same_v<T, ValueId>) {
    return std::make_unique<IdExpression>(child);
  } else if constexpr (std::is_same_v<T, Variable>) {
    return std::make_unique<VariableExpression>(child);
  } else if constexpr (std::is_same_v<T, Literal>) {
    return std::make_unique<StringLiteralExpression>(child);
  } else if constexpr (std::is_same_v<T, Iri>) {
    return std::make_unique<IriExpression>(child);
  } else if constexpr (std::is_same_v<T, SparqlPtr>) {
    return child;
  } else {
    throw std::runtime_error(
        "Can't create a LiteralExpression from provided (input) type, and a "
        "SparqlExpression wasn't provided either. Please provide a suitable "
        "value type.");
  }
};

//______________________________________________________________________________
auto getExpr = [](auto variantVal) -> std::unique_ptr<SparqlExpression> {
  return makeOptLiteralSparqlExpr(std::move(variantVal));
};

//______________________________________________________________________________
template <typename RelExpr>
std::unique_ptr<SparqlExpression> makeRelationalSparqlExprImpl(
    VariantArgs child0, VariantArgs child1) {
  return std::make_unique<RelExpr>(std::array<SparqlExpression::Ptr, 2>{
      std::visit(getExpr, std::move(child0)),
      std::visit(getExpr, std::move(child1))});
};

//______________________________________________________________________________
std::unique_ptr<SparqlExpression> makeStringStartsWithSparqlExpression(
    VariantArgs child0, VariantArgs child1) {
  return makeStrStartsExpression(std::visit(getExpr, std::move(child0)),
                                 std::visit(getExpr, std::move(child1)));
};

//______________________________________________________________________________
std::unique_ptr<SparqlExpression> makeYearSparqlExpression(VariantArgs child) {
  return makeYearExpression(std::visit(getExpr, std::move(child)));
};

//______________________________________________________________________________
template <prefilterExpressions::IsDatatype Datatype>
std::unique_ptr<SparqlExpression> makeIsDatatypeStartsWithExpression(
    VariantArgs child) {
  using enum prefilterExpressions::IsDatatype;
  auto childExpr = std::visit(getExpr, std::move(child));
  if constexpr (Datatype == IRI) {
    return makeIsIriExpression(std::move(childExpr));
  } else if constexpr (Datatype == LITERAL) {
    return makeIsLiteralExpression(std::move(childExpr));
  } else if constexpr (Datatype == NUMERIC) {
    return makeIsNumericExpression(std::move(childExpr));
  } else {
    static_assert(Datatype == BLANK);
    return makeIsBlankExpression(std::move(childExpr));
  }
}

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

//______________________________________________________________________________
// Create SparqlExpression `isIri`
constexpr auto isIriSprql =
    &makeIsDatatypeStartsWithExpression<prefilterExpressions::IsDatatype::IRI>;
// Create SparqlExpression `isLiteral`
constexpr auto isLiteralSprql = &makeIsDatatypeStartsWithExpression<
    prefilterExpressions::IsDatatype::LITERAL>;
// Create SparqlExpression `isNumeric`
constexpr auto isNumericSprql = &makeIsDatatypeStartsWithExpression<
    prefilterExpressions::IsDatatype::NUMERIC>;
// Create SparqlExpression `isBlank`
constexpr auto isBlankSprql = &makeIsDatatypeStartsWithExpression<
    prefilterExpressions::IsDatatype::BLANK>;
// Create SparqlExpression `YEAR`.
constexpr auto yearSprqlExpr = &makeYearSparqlExpression;

}  // namespace makeSparqlExpression

#endif  // QLEVER_TEST_PREFILTEREXPRESSIONTESTHELPERS_H
