//  Copyright 2024 - 2025, University of Freiburg,
//                  Chair of Algorithms and Data Structures
//  Author: Hannes Baumann <baumannh@informatik.uni-freiburg.de>

#ifndef QLEVER_TEST_PREFILTEREXPRESSIONTESTHELPERS_H
#define QLEVER_TEST_PREFILTEREXPRESSIONTESTHELPERS_H

#include <memory>

#include "./engine/sparqlExpressions/LiteralExpression.h"
#include "./engine/sparqlExpressions/NaryExpression.h"
#include "./engine/sparqlExpressions/PrefilterExpressionIndex.h"
#include "./engine/sparqlExpressions/RegexExpression.h"
#include "./engine/sparqlExpressions/RelationalExpressions.h"
#include "./engine/sparqlExpressions/SparqlExpression.h"
#include "util/DateYearDuration.h"
#include "util/IdTestHelpers.h"

using ad_utility::testing::DateId;

constexpr auto DateParser = &DateYearOrDuration::parseXsdDate;

namespace makeFilterExpression {
using namespace prefilterExpressions;

// Make RelationalExpression
template <typename RelExpr>
inline auto relExpr = [](const IdOrLocalVocabEntry& referenceId)
    -> std::unique_ptr<PrefilterExpression> {
  return std::make_unique<RelExpr>(referenceId);
};

// Make IsInExpression
inline auto isInExpression =
    [](std::vector<IdOrLocalVocabEntry>&& referenceValues,
       bool isNegated = false) {
      return std::make_unique<IsInExpression>(referenceValues, isNegated);
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
inline auto notPrefilterExpression =
    [](std::unique_ptr<PrefilterExpression> child)
    -> std::unique_ptr<PrefilterExpression> {
  return std::make_unique<NotExpression>(std::move(child));
};

// Make PrefixRegexExpression
inline auto makePrefixRegexExpression =
    [](const TripleComponent::Literal& prefix, bool isNegated = false) {
      return std::make_unique<PrefixRegexExpression>(prefix, isNegated);
    };

// Make PrefilterExpression
//______________________________________________________________________________
// instantiation relational
// LESS THAN (`<`)
constexpr inline auto lt = relExpr<LessThanExpression>;
// LESS EQUAL (`<=`)
constexpr inline auto le = relExpr<LessEqualExpression>;
// GREATER EQUAL (`>=`)
constexpr inline auto ge = relExpr<GreaterEqualExpression>;
// GREATER THAN (`>`)
constexpr inline auto gt = relExpr<GreaterThanExpression>;
// EQUAL (`==`)
constexpr inline auto eq = relExpr<EqualExpression>;
// NOT EQUAL (`!=`)
constexpr inline auto neq = relExpr<NotEqualExpression>;
// IS IRI
constexpr inline auto isIri = isDtypeExpr<IsIriExpression>;
// IS LITERAL
constexpr inline auto isLit = isDtypeExpr<IsLiteralExpression>;
// IS NUMERIC
constexpr inline auto isNum = isDtypeExpr<IsNumericExpression>;
// IS BLANK
constexpr inline auto isBlank = isDtypeExpr<IsBlankExpression>;
// AND (`&&`)
constexpr inline auto andExpr = logExpr<AndExpression>;
// OR (`||`)
constexpr inline auto orExpr = logExpr<OrExpression>;
// NOT (`!`)
constexpr inline auto notExpr = notPrefilterExpression;
// `IN`, or `NOT IN` if the `isNegated` flag is set to true.
constexpr inline auto inExpr = isInExpression;
// PREFIX REGEX
constexpr inline auto prefixRegex = makePrefixRegexExpression;

namespace filterHelper {
//______________________________________________________________________________
// Create `LocalVocabEntry` / `LiteralOrIri`.
// Note: `Iri` string value must start and end with `<`/`>` and the `Literal`
// value with `'`/`'`.
constexpr inline auto LVE = [](const std::string& litOrIri) -> LocalVocabEntry {
  return LocalVocabEntry::fromStringRepresentation(litOrIri);
};

//______________________________________________________________________________
// Construct a `PAIR` with the given `PrefilterExpression` and `Variable`
// value.
constexpr inline auto pr =
    [](std::unique_ptr<prefilterExpressions::PrefilterExpression> expr,
       const Variable& var) -> sparqlExpression::PrefilterExprVariablePair {
  return {std::move(expr), var};
};

//______________________________________________________________________________
// Create a vector containing the provided `<PrefilterExpression, Variable>`
// pairs.
struct MakePrefilterVec {
  template <QL_CONCEPT_OR_TYPENAME(
      std::convertible_to<sparqlExpression::PrefilterExprVariablePair>)... Args>
  constexpr auto operator()(Args&&... prefilterArgs) const {
    std::vector<sparqlExpression::PrefilterExprVariablePair> prefilterVarPairs =
        {};
    if constexpr (sizeof...(prefilterArgs) > 0) {
      (prefilterVarPairs.emplace_back(
           std::forward<sparqlExpression::PrefilterExprVariablePair>(
               prefilterArgs)),
       ...);
    }
    return prefilterVarPairs;
  }
};
constexpr inline MakePrefilterVec makePrefilterVec;

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
constexpr inline auto makeOptLiteralSparqlExpr =
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
constexpr inline auto getExpr =
    [](auto variantVal) -> std::unique_ptr<SparqlExpression> {
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
std::unique_ptr<SparqlExpression> makePrefixRegexExpression(
    VariantArgs varExpr, VariantArgs litExpr) {
  return sparqlExpression::makeRegexExpression(
      std::visit(getExpr, std::move(varExpr)),
      std::visit(getExpr, std::move(litExpr)), nullptr);
}

//______________________________________________________________________________
std::unique_ptr<SparqlExpression> makeStrSparqlExpression(
    VariantArgs childVal) {
  return makeStrExpression(std::visit(getExpr, std::move(childVal)));
}

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
CPP_template(typename... Args)(
    requires(std::convertible_to<Args, VariantArgs>&&...))
    std::unique_ptr<SparqlExpression> inSprqlExpr(VariantArgs first,
                                                  Args&&... argList) {
  std::vector<std::unique_ptr<SparqlExpression>> childrenSparql;
  childrenSparql.reserve(sizeof...(argList));
  (childrenSparql.push_back(
       std::visit(getExpr, VariantArgs{std::forward<Args>(argList)})),
   ...);
  return std::make_unique<sparqlExpression::InExpression>(
      std::visit(getExpr, std::move(first)), std::move(childrenSparql));
}

//______________________________________________________________________________
// LESS THAN (`<`, `SparqlExpression`)
constexpr inline auto ltSprql =
    &makeRelationalSparqlExprImpl<sparqlExpression::LessThanExpression>;
// LESS EQUAL (`<=`, `SparqlExpression`)
constexpr inline auto leSprql =
    &makeRelationalSparqlExprImpl<sparqlExpression::LessEqualExpression>;
// EQUAL (`==`, `SparqlExpression`)
constexpr inline auto eqSprql =
    &makeRelationalSparqlExprImpl<sparqlExpression::EqualExpression>;
// NOT EQUAL (`!=`, `SparqlExpression`)
constexpr inline auto neqSprql =
    &makeRelationalSparqlExprImpl<sparqlExpression::NotEqualExpression>;
// GREATER EQUAL (`>=`, `SparqlExpression`)
constexpr inline auto geSprql =
    &makeRelationalSparqlExprImpl<sparqlExpression::GreaterEqualExpression>;
// GREATER THAN (`>`, `SparqlExpression`)
constexpr inline auto gtSprql =
    &makeRelationalSparqlExprImpl<sparqlExpression::GreaterThanExpression>;
// AND (`&&`, `SparqlExpression`)
constexpr inline auto andSprqlExpr = &makeAndExpression;
// OR (`||`, `SparqlExpression`)
constexpr inline auto orSprqlExpr = &makeOrExpression;
// NOT (`!`, `SparqlExpression`)
constexpr inline auto notSprqlExpr = &makeUnaryNegateExpression;

//______________________________________________________________________________
// Create SparqlExpression `STRSTARTS`.
constexpr inline auto strStartsSprql = &makeStringStartsWithSparqlExpression;
// Create SparqlExpression `REGEX`.
constexpr inline auto regexSparql = &makePrefixRegexExpression;
// Create SparqlExpression `STR`
constexpr inline auto strSprql = &makeStrSparqlExpression;

//______________________________________________________________________________
// Create SparqlExpression `isIri`
constexpr inline auto isIriSprql =
    &makeIsDatatypeStartsWithExpression<prefilterExpressions::IsDatatype::IRI>;
// Create SparqlExpression `isLiteral`
constexpr inline auto isLiteralSprql = &makeIsDatatypeStartsWithExpression<
    prefilterExpressions::IsDatatype::LITERAL>;
// Create SparqlExpression `isNumeric`
constexpr inline auto isNumericSprql = &makeIsDatatypeStartsWithExpression<
    prefilterExpressions::IsDatatype::NUMERIC>;
// Create SparqlExpression `isBlank`
constexpr inline auto isBlankSprql = &makeIsDatatypeStartsWithExpression<
    prefilterExpressions::IsDatatype::BLANK>;
// Create SparqlExpression `YEAR`.
constexpr inline auto yearSprqlExpr = &makeYearSparqlExpression;

}  // namespace makeSparqlExpression

#endif  // QLEVER_TEST_PREFILTEREXPRESSIONTESTHELPERS_H
