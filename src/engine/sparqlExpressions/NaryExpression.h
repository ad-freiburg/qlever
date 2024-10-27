// Copyright 2021 - 2023, University of Freiburg,
// Chair of Algorithms and Data Structures
// Authors: Johannes Kalmbach <kalmbacj@cs.uni-freiburg.de>
//          Hannah Bast <bast@cs.uni-freiburg.de>

#pragma once

#include <charconv>
#include <concepts>
#include <cstdlib>

#include "engine/sparqlExpressions/SparqlExpression.h"

// Factory functions for all kinds of expressions that only have other
// expressions as arguments. The actual types and implementations of the
// expressions are hidden in the respective `.cpp` file to reduce compile times.
namespace sparqlExpression {

SparqlExpression::Ptr makeAddExpression(SparqlExpression::Ptr child1,
                                        SparqlExpression::Ptr child2);
SparqlExpression::Ptr makeAndExpression(SparqlExpression::Ptr child1,
                                        SparqlExpression::Ptr child2);
SparqlExpression::Ptr makeDivideExpression(SparqlExpression::Ptr child1,
                                           SparqlExpression::Ptr child2);
SparqlExpression::Ptr makeMultiplyExpression(SparqlExpression::Ptr child1,
                                             SparqlExpression::Ptr child2);
SparqlExpression::Ptr makeOrExpression(SparqlExpression::Ptr child1,
                                       SparqlExpression::Ptr child2);
SparqlExpression::Ptr makeSubtractExpression(SparqlExpression::Ptr child1,
                                             SparqlExpression::Ptr child2);

SparqlExpression::Ptr makeUnaryMinusExpression(SparqlExpression::Ptr child);
SparqlExpression::Ptr makeUnaryNegateExpression(SparqlExpression::Ptr child);

SparqlExpression::Ptr makeRoundExpression(SparqlExpression::Ptr child);
SparqlExpression::Ptr makeAbsExpression(SparqlExpression::Ptr child);
SparqlExpression::Ptr makeCeilExpression(SparqlExpression::Ptr child);
SparqlExpression::Ptr makeFloorExpression(SparqlExpression::Ptr child);
SparqlExpression::Ptr makeLogExpression(SparqlExpression::Ptr child);
SparqlExpression::Ptr makeExpExpression(SparqlExpression::Ptr child);
SparqlExpression::Ptr makeSqrtExpression(SparqlExpression::Ptr child);
SparqlExpression::Ptr makeSinExpression(SparqlExpression::Ptr child);
SparqlExpression::Ptr makeCosExpression(SparqlExpression::Ptr child);
SparqlExpression::Ptr makeTanExpression(SparqlExpression::Ptr child);
SparqlExpression::Ptr makePowExpression(SparqlExpression::Ptr child1,
                                        SparqlExpression::Ptr child2);

SparqlExpression::Ptr makeDistExpression(SparqlExpression::Ptr child1,
                                         SparqlExpression::Ptr child2);
SparqlExpression::Ptr makeLatitudeExpression(SparqlExpression::Ptr child);
SparqlExpression::Ptr makeLongitudeExpression(SparqlExpression::Ptr child);

SparqlExpression::Ptr makeSecondsExpression(SparqlExpression::Ptr child);
SparqlExpression::Ptr makeMinutesExpression(SparqlExpression::Ptr child);
SparqlExpression::Ptr makeHoursExpression(SparqlExpression::Ptr child);
SparqlExpression::Ptr makeDayExpression(SparqlExpression::Ptr child);
SparqlExpression::Ptr makeTimezoneStrExpression(SparqlExpression::Ptr child);
SparqlExpression::Ptr makeTimezoneExpression(SparqlExpression::Ptr child);
SparqlExpression::Ptr makeMonthExpression(SparqlExpression::Ptr child);
SparqlExpression::Ptr makeYearExpression(SparqlExpression::Ptr child);

SparqlExpression::Ptr makeStrIriDtExpression(SparqlExpression::Ptr child1,
                                             SparqlExpression::Ptr child2);
SparqlExpression::Ptr makeStrLangTagExpression(SparqlExpression::Ptr child1,
                                               SparqlExpression::Ptr child2);

SparqlExpression::Ptr makeStrExpression(SparqlExpression::Ptr child);
SparqlExpression::Ptr makeIriOrUriExpression(SparqlExpression::Ptr child);
SparqlExpression::Ptr makeStrlenExpression(SparqlExpression::Ptr child);
SparqlExpression::Ptr makeSubstrExpression(SparqlExpression::Ptr string,
                                           SparqlExpression::Ptr start,
                                           SparqlExpression::Ptr length);

SparqlExpression::Ptr makeUppercaseExpression(SparqlExpression::Ptr child);
SparqlExpression::Ptr makeLowercaseExpression(SparqlExpression::Ptr child);
SparqlExpression::Ptr makeStrStartsExpression(SparqlExpression::Ptr child1,
                                              SparqlExpression::Ptr child2);
SparqlExpression::Ptr makeStrEndsExpression(SparqlExpression::Ptr child1,
                                            SparqlExpression::Ptr child2);
SparqlExpression::Ptr makeContainsExpression(SparqlExpression::Ptr child1,
                                             SparqlExpression::Ptr child2);
SparqlExpression::Ptr makeStrAfterExpression(SparqlExpression::Ptr child1,
                                             SparqlExpression::Ptr child2);

SparqlExpression::Ptr makeReplaceExpression(SparqlExpression::Ptr input,
                                            SparqlExpression::Ptr pattern,
                                            SparqlExpression::Ptr replacement);
SparqlExpression::Ptr makeStrBeforeExpression(SparqlExpression::Ptr child1,
                                              SparqlExpression::Ptr child2);
SparqlExpression::Ptr makeLangMatchesExpression(SparqlExpression::Ptr child1,
                                                SparqlExpression::Ptr child2);

SparqlExpression::Ptr makeMD5Expression(SparqlExpression::Ptr child);
SparqlExpression::Ptr makeSHA1Expression(SparqlExpression::Ptr child);
SparqlExpression::Ptr makeSHA256Expression(SparqlExpression::Ptr child);
SparqlExpression::Ptr makeSHA384Expression(SparqlExpression::Ptr child);
SparqlExpression::Ptr makeSHA512Expression(SparqlExpression::Ptr child);

SparqlExpression::Ptr makeIfExpression(SparqlExpression::Ptr child1,
                                       SparqlExpression::Ptr child2,
                                       SparqlExpression::Ptr child3);

// Implemented in ConvertToNumeric.cpp
SparqlExpression::Ptr makeConvertToIntExpression(SparqlExpression::Ptr child);
SparqlExpression::Ptr makeConvertToDoubleExpression(
    SparqlExpression::Ptr child);

// Implemented in RdfTermExpressions.cpp
SparqlExpression::Ptr makeDatatypeExpression(SparqlExpression::Ptr child);

// Implemented in LangExpression.cpp
SparqlExpression::Ptr makeLangExpression(SparqlExpression::Ptr child);
std::optional<Variable> getVariableFromLangExpression(
    const SparqlExpression* child);

SparqlExpression::Ptr makeEncodeForUriExpression(SparqlExpression::Ptr child);

SparqlExpression::Ptr makeIsIriExpression(SparqlExpression::Ptr child);
SparqlExpression::Ptr makeIsLiteralExpression(SparqlExpression::Ptr child);
SparqlExpression::Ptr makeIsNumericExpression(SparqlExpression::Ptr child);
SparqlExpression::Ptr makeIsBlankExpression(SparqlExpression::Ptr child);
SparqlExpression::Ptr makeIsGeoPointExpression(SparqlExpression::Ptr child);
SparqlExpression::Ptr makeBoundExpression(SparqlExpression::Ptr child);

// For a `function` that takes `std::vector<SparqlExpression::Ptr>` (size only
// known at runtime), create a lambda that takes the `Ptr`s directly as a
// variable number of arguments (as a variadic template, number of arguments
// known at compile time). This makes the interface more similar to the
// `make...` functions for n-ary expressions that have a compile-time known
// number of children. This makes the testing easier, as we then can use the
// same test helpers for all expressions.
template <auto function>
requires std::is_invocable_r_v<SparqlExpression::Ptr, decltype(function),
                               std::vector<SparqlExpression::Ptr>>
constexpr auto variadicExpressionFactory =
    []<std::derived_from<SparqlExpression>... Exps>(
        std::unique_ptr<Exps>... children) {
      std::vector<SparqlExpression::Ptr> vec;
      (..., (vec.push_back(std::move(children))));
      return std::invoke(function, std::move(vec));
    };

SparqlExpression::Ptr makeCoalesceExpression(
    std::vector<SparqlExpression::Ptr> children);

// Construct a `CoalesceExpression` from a constant number of arguments. Used
// for testing.
constexpr auto makeCoalesceExpressionVariadic =
    variadicExpressionFactory<&makeCoalesceExpression>;

SparqlExpression::Ptr makeConcatExpression(
    std::vector<SparqlExpression::Ptr> children);
constexpr auto makeConcatExpressionVariadic =
    variadicExpressionFactory<&makeConcatExpression>;

}  // namespace sparqlExpression
