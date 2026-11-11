// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Christoph Ullinger <ullingec@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <absl/strings/str_cat.h>
#include <gmock/gmock.h>

#include <cmath>
#include <limits>

#include "./util/GTestHelpers.h"
#include "./util/ParsedQueryTestHelpers.h"
#include "engine/filterFacts/Canonicalizer.h"
#include "engine/filterFacts/Recognizers.h"
#include "engine/sparqlExpressions/SparqlExpression.h"

namespace {

using namespace filterFacts;
using ::testing::ElementsAre;
using valueIdComparators::Comparison;
using V = Variable;

// A parsed query and the canonical form of the FILTERs of its root group
// graph pattern (which shares the expressions of the query).
struct Canonical {
  ParsedQuery query_;
  std::vector<Conjunct> conjuncts_;
};

// Parse `SELECT * { ?a <p> ?ga . ?b <p> ?gb . FILTER(<filter>) }` and
// canonicalize its FILTER.
Canonical canonical(std::string_view filter) {
  Canonical result{
      ad_utility::testing::parseQuery(absl::StrCat(
          "PREFIX geof: <http://www.opengis.net/def/function/geosparql/> "
          "PREFIX unit: <http://qudt.org/vocab/unit/> "
          "SELECT * { ?a <p> ?ga . ?b <p> ?gb . FILTER(",
          filter, ") }")),
      {}};
  for (const auto& f : result.query_._rootGraphPattern._filters) {
    ql::ranges::move(canonicalize(f.expression_),
                     std::back_inserter(result.conjuncts_));
  }
  return result;
}

// Matcher for a `Conjunct` whose canonical form is `distance op meters`
// (compared with a tolerance, because the factors of the conversion of units
// are rounded).
auto distanceComparison(Comparison op, double meters, double tolerance = 1e-3) {
  using namespace ::testing;
  return Field(
      &Conjunct::canonical_,
      VariantWith<DistanceComparison>(AllOf(
          Field(&DistanceComparison::lhs_, Eq(Distance{V{"?ga"}, V{"?gb"}})),
          Field(&DistanceComparison::op_, op),
          Field(&DistanceComparison::rhs_, DoubleNear(meters, tolerance)))));
}

// Matcher for a `Conjunct` that is `Opaque` with the given `expression`.
auto opaque(const sparqlExpression::SparqlExpression* expression) {
  using namespace ::testing;
  return AllOf(
      Field(&Conjunct::canonical_, VariantWith<Opaque>(_)),
      Field(&Conjunct::expression_,
            ResultOf(
                [](const sparqlExpression::SparqlExpressionPimpl& pimpl) {
                  return pimpl.getPimpl();
                },
                expression)));
}

// Test that `distance < number` and `distance <= number` become a
// `DistanceComparison` with the number in meters, also in a nested `&&`.
TEST(FilterFactsCanonicalizer, Distance) {
  // Expect that `filter` is a single `DistanceComparison` `distance op meters`.
  auto expect = [](std::string_view filter, Comparison op, double meters,
                   ad_utility::source_location l = AD_CURRENT_SOURCE_LOC()) {
    auto trace = generateLocationTrace(l);
    EXPECT_THAT(canonical(filter).conjuncts_,
                ElementsAre(distanceComparison(op, meters)));
  };

  // Different distance functions and units.
  expect("geof:distance(?ga, ?gb, unit:M) <= 500", Comparison::LE, 500);
  expect("geof:distance(?ga, ?gb) <= 0.5", Comparison::LE, 500);
  expect("geof:metricDistance(?ga, ?gb) < 500", Comparison::LT, 500);
  expect("geof:distance(?ga, ?gb, unit:MI) < 1", Comparison::LT, 1609.344);

  // A number in meters is exactly the number of the FILTER (whereas
  // `1001 / 1000 * 1000` and `63.7 / 1000 * 1000` are not). The number `63.7`
  // of the FILTER is stored as an `Id`, which rounds its mantissa.
  EXPECT_THAT(canonical("geof:metricDistance(?ga, ?gb) <= 1001").conjuncts_,
              ElementsAre(distanceComparison(Comparison::LE, 1001, 0)));
  EXPECT_THAT(canonical("geof:distance(?ga, ?gb, unit:M) < 63.7").conjuncts_,
              ElementsAre(distanceComparison(
                  Comparison::LT, Id::makeFromDouble(63.7).getDouble(), 0)));

  // Each conjunct of `&&` is canonicalized on its own, and the `Opaque` one
  // has its subexpression.
  auto result = canonical(
      "geof:metricDistance(?ga, ?gb) <= 500 && "
      "(?a != ?b && geof:metricDistance(?ga, ?gb) < 400)");
  const auto& root =
      *result.query_._rootGraphPattern._filters[0].expression_.getPimpl();
  const auto* notEqual = root.children()[1]->children()[0].get();
  EXPECT_THAT(
      result.conjuncts_,
      ElementsAre(distanceComparison(Comparison::LE, 500), opaque(notEqual),
                  distanceComparison(Comparison::LT, 400)));
}

// Test that every other expression stays `Opaque`.
TEST(FilterFactsCanonicalizer, Opaque) {
  // Expect that `filter` is a single `Opaque` conjunct with the whole
  // expression of the FILTER.
  auto expectOpaque = [](std::string_view filter,
                         ad_utility::source_location l =
                             AD_CURRENT_SOURCE_LOC()) {
    auto trace = generateLocationTrace(l);
    auto result = canonical(filter);
    EXPECT_THAT(result.conjuncts_,
                ElementsAre(opaque(result.query_._rootGraphPattern._filters[0]
                                       .expression_.getPimpl())));
  };

  // A comparison other than `distance < number` or `distance <= number`.
  expectOpaque("geof:metricDistance(?ga, ?gb) > 500");
  expectOpaque("500 >= geof:metricDistance(?ga, ?gb)");

  // A right side that is not a number.
  expectOpaque("geof:metricDistance(?ga, ?gb) <= \"500\"");
  expectOpaque("geof:metricDistance(?ga, ?gb) <= ?a");

  // A left side that is not a distance in a unit of length.
  expectOpaque("geof:metricDistance(?ga, ?gb) + 1 <= 500");
  expectOpaque("?a <= 500");
  expectOpaque("geof:distance(?ga, ?gb, unit:M2) <= 500");

  // No comparison, or a comparison inside `||`.
  expectOpaque("BOUND(?a)");
  expectOpaque("geof:metricDistance(?ga, ?gb) <= 500 || ?a = 1");
}

// Test that `distance <= c` and `distance < c` for a finite number `c >= 0`
// between two different operands (at least one of them a variable) are
// recognized as `DistanceBound`.
TEST(FilterFactsRecognizers, DistanceBound) {
  // Recognize the `DistanceBound` of the single conjunct of `filter`, and a
  // constant point.
  auto recognize = [](std::string_view filter) {
    auto result = canonical(filter);
    EXPECT_EQ(result.conjuncts_.size(), 1u) << filter;
    return recognizeDistanceBound(result.conjuncts_.at(0));
  };
  auto point = std::string{
      "\"POINT(7.8 48.0)\"^^<http://www.opengis.net/ont/geosparql#wktLiteral>"};
  Distance distance{V{"?ga"}, V{"?gb"}};

  // `<=` and `<` (where the maximum distance is the largest `double` below
  // the number).
  EXPECT_THAT(recognize("geof:distance(?ga, ?gb, unit:M) <= 500"),
              ::testing::Optional(DistanceBound{distance, 500}));
  EXPECT_THAT(
      recognize("geof:metricDistance(?ga, ?gb) < 500"),
      ::testing::Optional(DistanceBound{
          distance,
          std::nextafter(500.0, -std::numeric_limits<double>::infinity())}));

  // A constant geometry on one side.
  EXPECT_THAT(
      recognize(absl::StrCat("geof:metricDistance(?ga, ", point, ") <= 500")),
      ::testing::Optional(::testing::Field(
          &DistanceBound::distance_,
          ::testing::Field(&Distance::a_, TripleComponent{V{"?ga"}}))));

  // A negative, infinite (also after the conversion to meters) or NaN maximum
  // distance, the same variable or two constants, or no `DistanceComparison` at
  // all.
  auto xsdDouble = std::string{"^^<http://www.w3.org/2001/XMLSchema#double>"};
  for (std::string filter :
       {std::string{"geof:metricDistance(?ga, ?gb) <= -1"},
        std::string{"geof:metricDistance(?ga, ?gb) < 0"},
        absl::StrCat("geof:metricDistance(?ga, ?gb) <= \"INF\"", xsdDouble),
        absl::StrCat("geof:metricDistance(?ga, ?gb) < \"INF\"", xsdDouble),
        std::string{"geof:distance(?ga, ?gb) < 1e306"},
        absl::StrCat("geof:metricDistance(?ga, ?gb) <= \"NaN\"", xsdDouble),
        std::string{"geof:metricDistance(?ga, ?ga) <= 500"},
        absl::StrCat("geof:metricDistance(", point, ", ", point, ") <= 500"),
        std::string{"BOUND(?a)"}}) {
    EXPECT_EQ(recognize(filter), std::nullopt) << filter;
  }
}

}  // namespace
