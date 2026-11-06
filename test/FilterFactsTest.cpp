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

namespace {

using namespace filterFacts;
using ::testing::ElementsAre;
using ::testing::VariantWith;
using valueIdComparators::Comparison;
using V = Variable;

// A parsed query and the canonical form of the FILTERs of its root group
// graph pattern (which point into the query).
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
    ql::ranges::move(canonicalize(*f.expression_.getPimpl()),
                     std::back_inserter(result.conjuncts_));
  }
  return result;
}

// Matcher for a `Constraint` `distance op meters` (compared with a tolerance,
// because the factors of the conversion of units are rounded).
auto constraint(Comparison op, double meters) {
  using namespace ::testing;
  return VariantWith<Constraint>(
      AllOf(Field(&Constraint::lhs_, Eq(Distance{V{"?ga"}, V{"?gb"}})),
            Field(&Constraint::op_, op),
            Field(&Constraint::rhs_, DoubleNear(meters, 1e-3))));
}

// Test that `distance < number` and `distance <= number` become a
// `Constraint` with the number in meters, also in a nested `&&`.
TEST(FilterFactsCanonicalizer, Distance) {
  auto expect = [](std::string_view filter, Comparison op, double meters,
                   ad_utility::source_location l = AD_CURRENT_SOURCE_LOC()) {
    auto trace = generateLocationTrace(l);
    EXPECT_THAT(canonical(filter).conjuncts_,
                ElementsAre(constraint(op, meters)));
  };

  // Different distance functions and units.
  expect("geof:distance(?ga, ?gb, unit:M) <= 500", Comparison::LE, 500);
  expect("geof:distance(?ga, ?gb) <= 0.5", Comparison::LE, 500);
  expect("geof:metricDistance(?ga, ?gb) < 500", Comparison::LT, 500);
  expect("geof:distance(?ga, ?gb, unit:MI) < 1", Comparison::LT, 1609.344);

  // Each conjunct of `&&` is canonicalized on its own.
  auto result = canonical(
      "geof:metricDistance(?ga, ?gb) <= 500 && "
      "(?a != ?b && geof:metricDistance(?ga, ?gb) < 400)");
  EXPECT_THAT(result.conjuncts_, ElementsAre(constraint(Comparison::LE, 500),
                                             VariantWith<Opaque>(::testing::_),
                                             constraint(Comparison::LT, 400)));
}

// Test that only the columns of `VALUES` without `UNDEF` and the `BIND`s whose
// target is equivalent to their expression are definitions.
TEST(FilterFactsCanonicalizer, CollectDefinitions) {
  using ::testing::Key;
  using ::testing::UnorderedElementsAre;
  auto query = ad_utility::testing::parseQuery(
      "SELECT * { ?a <p> ?ga . BIND(?ga AS ?x) BIND(?gb AS ?y) ?b <p> ?gb . "
      "BIND(RAND() AS ?z) BIND(NOW() AS ?n) BIND(?ga AS ?w) ?w <q> ?o . "
      "VALUES (?r ?s) { (1 UNDEF) } }");
  auto defs = collectDefinitions(query._rootGraphPattern._graphPatterns);

  // `?y` is bound before `?gb`, `?z` and `?n` are not deterministic, and `?w`
  // is bound again by a later triple.
  EXPECT_THAT(defs.expressions_, UnorderedElementsAre(Key(V{"?x"})));
  EXPECT_THAT(defs.constants_, UnorderedElementsAre(Key(V{"?r"})));
}

// Test that every other expression stays `Opaque`.
TEST(FilterFactsCanonicalizer, Opaque) {
  for (std::string_view filter :
       {"geof:metricDistance(?ga, ?gb) > 500",
        "500 >= geof:metricDistance(?ga, ?gb)",
        "geof:metricDistance(?ga, ?gb) <= \"500\"",
        "geof:metricDistance(?ga, ?gb) <= ?a",
        "geof:metricDistance(?ga, ?gb) + 1 <= 500", "?a <= 500", "BOUND(?a)",
        "geof:metricDistance(?ga, ?gb) <= 500 || ?a = 1"}) {
    auto result = canonical(filter);
    EXPECT_THAT(result.conjuncts_,
                ElementsAre(VariantWith<Opaque>(
                    Opaque{result.query_._rootGraphPattern._filters[0]
                               .expression_.getPimpl()})))
        << filter;
  }
}

// Test that `distance <= c` and `distance < c` for a number `c >= 0` between
// two different geometries (at least one of them a variable) are recognized
// as `DistanceBound`.
TEST(FilterFactsRecognizers, DistanceBound) {
  auto recognize = [](std::string_view filter) {
    auto conjuncts = canonical(filter).conjuncts_;
    AD_CORRECTNESS_CHECK(conjuncts.size() == 1);
    return recognizeDistanceBound(conjuncts[0]);
  };
  auto point = std::string{
      "\"POINT(7.8 48.0)\"^^<http://www.opengis.net/ont/geosparql#wktLiteral>"};

  // `<=` and `<` (where the maximum distance is the largest `double` below
  // the number).
  EXPECT_THAT(recognize("geof:distance(?ga, ?gb, unit:M) <= 500"),
              ::testing::Optional(DistanceBound{V{"?ga"}, V{"?gb"}, 500}));
  EXPECT_THAT(
      recognize("geof:metricDistance(?ga, ?gb) < 500"),
      ::testing::Optional(DistanceBound{
          V{"?ga"}, V{"?gb"},
          std::nextafter(500.0, -std::numeric_limits<double>::infinity())}));

  // A constant geometry on one side.
  EXPECT_THAT(
      recognize(absl::StrCat("geof:metricDistance(?ga, ", point, ") <= 500")),
      ::testing::Optional(
          ::testing::Field(&DistanceBound::a_, TripleComponent{V{"?ga"}})));

  // A negative maximum distance, the same variable or two constants, or no
  // `Constraint` at all.
  for (std::string filter :
       {std::string{"geof:metricDistance(?ga, ?gb) <= -1"},
        std::string{"geof:metricDistance(?ga, ?gb) < 0"},
        std::string{"geof:metricDistance(?ga, ?ga) <= 500"},
        absl::StrCat("geof:metricDistance(", point, ", ", point, ") <= 500"),
        std::string{"BOUND(?a)"}}) {
    EXPECT_EQ(recognize(filter), std::nullopt) << filter;
  }
}

}  // namespace
