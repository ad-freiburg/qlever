// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Nino Frey, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gmock/gmock.h>

#include "../QueryPlannerTestHelpers.h"
#include "../util/GTestHelpers.h"
#include "engine/OptionalJoinConditionPlanning.h"
#include "util/ParseException.h"

namespace {
namespace h = queryPlannerTestHelpers;
using qlever::queryPlanner::isPartOfOptionalJoinCondition;
using ::testing::HasSubstr;
using Variables = ad_utility::HashSet<Variable>;

// Return the result of `isPartOfOptionalJoinCondition` for the single FILTER
// of the `group`, which is given without the enclosing braces.
bool isCondition(const std::string& group, const Variables& variablesOfLeftSide,
                 const Variables& alwaysDefinedInGroup) {
  auto query = ad_utility::testing::parseQuery("SELECT * {" + group + "}");
  const auto& filters = query._rootGraphPattern._filters;
  AD_CORRECTNESS_CHECK(filters.size() == 1);
  return isPartOfOptionalJoinCondition(
      filters.at(0).expression_, variablesOfLeftSide,
      [&alwaysDefinedInGroup](const Variable& variable) {
        return alwaysDefinedInGroup.contains(variable);
      });
}

// Expect that planning the `query` throws a `NotSupportedException` with the
// given `message`.
void expectNotSupported(
    std::string query, std::string_view message,
    ad_utility::source_location l = AD_CURRENT_SOURCE_LOC()) {
  auto trace = generateLocationTrace(l);
  AD_EXPECT_THROW_WITH_MESSAGE_AND_TYPE(
      h::parseAndPlan(std::move(query), ad_utility::testing::getQec()),
      HasSubstr(message), NotSupportedException);
}

// Return the warnings of the plan for the `query`.
std::vector<std::string> getWarnings(std::string query) {
  return h::parseAndPlan(std::move(query), ad_utility::testing::getQec())
      ->getRootOperation()
      ->collectWarnings();
}

// Expect that the plan for the `query` has a warning with the given `message`.
void expectWarning(std::string query, std::string_view message,
                   ad_utility::source_location l = AD_CURRENT_SOURCE_LOC()) {
  auto trace = generateLocationTrace(l);
  EXPECT_THAT(getWarnings(std::move(query)),
              ::testing::Contains(HasSubstr(message)));
}

constexpr std::string_view conditionMessage =
    "so QLever evaluates it on the content of the OPTIONAL alone";
constexpr std::string_view existsMessage =
    "inside of an OPTIONAL contains an EXISTS that uses";

}  // namespace

// _____________________________________________________________________________
TEST(OptionalJoinConditionPlanning, isPartOfOptionalJoinCondition) {
  Variable x{"?x"};
  Variable y{"?y"};
  Variable z{"?z"};
  // A variable that is only bound on the left side.
  EXPECT_TRUE(isCondition("?x <q> ?z FILTER(?y < ?z)", {x, y}, {x, z}));
  // A variable of the left side that might be UNDEF in the group.
  EXPECT_TRUE(isCondition("?x <q> ?z FILTER(?z < 5)", {x, z}, {x}));
  // All variables of the left side are always defined in the group.
  EXPECT_FALSE(isCondition("?x <q> ?z FILTER(?x != ?z)", {x, y}, {x, z}));
  // No variable of the left side, no matter whether the variables are defined
  // in the group.
  EXPECT_FALSE(isCondition("?x <q> ?z FILTER(?y < ?z)", {x}, {x, z}));
  EXPECT_FALSE(isCondition("?x <q> ?z FILTER(?y < ?z)", {}, {}));
  EXPECT_FALSE(isCondition("FILTER(RAND() < 0.5)", {x, y}, {}));
}

// _____________________________________________________________________________
TEST(OptionalJoinConditionPlanning, isPartOfOptionalJoinConditionWithExists) {
  Variable x{"?x"};
  Variable y{"?y"};
  Variable z{"?z"};
  auto expectThrow = [](const std::string& group, const Variables& left,
                        const Variables& alwaysDefined) {
    AD_EXPECT_THROW_WITH_MESSAGE_AND_TYPE(
        isCondition(group, left, alwaysDefined), HasSubstr(existsMessage),
        NotSupportedException);
  };
  std::string exists = "?x <q> ?z FILTER EXISTS { ?z <r> ?y }";
  // The argument of the `EXISTS` only uses variables that have the same value
  // in the group and in the joined row.
  EXPECT_FALSE(isCondition(exists, {x}, {x, z}));
  EXPECT_FALSE(isCondition(exists, {x, z}, {x, z}));
  EXPECT_FALSE(isCondition(exists, {}, {}));
  // `?y` is only bound on the left side. It is not among the contained
  // variables of the FILTER, because it is not visible in the group.
  expectThrow(exists, {x, y}, {x, z});
  // `?z` might be UNDEF in the group.
  expectThrow(exists, {x, z}, {x});
  // The `EXISTS` is only a part of the FILTER.
  expectThrow("?x <q> ?z FILTER(?z < 5 && !EXISTS { ?z <r> ?y })", {y}, {x, z});
}

// _____________________________________________________________________________
TEST(OptionalJoinConditionPlanning, filterStaysInGroup) {
  auto scan = h::IndexScanFromStrings;
  // `?x` is bound on the left side, but also always bound in the group, and
  // `?z` is not bound on the left side.
  h::expect("SELECT * { ?x <p> ?y OPTIONAL { ?x <q> ?z FILTER(?x != ?z) } }",
            h::OptionalJoin(scan("?x", "<p>", "?y"),
                            h::Filter("?x != ?z", scan("?x", "<q>", "?z"))));
  auto expectNoWarning = [](std::string query, ad_utility::source_location l =
                                                   AD_CURRENT_SOURCE_LOC()) {
    auto trace = generateLocationTrace(l);
    EXPECT_THAT(getWarnings(std::move(query)),
                ::testing::Not(
                    ::testing::Contains(HasSubstr("inside of an OPTIONAL"))));
  };
  // `?y` is always bound by the `VALUES` or the `BIND` of the group.
  expectNoWarning(
      "SELECT * { ?x <p> ?y OPTIONAL { VALUES ?y { 1 2 } ?x <q> ?z "
      "FILTER(?y < ?z) } }");
  expectNoWarning(
      "SELECT * { ?x <p> ?y OPTIONAL { ?x <q> ?z BIND(?z AS ?y) "
      "FILTER(?y < 5) } }");
  // There is no left side.
  expectNoWarning("SELECT * { OPTIONAL { ?x <q> ?z FILTER(?y < ?z) } }");
  expectNoWarning(
      "SELECT * { OPTIONAL { ?x <q> ?z FILTER(?y < ?z) } ?x <p> ?y }");
  // `?w` is not bound at all.
  expectNoWarning(
      "SELECT * { ?x <p> ?y OPTIONAL { ?x <q> ?z FILTER(?w < ?z) } }");
  // The FILTER belongs to a nested group and not directly to the OPTIONAL.
  expectNoWarning(
      "SELECT * { ?x <p> ?y OPTIONAL { { ?x <q> ?z FILTER(?y < ?z) } } }");
  // Inside `GRAPH ?g`, the triple of the group always binds `?g`.
  expectNoWarning(
      "SELECT * { GRAPH ?g { ?x <p> ?y OPTIONAL { ?x <q> ?z "
      "FILTER(?g != <a>) } } }");
  // An `EXISTS` whose variables are all always bound in the group.
  expectNoWarning(
      "SELECT * { ?x <p> ?y OPTIONAL { ?x <q> ?z FILTER NOT EXISTS "
      "{ ?x <r> ?z } } }");
}

// _____________________________________________________________________________
TEST(OptionalJoinConditionPlanning, conditionIsAppliedToGroupWithWarning) {
  auto scan = h::IndexScanFromStrings;
  // `?y` is only bound on the left side. The FILTER is applied to the group,
  // after all other FILTERs, like before the condition was determined.
  h::expect(
      "SELECT * { ?x <p> ?y OPTIONAL { ?x <q> ?z FILTER(?y < ?z) } }",
      h::QetWithWarnings(
          {std::string{conditionMessage}},
          h::OptionalJoin(scan("?x", "<p>", "?y"),
                          h::Filter("?y < ?z", scan("?x", "<q>", "?z")))));
  h::expect(
      "SELECT * { ?x <p> ?y OPTIONAL { ?x <q> ?z FILTER(?y < ?z) "
      "FILTER(?z > 3) } }",
      h::QetWithWarnings(
          {std::string{conditionMessage}},
          h::OptionalJoin(
              scan("?x", "<p>", "?y"),
              h::Filter("?y < ?z",
                        h::Filter("?z > 3", scan("?x", "<q>", "?z"))))));
  // The same for an OPTIONAL that shares no variable with the left side, and
  // for an OPTIONAL with an empty group.
  expectWarning("SELECT * { ?x <p> ?y OPTIONAL { ?a <q> ?z FILTER(?y < ?z) } }",
                conditionMessage);
  expectWarning("SELECT * { ?x <p> ?y OPTIONAL { FILTER(?y < 5) } }",
                conditionMessage);
  // `?y` might be UNDEF in the group.
  expectWarning(
      "SELECT * { ?x <p> ?y OPTIONAL { ?x <q> ?z OPTIONAL { ?z <r> ?y } "
      "FILTER(?y < 5) } }",
      conditionMessage);
  expectWarning(
      "SELECT * { ?x <p> ?y OPTIONAL { VALUES ?y { 1 UNDEF } ?x <q> ?z "
      "FILTER(?y < ?z) } }",
      conditionMessage);
  // The left side is a previous OPTIONAL.
  expectWarning(
      "SELECT * { ?x <p> ?y OPTIONAL { ?x <q> ?z } OPTIONAL { ?x <r> ?w "
      "FILTER(?z < ?w) } }",
      conditionMessage);
  // Inside `GRAPH ?g`, the left side binds `?g`, but the group doesn't.
  expectWarning(
      "SELECT * { GRAPH ?g { ?x <p> ?y OPTIONAL { VALUES ?z { 1 } "
      "FILTER(?g != <a>) } } }",
      conditionMessage);
  // A FILTER that can be replaced by a `SpatialJoin`, which is already done
  // when one of its variables is bound in the group. The plans with the
  // `SpatialJoin` keep it, the other plans get the FILTER.
  EXPECT_NO_THROW(h::parseAndPlan(
      "PREFIX geof: <http://www.opengis.net/def/function/geosparql/> "
      "SELECT * { ?x <p> ?y OPTIONAL { ?x <q> ?b "
      "FILTER(geof:distance(?y, ?b) <= 0.5) } }",
      ad_utility::testing::getQec()));
}

// _____________________________________________________________________________
TEST(OptionalJoinConditionPlanning, existsThatDependsOnLeftSide) {
  // `?y` is only bound on the left side.
  expectNotSupported(
      "SELECT * { ?x <p> ?y OPTIONAL { ?x <q> ?z FILTER EXISTS { ?z <r> ?y } "
      "} }",
      existsMessage);
  expectNotSupported(
      "SELECT * { ?x <p> ?y OPTIONAL { ?x <q> ?z FILTER(?z < 5 && NOT EXISTS "
      "{ ?z <r> ?y }) } }",
      existsMessage);
  // `?y` might be UNDEF in the group.
  expectNotSupported(
      "SELECT * { ?x <p> ?y OPTIONAL { ?x <q> ?z OPTIONAL { ?z <r> ?y } "
      "FILTER EXISTS { ?y <s> ?w } } }",
      existsMessage);
}

// _____________________________________________________________________________
TEST(OptionalJoinConditionPlanning, messages) {
  // The messages name the FILTER and the variables that make it part of the
  // condition, but not the other variables, and the rewrite that gives the
  // result of the SPARQL standard.
  auto query = "SELECT * { ?x <p> ?y OPTIONAL { ?x <q> ?z FILTER(?y < ?z) } }";
  EXPECT_THAT(
      getWarnings(query),
      ::testing::ElementsAre(
          "The FILTER (?y < ?z) inside of an OPTIONAL uses ?y, which is bound "
          "outside of the OPTIONAL but not always inside of it. The SPARQL "
          "standard evaluates such a FILTER on the joined rows, which is not "
          "supported yet, so QLever evaluates it on the content of the "
          "OPTIONAL alone. To get the standard result, repeat the patterns "
          "that bind ?y outside of the OPTIONAL also inside of it."));
  expectWarning(
      "SELECT * { ?x <p> ?y . ?x <r> ?w OPTIONAL { ?x <q> ?z "
      "FILTER(?y < ?w || ?y = ?z) } }",
      "uses ?y, ?w, which are bound outside of the OPTIONAL but not always "
      "inside of it. The SPARQL standard evaluates such a FILTER on the joined "
      "rows, which is not supported yet, so QLever evaluates it on the content "
      "of the OPTIONAL alone. To get the standard result, repeat the patterns "
      "that bind ?y, ?w outside of the OPTIONAL also inside of it.");
  expectNotSupported(
      "SELECT * { ?x <p> ?y OPTIONAL { ?x <q> ?z FILTER EXISTS { ?z <r> ?y } "
      "} }",
      "The FILTER EXISTS { ?z <r> ?y } inside of an OPTIONAL contains an "
      "EXISTS that uses ?y, which is bound outside of the OPTIONAL but not "
      "always inside of it. The SPARQL standard evaluates such an EXISTS on "
      "the joined rows, which is not supported yet. To get the standard "
      "result, repeat the patterns that bind ?y outside of the OPTIONAL also "
      "inside of it.");
}
