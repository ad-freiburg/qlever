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
#include "engine/operation/OptionalJoin.h"
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

constexpr std::string_view crossProductMessage =
    "is evaluated by comparing all pairs of rows (Cartesian product)";
constexpr std::string_view existsMessage =
    "inside of an OPTIONAL contains an EXISTS that uses";

}  // namespace

// Test that `isPartOfOptionalJoinCondition` is true exactly for a FILTER that
// uses a variable of the left side that is not always defined in the group.
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

// Test that `isPartOfOptionalJoinCondition` throws if the argument of an
// `EXISTS` uses a variable of the left side that is not always defined in the
// group.
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

// Test that a FILTER that is not part of the condition is applied to the group
// of the OPTIONAL without a warning.
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

// Test that a FILTER of the condition is passed to the `OptionalJoin`, which
// evaluates it on the joined rows, without a warning.
TEST(OptionalJoinConditionPlanning, conditionIsPassedToOptionalJoin) {
  // Expect that the root of the plan for the `query` is an `OptionalJoin` with
  // the `condition` (the expressions as written in the FILTERs), and that
  // there is no warning.
  auto expectCondition = [](std::string query,
                            std::vector<std::string> condition,
                            ad_utility::source_location l =
                                AD_CURRENT_SOURCE_LOC()) {
    auto trace = generateLocationTrace(l);
    SCOPED_TRACE(query);
    auto qet = h::parseAndPlan(std::move(query), ad_utility::testing::getQec());
    auto join =
        std::dynamic_pointer_cast<OptionalJoin>(qet->getRootOperation());
    ASSERT_NE(join, nullptr);
    // The descriptor of the expression of a FILTER is in parentheses.
    std::vector<std::string> descriptors;
    for (const auto& expression : join->condition()) {
      descriptors.push_back(expression.getDescriptor());
    }
    for (auto& expression : condition) {
      expression = absl::StrCat("(", expression, ")");
    }
    EXPECT_EQ(descriptors, condition);
    EXPECT_THAT(qet->collectWarnings(), ::testing::IsEmpty());
  };
  auto scan = h::IndexScanFromStrings;
  // `?y` is only bound on the left side.
  auto query = "SELECT * { ?x <p> ?y OPTIONAL { ?x <q> ?z FILTER(?y < ?z) } }";
  expectCondition(query, {"?y < ?z"});
  h::expect(query,
            h::OptionalJoin(scan("?x", "<p>", "?y"), scan("?x", "<q>", "?z")));
  // A FILTER that is not part of the condition is applied to the group.
  query =
      "SELECT * { ?x <p> ?y OPTIONAL { ?x <q> ?z FILTER(?y < ?z) "
      "FILTER(?z > 3) } }";
  expectCondition(query, {"?y < ?z"});
  h::expect(query,
            h::OptionalJoin(scan("?x", "<p>", "?y"),
                            h::Filter("?z > 3", scan("?x", "<q>", "?z"))));
  // Two FILTERs of the condition.
  expectCondition(
      "SELECT * { ?x <p> ?y OPTIONAL { ?x <q> ?z FILTER(?y < ?z) "
      "FILTER(?y > 3) } }",
      {"?y < ?z", "?y > 3"});
  // `?y` might be UNDEF in the group, so it is a second join column, which
  // might be UNDEF on the right side.
  expectCondition(
      "SELECT * { ?x <p> ?y OPTIONAL { ?x <q> ?z OPTIONAL { ?z <r> ?y } "
      "FILTER(?y < 5) } }",
      {"?y < 5"});
  expectCondition(
      "SELECT * { ?x <p> ?y OPTIONAL { VALUES ?y { 1 UNDEF } ?x <q> ?z "
      "FILTER(?y < ?z) } }",
      {"?y < ?z"});
  // The left side is a previous OPTIONAL, so `?z` might be UNDEF there.
  expectCondition(
      "SELECT * { ?x <p> ?y OPTIONAL { ?x <q> ?z } OPTIONAL { ?x <r> ?w "
      "FILTER(?z < ?w) } }",
      {"?z < ?w"});
  // The join variable `?z` might be UNDEF on the left side.
  expectCondition(
      "SELECT * { ?x <p> ?y OPTIONAL { ?x <q> ?z } OPTIONAL { ?z <r> ?w "
      "FILTER(?y < ?w) } }",
      {"?y < ?w"});
  // Two join columns, `?x` and `?p`.
  expectCondition("SELECT * { ?x ?p ?y OPTIONAL { ?x ?p ?z FILTER(?y < ?z) } }",
                  {"?y < ?z"});
  // A FILTER that can be replaced by a `SpatialJoin`, see the NOTE in
  // `QueryPlanner::applyRemainingFilters`. If `?y` doesn't occur in the group,
  // the `SpatialJoin` can't be completed there, so the FILTER is part of the
  // condition.
  std::string geof =
      "PREFIX geof: <http://www.opengis.net/def/function/geosparql/> ";
  expectCondition(geof +
                      "SELECT * { ?x <p> ?y OPTIONAL { ?x <q> ?b "
                      "FILTER(geof:distance(?y, ?b) <= 0.5) } }",
                  {"geof:distance(?y, ?b) <= 0.5"});
  expectCondition(geof +
                      "SELECT * { ?x <p> ?y OPTIONAL { ?x <q> ?b . ?b <r> ?c "
                      "FILTER(geof:distance(?y, ?b) <= 0.5) } }",
                  {"geof:distance(?y, ?b) <= 0.5"});
  // If `?y` occurs in the group, but might be UNDEF there, the `SpatialJoin`
  // can be completed in the group. Then the FILTER is evaluated on the content
  // of the group like before, without a warning.
  auto qet = h::parseAndPlan(
      geof +
          "SELECT * { ?x <p> ?y OPTIONAL { ?x <q> ?b VALUES ?y { <a> UNDEF } "
          "FILTER(geof:distance(?y, ?b) <= 0.5) } }",
      ad_utility::testing::getQec());
  EXPECT_THAT(qet->getCacheKey(), HasSubstr("SpatialJoin"));
  EXPECT_THAT(qet->getRootOperation()->collectWarnings(), ::testing::IsEmpty());
}

// Test that an OPTIONAL without a shared variable is planned as an
// `OptionalJoin` without join columns if it has a condition, with a warning
// about the Cartesian product.
TEST(OptionalJoinConditionPlanning, conditionWithoutSharedVariable) {
  // Expect that the root of the plan for `query` is an `OptionalJoin` without
  // join columns and with the `condition`, and that there is the warning about
  // the Cartesian product.
  auto expectCrossProduct = [](std::string query, std::string_view condition,
                               ad_utility::source_location l =
                                   AD_CURRENT_SOURCE_LOC()) {
    auto trace = generateLocationTrace(l);
    auto qet = h::parseAndPlan(std::move(query), ad_utility::testing::getQec());
    auto join =
        std::dynamic_pointer_cast<OptionalJoin>(qet->getRootOperation());
    ASSERT_NE(join, nullptr);
    EXPECT_THAT(join->getDescriptor(), HasSubstr("Cartesian product"));
    ASSERT_EQ(join->condition().size(), 1);
    EXPECT_THAT(join->condition().at(0).getDescriptor(), HasSubstr(condition));
    EXPECT_THAT(qet->collectWarnings(),
                ::testing::Contains(HasSubstr(crossProductMessage)));
  };
  auto scan = h::IndexScanFromStrings;
  // A FILTER that only uses variables of the group stays in the group.
  h::expect(
      "SELECT * { ?x <p> ?y OPTIONAL { ?a <q> ?z FILTER(?y < ?z) "
      "FILTER(?z > 3) } }",
      h::QetWithWarnings(
          {std::string{crossProductMessage}},
          h::OptionalJoin(scan("?x", "<p>", "?y"),
                          h::Filter("?z > 3", scan("?a", "<q>", "?z")))));
  expectCrossProduct(
      "SELECT * { ?x <p> ?y OPTIONAL { ?a <q> ?z FILTER(?y < ?z) } }",
      "?y < ?z");
  // An empty group, and a group that only consists of a MINUS, which is
  // planned like an empty group (with the warning that the MINUS is ignored).
  expectCrossProduct("SELECT * { ?x <p> ?y OPTIONAL { FILTER(?y < 5) } }",
                     "?y < 5");
  expectCrossProduct(
      "SELECT * { ?x <p> ?y OPTIONAL { MINUS { ?x <q> ?z } FILTER(?y != <a>) "
      "} }",
      "?y != <a>");
  // Inside `GRAPH ?g`, the left side binds `?g`, but the group doesn't.
  expectCrossProduct(
      "SELECT * { GRAPH ?g { ?x <p> ?y OPTIONAL { VALUES ?z { 1 } "
      "FILTER(?g != <a>) } } }",
      "?g != <a>");
  // Without a condition, there is no warning (the OPTIONAL is computed with a
  // `NeutralOptional`).
  EXPECT_THAT(
      getWarnings(
          "SELECT * { ?x <p> ?y OPTIONAL { ?a <q> ?z FILTER(?z > 3) } }"),
      ::testing::IsEmpty());
}

// Test that a query is rejected if the argument of an `EXISTS` in a FILTER of
// an OPTIONAL depends on the left side.
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

// Test the exact messages of the warning and of the exception.
TEST(OptionalJoinConditionPlanning, messages) {
  EXPECT_THAT(
      getWarnings(
          "SELECT * { ?x <p> ?y OPTIONAL { ?a <q> ?z FILTER(?y < ?z) } }"),
      ::testing::ElementsAre(
          "An OPTIONAL clause that shares no variables with the patterns "
          "before it, but whose FILTER uses variables of these patterns, is "
          "evaluated by comparing all pairs of rows (Cartesian product). This "
          "can be very slow for large inputs."));
  // The message for an EXISTS names the FILTER and the variables that are
  // bound outside of the OPTIONAL.
  AD_EXPECT_THROW_WITH_MESSAGE_AND_TYPE(
      h::parseAndPlan("SELECT * { ?x <p> ?y OPTIONAL { ?x <q> ?z FILTER EXISTS "
                      "{ ?z <r> ?y } } }",
                      ad_utility::testing::getQec()),
      ::testing::EndsWith(
          "The FILTER EXISTS { ?z <r> ?y } inside of an OPTIONAL contains an "
          "EXISTS that uses ?y, which is bound outside of the OPTIONAL but not "
          "always inside of it. The SPARQL standard evaluates such an EXISTS "
          "on the joined rows, which is not supported yet."),
      NotSupportedException);
}
