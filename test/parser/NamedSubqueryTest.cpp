// Copyright 2026, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Hannah Bast <bast@cs.uni-freiburg.de>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "../QueryPlannerTestHelpers.h"
#include "../util/GTestHelpers.h"
#include "../util/IndexTestHelpers.h"
#include "parser/SparqlParser.h"

namespace {

// Parse the given query and return the resulting `ParsedQuery`.
ParsedQuery parse(std::string query) {
  static EncodedIriManager encodedIriManager;
  return SparqlParser::parseQuery(&encodedIriManager, std::move(query));
}

// The dataset for the equivalence tests below.
constexpr std::string_view testTurtle =
    "<a> <p> <b> . <a> <p> <c> . <b> <p> <c> . "
    "<a> <q> <d> . <b> <q> <e> . <d> <q> <a> .";

// Compute the result of the given query execution tree and return it in a
// canonical form that is independent of the column order and the row order:
// one string per row, with the values ordered by variable name, and the rows
// sorted.
std::vector<std::string> canonicalResult(const QueryExecutionTree& tree) {
  auto result = tree.getResult();
  const auto& table = result->idTableView();
  std::vector<std::pair<std::string, ColumnIndex>> varsAndColumns;
  for (const auto& [variable, columnInfo] : tree.getVariableColumns()) {
    varsAndColumns.emplace_back(variable.name(), columnInfo.columnIndex_);
  }
  ql::ranges::sort(varsAndColumns);
  std::vector<std::string> rows;
  for (size_t i = 0; i < table.numRows(); ++i) {
    std::string row;
    for (const auto& [variableName, column] : varsAndColumns) {
      absl::StrAppend(&row, variableName, "=", table(i, column).getBits(), " ");
    }
    rows.push_back(std::move(row));
  }
  ql::ranges::sort(rows);
  return rows;
}

// Expect that the two given queries (one with named subqueries and its manual
// expansion) yield exactly the same result on the test dataset. Also expect a
// nonempty result, so that the equivalence check cannot pass vacuously.
void expectEquivalent(
    std::string queryWithNamedSubqueries, std::string expandedQuery,
    ad_utility::source_location l = ad_utility::source_location::current()) {
  auto trace = generateLocationTrace(l);
  auto* qec = ad_utility::testing::getQec(std::string{testTurtle});
  auto treeA = queryPlannerTestHelpers::parseAndPlan(
      std::move(queryWithNamedSubqueries), qec);
  auto treeB =
      queryPlannerTestHelpers::parseAndPlan(std::move(expandedQuery), qec);
  auto rowsA = canonicalResult(treeA);
  EXPECT_GT(rowsA.size(), 0u);
  EXPECT_EQ(rowsA, canonicalResult(treeB));
}

}  // namespace

// _____________________________________________________________________________
TEST(NamedSubquery, simpleInclude) {
  expectEquivalent(
      "WITH %sub AS { SELECT ?s WHERE { ?s ?p ?o } }"
      "SELECT * WHERE { INCLUDE %sub }",
      "SELECT * WHERE { { SELECT ?s WHERE { ?s ?p ?o } } }");
}

// _____________________________________________________________________________
TEST(NamedSubquery, includeIsJoinedWithSiblingPatterns) {
  expectEquivalent(
      "WITH %sub AS { SELECT ?s WHERE { ?s <p> ?o } }"
      "SELECT * WHERE { INCLUDE %sub . ?s <q> ?t }",
      "SELECT * WHERE { { SELECT ?s WHERE { ?s <p> ?o } } ?s <q> ?t }");
}

// _____________________________________________________________________________
TEST(NamedSubquery, multipleIncludesAndRenaming) {
  // The classical self-join: the same named subquery is included twice, the
  // second time with all variables renamed.
  expectEquivalent(
      "WITH %sub AS { SELECT ?s ?o WHERE { ?s ?p ?o } }"
      "SELECT * WHERE { INCLUDE %sub . INCLUDE %sub (?s AS ?s2) (?o AS ?o2) ."
      " FILTER (?o2 > ?o) }",
      "SELECT * WHERE { { SELECT ?s ?o WHERE { ?s ?p ?o } }"
      " { SELECT (?s AS ?s2) (?o AS ?o2) WHERE {"
      " { SELECT ?s ?o WHERE { ?s ?p ?o } } } } FILTER (?o2 > ?o) }");
}

// _____________________________________________________________________________
TEST(NamedSubquery, partialRenaming) {
  // Variables that are not renamed keep their name and hence join with the
  // other occurrence of the named subquery.
  expectEquivalent(
      "WITH %sub AS { SELECT ?s ?o WHERE { ?s ?p ?o } }"
      "SELECT * WHERE { INCLUDE %sub . INCLUDE %sub (?o AS ?o2) }",
      "SELECT * WHERE { { SELECT ?s ?o WHERE { ?s ?p ?o } }"
      " { SELECT ?s (?o AS ?o2) WHERE {"
      " { SELECT ?s ?o WHERE { ?s ?p ?o } } } } }");
}

// _____________________________________________________________________________
TEST(NamedSubquery, includeInLaterDefinition) {
  // A named subquery can include previously defined named subqueries.
  expectEquivalent(
      "WITH %a AS { SELECT ?s WHERE { ?s <p> ?o } }"
      "WITH %b AS { SELECT ?s WHERE { INCLUDE %a . ?s <q> ?t } }"
      "SELECT * WHERE { INCLUDE %b }",
      "SELECT * WHERE { { SELECT ?s WHERE {"
      " { SELECT ?s WHERE { ?s <p> ?o } } ?s <q> ?t } } }");
}

// _____________________________________________________________________________
TEST(NamedSubquery, includeInUnionAndOptional) {
  expectEquivalent(
      "WITH %sub AS { SELECT ?s WHERE { ?s <p> ?o } }"
      "SELECT * WHERE { { INCLUDE %sub } UNION { ?s <q> ?t } }",
      "SELECT * WHERE { { { SELECT ?s WHERE { ?s <p> ?o } } }"
      " UNION { ?s <q> ?t } }");
  expectEquivalent(
      "WITH %sub AS { SELECT ?s WHERE { ?s <p> ?o } }"
      "SELECT * WHERE { ?s <q> ?t OPTIONAL { INCLUDE %sub } }",
      "SELECT * WHERE { ?s <q> ?t"
      " OPTIONAL { { SELECT ?s WHERE { ?s <p> ?o } } } }");
}

// _____________________________________________________________________________
TEST(NamedSubquery, aggregationInsideDefinition) {
  // A named subquery with GROUP BY and an alias, where the alias target is
  // renamed at the use site.
  expectEquivalent(
      "WITH %sub AS { SELECT ?s (COUNT(?o) AS ?cnt) WHERE { ?s ?p ?o }"
      " GROUP BY ?s }"
      "SELECT * WHERE { INCLUDE %sub (?cnt AS ?numObjects) }",
      "SELECT * WHERE { { SELECT ?s (?cnt AS ?numObjects) WHERE {"
      " { SELECT ?s (COUNT(?o) AS ?cnt) WHERE { ?s ?p ?o } GROUP BY ?s }"
      " } } }");
}

// _____________________________________________________________________________
TEST(NamedSubquery, invalidQueries) {
  // Reference to an undefined named subquery.
  AD_EXPECT_THROW_WITH_MESSAGE(
      parse("SELECT * WHERE { INCLUDE %nope }"),
      ::testing::HasSubstr("\"%nope\" is not defined"));
  // Named subqueries must be defined before their use, so a definition cannot
  // include a named subquery that is defined later.
  AD_EXPECT_THROW_WITH_MESSAGE(
      parse("WITH %a AS { SELECT ?s WHERE { INCLUDE %b } }"
            "WITH %b AS { SELECT ?s WHERE { ?s ?p ?o } }"
            "SELECT * WHERE { INCLUDE %a }"),
      ::testing::HasSubstr("\"%b\" is not defined"));
  // Duplicate definition.
  AD_EXPECT_THROW_WITH_MESSAGE(
      parse("WITH %a AS { SELECT ?s WHERE { ?s ?p ?o } }"
            "WITH %a AS { SELECT ?s WHERE { ?s ?p ?o } }"
            "SELECT * WHERE { INCLUDE %a }"),
      ::testing::HasSubstr("\"%a\" is defined more than once"));
  // Renaming of a variable that the named subquery does not select.
  AD_EXPECT_THROW_WITH_MESSAGE(
      parse("WITH %a AS { SELECT ?s WHERE { ?s ?p ?o } }"
            "SELECT * WHERE { INCLUDE %a (?x AS ?y) }"),
      ::testing::HasSubstr("?x cannot be renamed"));
  // Renaming the same variable twice.
  AD_EXPECT_THROW_WITH_MESSAGE(
      parse("WITH %a AS { SELECT ?s WHERE { ?s ?p ?o } }"
            "SELECT * WHERE { INCLUDE %a (?s AS ?x) (?s AS ?y) }"),
      ::testing::HasSubstr("?s is renamed more than once"));
  // Renaming that makes two variables collide.
  AD_EXPECT_THROW_WITH_MESSAGE(
      parse("WITH %a AS { SELECT ?s ?o WHERE { ?s ?p ?o } }"
            "SELECT * WHERE { INCLUDE %a (?s AS ?o) }"),
      ::testing::HasSubstr("not distinct anymore after the renaming"));
  // A VALUES clause at the end of a definition is currently not supported.
  AD_EXPECT_THROW_WITH_MESSAGE(
      parse("WITH %a AS { SELECT ?s WHERE { ?s ?p ?o } VALUES ?s { <x> } }"
            "SELECT * WHERE { INCLUDE %a }"),
      ::testing::HasSubstr("VALUES clause at the end of the named subquery"));
}
