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
void expectEquivalent(std::string queryWithNamedSubqueries,
                      std::string expandedQuery,
                      ad_utility::source_location l = AD_CURRENT_SOURCE_LOC()) {
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
      "WITH %sub AS { ?s ?p ?o }"
      "SELECT * WHERE { { SELECT ?s WHERE { INCLUDE %sub } } }",
      "SELECT * WHERE { { SELECT ?s WHERE { ?s ?p ?o } } }");
}

// _____________________________________________________________________________
TEST(NamedSubquery, includeIsJoinedWithSiblingPatterns) {
  expectEquivalent(
      "WITH %sub AS { ?s <p> ?o }"
      "SELECT * WHERE { { SELECT ?s WHERE { INCLUDE %sub } } ?s <q> ?t }",
      "SELECT * WHERE { { SELECT ?s WHERE { ?s <p> ?o } } ?s <q> ?t }");
}

// _____________________________________________________________________________
TEST(NamedSubquery, multipleIncludesAndRenaming) {
  // The classical self-join: the same named subquery is included twice, the
  // second time with all variables renamed via the SELECT clause of the
  // subquery around the `INCLUDE`.
  expectEquivalent(
      "WITH %sub AS { ?s ?p ?o }"
      "SELECT * WHERE {"
      " { SELECT ?s ?o WHERE { INCLUDE %sub } }"
      " { SELECT (?s AS ?s2) (?o AS ?o2) WHERE { INCLUDE %sub } }"
      " FILTER (?o2 > ?o) }",
      "SELECT * WHERE {"
      " { SELECT ?s ?o WHERE { ?s ?p ?o } }"
      " { SELECT (?s AS ?s2) (?o AS ?o2) WHERE { ?s ?p ?o } }"
      " FILTER (?o2 > ?o) }");
}

// _____________________________________________________________________________
TEST(NamedSubquery, partialRenaming) {
  // Variables that are selected without renaming keep their name and hence
  // join with the other occurrence of the named subquery.
  expectEquivalent(
      "WITH %sub AS { ?s ?p ?o }"
      "SELECT * WHERE {"
      " { SELECT ?s ?o WHERE { INCLUDE %sub } }"
      " { SELECT ?s (?o AS ?o2) WHERE { INCLUDE %sub } } }",
      "SELECT * WHERE {"
      " { SELECT ?s ?o WHERE { ?s ?p ?o } }"
      " { SELECT ?s (?o AS ?o2) WHERE { ?s ?p ?o } } }");
}

// _____________________________________________________________________________
TEST(NamedSubquery, includeInLaterDefinition) {
  // A named subquery can include previously defined named subqueries.
  expectEquivalent(
      "WITH %a AS { ?s <p> ?o }"
      "WITH %b AS { { SELECT ?s WHERE { INCLUDE %a } } ?s <q> ?t }"
      "SELECT * WHERE { { SELECT ?s ?t WHERE { INCLUDE %b } } }",
      "SELECT * WHERE { { SELECT ?s ?t WHERE {"
      " { SELECT ?s WHERE { ?s <p> ?o } } ?s <q> ?t } } }");
}

// _____________________________________________________________________________
TEST(NamedSubquery, includeInUnionAndOptional) {
  expectEquivalent(
      "WITH %sub AS { ?s <p> ?o }"
      "SELECT * WHERE {"
      " { SELECT ?s WHERE { INCLUDE %sub } } UNION { ?s <q> ?t } }",
      "SELECT * WHERE {"
      " { SELECT ?s WHERE { ?s <p> ?o } } UNION { ?s <q> ?t } }");
  expectEquivalent(
      "WITH %sub AS { ?s <p> ?o }"
      "SELECT * WHERE { ?s <q> ?t"
      " OPTIONAL { { SELECT ?s ?o WHERE { INCLUDE %sub } } } }",
      "SELECT * WHERE { ?s <q> ?t"
      " OPTIONAL { { SELECT ?s ?o WHERE { ?s <p> ?o } } } }");
}

// _____________________________________________________________________________
TEST(NamedSubquery, aggregationInWrappingSubquery) {
  // The subquery around an `INCLUDE` can aggregate directly, no additional
  // nesting is needed.
  expectEquivalent(
      "WITH %sub AS { ?s ?p ?o }"
      "SELECT * WHERE { { SELECT ?s (COUNT(?o) AS ?cnt) WHERE {"
      " INCLUDE %sub } GROUP BY ?s } }",
      "SELECT * WHERE { { SELECT ?s (COUNT(?o) AS ?cnt) WHERE {"
      " ?s ?p ?o } GROUP BY ?s } }");
}

// _____________________________________________________________________________
TEST(NamedSubquery, repeatedIncludeHasIdenticalCacheKey) {
  // Two identical subqueries around the same `INCLUDE` must be planned into
  // subtrees with identical cache keys, so that the result is computed only
  // once and the second occurrence is answered from the subtree cache.
  auto* qec = ad_utility::testing::getQec(std::string{testTurtle});
  auto tree = queryPlannerTestHelpers::parseAndPlan(
      "WITH %sub AS { ?s ?p ?o }"
      "SELECT * WHERE {"
      " { SELECT ?s (COUNT(?o) AS ?cnt) WHERE { INCLUDE %sub } GROUP BY ?s }"
      " UNION"
      " { SELECT ?s (COUNT(?o) AS ?cnt) WHERE { INCLUDE %sub } GROUP BY ?s }"
      " }",
      qec);
  // Descend to the `UNION` and compare the cache keys of its two children.
  const QueryExecutionTree* unionTree = &tree;
  while (unionTree->getRootOperation()->getChildren().size() == 1) {
    unionTree = unionTree->getRootOperation()->getChildren().at(0);
  }
  const auto& children = unionTree->getRootOperation()->getChildren();
  ASSERT_EQ(children.size(), 2u);
  EXPECT_EQ(children.at(0)->getCacheKey(), children.at(1)->getCacheKey());
}

namespace {
// Collect the warnings of the given query and of all its (transitive)
// subqueries.
std::vector<std::string> allWarnings(const ParsedQuery& query) {
  std::vector<std::string> warnings = query.warnings();
  for (const auto& operation : query._rootGraphPattern._graphPatterns) {
    if (auto* subquery = std::get_if<parsedQuery::Subquery>(&operation)) {
      ql::ranges::copy(allWarnings(subquery->get()),
                       std::back_inserter(warnings));
    }
  }
  return warnings;
}
}  // namespace

// _____________________________________________________________________________
TEST(NamedSubquery, noWarningsForRenaming) {
  // The `INCLUDE` must register the variables of the named subquery as
  // visible, otherwise QLever warns that the aliased variables are not
  // defined in the query body.
  auto query = parse(
      "WITH %sub AS { ?s ?p ?o }"
      "SELECT * WHERE {"
      " { SELECT (?s AS ?s2) (?o AS ?o2) WHERE { INCLUDE %sub } } }");
  EXPECT_THAT(allWarnings(query), ::testing::IsEmpty());
}

// _____________________________________________________________________________
TEST(NamedSubquery, invalidQueries) {
  // Reference to an undefined named subquery.
  AD_EXPECT_THROW_WITH_MESSAGE(
      parse("SELECT * WHERE { { SELECT ?s WHERE { INCLUDE %nope } } }"),
      ::testing::HasSubstr("\"%nope\" is not defined"));
  // Named subqueries must be defined before their use, so a definition cannot
  // include a named subquery that is defined later.
  AD_EXPECT_THROW_WITH_MESSAGE(
      parse("WITH %a AS { { SELECT ?s WHERE { INCLUDE %b } } }"
            "WITH %b AS { ?s ?p ?o }"
            "SELECT * WHERE { { SELECT ?s WHERE { INCLUDE %a } } }"),
      ::testing::HasSubstr("\"%b\" is not defined"));
  // Duplicate definition.
  AD_EXPECT_THROW_WITH_MESSAGE(
      parse("WITH %a AS { ?s ?p ?o }"
            "WITH %a AS { ?s ?p ?o }"
            "SELECT * WHERE { { SELECT ?s WHERE { INCLUDE %a } } }"),
      ::testing::HasSubstr("\"%a\" is defined more than once"));
  // Blazegraph's syntax for named subqueries, where the name comes after the
  // body, gets an informative error.
  AD_EXPECT_THROW_WITH_MESSAGE(
      parse("WITH { ?s ?p ?o } AS %a "
            "SELECT * WHERE { { SELECT ?s WHERE { INCLUDE %a } } }"),
      ::testing::HasSubstr(
          "The reverse order `WITH { ... } AS %a`, as used by Blazegraph"));
  // An `INCLUDE` is only allowed as the entire body of a subquery.
  AD_EXPECT_THROW_WITH_MESSAGE(
      parse("WITH %a AS { ?s ?p ?o }"
            "SELECT * WHERE { INCLUDE %a }"),
      ::testing::HasSubstr("must be the entire body of a subquery"));
  AD_EXPECT_THROW_WITH_MESSAGE(
      parse("WITH %a AS { ?s <p> ?o }"
            "SELECT * WHERE {"
            " { SELECT ?s WHERE { INCLUDE %a . ?s <q> ?t } } }"),
      ::testing::HasSubstr("must be the entire body of a subquery"));
  AD_EXPECT_THROW_WITH_MESSAGE(
      parse("WITH %a AS { ?s ?p ?o }"
            "SELECT * WHERE { OPTIONAL { INCLUDE %a } }"),
      ::testing::HasSubstr("must be the entire body of a subquery"));
  // The subquery around an `INCLUDE` must select its variables explicitly.
  AD_EXPECT_THROW_WITH_MESSAGE(
      parse("WITH %a AS { ?s ?p ?o }"
            "SELECT * WHERE { { SELECT * WHERE { INCLUDE %a } } }"),
      ::testing::HasSubstr("`SELECT *` is not allowed here"));
}
