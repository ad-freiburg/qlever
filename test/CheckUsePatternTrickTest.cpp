//  Copyright 2022, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include "./util/GTestHelpers.h"
#include "engine/CheckUsePatternTrick.h"
#include "gtest/gtest.h"
#include "parser/SparqlParser.h"
#include "util/SourceLocation.h"

using namespace checkUsePatternTrick;
using ad_utility::source_location;

// Parse the SPARQL query `SELECT * WHERE { <whereClause> }`. Not that the
// `whereClause` does not need to be enclosed by braces `{}`.
ParsedQuery parseWhereClause(const std::string& whereClause) {
  std::string query = absl::StrCat("SELECT * WHERE {", whereClause, "}");
  return SparqlParser::parseQuery(query);
}

// Test that `whereClause`, when parsed as the WHERE clause as a SPARQL query,
// contains the `variable`. If `shouldBeContained` is false, then the variable
// must NOT be contained in that where clause.
auto expectContained = [](const std::string& whereClause,
                          const std::string& variable,
                          bool shouldBeContained = true,
                          source_location l = source_location::current()) {
  auto trace = generateLocationTrace(l, "expectContained");
  auto pq = parseWhereClause(whereClause);
  bool b = isVariableContainedInGraphPattern(Variable{variable},
                                             pq._rootGraphPattern, nullptr);
  if (shouldBeContained) {
    EXPECT_TRUE(b);
  } else {
    EXPECT_FALSE(b);
  }
};

// Test that `whereClause`, when parsed as the WHERE clause as a SPARQL query,
// does not contain the `variable`.
auto expectNotContained = [](const std::string& whereClause,
                             const std::string& variable,
                             source_location l = source_location::current()) {
  auto trace = generateLocationTrace(l, "expectNotContained");
  expectContained(whereClause, variable, false);
};

// Assume that the WHERE clause of the `parsedQuery` starts with a triple (else
// throw an exception). Return a reference to this triple.
const SparqlTriple& getFirstTriple(const ParsedQuery& parsedQuery) {
  const auto& children = parsedQuery._rootGraphPattern._graphPatterns;
  AD_CHECK_GE(children.size(), 1);
  const auto& firstChild =
      std::get<parsedQuery::BasicGraphPattern>(children[0]);
  AD_CHECK_GE(firstChild._triples.size(), 1);
  return firstChild._triples[0];
}

// Test that `whereClause`, when parsed as the WHERE clause as a SPARQL query,
// contains the variables `?x`, `?y`, and `?z`, but not the variable `?not`
auto expectXYZContained(const std::string& whereClause,
                        source_location l = source_location::current()) {
  auto trace = generateLocationTrace(l, "expectContained");
  expectContained(whereClause, "?x");
  expectContained(whereClause, "?y");
  expectContained(whereClause, "?z");
  expectNotContained(whereClause, "?not");
}

TEST(CheckUsePatternTrick, isVariableContainedInGraphPattern) {
  expectXYZContained("?x ?y ?z");
  expectXYZContained("?x ?y <a>. <b> ?y ?z");
  expectXYZContained("?x <is-a> ?y. FILTER (?y > ?z)");
  expectXYZContained("OPTIONAL {?x ?y ?z}");
  expectXYZContained("MINUS {?x ?y ?z}");
  expectXYZContained("{{{?x ?y ?z}}}");
  expectXYZContained("{?x <is-a> ?y} UNION {?z <is-a> <something>}");
  expectXYZContained("?x <is-a> ?y {SELECT ?z WHERE {?z <is-a> ?not}}");
  expectXYZContained("BIND (3 AS ?x) . ?y <is-a> ?z");
  expectXYZContained("?x <is-a> ?z. BIND(?y AS ?t)");
  expectXYZContained("VALUES ?x {<a> <b>}. ?y <is-a> ?z");
  expectXYZContained("VALUES ?x {<a> <b>}. ?y <is-a> ?z");
}

TEST(CheckUsePatternTrick, isVariableContainedInGraphPatternIgnoredTriple) {
  auto pq = parseWhereClause("?x ?not ?z. ?x ?y ?z");
  // Get a pointer to the first triple.
  const auto* ignoredTriple = &getFirstTriple(pq);
  // The variable `?not` is containedd in the WHERE clause.
  ASSERT_TRUE(isVariableContainedInGraphPattern(Variable{"?not"},
                                                pq._rootGraphPattern, nullptr));
  // The variable `?not` is contained only in the second triple, which is
  // explicitly ignored.
  ASSERT_FALSE(isVariableContainedInGraphPattern(
      Variable{"?not"}, pq._rootGraphPattern, ignoredTriple));
}

// Expect that `query` is a valid SELECT query, the WHERE clause of which starts
// with a triple that is suitable for the pattern trick.
auto expectFirstTripleSuitableForPatternTrick =
    [](const std::string& query, const std::string& subjectVariable,
       const std::string& predicateVariable,
       const std::optional<
           sparqlExpression::SparqlExpressionPimpl::VariableAndDistinctness>&
           countedVariable = std::nullopt,
       bool shouldBeSuitable = true,
       source_location l = source_location::current()) {
      auto trace =
          generateLocationTrace(l, "expectFirstTripleSuitableForPatternTrick");
      auto pq = SparqlParser::parseQuery(query);
      const auto& firstTriple = getFirstTriple(pq);

      auto tripleSuitable =
          isTripleSuitableForPatternTrick(firstTriple, &pq, countedVariable);
      if (shouldBeSuitable) {
        ASSERT_TRUE(tripleSuitable.has_value());
        EXPECT_EQ(tripleSuitable->subject_, Variable{subjectVariable});
        EXPECT_EQ(tripleSuitable->predicate_, Variable{predicateVariable});
      } else {
        EXPECT_FALSE(tripleSuitable.has_value());
      }
    };

// Similar to the previous function, but expect that the first triple is NOT
// suitable for the pattern trick.
auto expectFirstTripleNotSuitableForPatternTrick =
    [](const std::string& query,
       const std::optional<
           sparqlExpression::SparqlExpressionPimpl::VariableAndDistinctness>&
           countedVariable = std::nullopt,
       source_location l = source_location::current()) {
      auto trace = generateLocationTrace(l, "firstTripleNotSuitable");
      expectFirstTripleSuitableForPatternTrick(query, "", "", countedVariable,
                                               false);
    };

TEST(CheckUsePatternTrick, isTripleSuitable) {
  auto expect = expectFirstTripleSuitableForPatternTrick;
  auto expectNot = expectFirstTripleNotSuitableForPatternTrick;
  expect("SELECT ?p WHERE {?s ql:has-predicate ?p} GROUP BY ?p", "?s", "?p");
  expect("SELECT ?p WHERE {?s ?p ?o} GROUP BY ?p", "?s", "?p");
  expect("SELECT ?p WHERE {?s ?p ?o . ?s <is-a> ?z} GROUP BY ?p", "?s", "?p");
  expect("SELECT ?p WHERE {?s ?p ?o . ?s <is-a> ?z} GROUP BY ?p", "?s", "?p");

  // Not the correct form of the triple
  expectNot("SELECT ?p WHERE {?p <is-a> ?o} GROUP BY ?p");
  expectNot("SELECT ?p WHERE {?s <is-a> ?p} GROUP BY ?p");

  // The variables in the pattern trick triple must be distinct.
  expectNot("SELECT ?p WHERE {?p ql:has-predicate ?p} GROUP BY ?p");
  expectNot("SELECT ?p WHERE {?s ?p ?p} GROUP BY ?p");
  expectNot("SELECT ?p WHERE {?s ?p ?s} GROUP BY ?p");
  expectNot("SELECT ?p WHERE {?p ?p ?s} GROUP BY ?p");

  // Predicate and object variable must not appear elsewhere in the query.
  expectNot("SELECT ?p WHERE {?s ?p ?o . ?p <is-a> ?z} GROUP BY ?p");
  expectNot("SELECT ?p WHERE {?s ?p ?o . ?o <is-a> ?z} GROUP BY ?p");
  expectNot(
      "SELECT ?p WHERE {?s ql:has-predicate ?p . ?p <is-a> ?z} GROUP BY ?p");

  // Wrong groupBy Variable
  expectNot("SELECT ?s WHERE {?s ql:has-predicate ?p} GROUP BY ?s");
  expectNot("SELECT ?s WHERE {?s ?p ?o} GROUP BY ?s");
  expectNot("SELECT ?o WHERE {?s ?p ?o} GROUP BY ?o");

  // Check for the cases with explicit COUNT variables.
  sparqlExpression::SparqlExpressionPimpl::VariableAndDistinctness
      variableAndDistinctness{Variable{"?s"}, true};
  expect("SELECT ?p WHERE {?s ql:has-predicate ?p} GROUP BY ?p", "?s", "?p",
         variableAndDistinctness);
  expect("SELECT ?p WHERE {?s ?p ?o} GROUP BY ?p", "?s", "?p",
         variableAndDistinctness);
  // Mismatch in counted variable: `?x` vs `?s`.
  expectNot("SELECT ?p WHERE {?x ?p ?o} GROUP BY ?p", variableAndDistinctness);

  // Currently the counted variable always has to be DISTINCT.
  variableAndDistinctness.isDistinct_ = false;
  expectNot("SELECT ?p WHERE {?s ?p ?o} GROUP BY ?p", variableAndDistinctness);
  // TODO<joka921> Add a test here as soon as we allow the non-distinct COUNT of
  // the `ql:has-predicate`.
  expectNot("SELECT ?p WHERE {?s ql:has-predicate ?p} GROUP BY ?p",
            variableAndDistinctness);
}

// Expect that the pattern trick can be applied to the given SPARQL query, and
// that the `subjectVariable` and `objectVariable` of the pattern trick match.
auto expectQuerySuitableForPatternTrick =
    [](const std::string& query, const std::string& subjectVariable,
       const std::string& predicateVariable, bool shouldBeSuitable = true,
       source_location l = source_location::current()) {
      auto trace = generateLocationTrace(l, "expectQuerySuitable");
      auto pq = SparqlParser::parseQuery(query);
      auto querySuitable = checkUsePatternTrick::checkUsePatternTrick(&pq);

      if (shouldBeSuitable) {
        ASSERT_TRUE(querySuitable.has_value());
        EXPECT_EQ(querySuitable->subject_, Variable{subjectVariable});
        EXPECT_EQ(querySuitable->predicate_, Variable{predicateVariable});
      } else {
        EXPECT_FALSE(querySuitable.has_value());
      }
    };

// Expect that the pattern trick cannot be applied to the given `query`.
auto expectQueryNotSuitableForPatternTrick =
    [](const std::string& query,
       source_location l = source_location::current()) {
      auto trace = generateLocationTrace(l, "expectQueryNotSuitable");
      expectQuerySuitableForPatternTrick(query, "", "", false);
    };

TEST(CheckUsePatternTrick, checkUsePatternTrick) {
  auto expect = expectQuerySuitableForPatternTrick;
  auto expectNot = expectQueryNotSuitableForPatternTrick;

  expect("SELECT ?p WHERE {?s ql:has-predicate ?p} GROUP BY ?p", "?s", "?p");
  expect(
      "SELECT ?p (COUNT(DISTINCT ?s) as ?cnt) WHERE {?s ql:has-predicate ?p} "
      "GROUP BY ?p",
      "?s", "?p");
  expect(
      "SELECT (COUNT(DISTINCT ?s) as ?cnt) WHERE {?s ql:has-predicate ?p} "
      "GROUP BY ?p",
      "?s", "?p");
  expect(
      "SELECT (COUNT(DISTINCT ?s) as ?cnt) ?p WHERE {?s ql:has-predicate ?p} "
      "GROUP BY ?p",
      "?s", "?p");
  expect("SELECT (COUNT(DISTINCT ?s) as ?cnt) ?p WHERE {?s ?p ?o} GROUP BY ?p",
         "?s", "?p");
  expect(
      "SELECT ?p WHERE {OPTIONAL {?s <is-a> ?y} ?s ql:has-predicate ?p} GROUP "
      "BY ?p",
      "?s", "?p");
  // TODO<joka921> Add tests for the not distinct ql:has-predicate case as soon
  // as we support that.

  // GROUP BY, but no suitable triple
  expectNot("SELECT ?p WHERE {?x <is-a> ?p } GROUP BY ?p");

  // More than one alias.
  expectNot(
      "SELECT (COUNT(DISTINCT ?s) as ?cnt) (MAX(?s) as ?max) WHERE {?s "
      "ql:has-predicate ?p} GROUP BY ?p");

  // More than one GroupBy variable
  expectNot("SELECT ?p WHERE {?s ql:has-predicate ?p} GROUP BY ?p ?s");

  // Wrong alias (not a count of a single variable)
  expectNot(
      "SELECT (MAX(?s) as ?max) WHERE {?s ql:has-predicate ?p} GROUP BY ?p");
  expectNot(
      "SELECT (COUNT(DISTINCT ?s + ?p) as ?cnt) WHERE {?s ql:has-predicate ?p} "
      "GROUP BY ?p");
}

TEST(CheckUsePatternTrick, tripleIsCorrectlyRemoved) {
  auto pq = SparqlParser::parseQuery(
      "SELECT ?p WHERE {?x ql:has-predicate ?p} GROUP BY ?p");
  auto patternTrickTuple = checkUsePatternTrick::checkUsePatternTrick(&pq);
  ASSERT_TRUE(patternTrickTuple.has_value());
  // The pattern trick triple has been removed from the query.
  const auto& triples = std::get<parsedQuery::BasicGraphPattern>(
                            pq._rootGraphPattern._graphPatterns.at(0))
                            ._triples;
  ASSERT_TRUE(triples.empty());

  pq = SparqlParser::parseQuery(
      "SELECT ?p WHERE {?x ql:has-predicate ?p . ?x <is-a> ?y } GROUP BY ?p");
  patternTrickTuple = checkUsePatternTrick::checkUsePatternTrick(&pq);
  ASSERT_TRUE(patternTrickTuple.has_value());
  // The pattern trick triple has been removed from the query.,
  const auto& triples2 = std::get<parsedQuery::BasicGraphPattern>(
                             pq._rootGraphPattern._graphPatterns.at(0))
                             ._triples;
  ASSERT_EQ(triples2.size(), 1u);
  const auto& triple = triples2[0];
  ASSERT_EQ(triple._s.getString(), "?x");
  ASSERT_EQ(triple._p.asString(), "<is-a>");
  ASSERT_EQ(triple._o.getString(), "?y");
}
