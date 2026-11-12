// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Prashanth Premakumar <prashanthp0703@gmail.com>, UFR

// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

// Tests for RDF 1.2 triple terms in SPARQL triple patterns: the parser replaces
// `<<( s p o )>>` by a fresh internal variable plus three pseudo-triples (see
// `TRIPLE_TERM_SUBJECT_PREDICATE` in `Constants.h`).

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <string>
#include <vector>

#include "QueryPlannerTestHelpers.h"
#include "engine/ExportQueryExecutionTrees.h"
#include "engine/QueryPlanner.h"
#include "global/Constants.h"
#include "index/ExportIds.h"
#include "parser/SparqlParser.h"
#include "util/GTestHelpers.h"
#include "util/IndexTestHelpers.h"
#include "util/Timer.h"
#include "util/json.h"

namespace {
using ::testing::HasSubstr;

ParsedQuery parse(std::string query) {
  EncodedIriManager manager;
  return SparqlParser::parseQuery(&manager, std::move(query));
}

// The triples of the single basic graph pattern of `query`.
std::vector<SparqlTriple> triplesOf(std::string query) {
  auto parsed = parse(std::move(query));
  EXPECT_EQ(parsed.children().size(), 1);
  return parsed.children().at(0).getBasic()._triples;
}

std::string predicateOf(const SparqlTriple& triple) {
  return std::string{triple.getSimplePredicate().value()};
}

// Return the components bound to the triple-term variable `term` by the
// pseudo-triples in `triples`, as strings in the order subject, predicate,
// object.
std::vector<std::string> componentsOf(const std::vector<SparqlTriple>& triples,
                                      const Variable& term) {
  std::vector<std::string> result;
  for (auto pseudoPredicate :
       {TRIPLE_TERM_SUBJECT_PREDICATE, TRIPLE_TERM_PREDICATE_PREDICATE,
        TRIPLE_TERM_OBJECT_PREDICATE}) {
    for (const auto& triple : triples) {
      if (triple.s_ == term && predicateOf(triple) == pseudoPredicate) {
        result.push_back(triple.o_.toString());
      }
    }
  }
  return result;
}
}  // namespace

// _____________________________________________________________________________
TEST(TripleTermSparql, ObjectIsReplacedByAFreshVariable) {
  auto triples = triplesOf(
      "SELECT * WHERE { ?report <http://ex/reifies> "
      "<<( ?person <http://ex/p> <http://ex/bob> )>> }");
  // The outer triple plus the three pseudo-triples, and no asserted triple
  // `?person <p> <bob>`.
  ASSERT_EQ(triples.size(), 4);
  // The pseudo-triples come before the outer triple (like the triples of a
  // blank node property list), so find the outer triple by its predicate.
  auto outerIt = ql::ranges::find_if(triples, [](const SparqlTriple& t) {
    return predicateOf(t) == "<http://ex/reifies>";
  });
  ASSERT_NE(outerIt, triples.end());
  const auto& outer = *outerIt;
  EXPECT_EQ(outer.s_, Variable{"?report"});
  ASSERT_TRUE(outer.o_.isVariable());
  const Variable& term = outer.o_.getVariable();
  EXPECT_THAT(term.name(),
              ::testing::StartsWith(QLEVER_INTERNAL_VARIABLE_PREFIX));
  const std::vector<std::string> expected{"?person", "<http://ex/p>",
                                          "<http://ex/bob>"};
  EXPECT_EQ(componentsOf(triples, term), expected);
  for (const auto& triple : triples) {
    EXPECT_FALSE(triple.s_ == Variable{"?person"});
  }
}

// _____________________________________________________________________________
TEST(TripleTermSparql, InternalVariableIsNotVisible) {
  auto parsed = parse(
      "SELECT * WHERE { ?report <http://ex/reifies> "
      "<<( ?person <http://ex/p> <http://ex/bob> )>> }");
  // Exactly the two variables of the query, without the internal one. (Their
  // order is the order of first appearance in the triples, and the
  // pseudo-triples come first.)
  EXPECT_THAT(parsed.selectClause().getSelectedVariables(),
              ::testing::UnorderedElementsAre(Variable{"?report"},
                                              Variable{"?person"}));
}

// _____________________________________________________________________________
TEST(TripleTermSparql, ComponentsCanBeVariablesIrisAndA) {
  auto triples = triplesOf(
      "PREFIX ex: <http://ex/> SELECT * WHERE { "
      "?r ex:reifies <<( ?s ?p ?o )>> . "
      "?r ex:about <<( ex:alice a ex:Person )>> , ?other }");
  // Two outer triples with two different fresh variables, plus ?r ex:about
  // ?other, plus three pseudo-triples for each term.
  ASSERT_EQ(triples.size(), 9);
  std::vector<Variable> terms;
  for (const auto& triple : triples) {
    if (triple.s_ == Variable{"?r"} && triple.o_.isVariable() &&
        triple.o_.getVariable() != Variable{"?other"}) {
      terms.push_back(triple.o_.getVariable());
    }
  }
  ASSERT_EQ(terms.size(), 2);
  EXPECT_NE(terms[0], terms[1]);
  const std::vector<std::string> variables{"?s", "?p", "?o"};
  const std::vector<std::string> iris{
      "<http://ex/alice>", "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>",
      "<http://ex/Person>"};
  EXPECT_EQ(componentsOf(triples, terms[0]), variables);
  EXPECT_EQ(componentsOf(triples, terms[1]), iris);
}

// _____________________________________________________________________________
TEST(TripleTermSparql, BlankNodeComponentActsLikeAVariable) {
  // In a query pattern, a blank node is a variable that is not selected (as in
  // every other triple of a WHERE clause).
  auto triples = triplesOf(
      "SELECT * WHERE { ?x <http://ex/r> <<( ?s <http://ex/p> _:b )>> }");
  ASSERT_EQ(triples.size(), 4);
  auto objectTriple = ql::ranges::find_if(triples, [](const SparqlTriple& t) {
    return predicateOf(t) == TRIPLE_TERM_OBJECT_PREDICATE;
  });
  ASSERT_NE(objectTriple, triples.end());
  ASSERT_TRUE(objectTriple->o_.isVariable());
  EXPECT_THAT(objectTriple->o_.getVariable().name(),
              ::testing::StartsWith(QLEVER_INTERNAL_BLANKNODE_VARIABLE_PREFIX));
}

// _____________________________________________________________________________
TEST(TripleTermSparql, UnsupportedPositionsAndComponents) {
  // RDF 1.2 has triple terms only in object position.
  EXPECT_ANY_THROW(
      parse("SELECT * WHERE { <<( ?s <http://ex/p> ?o )>> <http://ex/q> ?x }"));
  EXPECT_ANY_THROW(
      parse("SELECT * WHERE { ?x <<( ?s <http://ex/p> ?o )>> ?y }"));
  // Prototype limitations: no nesting, no literals or blank nodes inside.
  EXPECT_ANY_THROW(
      parse("SELECT * WHERE { ?x <http://ex/r> "
            "<<( ?s <http://ex/p> <<( ?a <http://ex/b> ?c )>> )>> }"));
  AD_EXPECT_THROW_WITH_MESSAGE(
      parse("SELECT * WHERE { ?x <http://ex/r> <<( ?s <http://ex/p> 42 )>> }"),
      HasSubstr("Literals inside a triple term"));
  AD_EXPECT_THROW_WITH_MESSAGE(parse("SELECT * WHERE { ?x <http://ex/r> "
                                     "<<( ?s <http://ex/p> \"text\" )>> }"),
                               HasSubstr("Literals inside a triple term"));
  // Not (yet) in CONSTRUCT templates.
  EXPECT_ANY_THROW(
      parse("CONSTRUCT { ?x <http://ex/r> <<( ?s <http://ex/p> ?o )>> } "
            "WHERE { ?x ?y ?z }"));
}

// _____________________________________________________________________________
TEST(TripleTermSparql, OrdinaryComparisonsStillParse) {
  // The new tokens `<<(` and `)>>` must not break `<` and `>` in expressions.
  EXPECT_NO_THROW(
      parse("SELECT * WHERE { ?x ?y ?z FILTER((?z)>1 && ?z<(2)) }"));
}

// End-to-end tests: real query text, evaluated on an index that was built from
// Turtle with triple terms (see also `TripleTermIngestionTest.cpp`).
namespace {
const std::string reports =
    "<http://ex/reportA> <http://ex/reifies> "
    "<<( <http://ex/alice> <http://ex/p> <http://ex/bob> )>> .\n"
    "<http://ex/reportA> <http://ex/date> \"2026\" .\n"
    "<http://ex/reportB> <http://ex/reifies> "
    "<<( <http://ex/alice> <http://ex/p> <http://ex/bob> )>> .\n"
    "<http://ex/reportC> <http://ex/reifies> "
    "<<( <http://ex/alice> <http://ex/p> <http://ex/charlie> )>> .\n"
    "<http://ex/dave> <http://ex/p> <http://ex/bob> .\n";

// Evaluate `query` and return the rows of the result for the given
// `variables`, each value as the string of its IRI, sorted.
std::vector<std::vector<std::string>> evaluate(
    const std::string& query, const std::vector<std::string>& variables) {
  auto* qec = ad_utility::testing::getQec(reports);
  auto qet = queryPlannerTestHelpers::parseAndPlan(query, qec);
  auto result = qet->getResult();
  const auto& table = result->idTableView();
  const auto& columns = qet->getVariableColumns();
  const auto& index = qec->getIndex();
  std::vector<std::vector<std::string>> rows;
  for (size_t row = 0; row < table.numRows(); ++row) {
    std::vector<std::string> values;
    for (const auto& name : variables) {
      Id id = table(row, columns.at(Variable{name}).columnIndex_);
      values.push_back(ql::exportIds::idToStringAndType(index, id, LocalVocab{})
                           .value()
                           .first);
    }
    rows.push_back(std::move(values));
  }
  ql::ranges::sort(rows);
  return rows;
}

using Rows = std::vector<std::vector<std::string>>;
const std::string reifies = "<http://ex/reifies>";
}  // namespace

// _____________________________________________________________________________
TEST(TripleTermSparql, QueryReturnsTheReportsOfAStatement) {
  // Two reports mention the same statement `alice p bob`; reportC mentions a
  // different one.
  EXPECT_EQ(evaluate("SELECT ?report ?person WHERE { ?report " + reifies +
                         " <<( ?person <http://ex/p> <http://ex/bob> )>> }",
                     {"?report", "?person"}),
            (Rows{{"<http://ex/reportA>", "<http://ex/alice>"},
                  {"<http://ex/reportB>", "<http://ex/alice>"}}));
  EXPECT_EQ(evaluate("SELECT ?report ?person WHERE { ?report " + reifies +
                         " <<( ?person <http://ex/p> <http://ex/charlie> )>> }",
                     {"?report", "?person"}),
            (Rows{{"<http://ex/reportC>", "<http://ex/alice>"}}));
}

// _____________________________________________________________________________
TEST(TripleTermSparql, TermPatternsCombineWithOrdinaryTriples) {
  // Only reportA has a date.
  EXPECT_EQ(
      evaluate("SELECT ?report WHERE { ?report <http://ex/date> ?d . "
               "?report " +
                   reifies + " <<( ?person <http://ex/p> <http://ex/bob> )>> }",
               {"?report"}),
      (Rows{{"<http://ex/reportA>"}}));
  // The subject of the term joins with ordinary triples, too: `dave p bob` is
  // asserted, but no report mentions `dave p bob`.
  EXPECT_EQ(
      evaluate("SELECT ?report ?person WHERE { ?person <http://ex/p> "
               "<http://ex/bob> . ?report " +
                   reifies + " <<( ?person <http://ex/p> <http://ex/bob> )>> }",
               {"?report", "?person"}),
      Rows{});
}

// _____________________________________________________________________________
TEST(TripleTermSparql, MentionedStatementsAreNotAsserted) {
  // `alice p bob` is only mentioned, `dave p bob` is asserted.
  EXPECT_EQ(evaluate("SELECT ?person WHERE { ?person <http://ex/p> "
                     "<http://ex/bob> }",
                     {"?person"}),
            (Rows{{"<http://ex/dave>"}}));
}

// _____________________________________________________________________________
TEST(TripleTermSparql, UnknownComponentsGiveNoResults) {
  EXPECT_EQ(evaluate("SELECT ?report WHERE { ?report " + reifies +
                         " <<( ?person <http://ex/p> <http://ex/nobody> )>> }",
                     {"?report"}),
            Rows{});
  EXPECT_EQ(evaluate("SELECT ?report WHERE { ?report " + reifies +
                         " <<( ?person <http://ex/q> <http://ex/bob> )>> }",
                     {"?report"}),
            Rows{});
}

// _____________________________________________________________________________
TEST(TripleTermSparql, SelectStarShowsOnlyTheQueryVariables) {
  auto* qec = ad_utility::testing::getQec(reports);
  auto qet = queryPlannerTestHelpers::parseAndPlan(
      "SELECT * WHERE { ?report " + reifies +
          " <<( ?person <http://ex/p> <http://ex/bob> )>> }",
      qec);
  auto parsed = parse("SELECT * WHERE { ?report " + reifies +
                      " <<( ?person <http://ex/p> <http://ex/bob> )>> }");
  EXPECT_THAT(parsed.selectClause().getSelectedVariables(),
              ::testing::UnorderedElementsAre(Variable{"?report"},
                                              Variable{"?person"}));
  EXPECT_EQ(qet->getResult()->idTableView().numRows(), 2);
}

// _____________________________________________________________________________
TEST(TripleTermSparql, UnsupportedShapesFailLoudly) {
  // Only `<<( ?s <p> <o> )>>` has an access path so far. Other shapes must not
  // be planned as ordinary scans (which would silently return no results).
  auto* qec = ad_utility::testing::getQec(reports);
  for (std::string term :
       {"<<( ?s <http://ex/p> ?o )>>", "<<( ?s ?p <http://ex/bob> )>>",
        "<<( <http://ex/alice> <http://ex/p> ?o )>>"}) {
    SCOPED_TRACE(term);
    AD_EXPECT_THROW_WITH_MESSAGE(
        queryPlannerTestHelpers::parseAndPlan(
            "SELECT * WHERE { ?r " + reifies + " " + term + " }", qec),
        HasSubstr("can currently only be evaluated in the form"));
  }
  // The internal predicates cannot be used directly.
  AD_EXPECT_THROW_WITH_MESSAGE(
      queryPlannerTestHelpers::parseAndPlan(
          absl::StrCat("SELECT * WHERE { ?t ", TRIPLE_TERM_SUBJECT_PREDICATE,
                       " ?s }"),
          qec),
      HasSubstr("can only be used via the triple-term syntax"));
}

// Output of triple terms in query results (see `ExportIds.h` and
// `ExportQueryExecutionTrees.cpp`).
namespace {
// Run `query` on the index of `reports` and return the complete result in the
// given `mediaType`.
std::string exportResult(const std::string& query,
                         ad_utility::MediaType mediaType,
                         const std::string& data = reports) {
  auto* qec = ad_utility::testing::getQec(data);
  auto handle = std::make_shared<ad_utility::CancellationHandle<>>();
  QueryPlanner planner{qec, handle};
  auto parsed = parse(query);
  auto qet = planner.createExecutionTree(parsed);
  ad_utility::Timer timer{ad_utility::Timer::Started};
  std::string result;
  for (const auto& block : ExportQueryExecutionTrees::computeResult(
           parsed, *qet, mediaType, timer, std::move(handle))) {
    result += block;
  }
  return result;
}

const std::string selectTerms =
    "SELECT ?report ?t WHERE { ?report <http://ex/reifies> ?t } "
    "ORDER BY ?report";
const std::string bobTerm =
    "<<( <http://ex/alice> <http://ex/p> <http://ex/bob> )>>";
const std::string charlieTerm =
    "<<( <http://ex/alice> <http://ex/p> <http://ex/charlie> )>>";
}  // namespace

// _____________________________________________________________________________
TEST(TripleTermSparql, TsvAndCsvShowTheNTriplesForm) {
  auto tsv = exportResult(selectTerms, ad_utility::MediaType::tsv);
  EXPECT_THAT(tsv, HasSubstr("<http://ex/reportA>\t" + bobTerm + "\n"));
  EXPECT_THAT(tsv, HasSubstr("<http://ex/reportB>\t" + bobTerm + "\n"));
  EXPECT_THAT(tsv, HasSubstr("<http://ex/reportC>\t" + charlieTerm + "\n"));
  // In CSV, IRIs lose their angle brackets, but the components of a term keep
  // them, so that the term stays recognizable as a term.
  auto csv = exportResult(selectTerms, ad_utility::MediaType::csv);
  EXPECT_THAT(csv, HasSubstr("http://ex/reportA," + bobTerm));
  EXPECT_THAT(csv, HasSubstr("http://ex/reportC," + charlieTerm));
}

// _____________________________________________________________________________
TEST(TripleTermSparql, SparqlJsonHasStructuredTerms) {
  auto json = nlohmann::json::parse(
      exportResult(selectTerms, ad_utility::MediaType::sparqlJson));
  const auto& bindings = json["results"]["bindings"];
  ASSERT_EQ(bindings.size(), 3);
  auto iri = [](std::string_view s) {
    return nlohmann::json{{"type", "uri"}, {"value", s}};
  };
  auto term = [&iri](std::string_view object) {
    return nlohmann::json{{"type", "triple"},
                          {"value",
                           {{"subject", iri("http://ex/alice")},
                            {"predicate", iri("http://ex/p")},
                            {"object", iri(object)}}}};
  };
  EXPECT_EQ(bindings[0]["report"], iri("http://ex/reportA"));
  EXPECT_EQ(bindings[0]["t"], term("http://ex/bob"));
  EXPECT_EQ(bindings[1]["t"], term("http://ex/bob"));
  EXPECT_EQ(bindings[2]["t"], term("http://ex/charlie"));
}

// _____________________________________________________________________________
TEST(TripleTermSparql, QleverJsonShowsTheNTriplesForm) {
  auto json = nlohmann::json::parse(
      exportResult(selectTerms, ad_utility::MediaType::qleverJson));
  const auto& rows = json["res"];
  ASSERT_EQ(rows.size(), 3);
  EXPECT_EQ(rows[0][1], bobTerm);
  EXPECT_EQ(rows[2][1], charlieTerm);
}

// _____________________________________________________________________________
TEST(TripleTermSparql, ConstructOutputsTermsAsObjects) {
  // `<<( s p o )>>` is valid N-Triples 1.2 and Turtle 1.2 in object position.
  auto ntriples = exportResult(
      "CONSTRUCT { ?report <http://ex/about> ?t } WHERE { "
      "?report <http://ex/reifies> ?t }",
      ad_utility::MediaType::ntriples);
  EXPECT_THAT(ntriples, HasSubstr("<http://ex/reportA> <http://ex/about> " +
                                  bobTerm + " ."));
  EXPECT_THAT(ntriples, HasSubstr("<http://ex/reportC> <http://ex/about> " +
                                  charlieTerm + " ."));
}

// _____________________________________________________________________________
TEST(TripleTermSparql, XmlRejectsTermsExplicitly) {
  AD_EXPECT_THROW_WITH_MESSAGE(
      exportResult(selectTerms, ad_utility::MediaType::sparqlXml),
      HasSubstr("does not support RDF 1.2 triple terms yet"));
  // Results without terms are unaffected.
  EXPECT_THAT(exportResult("SELECT ?report WHERE { ?report "
                           "<http://ex/reifies> ?t }",
                           ad_utility::MediaType::sparqlXml),
              HasSubstr("<uri>http://ex/reportA</uri>"));
}

// _____________________________________________________________________________
TEST(TripleTermSparql, CsvEscapesTheWholeTerm) {
  const std::string data =
      "<http://ex/report> <http://ex/reifies> "
      "<<( <http://ex/a,b> <http://ex/p,c> <http://ex/o,d> )>> .";
  const std::string query =
      "SELECT ?report ?t WHERE { ?report <http://ex/reifies> ?t }";
  EXPECT_EQ(exportResult(query, ad_utility::MediaType::csv, data),
            "report,t\nhttp://ex/report,\"<<( <http://ex/a,b> <http://ex/p,c> "
            "<http://ex/o,d> )>>\"\n");
  EXPECT_EQ(exportResult(query, ad_utility::MediaType::tsv, data),
            "?report\t?t\n<http://ex/report>\t<<( <http://ex/a,b> "
            "<http://ex/p,c> <http://ex/o,d> )>>\n");
}

// _____________________________________________________________________________
TEST(TripleTermSparql, ConstructDropsTermsInSubjectAndPredicatePositions) {
  const std::string query =
      "CONSTRUCT { ?t <http://ex/about> ?report . "
      "?report ?t <http://ex/value> . "
      "?report <http://ex/about> ?t } WHERE { "
      "?report <http://ex/reifies> ?t }";
  const std::string expected =
      "<http://ex/reportA> <http://ex/about> " + bobTerm + " .\n" +
      "<http://ex/reportB> <http://ex/about> " + bobTerm + " .\n" +
      "<http://ex/reportC> <http://ex/about> " + charlieTerm + " .\n";
  // Invalid template instances are omitted, but the valid object instance
  // survives for each solution. Check both RDF graph serializers.
  for (auto format : {ad_utility::MediaType::ntriples,
                      ad_utility::MediaType::turtle}) {
    EXPECT_EQ(exportResult(query, format), expected);
    EXPECT_TRUE(exportResult(
                    "CONSTRUCT { ?t <http://ex/about> ?r . "
                    "?r ?t <http://ex/value> } WHERE { "
                    "?r <http://ex/reifies> ?t }", format)
                    .empty());
  }
}
