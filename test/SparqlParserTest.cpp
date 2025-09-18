// Copyright 2014 - 2022, University of Freiburg
// Chair of Algorithms and Data Structures.
// Authors: Björn Buchhold <b.buchhold@gmail.com>
//          Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>
//          Hannah Bast <bast@cs.uni-freiburg.de>

#include <gmock/gmock.h>

#include <utility>
#include <variant>

#include "./parser/SparqlAntlrParserTestHelpers.h"
#include "./util/GTestHelpers.h"
#include "global/Constants.h"
#include "parser/SparqlParser.h"
#include "util/Conversions.h"
#include "util/TripleComponentTestHelpers.h"

namespace m = matchers;
namespace p = parsedQuery;

using Var = Variable;
namespace {
auto lit = ad_utility::testing::tripleComponentLiteral;
auto iri = ad_utility::testing::iri;

const std::string& getIriString(
    const ad_utility::sparql_types::VarOrPath& varOrPath) {
  const auto& tripleComponent = std::get<PropertyPath>(varOrPath).getIri();
  return tripleComponent.toStringRepresentation();
}
auto parseQuery(std::string query,
                const std::vector<DatasetClause>& datasets = {}) {
  static EncodedIriManager evM;
  return SparqlParser::parseQuery(&evM, std::move(query), datasets);
}
}  // namespace

// _____________________________________________________________________________
TEST(ParserTest, testParse) {
  {
    auto pq = parseQuery("SELECT ?x WHERE {?x ?y ?z}");
    ASSERT_TRUE(pq.hasSelectClause());
    const auto& selectClause = pq.selectClause();
    ASSERT_EQ(1u, selectClause.getSelectedVariables().size());
    ASSERT_EQ(1u, pq._rootGraphPattern._graphPatterns.size());
    ASSERT_EQ(
        1u, pq._rootGraphPattern._graphPatterns[0].getBasic()._triples.size());
  }

  {
    auto pq = parseQuery(
        "PREFIX : <http://rdf.myprefix.com/>\n"
        "PREFIX ns: <http://rdf.myprefix.com/ns/>\n"
        "PREFIX xxx: <http://rdf.myprefix.com/xxx/>\n"
        "SELECT ?x ?z \n "
        "WHERE \t {?x :myrel ?y. ?y ns:myrel ?z.?y <nsx:rel2> "
        "<http://abc.de>}");
    ASSERT_TRUE(pq.hasSelectClause());
    const auto& selectClause2 = pq.selectClause();
    ASSERT_EQ(2u, selectClause2.getSelectedVariables().size());
    ASSERT_EQ(1u, pq.children().size());
    const auto& triples = pq.children()[0].getBasic()._triples;
    ASSERT_EQ(3u, triples.size());
    ASSERT_EQ(Var{"?x"}, selectClause2.getSelectedVariables()[0]);
    ASSERT_EQ(Var{"?z"}, selectClause2.getSelectedVariables()[1]);
    ASSERT_EQ(Var{"?x"}, triples[0].s_);
    ASSERT_EQ("<http://rdf.myprefix.com/myrel>", getIriString(triples[0].p_));
    ASSERT_EQ(Var{"?y"}, triples[0].o_);
    ASSERT_EQ(Var{"?y"}, triples[1].s_);
    ASSERT_EQ("<http://rdf.myprefix.com/ns/myrel>",
              getIriString(triples[1].p_));
    ASSERT_EQ(Var{"?z"}, triples[1].o_);
    ASSERT_EQ(Var{"?y"}, triples[2].s_);
    ASSERT_EQ("<nsx:rel2>", getIriString(triples[2].p_));
    ASSERT_EQ(iri("<http://abc.de>"), triples[2].o_);
    ASSERT_EQ(std::nullopt, pq._limitOffset._limit);
    ASSERT_EQ(0, pq._limitOffset._offset);
  }

  {
    auto pq = parseQuery(
        "PREFIX : <http://rdf.myprefix.com/>\n"
        "PREFIX ns: <http://rdf.myprefix.com/ns/>\n"
        "PREFIX xxx: <http://rdf.myprefix.com/xxx/>\n"
        "SELECT ?x ?z \n "
        "WHERE \t {\n?x :myrel ?y. ?y ns:myrel ?z.\n?y <nsx:rel2> "
        "<http://abc.de>\n}");
    ASSERT_TRUE(pq.hasSelectClause());
    const auto& selectClause = pq.selectClause();
    ASSERT_EQ(2u, selectClause.getSelectedVariables().size());
    ASSERT_EQ(1u, pq.children().size());
    const auto& triples = pq.children()[0].getBasic()._triples;
    ASSERT_EQ(3u, triples.size());
    ASSERT_EQ(Var{"?x"}, selectClause.getSelectedVariables()[0]);
    ASSERT_EQ(Var{"?z"}, selectClause.getSelectedVariables()[1]);
    ASSERT_EQ(Var{"?x"}, triples[0].s_);
    ASSERT_EQ("<http://rdf.myprefix.com/myrel>", getIriString(triples[0].p_));
    ASSERT_EQ(Var{"?y"}, triples[0].o_);
    ASSERT_EQ(Var{"?y"}, triples[1].s_);
    ASSERT_EQ("<http://rdf.myprefix.com/ns/myrel>",
              getIriString(triples[1].p_));
    ASSERT_EQ(Var{"?z"}, triples[1].o_);
    ASSERT_EQ(Var{"?y"}, triples[2].s_);
    ASSERT_EQ("<nsx:rel2>", getIriString(triples[2].p_));
    ASSERT_EQ(iri("<http://abc.de>"), triples[2].o_);
    ASSERT_EQ(std::nullopt, pq._limitOffset._limit);
    ASSERT_EQ(0, pq._limitOffset._offset);
  }

  {
    auto pq = parseQuery(
        "PREFIX ns: <http://ns/>"
        "SELECT ?x ?z \n "
        "WHERE \t {\n?x <Directed_by> ?y. ?y ns:myrel.extend ?z.\n"
        "?y <nsx:rel2> \"Hello... World\"}");
    ASSERT_TRUE(pq.hasSelectClause());
    const auto& selectClause = pq.selectClause();
    ASSERT_EQ(2u, selectClause.getSelectedVariables().size());
    ASSERT_EQ(1u, pq.children().size());
    const auto& triples = pq.children()[0].getBasic()._triples;
    ASSERT_EQ(3u, triples.size());

    ASSERT_EQ(Var{"?x"}, selectClause.getSelectedVariables()[0]);
    ASSERT_EQ(Var{"?z"}, selectClause.getSelectedVariables()[1]);
    ASSERT_EQ(Var{"?x"}, triples[0].s_);
    ASSERT_EQ("<Directed_by>", getIriString(triples[0].p_));
    ASSERT_EQ(Var{"?y"}, triples[0].o_);
    ASSERT_EQ(Var{"?y"}, triples[1].s_);
    ASSERT_EQ("<http://ns/myrel.extend>", getIriString(triples[1].p_));
    ASSERT_EQ(Var{"?z"}, triples[1].o_);
    ASSERT_EQ(Var{"?y"}, triples[2].s_);
    ASSERT_EQ("<nsx:rel2>", getIriString(triples[2].p_));
    ASSERT_EQ(lit("\"Hello... World\""), triples[2].o_);
    ASSERT_EQ(std::nullopt, pq._limitOffset._limit);
    ASSERT_EQ(0, pq._limitOffset._offset);
  }

  {
    auto pq = parseQuery(
        "SELECT ?x ?y WHERE {?x <is-a> <Actor> .  FILTER(?x != ?y)."
        "?y <is-a> <Actor> . FILTER(?y < ?x)} LIMIT 10");
    ASSERT_EQ(1u, pq.children().size());
    const auto& triples = pq.children()[0].getBasic()._triples;
    auto filters = pq._rootGraphPattern._filters;
    ASSERT_EQ(2u, filters.size());
    ASSERT_EQ("(?x != ?y)", filters[0].expression_.getDescriptor());
    ASSERT_EQ("(?y < ?x)", filters[1].expression_.getDescriptor());
    ASSERT_EQ(2u, triples.size());
  }

  {
    auto pq = parseQuery(
        "SELECT ?x ?y WHERE {?x <is-a> <Actor> .  FILTER(?x != ?y)."
        "?y <is-a> <Actor>} LIMIT 10");
    ASSERT_EQ(1u, pq.children().size());
    const auto& triples = pq.children()[0].getBasic()._triples;
    auto filters = pq._rootGraphPattern._filters;
    ASSERT_EQ(1u, filters.size());
    ASSERT_EQ("(?x != ?y)", filters[0].expression_.getDescriptor());
    ASSERT_EQ(2u, triples.size());
  }

  {
    auto pq = parseQuery(
        "SELECT ?x ?y WHERE {?x <is-a> <Actor> .  FILTER(?x != ?y)."
        "?y <is-a> <Actor>. ?c ql:contains-entity ?x."
        "?c ql:contains-word \"coca* abuse\"} LIMIT 10");
    ASSERT_EQ(1u, pq.children().size());
    const auto& triples = pq.children()[0].getBasic()._triples;
    auto filters = pq._rootGraphPattern._filters;
    ASSERT_EQ(1u, filters.size());
    ASSERT_EQ("(?x != ?y)", filters[0].expression_.getDescriptor());
    ASSERT_EQ(4u, triples.size());
    ASSERT_EQ(Var{"?c"}, triples[2].s_);
    ASSERT_EQ(CONTAINS_ENTITY_PREDICATE, getIriString(triples[2].p_));
    ASSERT_EQ(Var{"?x"}, triples[2].o_);
    ASSERT_EQ(Var{"?c"}, triples[3].s_);
    ASSERT_EQ(CONTAINS_WORD_PREDICATE, getIriString(triples[3].p_));
    ASSERT_EQ(lit("\"coca* abuse\""), triples[3].o_);
  }

  {
    auto pq = parseQuery(
        "PREFIX : <>\n"
        "SELECT ?x ?y ?z ?c ?ql_textscore_c ?c WHERE {\n"
        "?x :is-a :Politician .\n"
        "?c ql:contains-entity ?x .\n"
        "?c ql:contains-word \"friend\" .\n"
        "?c ql:contains-entity ?y .\n"
        "?y :is-a :Scientist .\n"
        "FILTER(?x != ?y) .\n"
        "} ORDER BY ?c");
    ASSERT_EQ(1u, pq._rootGraphPattern._filters.size());

    // shouldn't have more checks??
  }

  {
    auto pq = parseQuery(
        "SELECT ?x ?z WHERE {\n"
        "  ?x <test> ?y .\n"
        "  OPTIONAL {\n"
        "    ?y <test2> ?z .\n"
        "  }\n"
        "}");

    ASSERT_EQ(2u, pq.children().size());
    const auto& opt =
        std::get<p::Optional>(pq._rootGraphPattern._graphPatterns[1]);
    auto& child = opt._child;
    const auto& triples = child._graphPatterns[0].getBasic()._triples;
    auto filters = child._filters;
    ASSERT_EQ(1u, triples.size());
    ASSERT_EQ(Var{"?y"}, triples[0].s_);
    ASSERT_EQ("<test2>", getIriString(triples[0].p_));
    ASSERT_EQ(Var{"?z"}, triples[0].o_);
    ASSERT_EQ(0u, filters.size());
    ASSERT_TRUE(child._optional);
  }

  {
    auto pq = parseQuery(
        "SELECT ?x ?z WHERE {\n"
        "  ?x <test> ?y .\n"
        "  OPTIONAL {\n"
        "    ?y <test2> ?z .\n"
        "    optional {\n"
        "      ?a ?b ?c .\n"
        "      FILTER(?c > 3)\n"
        "    }\n"
        "    optional {\n"
        "      ?d ?e ?f\n"
        "    }\n"
        "  }\n"
        "}");
    ASSERT_EQ(2u, pq._rootGraphPattern._graphPatterns.size());
    const auto& optA = std::get<p::Optional>(
        pq._rootGraphPattern._graphPatterns[1]);  // throws on error
    auto& child = optA._child;
    ASSERT_EQ(3u, child._graphPatterns.size());
    const auto& opt2 =
        std::get<p::Optional>(child._graphPatterns[1]);  // throws on error
    const auto& opt3 =
        std::get<p::Optional>(child._graphPatterns[2]);  // throws on error
    const auto& child2 = opt2._child._graphPatterns[0].getBasic();
    const auto& child3 = opt3._child._graphPatterns[0].getBasic();
    ASSERT_EQ(1u, child2._triples.size());
    ASSERT_EQ(1u, opt2._child._filters.size());
    ASSERT_EQ(1u, child3._triples.size());
    ASSERT_EQ(0u, opt3._child._filters.size());
    ASSERT_TRUE(child._optional);
    ASSERT_TRUE(opt2._child._optional);
    ASSERT_TRUE(opt3._child._optional);
  }

  {
    auto pq = parseQuery(
        "SELECT ?a WHERE {\n"
        "  VALUES ?a { <1> 2}\n"
        "  VALUES (?b ?c) {(<1> <2>) (1 2)}\n"
        "  ?a <rel> ?b ."
        "}");
    ASSERT_EQ(3u, pq._rootGraphPattern._graphPatterns.size());
    const auto& c = pq.children()[2].getBasic();
    ASSERT_EQ(1u, c._triples.size());
    ASSERT_EQ(0u, pq._rootGraphPattern._filters.size());
    const auto& values1 = std::get<p::Values>(pq.children()[0])._inlineValues;
    const auto& values2 = std::get<p::Values>(pq.children()[1])._inlineValues;

    std::vector<Variable> vvars = {Var{"?a"}};
    ASSERT_EQ(vvars, values1._variables);
    std::vector<std::vector<TripleComponent>> vvals = {{iri("<1>")}, {2}};
    ASSERT_EQ(vvals, values1._values);

    vvars = {Var{"?b"}, Var{"?c"}};
    ASSERT_EQ(vvars, values2._variables);
    vvals = {{iri("<1>"), iri("<2>")}, {1, 2}};
    ASSERT_EQ(vvals, values2._values);
  }

  {
    auto pq = parseQuery(
        R"(SELECT ?a ?b ?c WHERE {
                        VALUES ?a { <Albert_Einstein>}
                        VALUES (?b ?c) {
        (<Marie_Curie> <Joseph_Jacobson>) (<Freiherr> <Lord_of_the_Isles>) }
                        }
                    )");

    ASSERT_EQ(2u, pq.children().size());
    ASSERT_EQ(0u, pq._rootGraphPattern._filters.size());
    const auto& values1 = std::get<p::Values>(pq.children()[0])._inlineValues;
    const auto& values2 = std::get<p::Values>(pq.children()[1])._inlineValues;

    std::vector<Variable> vvars = {Var{"?a"}};
    ASSERT_EQ(vvars, values1._variables);
    std::vector<std::vector<TripleComponent>> vvals = {
        {iri("<Albert_Einstein>")}};
    ASSERT_EQ(vvals, values1._values);

    vvars = {Var{"?b"}, Var{"?c"}};
    ASSERT_EQ(vvars, values2._variables);
    vvals = {{iri("<Marie_Curie>"), iri("<Joseph_Jacobson>")},
             {iri("<Freiherr>"), iri("<Lord_of_the_Isles>")}};
    ASSERT_EQ(vvals, values2._values);
  }

  {
    auto pq = parseQuery(
        ""
        "PREFIX wd: <http://www.wikidata.org/entity/>\n"
        "PREFIX wdt: <http://www.wikidata.org/prop/direct/>\n"
        "SELECT ?city WHERE {\n"
        "  VALUES ?citytype { wd:Q515 wd:Q262166}\n"
        "  ?city wdt:P31 ?citytype .\n"
        "}\n");

    ASSERT_EQ(2u, pq.children().size());
    const auto& c = pq.children()[1].getBasic();
    ASSERT_EQ(1u, c._triples.size());
    ASSERT_EQ(0u, pq._rootGraphPattern._filters.size());

    ASSERT_EQ(c._triples[0].s_, Var{"?city"});
    ASSERT_EQ(getIriString(c._triples[0].p_),
              "<http://www.wikidata.org/prop/direct/P31>");
    ASSERT_EQ(c._triples[0].o_, Var{"?citytype"});

    const auto& values1 = std::get<p::Values>(pq.children()[0])._inlineValues;
    std::vector<Variable> vvars = {Var{"?citytype"}};
    ASSERT_EQ(vvars, values1._variables);
    std::vector<std::vector<TripleComponent>> vvals = {
        {iri("<http://www.wikidata.org/entity/Q515>")},
        {iri("<http://www.wikidata.org/entity/Q262166>")}};
    ASSERT_EQ(vvals, values1._values);
  }

  // TODO @joaomarques90: Finish when the required functionalities
  // are implemented
  /*
  {
   // C++ exception with description "ParseException, cause:
   // Expected a token of type AGGREGATE but got a token of
   // type RDFLITERAL (() in the input at pos 373 : (YEAR(?year))
    auto pq = parseQuery(
                  "SELECT DISTINCT * WHERE { \n"
                  "  ?movie <directed-by> <Scott%2C%20Ridley> .\n"
                  "\t{ \n"
                  "\t SELECT ?movie WHERE { \n"
                  "\t\t\t ?movie <directed-by> ?director .\n"
                  "\t\t\t ?movie <from-year> ?year .\n"
                  "\t\t\t FILTER(regex(?title, \"^the\", \"i\" )) ."
                  "\t\t } \n"
                  "\t\t GROUP BY (YEAR(?year)) \n"
                  "\t} \n"
                  "} \n"
                  "LIMIT 10 \n"
                  "ORDER BY DESC(?movie) ASC(?year) \n")
                  ;
  }

  {
    auto pq = parseQuery(
              "SELECT * WHERE { \n"
              "  VALUES ?x { 1 2 3 4 } .\n"
              "\t{ \n"
              "\t SELECT ?y ?concat WHERE { \n"
              "\t\t\t VALUES ?y { \"5\" \"6\" \"7\" \"8\" } .\n"
              "\t\t\t BIND(CONCAT(STR(?y),STR(?y)) AS ?concat) .\n"
              "\t\t } \n"
              "\t\t LIMIT 2 \n"
              "\t} \n"
              "\t BIND((?x * ?y) AS ?xy) ."
              "} \n"
              "LIMIT 5 \n"
              "OFFSET 2 \n"
              "ORDER BY DESC(?x) ASC(?concat) \n")
              ;
  }

   {
      auto pq = parseQuery(
             "SELECT REDUCED * WHERE { \n"
             "  ?movie <directed-by> <Scott%2C%20Ridley> .\n"
             "\t{ \n"
             "\t SELECT ?movie WHERE { \n"
             "\t\t\t ?movie <directed-by> ?director .\n"
             "\t\t\t ?movie <from-year> ?year .\n"
             "\t\t\t FILTER(regex(?title, \"^the\", \"i\" )) ."
             "\t\t } \n"
             "\t\t GROUP BY (YEAR(?year)) \n"
             "\t\t ORDER BY DESC(?year) \n"
             "\t\t OFFSET 2 \n"
             "\t} \n"
             "} \n"
             "LIMIT 10 \n"
             "ORDER BY DESC(?movie) ASC(?year) \n")
             ;
  }

  {
      auto pq = parseQuery(
             "SELECT DISTINCT * WHERE { \n"
             "  ?movie <directed-by> <Scott%2C%20Ridley> .\n"
             "\t{ \n"
             "\t SELECT * WHERE { \n"
             "\t\t\t ?movie <directed-by> ?director .\n"
             "\t\t\t ?movie <from-year> ?year .\n"
             "\t\t\t FILTER(YEAR(?year) > 2000) ."
             "\t\t } \n"
             "\t\t ORDER BY DESC(?director) \n"
             "\t} \n"
             "} \n"
             "LIMIT 20 \n"
             "OFFSET 3 \n"
             "ORDER BY DESC(?movie)\n")
             ;
  }
  */

  {
    auto pq = parseQuery(
        "SELECT REDUCED * WHERE { \n"
        "  ?movie <directed-by> ?director .\n"
        "} \n"
        "ORDER BY ASC(?movie)\n"
        "LIMIT 10 \n");
    ASSERT_EQ(1u, pq._rootGraphPattern._graphPatterns.size());

    const auto& c = pq.children()[0].getBasic();
    ASSERT_EQ(1u, c._triples.size());
    ASSERT_EQ(0u, pq._rootGraphPattern._filters.size());

    ASSERT_EQ(c._triples[0].s_, Var{"?movie"});
    ASSERT_EQ(getIriString(c._triples[0].p_), "<directed-by>");
    ASSERT_EQ(c._triples[0].o_, Var{"?director"});

    ASSERT_EQ(10u, pq._limitOffset._limit);
    ASSERT_EQ(false, pq._orderBy[0].isDescending_);
    ASSERT_EQ(Var{"?movie"}, pq._orderBy[0].variable_);

    auto sc = get<p::SelectClause>(pq._clause);
    ASSERT_EQ(true, sc.reduced_);
    ASSERT_EQ(true, sc.isAsterisk());

    std::vector<std::string> vvars = {"?movie", "?director"};
    ASSERT_EQ(vvars, sc.getSelectedVariablesAsStrings());
  }

  {
    auto pq = parseQuery(
        "SELECT DISTINCT * WHERE { \n"
        "  ?movie <directed-by> ?director .\n"
        "} \n"
        "ORDER BY DESC(?movie)\n"
        "LIMIT 10 \n");

    ASSERT_EQ(1u, pq._rootGraphPattern._graphPatterns.size());

    const auto& c = pq.children()[0].getBasic();
    ASSERT_EQ(1u, c._triples.size());
    ASSERT_EQ(0u, pq._rootGraphPattern._filters.size());

    ASSERT_EQ(c._triples[0].s_, Var{"?movie"});
    ASSERT_EQ(getIriString(c._triples[0].p_), "<directed-by>");
    ASSERT_EQ(c._triples[0].o_, Var{"?director"});

    ASSERT_EQ(10u, pq._limitOffset._limit);
    ASSERT_EQ(true, pq._orderBy[0].isDescending_);
    ASSERT_EQ(Var{"?movie"}, pq._orderBy[0].variable_);

    auto sc = pq.selectClause();
    ASSERT_EQ(true, sc.distinct_);
    ASSERT_EQ(true, sc.isAsterisk());

    std::vector<std::string> vvars = {"?movie", "?director"};
    ASSERT_EQ(vvars, sc.getSelectedVariablesAsStrings());
  }

  {
    auto pq = parseQuery(
        "SELECT DISTINCT * WHERE { \n"
        "  ?movie <directed-by> <Scott%2C%20Ridley> .\n"
        "\t{ \n"
        "\t SELECT * WHERE { \n"
        "\t\t\t ?movie <directed-by> ?director .\n"
        "\t\t\t ?movie <from-year> ?year .\n"
        "\t\t\t FILTER(?year > \"00-00-2000\") ."
        "\t\t } \n"
        "\t\t ORDER BY DESC(?director) \n"
        "\t} \n"
        "} \n"
        "ORDER BY DESC(?movie)\n"
        "LIMIT 20 \n"
        "OFFSET 3 \n");

    ASSERT_EQ(2u, pq._rootGraphPattern._graphPatterns.size());

    const auto& c = pq.children()[0].getBasic();
    ASSERT_EQ(1u, c._triples.size());
    ASSERT_EQ(0, pq._rootGraphPattern._filters.size());
    ASSERT_EQ(3u, pq._limitOffset._offset);

    ASSERT_EQ(c._triples[0].s_, Var{"?movie"});
    ASSERT_EQ(getIriString(c._triples[0].p_), "<directed-by>");
    ASSERT_EQ(c._triples[0].o_, iri("<Scott%2C%20Ridley>"));

    ASSERT_EQ(20u, pq._limitOffset._limit);
    ASSERT_EQ(true, pq._orderBy[0].isDescending_);
    ASSERT_EQ(Var{"?movie"}, pq._orderBy[0].variable_);

    auto sc = pq.selectClause();
    ASSERT_EQ(true, sc.distinct_);
    ASSERT_EQ(true, sc.isAsterisk());

    std::vector<std::string> vvars = {"?movie", "?director", "?year"};
    ASSERT_EQ(vvars, sc.getSelectedVariablesAsStrings());

    // -- SubQuery
    auto subQueryGroup =
        get<p::GroupGraphPattern>(pq._rootGraphPattern._graphPatterns[1]);
    auto parsed_sub_query =
        get<p::Subquery>(subQueryGroup._child._graphPatterns[0]);
    const auto& c_subquery = get<p::BasicGraphPattern>(
        parsed_sub_query.get()._rootGraphPattern._graphPatterns[0]);
    ASSERT_EQ(2u, c_subquery._triples.size());
    ASSERT_EQ(1u, parsed_sub_query.get()._rootGraphPattern._filters.size());
    const auto& filter = parsed_sub_query.get()._rootGraphPattern._filters[0];
    ASSERT_EQ("(?year > \"00-00-2000\")", filter.expression_.getDescriptor());
    ASSERT_EQ(0, parsed_sub_query.get()._limitOffset._offset);

    ASSERT_EQ(c_subquery._triples[0].s_, Var{"?movie"});
    ASSERT_EQ(getIriString(c_subquery._triples[0].p_), "<directed-by>");
    ASSERT_EQ(c_subquery._triples[0].o_, Var{"?director"});

    ASSERT_EQ(c_subquery._triples[1].s_, Var{"?movie"});
    ASSERT_EQ(getIriString(c_subquery._triples[1].p_), "<from-year>");
    ASSERT_EQ(c_subquery._triples[1].o_, Var{"?year"});

    ASSERT_EQ(std::nullopt, parsed_sub_query.get()._limitOffset._limit);
    ASSERT_EQ(true, parsed_sub_query.get()._orderBy[0].isDescending_);
    ASSERT_EQ(Var{"?director"}, parsed_sub_query.get()._orderBy[0].variable_);

    auto sc_subquery = parsed_sub_query.get().selectClause();
    ASSERT_EQ(false, sc_subquery.distinct_);
    ASSERT_EQ(false, sc_subquery.reduced_);
    ASSERT_EQ(true, sc_subquery.isAsterisk());
    std::vector<std::string> vvars_subquery = {"?movie", "?director", "?year"};
    ASSERT_EQ(vvars_subquery, sc_subquery.getSelectedVariablesAsStrings());
  }

  {
    // Query proving Select * working for n-subQuery
    auto pq = parseQuery(
        "SELECT DISTINCT * WHERE { \n"
        "  ?movie <directed-by> <Scott%2C%20Ridley> .\n"
        "\t{ \n"
        "\t SELECT * WHERE { \n"
        "\t\t\t ?movie <directed-by> ?director .\n"
        "\t\t\t { \n"
        "\t\t\t\t SELECT ?year WHERE { \n"
        "\t\t\t\t\t ?movie <from-year> ?year . \n"
        "\t\t\t\t\t } \n"
        "\t\t\t } \n"
        "\t\t\t FILTER(?year > \"00-00-2000\") ."
        "\t\t } \n"
        "\t\t ORDER BY DESC(?director) \n"
        "\t} \n"
        "} \n"
        "ORDER BY DESC(?movie)\n"
        "LIMIT 20 \n"
        "OFFSET 3 \n");

    ASSERT_EQ(2u, pq._rootGraphPattern._graphPatterns.size());

    const auto& c = pq.children()[0].getBasic();
    ASSERT_EQ(1u, c._triples.size());
    ASSERT_EQ(0, pq._rootGraphPattern._filters.size());
    ASSERT_EQ(3u, pq._limitOffset._offset);

    ASSERT_EQ(c._triples[0].s_, Var{"?movie"});
    ASSERT_EQ(getIriString(c._triples[0].p_), "<directed-by>");
    ASSERT_EQ(c._triples[0].o_, iri("<Scott%2C%20Ridley>"));

    ASSERT_EQ(20u, pq._limitOffset._limit);
    ASSERT_EQ(true, pq._orderBy[0].isDescending_);
    ASSERT_EQ(Var{"?movie"}, pq._orderBy[0].variable_);

    auto sc = pq.selectClause();
    ASSERT_EQ(true, sc.distinct_);
    ASSERT_EQ(true, sc.isAsterisk());

    std::vector<std::string> vvars = {"?movie", "?director", "?year"};
    ASSERT_EQ(vvars, sc.getSelectedVariablesAsStrings());

    // -- SubQuery (level 1)
    auto subQueryGroup =
        get<p::GroupGraphPattern>(pq._rootGraphPattern._graphPatterns[1]);
    auto parsed_sub_query =
        get<p::Subquery>(subQueryGroup._child._graphPatterns[0]);
    const auto& c_subquery = get<p::BasicGraphPattern>(
        parsed_sub_query.get()._rootGraphPattern._graphPatterns[0]);
    ASSERT_EQ(1u, c_subquery._triples.size());
    ASSERT_EQ(1u, parsed_sub_query.get()._rootGraphPattern._filters.size());
    const auto& filter = parsed_sub_query.get()._rootGraphPattern._filters[0];
    ASSERT_EQ("(?year > \"00-00-2000\")", filter.expression_.getDescriptor());
    ASSERT_EQ(0, parsed_sub_query.get()._limitOffset._offset);

    ASSERT_EQ(c_subquery._triples[0].s_, Var{"?movie"});
    ASSERT_EQ(getIriString(c_subquery._triples[0].p_), "<directed-by>");
    ASSERT_EQ(c_subquery._triples[0].o_, Var{"?director"});

    ASSERT_EQ(std::nullopt, parsed_sub_query.get()._limitOffset._limit);
    ASSERT_EQ(true, parsed_sub_query.get()._orderBy[0].isDescending_);
    ASSERT_EQ(Var{"?director"}, parsed_sub_query.get()._orderBy[0].variable_);

    auto sc_subquery = parsed_sub_query.get().selectClause();
    ASSERT_EQ(false, sc_subquery.distinct_);
    ASSERT_EQ(false, sc_subquery.reduced_);
    ASSERT_EQ(true, sc_subquery.isAsterisk());
    std::vector<std::string> vvars_subquery = {"?movie", "?director", "?year"};
    ASSERT_EQ(vvars_subquery, sc_subquery.getSelectedVariablesAsStrings());

    // -- SubQuery (level 2)
    auto subsubQueryGroup = get<p::GroupGraphPattern>(
        parsed_sub_query.get()._rootGraphPattern._graphPatterns[1]);
    auto aux_parsed_sub_sub_query =
        get<p::Subquery>(subsubQueryGroup._child._graphPatterns[0]).get();
    const auto& c_sub_subquery = get<p::BasicGraphPattern>(
        aux_parsed_sub_sub_query._rootGraphPattern._graphPatterns[0]);
    ASSERT_EQ(1u, c_sub_subquery._triples.size());
    ASSERT_EQ(0u, aux_parsed_sub_sub_query._rootGraphPattern._filters.size());
    ASSERT_EQ(0, aux_parsed_sub_sub_query._limitOffset._offset);

    ASSERT_EQ(c_sub_subquery._triples[0].s_, Var{"?movie"});
    ASSERT_EQ(getIriString(c_sub_subquery._triples[0].p_), "<from-year>");
    ASSERT_EQ(c_sub_subquery._triples[0].o_, Var{"?year"});

    ASSERT_EQ(std::nullopt, aux_parsed_sub_sub_query._limitOffset._limit);
    ASSERT_EQ(0u, aux_parsed_sub_sub_query._orderBy.size());

    auto sc_sub_subquery = aux_parsed_sub_sub_query.selectClause();
    ASSERT_EQ(false, sc_sub_subquery.distinct_);
    ASSERT_EQ(false, sc_sub_subquery.reduced_);
    ASSERT_EQ(false, sc_sub_subquery.isAsterisk());
    std::vector<std::string> vvars_sub_subquery = {"?year"};
    ASSERT_EQ(vvars_sub_subquery,
              sc_sub_subquery.getSelectedVariablesAsStrings());
  }

  {
    namespace m = matchers;
    // Check Parse Construct (1)
    auto pq_1 = parseQuery(
        "PREFIX foaf:   <http://xmlns.com/foaf/0.1/> \n"
        "PREFIX org:    <http://example.com/ns#> \n"
        "CONSTRUCT { ?x foaf:name ?name } \n"
        "WHERE  { ?x org:employeeName ?name }");

    EXPECT_THAT(
        pq_1,
        m::ConstructQuery(
            {{Variable{"?x"}, Iri{"<http://xmlns.com/foaf/0.1/name>"},
              Variable{"?name"}}},
            m::GraphPattern(m::Triples({SparqlTriple{
                Variable{"?x"}, iri("<http://example.com/ns#employeeName>"),
                Variable{"?name"}}}))));

    // Check Parse Construct (2)
    auto pq_2 = parseQuery(
        "PREFIX foaf:    <http://xmlns.com/foaf/0.1/>\n"
        "PREFIX vcard:   <http://www.w3.org/2001/vcard-rdf/3.0#>\n"
        "CONSTRUCT   { <http://example.org/person#Alice> vcard:FN ?name }\n"
        "WHERE       { ?x foaf:name ?name } ");

    EXPECT_THAT(pq_2,
                m::ConstructQuery(
                    {{Iri{"<http://example.org/person#Alice>"},
                      Iri{"<http://www.w3.org/2001/vcard-rdf/3.0#FN>"},
                      Variable{"?name"}}},
                    m::GraphPattern(m::Triples({SparqlTriple{
                        Variable{"?x"}, iri("<http://xmlns.com/foaf/0.1/name>"),
                        Variable{"?name"}}}))));
  }

  {
    // Check if the correct ParseException is thrown after
    // GroupBy with Select '*'
    ASSERT_THROW(
        parseQuery(
            "SELECT DISTINCT * WHERE { \n?a <b> ?c .\n} \nGROUP BY ?a ?c \n"),
        ParseException);
  }

  {
    // Check if the correct ParseException is thrown after:
    // Select [var_name]+ '*'
    ASSERT_THROW(parseQuery("SELECT DISTINCT ?a * WHERE { \n?a <b> ?c .\n} \n"),
                 ParseException);
  }

  {
    // Check if the correct ParseException is thrown after:
    // Select '*' [var_name]+
    ASSERT_THROW(parseQuery("SELECT DISTINCT * ?a WHERE { \n?a <b> ?c .\n} \n"),
                 ParseException);
  }

  {
    // Check if the correct ParseException is thrown after: Select ['*']{2,}
    ASSERT_THROW(parseQuery("SELECT DISTINCT * * WHERE { \n?a <b> ?c .\n} \n"),
                 ParseException);
  }
}

// _____________________________________________________________________________
TEST(ParserTest, testFilterWithoutDot) {
  ParsedQuery pq = parseQuery(
      "PREFIX fb: <http://rdf.freebase.com/ns/>\n"
      "\n"
      "SELECT DISTINCT ?1 WHERE {\n"
      " fb:m.0fkvn fb:government.government_office_category.officeholders "
      "?0 "
      ".\n"
      " ?0 fb:government.government_position_held.jurisdiction_of_office "
      "fb:m.0vmt .\n"
      " ?0 fb:government.government_position_held.office_holder ?1 .\n"
      " FILTER (?1 != fb:m.0fkvn)\n"
      " FILTER (?1 != fb:m.0vmt)\n"
      "FILTER (?1 != fb:m.018mts) \n"
      "} LIMIT 300");
  ASSERT_TRUE(pq.hasSelectClause());
  const auto& selectClause = pq.selectClause();
  ASSERT_EQ(1u, selectClause.getSelectedVariables().size());
  ASSERT_EQ(1u, pq.children().size());
  const auto& c = pq.children()[0].getBasic();
  ASSERT_EQ(3u, c._triples.size());
  const auto& filters = pq._rootGraphPattern._filters;
  ASSERT_EQ(3u, filters.size());
  ASSERT_EQ("(?1 != fb:m.0fkvn)", filters[0].expression_.getDescriptor());
  ASSERT_EQ("(?1 != fb:m.0vmt)", filters[1].expression_.getDescriptor());
  ASSERT_EQ("(?1 != fb:m.018mts)", filters[2].expression_.getDescriptor());
}

// _____________________________________________________________________________
TEST(ParserTest, testExpandPrefixes) {
  ParsedQuery pq = parseQuery(
      "PREFIX : <http://rdf.myprefix.com/>\n"
      "PREFIX ns: <http://rdf.myprefix.com/ns/>\n"
      "PREFIX xxx: <http://rdf.myprefix.com/xxx/>\n"
      "SELECT ?x ?z \n WHERE \t {?x :myrel ?y. ?y ns:myrel "
      "?z.?y <nsx:rel2> <http://abc.de>}");
  ASSERT_TRUE(pq.hasSelectClause());
  const auto& selectClause = pq.selectClause();
  ASSERT_EQ(1u, pq.children().size());
  const auto& c = pq.children()[0].getBasic();
  ASSERT_EQ(2u, selectClause.getSelectedVariables().size());
  ASSERT_EQ(3u, c._triples.size());
  ASSERT_EQ(Var{"?x"}, selectClause.getSelectedVariables()[0]);
  ASSERT_EQ(Var{"?z"}, selectClause.getSelectedVariables()[1]);
  ASSERT_EQ(Var{"?x"}, c._triples[0].s_);
  ASSERT_EQ("<http://rdf.myprefix.com/myrel>", getIriString(c._triples[0].p_));
  ASSERT_EQ(Var{"?y"}, c._triples[0].o_);
  ASSERT_EQ(Var{"?y"}, c._triples[1].s_);
  ASSERT_EQ("<http://rdf.myprefix.com/ns/myrel>",
            getIriString(c._triples[1].p_));
  ASSERT_EQ(Var{"?z"}, c._triples[1].o_);
  ASSERT_EQ(Var{"?y"}, c._triples[2].s_);
  ASSERT_EQ("<nsx:rel2>", getIriString(c._triples[2].p_));
  ASSERT_EQ(iri("<http://abc.de>"), c._triples[2].o_);
  ASSERT_EQ(std::nullopt, pq._limitOffset._limit);
  ASSERT_EQ(0, pq._limitOffset._offset);
}

// _____________________________________________________________________________
TEST(ParserTest, testLiterals) {
  ParsedQuery pq = parseQuery(
      "PREFIX xsd: <http://www.w3.org/2001/XMLSchema#> SELECT * WHERE { "
      "true <test:myrel> 10 . 10.2 <test:myrel> \"2000-01-01\"^^xsd:date }");
  ASSERT_TRUE(pq.hasSelectClause());
  const auto& selectClause = pq.selectClause();
  ASSERT_EQ(1u, pq.children().size());
  const auto& c = pq.children()[0].getBasic();
  ASSERT_TRUE(selectClause.isAsterisk());
  ASSERT_EQ(2u, c._triples.size());
  ASSERT_EQ(true, c._triples[0].s_);
  ASSERT_EQ("<test:myrel>", getIriString(c._triples[0].p_));
  ASSERT_EQ(10, c._triples[0].o_);
  ASSERT_EQ(10.2, c._triples[1].s_);
  ASSERT_EQ("<test:myrel>", getIriString(c._triples[1].p_));
  ASSERT_EQ(DateYearOrDuration{Date(2000, 1, 1, -1)}, c._triples[1].o_);
}

// _____________________________________________________________________________
TEST(ParserTest, testSolutionModifiers) {
  {
    ParsedQuery pq = parseQuery("SELECT ?x WHERE \t {?x <test:myrel> ?y}");
    ASSERT_TRUE(pq.hasSelectClause());
    const auto& selectClause = pq.selectClause();
    ASSERT_EQ(1u, pq.children().size());
    const auto& c = pq.children()[0].getBasic();
    ASSERT_EQ(1u, selectClause.getSelectedVariables().size());
    ASSERT_EQ(1u, c._triples.size());
    ASSERT_EQ(std::nullopt, pq._limitOffset._limit);
    ASSERT_EQ(0, pq._limitOffset._offset);
    ASSERT_EQ(size_t(0), pq._orderBy.size());
    ASSERT_FALSE(selectClause.distinct_);
    ASSERT_FALSE(selectClause.reduced_);
  }

  {
    auto pq = parseQuery("SELECT ?x WHERE \t {?x <test:myrel> ?y} LIMIT 10");
    ASSERT_TRUE(pq.hasSelectClause());
    const auto& selectClause = pq.selectClause();
    ASSERT_EQ(1u, selectClause.getSelectedVariables().size());
    ASSERT_EQ(1u, pq.children().size());
    const auto& c = pq.children()[0].getBasic();
    ASSERT_EQ(1u, c._triples.size());
    ASSERT_EQ(10ul, pq._limitOffset._limit);
    ASSERT_EQ(0, pq._limitOffset._offset);
    ASSERT_EQ(size_t(0), pq._orderBy.size());
    ASSERT_FALSE(selectClause.distinct_);
    ASSERT_FALSE(selectClause.reduced_);
  }

  {
    auto pq = parseQuery(
        "SELECT ?x WHERE \t {?x <test:myrel> ?y}\n"
        "LIMIT 10 OFFSET 15");
    ASSERT_TRUE(pq.hasSelectClause());
    const auto& selectClause = pq.selectClause();
    ASSERT_EQ(1u, pq.children().size());
    const auto& c = pq.children()[0].getBasic();
    ASSERT_EQ(1u, selectClause.getSelectedVariables().size());
    ASSERT_EQ(1u, c._triples.size());
    ASSERT_EQ(10u, pq._limitOffset._limit);
    ASSERT_EQ(15u, pq._limitOffset._offset);
    ASSERT_EQ(size_t(0), pq._orderBy.size());
    ASSERT_FALSE(selectClause.distinct_);
    ASSERT_FALSE(selectClause.reduced_);
  }

  {
    auto pq = parseQuery(
        "SELECT DISTINCT ?x ?y WHERE \t {?x <test:myrel> ?y}\n"
        "ORDER BY ?y LIMIT 10 OFFSET 15");
    ASSERT_TRUE(pq.hasSelectClause());
    const auto& selectClause = pq.selectClause();
    ASSERT_EQ(1u, pq.children().size());
    const auto& c = pq.children()[0].getBasic();
    ASSERT_EQ(2u, selectClause.getSelectedVariables().size());
    ASSERT_EQ(1u, c._triples.size());
    ASSERT_EQ(10u, pq._limitOffset._limit);
    ASSERT_EQ(15u, pq._limitOffset._offset);
    ASSERT_EQ(size_t(1), pq._orderBy.size());
    ASSERT_EQ(Var{"?y"}, pq._orderBy[0].variable_);
    ASSERT_FALSE(pq._orderBy[0].isDescending_);
    ASSERT_TRUE(selectClause.distinct_);
    ASSERT_FALSE(selectClause.reduced_);
  }

  {
    auto pq = parseQuery(
        "SELECT DISTINCT ?x ?ql_score_x_var_y ?y WHERE \t {?x "
        "ql:contains-entity ?y}\n"
        "ORDER BY ASC(?y) DESC(?ql_score_x_var_y) LIMIT 10 OFFSET 15");
    ASSERT_TRUE(pq.hasSelectClause());
    const auto& selectClause = pq.selectClause();
    ASSERT_EQ(1u, pq.children().size());
    const auto& c = pq.children()[0].getBasic();
    ASSERT_EQ(3u, selectClause.getSelectedVariables().size());
    ASSERT_EQ(Var{"?ql_score_x_var_y"}, selectClause.getSelectedVariables()[1]);
    ASSERT_EQ(1u, c._triples.size());
    ASSERT_EQ(10u, pq._limitOffset._limit);
    ASSERT_EQ(15u, pq._limitOffset._offset);
    ASSERT_EQ(size_t(2), pq._orderBy.size());
    ASSERT_EQ(Var{"?y"}, pq._orderBy[0].variable_);
    ASSERT_FALSE(pq._orderBy[0].isDescending_);
    ASSERT_EQ(Var{"?ql_score_x_var_y"}, pq._orderBy[1].variable_);
    ASSERT_TRUE(pq._orderBy[1].isDescending_);
    ASSERT_TRUE(selectClause.distinct_);
    ASSERT_FALSE(selectClause.reduced_);
  }

  {
    auto pq = parseQuery(
        "SELECT REDUCED ?x ?y WHERE \t {?x <test:myrel> ?y}\n"
        "ORDER BY DESC(?x) ASC(?y) LIMIT 10 OFFSET 15");
    ASSERT_TRUE(pq.hasSelectClause());
    const auto& selectClause = pq.selectClause();
    ASSERT_EQ(1u, pq.children().size());
    const auto& c = pq.children()[0].getBasic();
    ASSERT_EQ(2u, selectClause.getSelectedVariables().size());
    ASSERT_EQ(1u, c._triples.size());
    ASSERT_EQ(10u, pq._limitOffset._limit);
    ASSERT_EQ(15u, pq._limitOffset._offset);
    ASSERT_EQ(size_t(2), pq._orderBy.size());
    ASSERT_EQ(Var{"?x"}, pq._orderBy[0].variable_);
    ASSERT_TRUE(pq._orderBy[0].isDescending_);
    ASSERT_EQ(Var{"?y"}, pq._orderBy[1].variable_);
    ASSERT_FALSE(pq._orderBy[1].isDescending_);
    ASSERT_FALSE(selectClause.distinct_);
    ASSERT_TRUE(selectClause.reduced_);
  }

  {
    auto pq = parseQuery("SELECT ?x ?y WHERE {?x <is-a> <Actor>} LIMIT 10");
    ASSERT_EQ(10u, pq._limitOffset._limit);
  }

  {
    auto pq = parseQuery(
        "PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>"
        "SELECT DISTINCT ?movie WHERE { \n"
        "\n"
        "?movie <from-year> \"2000-01-01\"^^xsd:date .\n"
        "\n"
        "?movie <directed-by> <Scott%2C%20Ridley> .   }  LIMIT 50");
    ASSERT_TRUE(pq.hasSelectClause());
    const auto& selectClause = pq.selectClause();
    ASSERT_EQ(1u, pq.children().size());
    const auto& c = pq.children()[0].getBasic();
    ASSERT_EQ(1u, selectClause.getSelectedVariables().size());
    ASSERT_EQ(Var{"?movie"}, selectClause.getSelectedVariables()[0]);
    ASSERT_EQ(2u, c._triples.size());
    ASSERT_EQ(Var{"?movie"}, c._triples[0].s_);
    ASSERT_EQ("<from-year>", getIriString(c._triples[0].p_));
    ASSERT_EQ(DateYearOrDuration{Date(2000, 1, 1)}, c._triples[0].o_);
    ASSERT_EQ(Var{"?movie"}, c._triples[1].s_);
    ASSERT_EQ("<directed-by>", getIriString(c._triples[1].p_));
    ASSERT_EQ(iri("<Scott%2C%20Ridley>"), c._triples[1].o_);
  }

  {
    auto pq = parseQuery(
        "PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>"
        "SELECT DISTINCT ?movie WHERE { \n"
        "\n"
        "?movie <from-year> \"2000-01-01\"^^xsd:date .\n"
        "\n"
        "?movie <directed-by> <Scott%2C%20Ridley> .   }  LIMIT 50");
    ASSERT_TRUE(pq.hasSelectClause());
    const auto& selectClause = pq.selectClause();
    ASSERT_EQ(1u, pq.children().size());
    const auto& c = pq.children()[0].getBasic();
    ASSERT_EQ(1u, selectClause.getSelectedVariables().size());
    ASSERT_EQ(Var{"?movie"}, selectClause.getSelectedVariables()[0]);
    ASSERT_EQ(2u, c._triples.size());
    ASSERT_EQ(Var{"?movie"}, c._triples[0].s_);
    ASSERT_EQ("<from-year>", getIriString(c._triples[0].p_));
    ASSERT_EQ(DateYearOrDuration{Date(2000, 1, 1)}, c._triples[0].o_);
    ASSERT_EQ(Var{"?movie"}, c._triples[1].s_);
    ASSERT_EQ("<directed-by>", getIriString(c._triples[1].p_));
    ASSERT_EQ(iri("<Scott%2C%20Ridley>"), c._triples[1].o_);
  }

  {
    auto pq = parseQuery(
        "SELECT ?r (AVG(?r) as ?avg) WHERE {"
        "?a <http://schema.org/name> ?b ."
        "?a ql:has-relation ?r }"
        "GROUP BY ?r "
        "ORDER BY ?avg");
    ASSERT_EQ(1u, pq.children().size());
    ASSERT_EQ(1u, pq._orderBy.size());
    EXPECT_THAT(pq, m::GroupByVariables({Var{"?r"}}));
    ASSERT_EQ(Var{"?avg"}, pq._orderBy[0].variable_);
    ASSERT_FALSE(pq._orderBy[0].isDescending_);
  }

  {
    auto pq = parseQuery(
        "SELECT ?r (STDEV(?r) as ?stdev) WHERE {"
        "?a <http://schema.org/name> ?b ."
        "?a ql:has-relation ?r }"
        "GROUP BY ?r "
        "ORDER BY ?stdev");
    ASSERT_EQ(1u, pq.children().size());
    ASSERT_EQ(1u, pq._orderBy.size());
    EXPECT_THAT(pq, m::GroupByVariables({Var{"?r"}}));
    ASSERT_EQ(Var{"?stdev"}, pq._orderBy[0].variable_);
    ASSERT_FALSE(pq._orderBy[0].isDescending_);
  }

  {
    auto pq = parseQuery(
        "SELECT ?r (COUNT(DISTINCT ?r) as ?count) WHERE {"
        "?a <http://schema.org/name> ?b ."
        "?a ql:has-relation ?r }"
        "GROUP BY ?r "
        "ORDER BY ?count");
    ASSERT_EQ(1u, pq._orderBy.size());
    EXPECT_THAT(pq, m::GroupByVariables({Var{"?r"}}));
    ASSERT_EQ(Var{"?count"}, pq._orderBy[0].variable_);
    ASSERT_FALSE(pq._orderBy[0].isDescending_);
  }

  {
    auto pq = parseQuery(
        "SELECT ?r (GROUP_CONCAT(?r;SEPARATOR=\"Cake\") as ?concat) WHERE {"
        "?a <http://schema.org/name> ?b ."
        "?a ql:has-relation ?r }"
        "GROUP BY ?r "
        "ORDER BY ?concat");
    ASSERT_TRUE(pq.hasSelectClause());
    const auto& aliases = pq.selectClause().getAliases();
    ASSERT_EQ(1u, aliases.size());
    ASSERT_EQ("(GROUP_CONCAT(?r;SEPARATOR=\"Cake\") as ?concat)",
              aliases[0].getDescriptor());
  }
}

// _____________________________________________________________________________
TEST(ParserTest, testGroupByAndAlias) {
  ParsedQuery pq = parseQuery(
      "SELECT (COUNT(?a) as ?count) WHERE { ?b <rel> ?a } GROUP BY ?b");
  ASSERT_TRUE(pq.hasSelectClause());
  const auto& selectClause = pq.selectClause();
  ASSERT_EQ(1u, selectClause.getSelectedVariables().size());
  ASSERT_EQ(Var{"?count"}, selectClause.getSelectedVariables()[0]);

  const auto& aliases = selectClause.getAliases();
  ASSERT_EQ(1u, aliases.size());
  ASSERT_TRUE(aliases[0]._expression.isAggregate({}));
  ASSERT_EQ("(COUNT(?a) as ?count)", aliases[0].getDescriptor());
  EXPECT_THAT(pq, m::GroupByVariables({Var{"?b"}}));
}

// _____________________________________________________________________________
TEST(ParserTest, Bind) {
  ParsedQuery pq = parseQuery("SELECT ?a WHERE { BIND (10 - 5 as ?a) . }");
  ASSERT_TRUE(pq.hasSelectClause());
  ASSERT_EQ(pq.children().size(), 1);
  p::GraphPatternOperation child = pq.children()[0];
  ASSERT_TRUE(holds_alternative<p::Bind>(child));
  p::Bind bind = get<p::Bind>(child);
  ASSERT_EQ(bind._target, Var{"?a"});
  ASSERT_EQ(bind._expression.getDescriptor(), "10 - 5");
}

// _____________________________________________________________________________
TEST(ParserTest, Order) {
  {
    ParsedQuery pq = parseQuery("SELECT ?x ?y WHERE { ?x <test/myrel> ?y }");
    ASSERT_TRUE(pq._orderBy.empty());
    ASSERT_EQ(pq._rootGraphPattern._graphPatterns.size(), 1);
    ASSERT_TRUE(holds_alternative<p::BasicGraphPattern>(
        pq._rootGraphPattern._graphPatterns[0]));
  }
  {
    ParsedQuery pq =
        parseQuery("SELECT ?x ?y WHERE { ?x <test/myrel> ?y } ORDER BY ?x");
    ASSERT_EQ(pq._orderBy.size(), 1);
    EXPECT_THAT(pq._orderBy[0], m::VariableOrderKey(Var{"?x"}, false));
    ASSERT_EQ(pq._rootGraphPattern._graphPatterns.size(), 1);
    ASSERT_TRUE(holds_alternative<p::BasicGraphPattern>(
        pq._rootGraphPattern._graphPatterns[0]));
  }
  {
    ParsedQuery pq = parseQuery(
        "SELECT ?x ?y WHERE { ?x <test/myrel> ?y } ORDER BY ASC(?y)");
    ASSERT_EQ(pq._orderBy.size(), 1);
    EXPECT_THAT(pq._orderBy[0], m::VariableOrderKey(Var{"?y"}, false));
    ASSERT_EQ(pq._rootGraphPattern._graphPatterns.size(), 1);
    ASSERT_TRUE(holds_alternative<p::BasicGraphPattern>(
        pq._rootGraphPattern._graphPatterns[0]));
  }
  {
    ParsedQuery pq = parseQuery(
        "SELECT ?x ?y WHERE { ?x <test/myrel> ?y } ORDER BY DESC(?x)");
    ASSERT_EQ(pq._orderBy.size(), 1);
    EXPECT_THAT(pq._orderBy[0], m::VariableOrderKey(Var{"?x"}, true));
    ASSERT_EQ(pq._rootGraphPattern._graphPatterns.size(), 1);
    ASSERT_TRUE(holds_alternative<p::BasicGraphPattern>(
        pq._rootGraphPattern._graphPatterns[0]));
  }
  {
    ParsedQuery pq = parseQuery(
        "SELECT ?x WHERE { ?x <test/myrel> ?y } GROUP BY ?x ORDER BY ?x");
    ASSERT_EQ(pq._orderBy.size(), 1);
    EXPECT_THAT(pq._orderBy[0], m::VariableOrderKey(Var{"?x"}, false));
    ASSERT_EQ(pq._rootGraphPattern._graphPatterns.size(), 1);
    ASSERT_TRUE(holds_alternative<p::BasicGraphPattern>(
        pq._rootGraphPattern._graphPatterns[0]));
  }
  {
    ParsedQuery pq = parseQuery(
        "SELECT ?x (COUNT(?y) as ?c) WHERE { ?x <test/myrel> "
        "?y } GROUP BY ?x ORDER BY ?c");
    ASSERT_EQ(pq._orderBy.size(), 1);
    EXPECT_THAT(pq._orderBy[0], m::VariableOrderKey(Var{"?c"}, false));
  }
  {
    ParsedQuery pq = parseQuery(
        "SELECT ?x ?y WHERE { ?x <test/myrel> ?y } ORDER BY (?x - ?y)");
    ASSERT_EQ(pq._orderBy.size(), 1);
    auto variant = pq._rootGraphPattern._graphPatterns[1];
    ASSERT_TRUE(holds_alternative<p::Bind>(variant));
    auto helperBind = get<p::Bind>(variant);
    ASSERT_EQ(helperBind._expression.getDescriptor(), "(?x - ?y)");
    ASSERT_EQ(pq._orderBy[0].variable_, helperBind._target);
  }
  // TODO<joka921> This works now. Adapt the unit tests accordingly.
  /*
  {
    // Ordering by an expression while grouping is currently not supported.
    EXPECT_THROW(parseQuery(
                     "SELECT ?y WHERE { ?x <test/myrel> ?y } GROUP BY "
                     "?y ORDER BY (?x - ?y)"),
                 ParseException);
  }
  {
    // Ordering by an expression while grouping is currently not supported.
    EXPECT_THROW(parseQuery(
                     "SELECT ?y WHERE { ?x <test/myrel> ?y } GROUP BY "
                     "?y ORDER BY (2 * ?y)"),
                 ParseException);
  }
   */
}

// _____________________________________________________________________________
TEST(ParserTest, Group) {
  {
    ParsedQuery pq =
        parseQuery("SELECT ?x WHERE { ?x <test/myrel> ?y } GROUP BY ?x");
    EXPECT_THAT(pq, m::GroupByVariables({Var{"?x"}}));
  }
  {
    // grouping by a variable
    ParsedQuery pq =
        parseQuery("SELECT ?x WHERE { ?x <test/myrel> ?y } GROUP BY ?y ?x");
    EXPECT_THAT(pq, m::GroupByVariables({Var{"?y"}, Var{"?x"}}));
  }
  {
    // grouping by an expression
    ParsedQuery pq = parseQuery(
        "SELECT ?x WHERE { ?x <test/myrel> ?y } GROUP BY (?x - ?y) ?x");
    auto variant = pq._rootGraphPattern._graphPatterns[1];
    ASSERT_TRUE(holds_alternative<p::Bind>(variant));
    auto helperBind = get<p::Bind>(variant);
    ASSERT_THAT(helperBind, m::BindExpression("?x - ?y"));
    EXPECT_THAT(pq, m::GroupByVariables({helperBind._target, Var{"?x"}}));
  }
  {
    // grouping by an expression with an alias
    ParsedQuery pq = parseQuery(
        "SELECT ?x WHERE { ?x <test/myrel> ?y } GROUP BY (?x "
        "- ?y AS ?foo) ?x");
    EXPECT_THAT(pq._rootGraphPattern._graphPatterns[1],
                m::Bind(Var{"?foo"}, "?x - ?y"));
    EXPECT_THAT(pq, m::GroupByVariables({Var{"?foo"}, Var{"?x"}}));
  }
  {
    // grouping by a builtin call
    ParsedQuery pq = parseQuery(
        "SELECT ?x WHERE { ?x <test/myrel> ?y } GROUP BY COUNT(?x) ?x");
    auto variant = pq._rootGraphPattern._graphPatterns[1];
    ASSERT_TRUE(holds_alternative<p::Bind>(variant));
    auto helperBind = get<p::Bind>(variant);
    ASSERT_THAT(helperBind, m::BindExpression("COUNT(?x)"));
    EXPECT_THAT(pq, m::GroupByVariables({helperBind._target, Var{"?x"}}));
  }
  {
    // grouping by a function call
    ParsedQuery pq = parseQuery(
        "SELECT ?x WHERE { ?x <test/myrel> ?y } GROUP BY "
        "<http://www.opengis.net/def/function/geosparql/"
        "latitude>(?y) ?x");
    auto variant = pq._rootGraphPattern._graphPatterns[1];
    ASSERT_TRUE(holds_alternative<p::Bind>(variant));
    auto helperBind = get<p::Bind>(variant);
    ASSERT_THAT(
        helperBind,
        m::BindExpression(
            "<http://www.opengis.net/def/function/geosparql/latitude>(?y)"));
    EXPECT_THAT(pq, m::GroupByVariables({helperBind._target, Var{"?x"}}));
  }
  {
    // selection of a variable that is not grouped/aggregated
    EXPECT_THROW(
        parseQuery("SELECT ?x ?y WHERE { ?x <test/myrel> ?y } GROUP BY ?x"),
        ParseException);
  }
}

// _____________________________________________________________________________
TEST(ParserTest, LanguageFilterPostProcessing) {
  {
    ParsedQuery q = parseQuery(
        "SELECT * WHERE {?x <label> ?y . FILTER (LANG(?y) = \"en\")}");
    ASSERT_TRUE(q._rootGraphPattern._filters.empty());
    const auto& triples =
        q._rootGraphPattern._graphPatterns[0].getBasic()._triples;
    ASSERT_EQ(1u, triples.size());
    ASSERT_EQ((SparqlTriple{Var{"?x"},
                            PropertyPath::fromIri(
                                ad_utility::convertToLanguageTaggedPredicate(
                                    iri("<label>"), "en")),
                            Var{"?y"}}),
              triples[0]);
  }
  {
    ParsedQuery q = parseQuery(
        "SELECT * WHERE {<somebody> ?p ?y . FILTER (LANG(?y) = \"en\")}");
    ASSERT_TRUE(q._rootGraphPattern._filters.empty());
    const auto& triples =
        q._rootGraphPattern._graphPatterns[0].getBasic()._triples;
    ASSERT_EQ(2u, triples.size());
    ASSERT_EQ((SparqlTriple{iri("<somebody>"), Var{"?p"}, Var{"?y"}}),
              triples[0]);
    ASSERT_EQ((SparqlTriple{
                  Var{"?y"},
                  PropertyPath::fromIri(iri("<http://qlever.cs.uni-freiburg.de/"
                                            "builtin-functions/langtag>")),
                  ad_utility::convertLangtagToEntityUri("en")}),
              triples[1]);
  }

  // Test that the language filter never changes triples with
  // `ql:contains-entity` etc.
  {
    ParsedQuery q = parseQuery(
        "SELECT * WHERE {?x <label> ?y . ?text ql:contains-entity ?y. FILTER "
        "(LANG(?y) = \"en\")}");
    ASSERT_TRUE(q._rootGraphPattern._filters.empty());
    const auto& triples =
        q._rootGraphPattern._graphPatterns[0].getBasic()._triples;
    ASSERT_EQ(2u, triples.size());
    ASSERT_EQ((SparqlTriple{Var{"?x"},
                            PropertyPath::fromIri(
                                ad_utility::convertToLanguageTaggedPredicate(
                                    iri("<label>"), "en")),
                            Var{"?y"}}),
              triples[0]);
    ASSERT_EQ(
        (SparqlTriple{Var{"?text"},
                      PropertyPath::fromIri(iri(CONTAINS_ENTITY_PREDICATE)),
                      Var{"?y"}}),
        triples[1]);
  }
  {
    ParsedQuery q = parseQuery(
        "SELECT * WHERE {<somebody> ?p ?y . ?text ql:contains-entity ?y FILTER "
        "(LANG(?y) = \"en\")}");
    ASSERT_TRUE(q._rootGraphPattern._filters.empty());
    const auto& triples =
        q._rootGraphPattern._graphPatterns[0].getBasic()._triples;
    ASSERT_EQ(3u, triples.size());
    ASSERT_EQ((SparqlTriple{iri("<somebody>"), Var{"?p"}, Var{"?y"}}),
              triples[0]);
    ASSERT_EQ(
        (SparqlTriple{Var{"?text"},
                      PropertyPath::fromIri(iri(CONTAINS_ENTITY_PREDICATE)),
                      Var{"?y"}}),
        triples[1]);
    ASSERT_EQ(
        (SparqlTriple{
            Var{"?y"},
            PropertyPath::fromIri(iri("<http://qlever.cs.uni-freiburg.de/"
                                      "builtin-functions/langtag>")),
            iri("<http://qlever.cs.uni-freiburg.de/builtin-functions/@en>")}),
        triples[2]);
  }
  // Ensure filter is applied regularly if variable does not originate from
  // triple
  {
    ParsedQuery q = parseQuery(
        "SELECT * { VALUES ?x { \"test\"@en } . FILTER (LANG(?x) = \"en\")}");

    EXPECT_TRUE(std::holds_alternative<parsedQuery::Values>(
        q._rootGraphPattern._graphPatterns[0]));
    ASSERT_EQ(q._rootGraphPattern._filters.size(), 1);
    ASSERT_EQ(q._rootGraphPattern._filters[0].expression_.getDescriptor(),
              "(LANG(?x) = \"en\")");
  }
  // Verify the filter is not applied as a regular filter if it is used
  // somewhere in a triple
  {
    ParsedQuery q =
        parseQuery("SELECT * { ?x ?y ?z . FILTER (LANG(?x) = \"en\")}");
    ASSERT_TRUE(q._rootGraphPattern._filters.empty());
  }
  {
    ParsedQuery q =
        parseQuery("SELECT * { ?x ?y ?z . FILTER (LANG(?z) = \"en\")}");
    ASSERT_TRUE(q._rootGraphPattern._filters.empty());
  }
  {
    ParsedQuery q =
        parseQuery("SELECT * { ?x ?y ?z . FILTER (LANG(?y) = \"en\")}");
    ASSERT_TRUE(q._rootGraphPattern._filters.empty());
  }
  {
    ParsedQuery q = parseQuery(
        "SELECT * { ?x ?y ?z . ?a ?b ?c ."
        "?d <a> ?f . FILTER (LANG(?a) = \"en\")}");
    ASSERT_TRUE(q._rootGraphPattern._filters.empty());
  }
}

// _____________________________________________________________________________
namespace {
std::string getFirstTriple(const ParsedQuery& q) {
  return q._rootGraphPattern._graphPatterns.at(0)
      .getBasic()
      ._triples.at(0)
      .asString();
}
}  // namespace

// _____________________________________________________________________________
TEST(ParserTest, HandlesBasicUnicodeEscapeSequences) {
  ParsedQuery q1 = parseQuery(
      R"(SELECT * WHERE { ?s <http://a.example/p1> '\u0080\u07FF\u0800\u0FFF\u1000\uCFFF\uD000\uD7FF\uE000\uFFFD\U00010000\U0003FFFD\U00040000\U000FFFFD\U00100000\U0010FFFD'})");
  EXPECT_EQ(getFirstTriple(q1),
            "{s: ?s, p: <http://a.example/p1>, o: "
            "\"\u0080\u07FF\u0800\u0FFF\u1000\uCFFF\uD000\uD7FF\uE000\uFFFD"
            "\U00010000\U0003FFFD\U00040000\U000FFFFD\U00100000\U0010FFFD\"}");

  ParsedQuery q2 = parseQuery(R"(SELECT * WHERE { ?s ?p "\U0001f46a" . })");
  EXPECT_EQ(getFirstTriple(q2), "{s: ?s, p: ?p, o: \"\U0001f46a\"}");

  ParsedQuery q3 = parseQuery(
      R"(PREFIX \u03B1: <http://example.com/\u00E9fg> SELECT * WHERE { ?s ?p α\u003Aba . })");
  EXPECT_EQ(getFirstTriple(q3),
            "{s: ?s, p: ?p, o: <http://example.com/éfgba>}");

  ParsedQuery q4 = parseQuery(
      R"(SELECT * WHERE { <http://example.com/\U0001F937\U0001F3FD\u200D\U00002642\ufe0F> ?p\u00201. })");
  EXPECT_EQ(getFirstTriple(q4),
            "{s: <http://example.com/🤷🏽‍♂️>, p: ?p, o: 1}");

  // Ensure we don't double-unescape, \u sequences are not allowed in literals
  EXPECT_THROW(parseQuery(R"(SELECT * WHERE { "\u005Cu2764" ?p 1. })"),
               InvalidSparqlQueryException);
}

// _____________________________________________________________________________
TEST(ParserTest, HandlesSurrogatesCorrectly) {
  using ::testing::HasSubstr;
  ParsedQuery q = parseQuery(
      R"(SELECT * WHERE { "\uD83E\udD37\uD83C\uDFFD\u200D\u2642\uFE0F" ?p 1. })");
  EXPECT_EQ(getFirstTriple(q), "{s: \"🤷🏽‍♂️\", p: ?p, o: 1}");

  AD_EXPECT_THROW_WITH_MESSAGE_AND_TYPE(
      parseQuery(R"(SELECT * WHERE { ?s ?p '\uD83C \uDFFD' })"),
      HasSubstr(
          "A high surrogate must be directly followed by a low surrogate."),
      InvalidSparqlQueryException);

  AD_EXPECT_THROW_WITH_MESSAGE_AND_TYPE(
      parseQuery(R"(SELECT * WHERE { ?s ?p '\uD800' })"),
      HasSubstr("A high surrogate must be followed by a low surrogate."),
      InvalidSparqlQueryException);

  AD_EXPECT_THROW_WITH_MESSAGE_AND_TYPE(
      parseQuery(R"(SELECT * WHERE { ?s ?p '\U0000D800' })"),
      HasSubstr("Surrogates should not be encoded as full code points."),
      InvalidSparqlQueryException);

  AD_EXPECT_THROW_WITH_MESSAGE_AND_TYPE(
      parseQuery(R"(SELECT * WHERE { ?s ?p '\uD800\uD800' })"),
      HasSubstr(
          "A high surrogate cannot be followed by another high surrogate."),
      InvalidSparqlQueryException);

  AD_EXPECT_THROW_WITH_MESSAGE_AND_TYPE(
      parseQuery(R"(SELECT * WHERE { ?s ?p '\U0000DFFD' })"),
      HasSubstr("Surrogates should not be encoded as full code points."),
      InvalidSparqlQueryException);

  AD_EXPECT_THROW_WITH_MESSAGE_AND_TYPE(
      parseQuery(R"(SELECT * WHERE { ?s ?p '\uDFFD' })"),
      HasSubstr("A low surrogate cannot be the first surrogate."),
      InvalidSparqlQueryException);

  AD_EXPECT_THROW_WITH_MESSAGE_AND_TYPE(
      parseQuery(R"(SELECT * WHERE { ?s ?p '\uD800\u0020' })"),
      HasSubstr("A high surrogate cannot be followed by a regular code point."),
      InvalidSparqlQueryException);

  // Note: We don't allow mixing escaped and unescape surrogates, that's just
  // weird and the C++ compiler rightfully won't compile strings like these:
  // SELECT * WHERE { ?s ?p '\\uD83C\uDFFD' }
  // SELECT * WHERE { ?s ?p '\uD83C\\uDFFD' }

  // So writing unit tests for these cases is not possible without creating
  // semi-invalid UTF-8 strings.
}

// _____________________________________________________________________________
TEST(ParserTest, BaseDeclaration) {
  // Simple case
  auto query1 =
      parseQuery("BASE <http://example.org/> SELECT * WHERE { ?s <p> <test> }");
  EXPECT_EQ(getFirstTriple(query1),
            "{s: ?s, p: <http://example.org/p>, o: <http://example.org/test>}");
  // Relative and absolute IRIs mixed
  auto query2 = parseQuery(
      "BASE <http://example.org/something> "
      "SELECT * WHERE { </root> <p> <http://other.example.org/p> }");
  EXPECT_EQ(getFirstTriple(query2),
            "{s: <http://example.org/root>,"
            " p: <http://example.org/something/p>,"
            " o: <http://other.example.org/p>}");

  // Cascading prefixes
  auto query3 = parseQuery(
      "BASE <http://example.org/> "
      "PREFIX ex1: <ex1/> "
      "BASE <http://other.example.org/> "
      "PREFIX ex2: <ex2/> "
      "BASE <http://alternative.example.org/> "
      "SELECT * WHERE { ex2:hello <world> ex1:test }");
  EXPECT_EQ(getFirstTriple(query3),
            "{s: <http://other.example.org/ex2/hello>,"
            " p: <http://alternative.example.org/world>,"
            " o: <http://example.org/ex1/test>}");

  AD_EXPECT_THROW_WITH_MESSAGE_AND_TYPE(
      parseQuery("BASE <http://example.com> BASE <relative> "
                 "SELECT * WHERE { ?s ?p ?o }"),
      ::testing::HasSubstr("absolute IRI"), InvalidSparqlQueryException);
}

TEST(ParserTest, parseWithDatasets) {
  // This test tests the correct behaviour and propagation of override datasets
  // (datasets passed as URL parameters overwrite all datasets in the
  // operation). `SparqlParser::Datasets` tests that datasets set in the
  // operation are propagated correctly.
  auto Iri = ad_utility::triple_component::Iri::fromIriref;
  auto query = "SELECT * WHERE { ?s ?p ?o }";
  auto noGraphs = m::Graphs{};
  auto queryGraphPatternMatcher =
      m::GraphPattern(m::Triples({{Var("?s"), Var{"?p"}, Var("?o")}}));
  EXPECT_THAT(parseQuery(query, {}),
              m::SelectQuery(m::AsteriskSelect(), queryGraphPatternMatcher));
  EXPECT_THAT(parseQuery(query, {{DatasetClause{Iri("<foo>"), true}}}),
              m::SelectQuery(m::AsteriskSelect(), queryGraphPatternMatcher,
                             noGraphs, {{Iri("<foo>")}}));
  EXPECT_THAT(parseQuery(query, {{DatasetClause{Iri("<bar>"), false}}}),
              m::SelectQuery(m::AsteriskSelect(), queryGraphPatternMatcher,
                             {{Iri("<bar>")}}, noGraphs));
  EXPECT_THAT(parseQuery(query, {{DatasetClause{Iri("<bar>"), false},
                                  DatasetClause{Iri("<foo>"), true},
                                  DatasetClause{Iri("<baz>"), false}}}),
              m::SelectQuery(m::AsteriskSelect(), queryGraphPatternMatcher,
                             {{Iri("<bar>"), Iri("<baz>")}}, {{Iri("<foo>")}}));
  ScanSpecificationAsTripleComponent::Graphs datasets{{Iri("<h>")}};
  auto filterGraphPattern = m::Filters(m::ExistsFilter(
      m::GraphPattern(m::Triples({{Var("?a"), Var{"?b"}, Var("?c")}})),
      datasets, noGraphs));

  ad_utility::BlankNodeManager bnm;
  EncodedIriManager ev;
  // If the datasets are specified externally, then `USING [NAMED]` is forbidden
  // by the SPARQL standard.
  AD_EXPECT_THROW_WITH_MESSAGE(
      SparqlParser::parseUpdate(&bnm, &ev,
                                "DELETE { ?x <b> <c> } USING <g> WHERE { ?x ?y "
                                "?z FILTER EXISTS {?a ?b ?c} }",
                                {{{Iri("<h>"), false}}}),
      ::testing::HasSubstr("`USING [NAMED]` is disallowed"));
  // Same goes for `WITH`
  AD_EXPECT_THROW_WITH_MESSAGE(
      SparqlParser::parseUpdate(&bnm, &ev,
                                "WITH <g> DELETE { ?x <b> <c> } WHERE { "
                                "?x ?y ?z "
                                "FILTER EXISTS {?a ?b ?c} }",
                                {{{Iri("<h>"), false}}}),
      ::testing::HasSubstr("`WITH` is disallowed"));
  EXPECT_THAT(
      parseQuery(
          "SELECT * FROM <g> WHERE { ?x ?y ?z FILTER EXISTS {?a ?b ?c} }",
          {{{Iri("<h>"), false}}}),
      m::SelectQuery(m::AsteriskSelect(), filterGraphPattern, datasets,
                     noGraphs));
  EXPECT_THAT(parseQuery("ASK FROM <g> { ?x ?y ?z FILTER EXISTS {?a ?b ?c}}",
                         {{{Iri("<h>"), false}}}),
              m::AskQuery(filterGraphPattern, datasets, noGraphs));
  EXPECT_THAT(parseQuery("CONSTRUCT {<a> <b> <c>} FROM <g> { "
                         "?x ?y ?z FILTER EXISTS {?a ?b?c}}",
                         {{{Iri("<h>"), false}}}),
              m::ConstructQuery({std::array<GraphTerm, 3>{
                                    ::Iri("<a>"), ::Iri("<b>"), ::Iri("<c>")}},
                                filterGraphPattern, datasets, noGraphs));
  EXPECT_THAT(
      parseQuery("Describe ?x FROM <g> { ?x ?y ?z FILTER EXISTS {?a ?b ?c}}",
                 {{{Iri("<h>"), false}}}),
      m::DescribeQuery(
          m::Describe({Var("?x")}, {datasets, {}},
                      m::SelectQuery(m::VariablesSelect({"?x"}, false, false),
                                     filterGraphPattern)),
          datasets, noGraphs));
  auto deleteWhereOp =
      m::GraphUpdate({SparqlTripleSimpleWithGraph{Var("?s"), Var("?p"),
                                                  Var("?o"), std::monostate{}}},
                     {});
  auto deleteWherePattern =
      m::GraphPattern(m::Triples({{Var("?s"), Var("?p"), Var("?o")}}));
  auto insertDataOp = m::GraphUpdate(
      {}, {SparqlTripleSimpleWithGraph{Iri("<a>"), Iri("<b>"), Iri("<c>"),
                                       std::monostate{}}});
  EXPECT_THAT(
      SparqlParser::parseUpdate(
          &bnm, &ev, "DELETE WHERE { ?s ?p ?o }; INSERT DATA { <a> <b> <c> }",
          {DatasetClause{Iri("<foo>"), false},
           DatasetClause{Iri("<bar>"), true}}),
      testing::ElementsAre(
          m::UpdateClause(
              deleteWhereOp, deleteWherePattern,
              m::datasetClausesMatcher({{Iri("<foo>")}}, {{Iri("<bar>")}})),
          m::UpdateClause(
              insertDataOp, m::GraphPattern(),
              m::datasetClausesMatcher({{Iri("<foo>")}}, {{Iri("<bar>")}}))));
}

// _____________________________________________________________________________
TEST(ParserTest, variablesInMinusAreHidden) {
  EXPECT_THAT(
      parseQuery(
          "SELECT * { VALUES ?a { 1 } MINUS { VALUES (?a ?b) { ( 2 2 ) } } }"),
      m::SelectQuery(
          m::VariablesSelect({"?a"}, false, false),
          m::GraphPattern(
              m::InlineData({Variable{"?a"}}, {{TripleComponent{1}}}),
              m::Minus(m::GraphPattern(m::InlineData(
                  {Variable{"?a"}, Variable{"?b"}},
                  {{TripleComponent{2}, TripleComponent{2}}}))))));
}

// _____________________________________________________________________________
TEST(ParserTest, ensureTypeIriDoesntViolateAssertion) {
  // This is a regression test for
  // https://github.com/ad-freiburg/qlever/issues/2350
  EXPECT_THAT(
      parseQuery(
          "SELECT * "
          "{ ?s !<http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?o }"),
      m::SelectQuery(
          m::AsteriskSelect(),
          m::GraphPattern(m::Triples({SparqlTriple{
              TripleComponent{Variable{"?s"}},
              PropertyPath::makeNegated({PropertyPath::fromIri(
                  ad_utility::triple_component::Iri::fromIriref(
                      "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>"))}),
              TripleComponent{Variable{"?o"}}}}))));

  // Other tests for similar variants.
  EXPECT_THAT(
      parseQuery("SELECT * { ?s !a ?o }"),
      m::SelectQuery(
          m::AsteriskSelect(),
          m::GraphPattern(m::Triples({SparqlTriple{
              TripleComponent{Variable{"?s"}},
              PropertyPath::makeNegated({PropertyPath::fromIri(
                  ad_utility::triple_component::Iri::fromIriref(
                      "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>"))}),
              TripleComponent{Variable{"?o"}}}}))));
  EXPECT_THAT(
      parseQuery("SELECT * { ?s !^a ?o }"),
      m::SelectQuery(m::AsteriskSelect(),
                     m::GraphPattern(m::Triples({SparqlTriple{
                         TripleComponent{Variable{"?s"}},
                         PropertyPath::makeNegated(
                             {PropertyPath::makeInverse(PropertyPath::fromIri(
                                 ad_utility::triple_component::Iri::fromIriref(
                                     "<http://www.w3.org/1999/02/"
                                     "22-rdf-syntax-ns#type>")))}),
                         TripleComponent{Variable{"?o"}}}}))));
}
