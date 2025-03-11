// Copyright 2015 - 2025, University of Freiburg
// Chair of Algorithms and Data Structures
// Authors: Björn Buchhold <buchhold@cs.uni-freiburg.de> [2015 - 2017]
//          Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include <gmock/gmock.h>

#include "./printers/PayloadVariablePrinters.h"
#include "QueryPlannerTestHelpers.h"
#include "engine/QueryPlanner.h"
#include "engine/SpatialJoin.h"
#include "parser/GraphPatternOperation.h"
#include "parser/MagicServiceQuery.h"
#include "parser/PayloadVariables.h"
#include "parser/SparqlParser.h"
#include "parser/SpatialQuery.h"
#include "parser/data/Variable.h"
#include "util/TripleComponentTestHelpers.h"

namespace h = queryPlannerTestHelpers;
namespace {
using Var = Variable;
constexpr auto iri = ad_utility::testing::iri;
}  // namespace
using ::testing::HasSubstr;

QueryPlanner makeQueryPlanner() {
  return QueryPlanner{ad_utility::testing::getQec(),
                      std::make_shared<ad_utility::CancellationHandle<>>()};
}

TEST(QueryPlanner, createTripleGraph) {
  using TripleGraph = QueryPlanner::TripleGraph;
  using Node = QueryPlanner::TripleGraph::Node;
  using std::vector;

  {
    ParsedQuery pq = SparqlParser::parseQuery(
        "PREFIX : <http://rdf.myprefix.com/>\n"
        "PREFIX ns: <http://rdf.myprefix.com/ns/>\n"
        "PREFIX xxx: <http://rdf.myprefix.com/xxx/>\n"
        "SELECT ?x ?z \n "
        "WHERE \t {?x :myrel ?y. ?y ns:myrel ?z.?y xxx:rel2 "
        "<http://abc.de>}");
    QueryPlanner qp = makeQueryPlanner();
    auto tg = qp.createTripleGraph(
        &pq._rootGraphPattern._graphPatterns[0].getBasic());
    TripleGraph expected =
        TripleGraph(std::vector<std::pair<Node, std::vector<size_t>>>(
            {std::make_pair<Node, vector<size_t>>(
                 QueryPlanner::TripleGraph::Node(
                     0,
                     SparqlTriple(Var{"?x"}, "<http://rdf.myprefix.com/myrel>",
                                  Var{"?y"})),
                 {1, 2}),
             std::make_pair<Node, vector<size_t>>(
                 QueryPlanner::TripleGraph::Node(
                     1, SparqlTriple(Var{"?y"},
                                     "<http://rdf.myprefix.com/ns/myrel>",
                                     Var{"?z"})),
                 {0, 2}),
             std::make_pair<Node, vector<size_t>>(
                 QueryPlanner::TripleGraph::Node(
                     2, SparqlTriple(Var{"?y"},
                                     "<http://rdf.myprefix.com/xxx/rel2>",
                                     iri("<http://abc.de>"))),
                 {0, 1})}));

    ASSERT_TRUE(tg.isSimilar(expected));
  }

  {
    ParsedQuery pq = SparqlParser::parseQuery(
        "SELECT ?x WHERE {?x ?p <X>. ?x ?p2 <Y>. <X> ?p <Y>}");
    QueryPlanner qp = makeQueryPlanner();
    auto tg = qp.createTripleGraph(&pq.children()[0].getBasic());
    TripleGraph expected =
        TripleGraph(std::vector<std::pair<Node, std::vector<size_t>>>(
            {std::make_pair<Node, vector<size_t>>(
                 QueryPlanner::TripleGraph::Node(
                     0, SparqlTriple(Var{"?x"}, "?p", iri("<X>"))),
                 {1, 2}),
             std::make_pair<Node, vector<size_t>>(
                 QueryPlanner::TripleGraph::Node(
                     1, SparqlTriple(Var{"?x"}, "?p2", iri("<Y>"))),
                 {0}),
             std::make_pair<Node, vector<size_t>>(
                 QueryPlanner::TripleGraph::Node(
                     2, SparqlTriple(iri("<X>"), "?p", iri("<Y>"))),
                 {0})}));
    ASSERT_TRUE(tg.isSimilar(expected));
  }

  {
    ParsedQuery pq = SparqlParser::parseQuery(
        "SELECT ?x WHERE { ?x <is-a> <Book> . \n"
        "?x <Author> <Anthony_Newman_(Author)> }");
    QueryPlanner qp = makeQueryPlanner();
    auto tg = qp.createTripleGraph(&pq.children()[0].getBasic());

    TripleGraph expected =
        TripleGraph(std::vector<std::pair<Node, std::vector<size_t>>>({
            std::make_pair<Node, vector<size_t>>(
                QueryPlanner::TripleGraph::Node(
                    0, SparqlTriple(Var{"?x"}, "<is-a>", iri("<Book>"))),
                {1}),
            std::make_pair<Node, vector<size_t>>(
                QueryPlanner::TripleGraph::Node(
                    1, SparqlTriple(Var{"?x"}, "<Author>",
                                    iri("<Anthony_Newman_(Author)>"))),
                {0}),
        }));
    ASSERT_TRUE(tg.isSimilar(expected));
  }
}

TEST(QueryPlanner, testCpyCtorWithKeepNodes) {
  {
    ParsedQuery pq = SparqlParser::parseQuery(
        "SELECT ?x WHERE {?x ?p <X>. ?x ?p2 <Y>. <X> ?p <Y>}");
    QueryPlanner qp = makeQueryPlanner();
    auto tg = qp.createTripleGraph(&pq.children()[0].getBasic());
    ASSERT_EQ(2u, tg._nodeMap.find(0)->second->_variables.size());
    ASSERT_EQ(2u, tg._nodeMap.find(1)->second->_variables.size());
    ASSERT_EQ(1u, tg._nodeMap.find(2)->second->_variables.size());
    ASSERT_EQ(
        "0 {s: ?x, p: ?p, o: <X>} : (1, 2)\n"
        "1 {s: ?x, p: ?p2, o: <Y>} : (0)\n"
        "2 {s: <X>, p: ?p, o: <Y>} : (0)",
        tg.asString());
    {
      vector<size_t> keep;
      QueryPlanner::TripleGraph tgnew(tg, keep);
      ASSERT_EQ("", tgnew.asString());
    }
    {
      vector<size_t> keep;
      keep.push_back(0);
      keep.push_back(1);
      keep.push_back(2);
      QueryPlanner::TripleGraph tgnew(tg, keep);
      ASSERT_EQ(
          "0 {s: ?x, p: ?p, o: <X>} : (1, 2)\n"
          "1 {s: ?x, p: ?p2, o: <Y>} : (0)\n"
          "2 {s: <X>, p: ?p, o: <Y>} : (0)",
          tgnew.asString());
      ASSERT_EQ(2u, tgnew._nodeMap.find(0)->second->_variables.size());
      ASSERT_EQ(2u, tgnew._nodeMap.find(1)->second->_variables.size());
      ASSERT_EQ(1u, tgnew._nodeMap.find(2)->second->_variables.size());
    }
    {
      vector<size_t> keep;
      keep.push_back(0);
      QueryPlanner::TripleGraph tgnew(tg, keep);
      ASSERT_EQ("0 {s: ?x, p: ?p, o: <X>} : ()", tgnew.asString());
      ASSERT_EQ(2u, tgnew._nodeMap.find(0)->second->_variables.size());
    }
    {
      vector<size_t> keep;
      keep.push_back(0);
      keep.push_back(1);
      QueryPlanner::TripleGraph tgnew(tg, keep);
      ASSERT_EQ(
          "0 {s: ?x, p: ?p, o: <X>} : (1)\n"
          "1 {s: ?x, p: ?p2, o: <Y>} : (0)",
          tgnew.asString());
      ASSERT_EQ(2u, tgnew._nodeMap.find(0)->second->_variables.size());
      ASSERT_EQ(2u, tgnew._nodeMap.find(1)->second->_variables.size());
    }
  }
}

TEST(QueryPlanner, testBFSLeaveOut) {
  {
    ParsedQuery pq = SparqlParser::parseQuery(
        "SELECT ?x WHERE {?x ?p <X>. ?x ?p2 <Y>. <X> ?p <Y>}");
    QueryPlanner qp = makeQueryPlanner();
    auto tg = qp.createTripleGraph(&pq.children()[0].getBasic());
    ASSERT_EQ(3u, tg._adjLists.size());
    ad_utility::HashSet<size_t> lo;
    auto out = tg.bfsLeaveOut(0, lo);
    ASSERT_EQ(3u, out.size());
    lo.insert(1);
    out = tg.bfsLeaveOut(0, lo);
    ASSERT_EQ(2u, out.size());
    lo.insert(2);
    out = tg.bfsLeaveOut(0, lo);
    ASSERT_EQ(1u, out.size());
    lo.clear();
    lo.insert(0);
    out = tg.bfsLeaveOut(1, lo);
    ASSERT_EQ(1u, out.size());
  }
  {
    ParsedQuery pq = SparqlParser::parseQuery(
        "SELECT ?x WHERE {<A> <B> ?x. ?x <C> ?y. ?y <X> <Y>}");
    QueryPlanner qp = makeQueryPlanner();
    auto tg = qp.createTripleGraph(&pq.children()[0].getBasic());
    ad_utility::HashSet<size_t> lo;
    auto out = tg.bfsLeaveOut(0, lo);
    ASSERT_EQ(3u, out.size());
    lo.insert(1);
    out = tg.bfsLeaveOut(0, lo);
    ASSERT_EQ(1u, out.size());
    lo.insert(2);
    out = tg.bfsLeaveOut(0, lo);
    ASSERT_EQ(1u, out.size());
    lo.clear();
    lo.insert(0);
    out = tg.bfsLeaveOut(1, lo);
    ASSERT_EQ(2u, out.size());
  }
}

TEST(QueryPlanner, indexScanZeroVariables) {
  auto scan = h::IndexScanFromStrings;
  using enum Permutation::Enum;
  h::expect(
      "SELECT * \n "
      "WHERE \t {<x> <y> <z>}",
      scan("<x>", "<y>", "<z>"));
  h::expect(
      "SELECT * \n "
      "WHERE \t {<x> <y> <z> . <x> <y> ?z}",
      h::CartesianProductJoin(scan("<x>", "<y>", "<z>"),
                              scan("<x>", "<y>", "?z")));
}

TEST(QueryPlanner, indexScanOneVariable) {
  auto scan = h::IndexScanFromStrings;
  using enum Permutation::Enum;
  h::expect(
      "PREFIX : <http://rdf.myprefix.com/>\n"
      "SELECT ?x \n "
      "WHERE \t {?x :myrel :obj}",
      scan("?x", "<http://rdf.myprefix.com/myrel>",
           "<http://rdf.myprefix.com/obj>", {POS}));

  h::expect(
      "PREFIX : <http://rdf.myprefix.com/>\n"
      "SELECT ?x \n "
      "WHERE \t {:subj :myrel ?x}",
      scan("<http://rdf.myprefix.com/subj>", "<http://rdf.myprefix.com/myrel>",
           "?x", {PSO}));
}

TEST(QueryPlanner, indexScanTwoVariables) {
  auto scan = h::IndexScanFromStrings;
  using enum Permutation::Enum;

  h::expect(
      "PREFIX : <http://rdf.myprefix.com/>\n"
      "SELECT ?x \n "
      "WHERE \t {?x :myrel ?y}",
      scan("?x", "<http://rdf.myprefix.com/myrel>", "?y", {POS, PSO}));
}

TEST(QueryPlanner, joinOfTwoScans) {
  auto scan = h::IndexScanFromStrings;
  using enum Permutation::Enum;
  h::expect(
      "PREFIX : <pre/>\n"
      "SELECT ?x \n "
      "WHERE \t {:s1 :r ?x. :s2 :r ?x}",
      h::Join(scan("<pre/s1>", "<pre/r>", "?x"),
              scan("<pre/s2>", "<pre/r>", "?x")));

  h::expect(
      "PREFIX : <pre/>\n"
      "SELECT ?x ?y \n "
      "WHERE  {?y :r ?x . :s2 :r ?x}",
      h::Join(scan("?y", "<pre/r>", "?x"), scan("<pre/s2>", "<pre/r>", "?x")));

  h::expect(
      "PREFIX : <pre/>\n"
      "SELECT ?x ?y ?z \n "
      "WHERE {?y :r ?x. ?z :r ?x}",
      h::Join(scan("?y", "<pre/r>", "?x"), scan("?z", "<pre/r>", "?x")));
}

// _____________________________________________________________________________
TEST(QueryPlanner, joinOfFullScans) {
  auto scan = h::IndexScanFromStrings;
  // Join between two full index scans on a single column
  h::expect("SELECT * {?s ?p ?x. ?x ?p2 ?o2 .}",
            h::Join(scan("?s", "?p", "?x"), scan("?x", "?p2", "?o2")));

  // Join between two full index scans on two columns.
  h::expect(
      "SELECT * {?s ?p ?x. ?x ?p2 ?s .}",
      h::MultiColumnJoin(scan("?s", "?p", "?x"), scan("?x", "?p2", "?s")));

  // Join between two full index scans, one of which has a FILTER which can be
  // applied before the JOIN.
  h::expect("SELECT * {?s ?p ?x. ?x ?p2 ?o2 . FILTER (?s = ?p)}",
            h::Join(h::Filter("?s = ?p", scan("?s", "?p", "?x")),
                    scan("?x", "?p2", "?o2")));
}

TEST(QueryPlanner, testActorsBornInEurope) {
  auto scan = h::IndexScanFromStrings;
  using enum ::OrderBy::AscOrDesc;
  h::expect(
      "PREFIX : <pre/>\n"
      "SELECT ?a \n "
      "WHERE {?a :profession :Actor . ?a :born-in ?c. ?c :in :Europe}\n"
      "ORDER BY ?a",
      h::OrderBy(
          {{Variable{"?a"}, Asc}},
          h::UnorderedJoins(scan("?a", "<pre/profession>", "<pre/Actor>"),
                            scan("?a", "<pre/born-in>", "?c"),
                            scan("?c", "<pre/in>", "<pre/Europe>"))));
}

TEST(QueryPlanner, testStarTwoFree) {
  auto scan = h::IndexScanFromStrings;
  h::expect(
      "PREFIX : <http://rdf.myprefix.com/>\n"
      "PREFIX ns: <http://rdf.myprefix.com/ns/>\n"
      "PREFIX xxx: <http://rdf.myprefix.com/xxx/>\n"
      "SELECT ?x ?z \n "
      "WHERE \t {?x :myrel ?y. ?y ns:myrel ?z. ?y xxx:rel2 "
      "<http://abc.de>}",
      h::UnorderedJoins(
          scan("?x", "<http://rdf.myprefix.com/myrel>", "?y"),
          scan("?y", "<http://rdf.myprefix.com/ns/myrel>", "?z"),
          scan("?y", "<http://rdf.myprefix.com/xxx/rel2>", "<http://abc.de>")));
}

TEST(QueryPlanner, testFilterAfterSeed) {
  auto scan = h::IndexScanFromStrings;
  auto qec = ad_utility::testing::getQec(
      "<s> <r> <x>, <x2>, <x3>. <s2> <r> <y1>, <y2>, <y3>.");
  // The following query leads to a different query plan with the dynamic
  // programming and the greedy query planner, because the greedy planner
  // also applies the filters greedily.
  std::string query =
      "SELECT ?x ?y ?z WHERE {"
      "?x <r> ?y . ?y <r> ?z . "
      "FILTER(?x != ?y) }";
  h::expectDynamicProgramming(
      query,
      h::Filter("?x != ?y",
                h::Join(scan("?x", "<r>", "?y"), scan("?y", "<r>", "?z"))),
      qec);
  h::expectGreedy(query,
                  h::Join(h::Filter("?x != ?y", scan("?x", "<r>", "?y")),
                          scan("?y", "<r>", "?z")),
                  qec);
}

TEST(QueryPlanner, testFilterAfterJoin) {
  auto scan = h::IndexScanFromStrings;
  auto qec = ad_utility::testing::getQec("<s> <r> <x>");
  h::expect(
      "SELECT ?x ?y ?z WHERE {"
      "?x <r> ?y . ?y <r> ?z . "
      "FILTER(?x != ?z) }",
      h::Filter("?x != ?z",
                h::Join(scan("?x", "<r>", "?y"), scan("?y", "<r>", "?z"))),
      qec);
}

TEST(QueryPlanner, threeVarTriples) {
  auto scan = h::IndexScanFromStrings;
  using enum Permutation::Enum;

  h::expect(
      "SELECT ?x ?p ?o WHERE {"
      "<s> <p> ?x . ?x ?p ?o }",
      h::Join(scan("<s>", "<p>", "?x", {SPO, PSO}),
              scan("?x", "?p", "?o", {SPO, SOP})));

  h::expect(
      "SELECT ?x ?p ?o WHERE {"
      "<s> ?x <o> . ?x ?p ?o }",
      h::Join(scan("<s>", "?x", "<o>", {SOP, OSP}),
              scan("?x", "?p", "?o", {SPO, SOP})));

  h::expect(
      "SELECT ?s ?p ?o WHERE {"
      "<s> <p> ?p . ?s ?p ?o }",
      h::Join(scan("<s>", "<p>", "?p", {SPO, PSO}),
              scan("?s", "?p", "?o", {PSO, POS})));
}

TEST(QueryPlanner, threeVarTriplesTCJ) {
  auto qec = ad_utility::testing::getQec("<s> <p> <x>");
  auto scan = h::IndexScanFromStrings;
  h::expect(
      "SELECT ?x ?p ?o WHERE {"
      "<s> ?p ?x . ?x ?p ?o }",
      h::MultiColumnJoin(scan("<s>", "?p", "?x"), scan("?x", "?p", "?o")), qec);

  h::expect(
      "SELECT ?s ?p ?o WHERE {"
      "?s ?p ?o . ?s ?p <x> }",
      h::MultiColumnJoin(scan("?s", "?p", "?o"), scan("?s", "?p", "<x>")), qec);
}

TEST(QueryPlanner, threeVarXthreeVarException) {
  auto scan = h::IndexScanFromStrings;
  h::expect(
      "SELECT ?s ?s2 WHERE {"
      "?s ?p ?o . ?s2 ?p ?o }",
      h::MultiColumnJoin(scan("?s", "?p", "?o"), scan("?s2", "?p", "?o")));
}

TEST(QueryExecutionTreeTest, testBooksbyNewman) {
  auto scan = h::IndexScanFromStrings;
  h::expect(
      "SELECT ?x WHERE { ?x <is-a> <Book> . "
      "?x <Author> <Anthony_Newman_(Author)> }",
      h::Join(scan("?x", "<is-a>", "<Book>"),
              scan("?x", "<Author>", "<Anthony_Newman_(Author)>")));
}

TEST(QueryExecutionTreeTest, testBooksGermanAwardNomAuth) {
  auto scan = h::IndexScanFromStrings;
  h::expect(
      "SELECT ?x ?y WHERE { "
      "?x <is-a> <Person> . "
      "?x <Country_of_nationality> <Germany> . "
      "?x <Author> ?y . "
      "?y <is-a> <Award-Nominated_Work> }",
      h::UnorderedJoins(scan("?x", "<is-a>", "<Person>"),
                        scan("?x", "<Country_of_nationality>", "<Germany>"),
                        scan("?x", "<Author>", "?y"),
                        scan("?y", "<is-a>", "<Award-Nominated_Work>")));
}

TEST(QueryExecutionTreeTest, testPlantsEdibleLeaves) {
  auto scan = h::IndexScanFromStrings;
  auto wordScan = h::TextIndexScanForWord;
  auto entityScan = h::TextIndexScanForEntity;
  h::expect(
      "SELECT ?a WHERE  {?a <is-a> <Plant> . ?c ql:contains-entity ?a. ?c "
      "ql:contains-word \"edible leaves\"}",
      h::UnorderedJoins(scan("?a", "<is-a>", "<Plant>"),
                        wordScan(Var{"?c"}, "edible"),
                        wordScan(Var{"?c"}, "leaves"),
                        entityScan(Var{"?c"}, Var{"?a"}, "edible")));
}

TEST(QueryExecutionTreeTest, testCoOccFreeVar) {
  auto scan = h::IndexScanFromStrings;
  auto wordScan = h::TextIndexScanForWord;
  auto entityScan = h::TextIndexScanForEntity;
  h::expect(
      "PREFIX : <> SELECT ?x ?y WHERE { ?x :is-a :Politician . ?c "
      "ql:contains-entity ?x . ?c ql:contains-word \"friend*\" . ?c "
      "ql:contains-entity ?y }",
      h::UnorderedJoins(scan("?x", "<is-a>", "<Politician>"),
                        entityScan(Var{"?c"}, Var{"?x"}, "friend*"),
                        wordScan(Var{"?c"}, "friend*"),
                        entityScan(Var{"?c"}, Var{"?y"}, "friend*")));
}

TEST(QueryExecutionTreeTest, testPoliticiansFriendWithScieManHatProj) {
  auto scan = h::IndexScanFromStrings;
  auto wordScan = h::TextIndexScanForWord;
  auto entityScan = h::TextIndexScanForEntity;
  h::expect(
      "SELECT ?p ?s"
      "WHERE {"
      "?a <is-a> <Politician> . "
      "?c ql:contains-entity ?a ."
      "?c ql:contains-word \"friend*\" ."
      "?c ql:contains-entity ?s ."
      "?s <is-a> <Scientist> ."
      "?c2 ql:contains-entity ?s ."
      "?c2 ql:contains-word \"manhattan project\"}",
      h::UnorderedJoins(scan("?a", "<is-a>", "<Politician>"),
                        entityScan(Var{"?c"}, Var{"?a"}, "friend*"),
                        wordScan(Var{"?c"}, "friend*"),
                        entityScan(Var{"?c"}, Var{"?s"}, "friend*"),
                        scan("?s", "<is-a>", "<Scientist>"),
                        entityScan(Var{"?c2"}, Var{"?s"}, "manhattan"),
                        wordScan(Var{"?c2"}, "manhattan"),
                        wordScan(Var{"?c2"}, "project")));
}

TEST(QueryExecutionTreeTest, testCyclicQuery) {
  ParsedQuery pq = SparqlParser::parseQuery(
      "SELECT ?x ?y ?m WHERE { ?x <Spouse_(or_domestic_partner)> ?y . "
      "?x <Film_performance> ?m . ?y <Film_performance> ?m }");
  QueryPlanner qp = makeQueryPlanner();
  QueryExecutionTree qet = qp.createExecutionTree(pq);

  // There are four possible outcomes of this test with the same size
  // estimate. It is currently very hard to make the query planning
  // deterministic in a test scenario, so we allow all four candidates

  // delete all whitespace from the strings to make the matching easier.
  auto strip = [](std::string s) {
    s.erase(std::remove_if(s.begin(), s.end(), ::isspace), s.end());
    return s;
  };
  std::string possible1 = strip(
      "{\n  MULTI_COLUMN_JOIN\n    {\n    SCAN PSO with P = "
      "\"<Film_performance>\"\n    qet-width: 2 \n  }\n  join-columns: [0 & "
      "1]\n  |X|\n    {\n    SORT(internal) on columns:asc(2) asc(1) \n    "
      "{\n      JOIN\n      {\n        SCAN PSO with P = "
      "\"<Film_performance>\"\n        qet-width: 2 \n      } join-column: "
      "[0]\n      |X|\n      {\n        SCAN PSO with P = "
      "\"<Spouse_(or_domestic_partner)>\"\n        qet-width: 2 \n      } "
      "join-column: [0]\n      qet-width: 3 \n    }\n    qet-width: 3 \n  "
      "}\n  join-columns: [2 & 1]\n  qet-width: 3 \n}");
  std::string possible2 = strip(
      "{\n  MULTI_COLUMN_JOIN\n    {\n    SCAN POS with P = "
      "\"<Film_performance>\"\n    qet-width: 2 \n  }\n  join-columns: [0 & "
      "1]\n  |X|\n    {\n    SORT(internal) on columns:asc(1) asc(2) \n    "
      "{\n      JOIN\n      {\n        SCAN PSO with P = "
      "\"<Film_performance>\"\n        qet-width: 2 \n      } join-column: "
      "[0]\n      |X|\n      {\n        SCAN PSO with P = "
      "\"<Spouse_(or_domestic_partner)>\"\n        qet-width: 2 \n      } "
      "join-column: [0]\n      qet-width: 3 \n    }\n    qet-width: 3 \n  "
      "}\n  join-columns: [1 & 2]\n  qet-width: 3 \n}");
  std::string possible3 = strip(
      "{\n  MULTI_COLUMN_JOIN\n    {\n    SCAN POS with P = "
      "\"<Spouse_(or_domestic_partner)>\"\n    qet-width: 2 \n  }\n  "
      "join-columns: [0 & 1]\n  |X|\n    {\n    SORT(internal) on "
      "columns:asc(1) asc(2) \n    {\n      JOIN\n      {\n        SCAN POS "
      "with P = \"<Film_performance>\"\n        qet-width: 2 \n      } "
      "join-column: [0]\n      |X|\n      {\n        SCAN POS with P = "
      "\"<Film_performance>\"\n        qet-width: 2 \n      } join-column: "
      "[0]\n      qet-width: 3 \n    }\n    qet-width: 3 \n  }\n  "
      "join-columns: [1 & 2]\n  qet-width: 3 \n}");
  std::string possible4 = strip(R"xxx(MULTI_COLUMN_JOIN
        {
          SCAN PSO with P = "<Film_performance>"
          qet-width: 2
        } join-columns: [0 & 1]
        |X|
        {
          SORT(internal) on columns:asc(1) asc(2)
          {
            JOIN
            {
              SCAN POS with P = "<Spouse_(or_domestic_partner)>"
              qet-width: 2
            } join-column: [0]
            |X|
            {
              SCAN PSO with P = "<Film_performance>"
              qet-width: 2
            } join-column: [0]
            qet-width: 3
          }
          qet-width: 3
        } join-columns: [1 & 2]
        qet-width: 3
        })xxx");
  std::string possible5 = strip(R"xxx(MULTI_COLUMN_JOIN
{
  SCAN POS with P = "<Film_performance>"
  qet-width: 2
} join-columns: [0 & 1]
|X|
{
  SORT / ORDER BY on columns:asc(2) asc(1)
  {
    JOIN
    {
      SCAN POS with P = "<Spouse_(or_domestic_partner)>"
      qet-width: 2
    } join-column: [0]
    |X|
    {
      SCAN PSO with P = "<Film_performance>"
      qet-width: 2
    } join-column: [0]
    qet-width: 3
  }
  qet-width: 3
} join-columns: [2 & 1]
qet-width: 3
}
)xxx");

  auto actual = strip(qet.getCacheKey());

  if (actual != possible1 && actual != possible2 && actual != possible3 &&
      actual != possible4 && actual != possible5) {
    // TODO<joka921> Make this work, there are just too many possibilities.
    /*
    FAIL() << "query execution tree is none of the possible trees, it is "
              "actually "
           << qet.getCacheKey() << '\n' << actual << '\n'
           */
  }
}

TEST(QueryExecutionTreeTest, testFormerSegfaultTriFilter) {
  ParsedQuery pq = SparqlParser::parseQuery(
      "PREFIX fb: <http://rdf.freebase.com/ns/>\n"
      "SELECT DISTINCT ?1 ?0 WHERE {\n"
      "fb:m.0fkvn fb:government.government_office_category.officeholders "
      "?0 "
      ".\n"
      "?0 fb:government.government_position_held.jurisdiction_of_office "
      "fb:m.0vmt .\n"
      "?0 fb:government.government_position_held.office_holder ?1 .\n"
      "FILTER (?1 != fb:m.0fkvn) .\n"
      "FILTER (?1 != fb:m.0vmt) .\n"
      "FILTER (?1 != fb:m.018mts)"
      "} LIMIT 300");
  QueryPlanner qp = makeQueryPlanner();
  QueryExecutionTree qet = qp.createExecutionTree(pq);
  ASSERT_TRUE(qet.isVariableCovered(Variable{"?1"}));
  ASSERT_TRUE(qet.isVariableCovered(Variable{"?0"}));
}

TEST(QueryPlanner, testSimpleOptional) {
  auto scan = h::IndexScanFromStrings;
  h::expect(
      "SELECT ?a ?b \n "
      "WHERE  {?a <rel1> ?b . OPTIONAL { ?a <rel2> ?c }}",
      h::OptionalJoin(scan("?a", "<rel1>", "?b"), scan("?a", "<rel2>", "?c")));
  h::expect(
      "SELECT ?a ?b \n "
      "WHERE  {?a <rel1> ?b . "
      "OPTIONAL { ?a <rel2> ?c }} ORDER BY ?b",
      h::OrderBy({{Variable{"?b"}, ::OrderBy::AscOrDesc::Asc}},
                 h::OptionalJoin(scan("?a", "<rel1>", "?b"),
                                 scan("?a", "<rel2>", "?c"))));
}

TEST(QueryPlanner, SimpleTripleOneVariable) {
  using enum Permutation::Enum;

  auto scan = h::IndexScanFromStrings;
  // With only one variable, there are always two permutations that will yield
  // exactly the same result. The query planner consistently chooses one of
  // them.
  h::expect("SELECT * WHERE { ?s <p> <o> }", scan("?s", "<p>", "<o>", {POS}));
  h::expect("SELECT * WHERE { <s> ?p <o> }", scan("<s>", "?p", "<o>", {SOP}));
  h::expect("SELECT * WHERE { <s> <p> ?o }", scan("<s>", "<p>", "?o", {PSO}));
}

TEST(QueryPlanner, SimpleTripleTwoVariables) {
  using enum Permutation::Enum;

  // In the following tests we need the query planner to be aware that the index
  // contains the entities `<s> <p> <o>` that are used below, otherwise it will
  // estimate that and Index scan has the same cost as an Index scan followed by
  // a sort (because both plans have a cost of zero if the index scan is known
  // to be empty).

  auto qec = ad_utility::testing::getQec("<s> <p> <o>");
  auto scan = h::IndexScanFromStrings;

  // Fixed predicate.

  // Without `Order By`, two orderings are possible, both are fine.
  h::expect("SELECT * WHERE { ?s <p> ?o }", scan("?s", "<p>", "?o", {POS, PSO}),
            qec);
  // Must always be a single index scan, never index scan + sorting.
  h::expect("SELECT * WHERE { ?s <p> ?o } INTERNAL SORT BY ?o",
            scan("?s", "<p>", "?o", {POS}), qec);
  h::expect("SELECT * WHERE { ?s <p> ?o } INTERNAL SORT BY ?s",
            scan("?s", "<p>", "?o", {PSO}), qec);

  // Fixed subject.
  h::expect("SELECT * WHERE { <s> ?p ?o }", scan("<s>", "?p", "?o", {SOP, SPO}),
            qec);
  h::expect("SELECT * WHERE { <s> ?p ?o } INTERNAL SORT BY ?o",
            scan("<s>", "?p", "?o", {SOP}), qec);
  h::expect("SELECT * WHERE { <s> ?p ?o } INTERNAL SORT BY ?p",
            scan("<s>", "?p", "?o", {SPO}), qec);

  // Fixed object.
  h::expect("SELECT * WHERE { <s> ?p ?o }", scan("<s>", "?p", "?o", {SOP, SPO}),
            qec);
  h::expect("SELECT * WHERE { <s> ?p ?o } INTERNAL SORT BY ?o",
            scan("<s>", "?p", "?o", {SOP}), qec);
  h::expect("SELECT * WHERE { <s> ?p ?o } INTERNAL SORT BY ?p",
            scan("<s>", "?p", "?o", {SPO}), qec);
}

TEST(QueryPlanner, SimpleTripleThreeVariables) {
  using enum Permutation::Enum;

  // Fixed predicate.
  // Don't care about the sorting.
  h::expect("SELECT * WHERE { ?s ?p ?o }",
            h::IndexScan(Var{"?s"}, Var{"?p"}, Var{"?o"},
                         {SPO, SOP, PSO, POS, OSP, OPS}));

  // Sorted by one variable, two possible permutations remain.
  h::expect("SELECT * WHERE { ?s ?p ?o } INTERNAL SORT BY ?s",
            h::IndexScan(Var{"?s"}, Var{"?p"}, Var{"?o"}, {SPO, SOP}));
  h::expect("SELECT * WHERE { ?s ?p ?o } INTERNAL SORT BY ?p",
            h::IndexScan(Var{"?s"}, Var{"?p"}, Var{"?o"}, {POS, PSO}));
  h::expect("SELECT * WHERE { ?s ?p ?o } INTERNAL SORT BY ?o",
            h::IndexScan(Var{"?s"}, Var{"?p"}, Var{"?o"}, {OSP, OPS}));

  // Sorted by two variables, this makes the permutation unique.
  h::expect("SELECT * WHERE { ?s ?p ?o } INTERNAL SORT BY ?s ?o",
            h::IndexScan(Var{"?s"}, Var{"?p"}, Var{"?o"}, {SOP}));
  h::expect("SELECT * WHERE { ?s ?p ?o } INTERNAL SORT BY ?s ?p",
            h::IndexScan(Var{"?s"}, Var{"?p"}, Var{"?o"}, {SPO}));
  h::expect("SELECT * WHERE { ?s ?p ?o } INTERNAL SORT BY ?o ?s",
            h::IndexScan(Var{"?s"}, Var{"?p"}, Var{"?o"}, {OSP}));
  h::expect("SELECT * WHERE { ?s ?p ?o } INTERNAL SORT BY ?o ?p",
            h::IndexScan(Var{"?s"}, Var{"?p"}, Var{"?o"}, {OPS}));
  h::expect("SELECT * WHERE { ?s ?p ?o } INTERNAL SORT BY ?p ?s",
            h::IndexScan(Var{"?s"}, Var{"?p"}, Var{"?o"}, {PSO}));
  h::expect("SELECT * WHERE { ?s ?p ?o } INTERNAL SORT BY ?p ?o",
            h::IndexScan(Var{"?s"}, Var{"?p"}, Var{"?o"}, {POS}));
}

TEST(QueryPlanner, CartesianProductJoin) {
  auto scan = h::IndexScanFromStrings;
  h::expect(
      "SELECT ?x ?p ?o WHERE {"
      "<s> <p> ?o . ?a <b> <c> }",
      h::CartesianProductJoin(scan("<s>", "<p>", "?o"),
                              scan("?a", "<b>", "<c>")));
  // This currently fails because of a bug, we have to fix the bug...
  h::expect(
      "SELECT ?x ?p ?o WHERE {"
      "<s> ?p ?o . ?a ?b ?c }",
      h::CartesianProductJoin(scan("<s>", "?p", "?o"), scan("?a", "?b", "?c")));
  h::expect(
      "SELECT * WHERE {"
      "?s <p> <o> . ?s <p2> ?o2 . ?x <b> ?c }",
      h::CartesianProductJoin(
          h::Join(scan("?s", "<p>", "<o>"), scan("?s", "<p2>", "?o2")),
          scan("?x", "<b>", "?c")));
}

namespace {
// A helper function to recreate the internal variables added by the query
// planner for transitive paths.
std::string internalVar(int i) {
  return absl::StrCat(QLEVER_INTERNAL_VARIABLE_QUERY_PLANNER_PREFIX, i);
}
}  // namespace

TEST(QueryPlanner, TransitivePathUnbound) {
  auto scan = h::IndexScanFromStrings;
  TransitivePathSide left{std::nullopt, 0, Variable("?x"), 0};
  TransitivePathSide right{std::nullopt, 1, Variable("?y"), 1};
  h::expect(
      "SELECT ?x ?y WHERE {"
      "?x <p>+ ?y }",
      h::TransitivePath(left, right, 1, std::numeric_limits<size_t>::max(),
                        scan(internalVar(0), "<p>", internalVar(1))));
}

TEST(QueryPlanner, TransitivePathLeftId) {
  auto scan = h::IndexScanFromStrings;
  auto qec = ad_utility::testing::getQec("<s> <p> <o>");

  auto getId = ad_utility::testing::makeGetId(qec->getIndex());

  TransitivePathSide left{std::nullopt, 0, getId("<s>"), 0};
  TransitivePathSide right{std::nullopt, 1, Variable("?y"), 1};
  h::expect(
      "SELECT ?y WHERE {"
      "<s> <p>+ ?y }",
      h::TransitivePath(left, right, 1, std::numeric_limits<size_t>::max(),
                        scan(internalVar(0), "<p>", internalVar(1))),
      qec);
}

TEST(QueryPlanner, TransitivePathRightId) {
  auto scan = h::IndexScanFromStrings;
  auto qec = ad_utility::testing::getQec("<s> <p> <o>");

  auto getId = ad_utility::testing::makeGetId(qec->getIndex());

  TransitivePathSide left{std::nullopt, 1, Variable("?x"), 0};
  TransitivePathSide right{std::nullopt, 0, getId("<o>"), 1};
  h::expect(
      "SELECT ?y WHERE {"
      "?x <p>+ <o> }",
      h::TransitivePath(left, right, 1, std::numeric_limits<size_t>::max(),
                        scan(internalVar(0), "<p>", internalVar(1))),
      qec);
}

TEST(QueryPlanner, TransitivePathBindLeft) {
  auto scan = h::IndexScanFromStrings;
  TransitivePathSide left{std::nullopt, 0, Variable("?x"), 0};
  TransitivePathSide right{std::nullopt, 1, Variable("?y"), 1};
  h::expect(
      "SELECT ?x ?y WHERE {"
      "<s> <p> ?x."
      "?x <p>* ?y }",
      h::TransitivePath(left, right, 0, std::numeric_limits<size_t>::max(),
                        scan("<s>", "<p>", "?x"),
                        scan(internalVar(0), "<p>", internalVar(1))));
}

TEST(QueryPlanner, TransitivePathBindRight) {
  auto scan = h::IndexScanFromStrings;
  TransitivePathSide left{std::nullopt, 1, Variable("?x"), 0};
  TransitivePathSide right{std::nullopt, 0, Variable("?y"), 1};
  h::expect(
      "SELECT ?x ?y WHERE {"
      "?x <p>* ?y."
      "?y <p> <o> }",
      h::TransitivePath(
          left, right, 0, std::numeric_limits<size_t>::max(),
          scan("?y", "<p>", "<o>"),
          scan(internalVar(0), "<p>", internalVar(1), {Permutation::POS})),
      ad_utility::testing::getQec("<x> <p> <o>. <x2> <p> <o2>"));
}

TEST(QueryPlanner, PathSearchSingleTarget) {
  auto scan = h::IndexScanFromStrings;
  auto qec = ad_utility::testing::getQec("<x> <p> <y>. <y> <p> <z>");
  auto getId = ad_utility::testing::makeGetId(qec->getIndex());

  std::vector<Id> sources{getId("<x>")};
  std::vector<Id> targets{getId("<z>")};
  PathSearchConfiguration config{PathSearchAlgorithm::ALL_PATHS,
                                 sources,
                                 targets,
                                 Variable("?start"),
                                 Variable("?end"),
                                 Variable("?path"),
                                 Variable("?edge"),
                                 {}};
  h::expect(
      "PREFIX pathSearch: <https://qlever.cs.uni-freiburg.de/pathSearch/>"
      "SELECT ?start ?end ?path ?edge WHERE {"
      "SERVICE pathSearch: {"
      "_:path pathSearch:algorithm pathSearch:allPaths ;"
      "pathSearch:source <x> ;"
      "pathSearch:target <z> ;"
      "pathSearch:pathColumn ?path ;"
      "pathSearch:edgeColumn ?edge ;"
      "pathSearch:start ?start;"
      "pathSearch:end ?end;"
      "{SELECT * WHERE {"
      "?start <p> ?end."
      "}}}}",
      h::PathSearch(config, true, true, scan("?start", "<p>", "?end")), qec);
}

TEST(QueryPlanner, PathSearchMultipleTargets) {
  auto scan = h::IndexScanFromStrings;
  auto qec = ad_utility::testing::getQec("<x> <p> <y>. <y> <p> <z>");
  auto getId = ad_utility::testing::makeGetId(qec->getIndex());

  std::vector<Id> sources{getId("<x>")};
  std::vector<Id> targets{getId("<y>"), getId("<z>")};
  PathSearchConfiguration config{PathSearchAlgorithm::ALL_PATHS,
                                 sources,
                                 targets,
                                 Variable("?start"),
                                 Variable("?end"),
                                 Variable("?path"),
                                 Variable("?edge"),
                                 {}};
  h::expect(
      "PREFIX pathSearch: <https://qlever.cs.uni-freiburg.de/pathSearch/>"
      "SELECT ?start ?end ?path ?edge WHERE {"
      "SERVICE pathSearch: {"
      "_:path pathSearch:algorithm pathSearch:allPaths ;"
      "pathSearch:source <x> ;"
      "pathSearch:target <y> ;"
      "pathSearch:target <z> ;"
      "pathSearch:pathColumn ?path ;"
      "pathSearch:edgeColumn ?edge ;"
      "pathSearch:start ?start;"
      "pathSearch:end ?end;"
      "{SELECT * WHERE {"
      "?start <p> ?end."
      "}}}}",
      h::PathSearch(config, true, true, scan("?start", "<p>", "?end")), qec);
}

TEST(QueryPlanner, PathSearchMultipleSourcesAndTargets) {
  auto scan = h::IndexScanFromStrings;
  auto qec =
      ad_utility::testing::getQec("<x1> <p> <y>. <x2> <p> <y>. <y> <p> <z>");
  auto getId = ad_utility::testing::makeGetId(qec->getIndex());

  std::vector<Id> sources{getId("<x1>"), getId("<x2>")};
  std::vector<Id> targets{getId("<y>"), getId("<z>")};
  PathSearchConfiguration config{PathSearchAlgorithm::ALL_PATHS,
                                 sources,
                                 targets,
                                 Variable("?start"),
                                 Variable("?end"),
                                 Variable("?path"),
                                 Variable("?edge"),
                                 {}};
  h::expect(
      "PREFIX pathSearch: <https://qlever.cs.uni-freiburg.de/pathSearch/>"
      "SELECT ?start ?end ?path ?edge WHERE {"
      "SERVICE pathSearch: {"
      "_:path pathSearch:algorithm pathSearch:allPaths ;"
      "pathSearch:source <x1> ;"
      "pathSearch:source <x2> ;"
      "pathSearch:target <y> ;"
      "pathSearch:target <z> ;"
      "pathSearch:pathColumn ?path ;"
      "pathSearch:edgeColumn ?edge ;"
      "pathSearch:start ?start;"
      "pathSearch:end ?end;"
      "{SELECT * WHERE {"
      "?start <p> ?end."
      "}}}}",
      h::PathSearch(config, true, true, scan("?start", "<p>", "?end")), qec);
}

TEST(QueryPlanner, PathSearchMultipleSourcesAndTargetsCartesian) {
  auto scan = h::IndexScanFromStrings;
  auto qec =
      ad_utility::testing::getQec("<x1> <p> <y>. <x2> <p> <y>. <y> <p> <z>");
  auto getId = ad_utility::testing::makeGetId(qec->getIndex());

  std::vector<Id> sources{getId("<x1>"), getId("<x2>")};
  std::vector<Id> targets{getId("<y>"), getId("<z>")};
  PathSearchConfiguration config{PathSearchAlgorithm::ALL_PATHS,
                                 sources,
                                 targets,
                                 Variable("?start"),
                                 Variable("?end"),
                                 Variable("?path"),
                                 Variable("?edge"),
                                 {}};
  h::expect(
      "PREFIX pathSearch: <https://qlever.cs.uni-freiburg.de/pathSearch/>"
      "SELECT ?start ?end ?path ?edge WHERE {"
      "SERVICE pathSearch: {"
      "_:path pathSearch:algorithm pathSearch:allPaths ;"
      "pathSearch:source <x1> ;"
      "pathSearch:source <x2> ;"
      "pathSearch:target <y> ;"
      "pathSearch:target <z> ;"
      "pathSearch:pathColumn ?path ;"
      "pathSearch:edgeColumn ?edge ;"
      "pathSearch:start ?start;"
      "pathSearch:end ?end;"
      "pathSearch:cartesian true;"
      "{SELECT * WHERE {"
      "?start <p> ?end."
      "}}}}",
      h::PathSearch(config, true, true, scan("?start", "<p>", "?end")), qec);
}

TEST(QueryPlanner, PathSearchMultipleSourcesAndTargetsNonCartesian) {
  auto scan = h::IndexScanFromStrings;
  auto qec =
      ad_utility::testing::getQec("<x1> <p> <y>. <x2> <p> <y>. <y> <p> <z>");
  auto getId = ad_utility::testing::makeGetId(qec->getIndex());

  std::vector<Id> sources{getId("<x1>"), getId("<x2>")};
  std::vector<Id> targets{getId("<y>"), getId("<z>")};
  PathSearchConfiguration config{PathSearchAlgorithm::ALL_PATHS,
                                 sources,
                                 targets,
                                 Variable("?start"),
                                 Variable("?end"),
                                 Variable("?path"),
                                 Variable("?edge"),
                                 {},
                                 false};
  h::expect(
      "PREFIX pathSearch: <https://qlever.cs.uni-freiburg.de/pathSearch/>"
      "SELECT ?start ?end ?path ?edge WHERE {"
      "SERVICE pathSearch: {"
      "_:path pathSearch:algorithm pathSearch:allPaths ;"
      "pathSearch:source <x1> ;"
      "pathSearch:source <x2> ;"
      "pathSearch:target <y> ;"
      "pathSearch:target <z> ;"
      "pathSearch:pathColumn ?path ;"
      "pathSearch:edgeColumn ?edge ;"
      "pathSearch:start ?start;"
      "pathSearch:end ?end;"
      "pathSearch:cartesian false;"
      "{SELECT * WHERE {"
      "?start <p> ?end."
      "}}}}",
      h::PathSearch(config, true, true, scan("?start", "<p>", "?end")), qec);
}

// _____________________________________________________________________________
TEST(QueryPlanner, numPathsPerTarget) {
  auto scan = h::IndexScanFromStrings;
  auto qec =
      ad_utility::testing::getQec("<x1> <p> <y>. <x2> <p> <y>. <y> <p> <z>");
  auto getId = ad_utility::testing::makeGetId(qec->getIndex());

  std::vector<Id> sources{getId("<x1>"), getId("<x2>")};
  std::vector<Id> targets{getId("<y>"), getId("<z>")};
  PathSearchConfiguration config{PathSearchAlgorithm::ALL_PATHS,
                                 sources,
                                 targets,
                                 Variable("?start"),
                                 Variable("?end"),
                                 Variable("?path"),
                                 Variable("?edge"),
                                 {},
                                 true,
                                 1};
  h::expect(
      "PREFIX pathSearch: <https://qlever.cs.uni-freiburg.de/pathSearch/>"
      "SELECT ?start ?end ?path ?edge WHERE {"
      "SERVICE pathSearch: {"
      "_:path pathSearch:algorithm pathSearch:allPaths ;"
      "pathSearch:source <x1> ;"
      "pathSearch:source <x2> ;"
      "pathSearch:target <y> ;"
      "pathSearch:target <z> ;"
      "pathSearch:pathColumn ?path ;"
      "pathSearch:edgeColumn ?edge ;"
      "pathSearch:start ?start;"
      "pathSearch:end ?end;"
      "pathSearch:numPathsPerTarget 1;"
      "{SELECT * WHERE {"
      "?start <p> ?end."
      "}}}}",
      h::PathSearch(config, true, true, scan("?start", "<p>", "?end")), qec);
}

TEST(QueryPlanner, PathSearchWithEdgeProperties) {
  auto scan = h::IndexScanFromStrings;
  auto join = h::Join;
  auto qec = ad_utility::testing::getQec(
      "<x> <p1> <m1>. <m1> <p2> <y>. <y> <p1> <m2>. <m2> <p2> <z>");
  auto getId = ad_utility::testing::makeGetId(qec->getIndex());

  std::vector<Id> sources{getId("<x>")};
  std::vector<Id> targets{getId("<z>")};
  PathSearchConfiguration config{PathSearchAlgorithm::ALL_PATHS,
                                 sources,
                                 targets,
                                 Variable("?start"),
                                 Variable("?end"),
                                 Variable("?path"),
                                 Variable("?edge"),
                                 {Variable("?middle")}};
  h::expect(
      "PREFIX pathSearch: <https://qlever.cs.uni-freiburg.de/pathSearch/>"
      "SELECT ?start ?end ?path ?edge WHERE {"
      "SERVICE pathSearch: {"
      "_:path pathSearch:algorithm pathSearch:allPaths ;"
      "pathSearch:source <x> ;"
      "pathSearch:target <z> ;"
      "pathSearch:pathColumn ?path ;"
      "pathSearch:edgeColumn ?edge ;"
      "pathSearch:start ?start;"
      "pathSearch:end ?end;"
      "pathSearch:edgeProperty ?middle;"
      "{SELECT * WHERE {"
      "?start <p1> ?middle."
      "?middle <p2> ?end."
      "}}}}",
      h::PathSearch(config, true, true,
                    h::Sort(join(scan("?start", "<p1>", "?middle"),
                                 scan("?middle", "<p2>", "?end")))),
      qec);
}

TEST(QueryPlanner, PathSearchWithMultipleEdgePropertiesAndTargets) {
  auto scan = h::IndexScanFromStrings;
  auto join = h::UnorderedJoins;
  auto qec = ad_utility::testing::getQec(
      "<x> <p1> <m1>."
      "<m1> <p3> <n1>."
      "<m1> <p2> <y>."
      "<y> <p1> <m2>."
      "<m2> <p3> <n2>."
      "<m2> <p2> <z>");
  auto getId = ad_utility::testing::makeGetId(qec->getIndex());

  std::vector<Id> sources{getId("<x>")};
  std::vector<Id> targets{getId("<z>"), getId("<y>")};
  PathSearchConfiguration config{
      PathSearchAlgorithm::ALL_PATHS,
      sources,
      targets,
      Variable("?start"),
      Variable("?end"),
      Variable("?path"),
      Variable("?edge"),
      {Variable("?middle"), Variable("?middleAttribute")}};
  h::expect(
      "PREFIX pathSearch: <https://qlever.cs.uni-freiburg.de/pathSearch/>"
      "SELECT ?start ?end ?path ?edge WHERE {"
      "SERVICE pathSearch: {"
      "_:path pathSearch:algorithm pathSearch:allPaths ;"
      "pathSearch:source <x> ;"
      "pathSearch:target <z> ;"
      "pathSearch:target <y> ;"
      "pathSearch:pathColumn ?path ;"
      "pathSearch:edgeColumn ?edge ;"
      "pathSearch:start ?start;"
      "pathSearch:end ?end;"
      "pathSearch:edgeProperty ?middle;"
      "pathSearch:edgeProperty ?middleAttribute;"
      "{SELECT * WHERE {"
      "?start <p1> ?middle."
      "?middle <p3> ?middleAttribute."
      "?middle <p2> ?end."
      "}}}}",
      h::PathSearch(config, true, true,
                    h::Sort(join(scan("?start", "<p1>", "?middle"),
                                 scan("?middle", "<p3>", "?middleAttribute"),
                                 scan("?middle", "<p2>", "?end")))),
      qec);
}

TEST(QueryPlanner, PathSearchJoinOnEdgeProperty) {
  auto scan = h::IndexScanFromStrings;
  auto join = h::Join;
  auto qec = ad_utility::testing::getQec(
      "<x> <p1> <m1>. <m1> <p2> <y>. <y> <p1> <m2>. <m2> <p2> <z>");
  auto getId = ad_utility::testing::makeGetId(qec->getIndex());

  std::vector<Id> sources{getId("<x>")};
  std::vector<Id> targets{getId("<z>")};
  PathSearchConfiguration config{PathSearchAlgorithm::ALL_PATHS,
                                 sources,
                                 targets,
                                 Variable("?start"),
                                 Variable("?end"),
                                 Variable("?path"),
                                 Variable("?edge"),
                                 {Variable("?middle")}};
  h::expect(
      "PREFIX pathSearch: <https://qlever.cs.uni-freiburg.de/pathSearch/>"
      "SELECT ?start ?end ?path ?edge WHERE {"
      "VALUES ?middle {<m1>} "
      "SERVICE pathSearch: {"
      "_:path pathSearch:algorithm pathSearch:allPaths ;"
      "pathSearch:source <x> ;"
      "pathSearch:target <z> ;"
      "pathSearch:pathColumn ?path ;"
      "pathSearch:edgeColumn ?edge ;"
      "pathSearch:start ?start;"
      "pathSearch:end ?end;"
      "pathSearch:edgeProperty ?middle;"
      "{SELECT * WHERE {"
      "?start <p1> ?middle."
      "?middle <p2> ?end."
      "}}}}",
      join(h::Sort(h::ValuesClause("VALUES (?middle) { (<m1>) }")),
           h::Sort(
               h::PathSearch(config, true, true,
                             h::Sort(join(scan("?start", "<p1>", "?middle"),
                                          scan("?middle", "<p2>", "?end")))))),
      qec);
}

TEST(QueryPlanner, PathSearchSourceBound) {
  auto scan = h::IndexScanFromStrings;
  auto qec = ad_utility::testing::getQec("<x> <p> <y>. <y> <p> <z>");
  auto getId = ad_utility::testing::makeGetId(qec->getIndex());

  Variable sources{"?source"};
  std::vector<Id> targets{getId("<z>")};
  PathSearchConfiguration config{PathSearchAlgorithm::ALL_PATHS,
                                 sources,
                                 targets,
                                 Variable("?start"),
                                 Variable("?end"),
                                 Variable("?path"),
                                 Variable("?edge"),
                                 {}};
  h::expect(
      "PREFIX pathSearch: <https://qlever.cs.uni-freiburg.de/pathSearch/>"
      "SELECT ?start ?end ?path ?edge WHERE {"
      "VALUES ?source {<x>}"
      "SERVICE pathSearch: {"
      "_:path pathSearch:algorithm pathSearch:allPaths ;"
      "pathSearch:source ?source ;"
      "pathSearch:target <z> ;"
      "pathSearch:pathColumn ?path ;"
      "pathSearch:edgeColumn ?edge ;"
      "pathSearch:start ?start;"
      "pathSearch:end ?end;"
      "{SELECT * WHERE {"
      "?start <p> ?end."
      "}}}}",
      h::PathSearch(config, true, true, scan("?start", "<p>", "?end"),
                    h::ValuesClause("VALUES (?source) { (<x>) }")),
      qec);
}

TEST(QueryPlanner, PathSearchTargetBound) {
  auto scan = h::IndexScanFromStrings;
  auto qec = ad_utility::testing::getQec("<x> <p> <y>. <y> <p> <z>");
  auto getId = ad_utility::testing::makeGetId(qec->getIndex());

  std::vector<Id> sources{getId("<x>")};
  Variable targets{"?target"};
  PathSearchConfiguration config{PathSearchAlgorithm::ALL_PATHS,
                                 sources,
                                 targets,
                                 Variable("?start"),
                                 Variable("?end"),
                                 Variable("?path"),
                                 Variable("?edge"),
                                 {}};
  h::expect(
      "PREFIX pathSearch: <https://qlever.cs.uni-freiburg.de/pathSearch/>"
      "SELECT ?start ?end ?path ?edge WHERE {"
      "VALUES ?target {<z>}"
      "SERVICE pathSearch: {"
      "_:path pathSearch:algorithm pathSearch:allPaths ;"
      "pathSearch:source <x> ;"
      "pathSearch:target ?target ;"
      "pathSearch:pathColumn ?path ;"
      "pathSearch:edgeColumn ?edge ;"
      "pathSearch:start ?start;"
      "pathSearch:end ?end;"
      "{SELECT * WHERE {"
      "?start <p> ?end."
      "}}}}",
      h::PathSearch(config, true, true, scan("?start", "<p>", "?end"),
                    h::ValuesClause("VALUES (?target) { (<z>) }")),
      qec);
}

TEST(QueryPlanner, PathSearchBothBound) {
  auto scan = h::IndexScanFromStrings;
  auto qec = ad_utility::testing::getQec("<x> <p> <y>. <y> <p> <z>");
  auto getId = ad_utility::testing::makeGetId(qec->getIndex());

  Variable sources{"?source"};
  Variable targets{"?target"};
  PathSearchConfiguration config{PathSearchAlgorithm::ALL_PATHS,
                                 sources,
                                 targets,
                                 Variable("?start"),
                                 Variable("?end"),
                                 Variable("?path"),
                                 Variable("?edge"),
                                 {}};
  h::expect(
      "PREFIX pathSearch: <https://qlever.cs.uni-freiburg.de/pathSearch/>"
      "SELECT ?start ?end ?path ?edge WHERE {"
      "VALUES (?source ?target) {(<x> <z>)}"
      "SERVICE pathSearch: {"
      "_:path pathSearch:algorithm pathSearch:allPaths ;"
      "pathSearch:source ?source ;"
      "pathSearch:target ?target ;"
      "pathSearch:pathColumn ?path ;"
      "pathSearch:edgeColumn ?edge ;"
      "pathSearch:start ?start;"
      "pathSearch:end ?end;"
      "{SELECT * WHERE {"
      "?start <p> ?end."
      "}}}}",
      h::PathSearch(config, true, true, scan("?start", "<p>", "?end"),
                    h::ValuesClause("VALUES (?source\t?target) { (<x> <z>) }")),
      qec);
}

TEST(QueryPlanner, PathSearchBothBoundIndividually) {
  auto scan = h::IndexScanFromStrings;
  auto qec = ad_utility::testing::getQec("<x> <p> <y>. <y> <p> <z>");
  auto getId = ad_utility::testing::makeGetId(qec->getIndex());

  Variable sources{"?source"};
  Variable targets{"?target"};
  PathSearchConfiguration config{PathSearchAlgorithm::ALL_PATHS,
                                 sources,
                                 targets,
                                 Variable("?start"),
                                 Variable("?end"),
                                 Variable("?path"),
                                 Variable("?edge"),
                                 {}};
  h::expect(
      "PREFIX pathSearch: <https://qlever.cs.uni-freiburg.de/pathSearch/>"
      "SELECT ?start ?end ?path ?edge WHERE {"
      "VALUES (?source) {(<x>)}"
      "VALUES (?target) {(<z>)}"
      "SERVICE pathSearch: {"
      "_:path pathSearch:algorithm pathSearch:allPaths ;"
      "pathSearch:source ?source ;"
      "pathSearch:target ?target ;"
      "pathSearch:pathColumn ?path ;"
      "pathSearch:edgeColumn ?edge ;"
      "pathSearch:start ?start;"
      "pathSearch:end ?end;"
      "{SELECT * WHERE {"
      "?start <p> ?end."
      "}}}}",
      h::PathSearch(config, true, true, scan("?start", "<p>", "?end"),
                    h::ValuesClause("VALUES (?source) { (<x>) }"),
                    h::ValuesClause("VALUES (?target) { (<z>) }")),
      qec);
}

// __________________________________________________________________________
TEST(QueryPlanner, PathSearchMissingStart) {
  auto qec = ad_utility::testing::getQec("<x> <p> <y>. <y> <p> <z>");
  auto getId = ad_utility::testing::makeGetId(qec->getIndex());

  auto query =
      "PREFIX pathSearch: <https://qlever.cs.uni-freiburg.de/pathSearch/>"
      "SELECT ?start ?end ?path ?edge WHERE {"
      "SERVICE pathSearch: {"
      "_:path pathSearch:algorithm pathSearch:allPaths ;"
      "pathSearch:source <x> ;"
      "pathSearch:target <z> ;"
      "pathSearch:pathColumn ?path ;"
      "pathSearch:edgeColumn ?edge ;"
      "pathSearch:end ?end;"
      "{SELECT * WHERE {"
      "?start <p> ?end."
      "}}}}";
  AD_EXPECT_THROW_WITH_MESSAGE_AND_TYPE(h::parseAndPlan(std::move(query), qec),
                                        HasSubstr("Missing parameter <start>"),
                                        parsedQuery::PathSearchException);
}

// __________________________________________________________________________
TEST(QueryPlanner, PathSearchMultipleStarts) {
  auto qec = ad_utility::testing::getQec("<x> <p> <y>. <y> <p> <z>");
  auto getId = ad_utility::testing::makeGetId(qec->getIndex());

  auto query =
      "PREFIX pathSearch: <https://qlever.cs.uni-freiburg.de/pathSearch/>"
      "SELECT ?start ?end ?path ?edge WHERE {"
      "SERVICE pathSearch: {"
      "_:path pathSearch:algorithm pathSearch:allPaths ;"
      "pathSearch:source <x> ;"
      "pathSearch:target <z> ;"
      "pathSearch:pathColumn ?path ;"
      "pathSearch:edgeColumn ?edge ;"
      "pathSearch:start ?start1;"
      "pathSearch:start ?start2;"
      "pathSearch:end ?end;"
      "{SELECT * WHERE {"
      "?start <p> ?end."
      "}}}}";
  AD_EXPECT_THROW_WITH_MESSAGE_AND_TYPE(
      h::parseAndPlan(std::move(query), qec),
      HasSubstr("parameter <start> has already been set "
                "to variable: '?start1'. New variable: '?start2'"),
      parsedQuery::MagicServiceException);
}

// __________________________________________________________________________
TEST(QueryPlanner, PathSearchMissingEnd) {
  auto qec = ad_utility::testing::getQec("<x> <p> <y>. <y> <p> <z>");
  auto getId = ad_utility::testing::makeGetId(qec->getIndex());

  auto query =
      "PREFIX pathSearch: <https://qlever.cs.uni-freiburg.de/pathSearch/>"
      "SELECT ?start ?end ?path ?edge WHERE {"
      "SERVICE pathSearch: {"
      "_:path pathSearch:algorithm pathSearch:allPaths ;"
      "pathSearch:source <x> ;"
      "pathSearch:target <z> ;"
      "pathSearch:pathColumn ?path ;"
      "pathSearch:edgeColumn ?edge ;"
      "pathSearch:start ?start;"
      "{SELECT * WHERE {"
      "?start <p> ?end."
      "}}}}";
  AD_EXPECT_THROW_WITH_MESSAGE_AND_TYPE(h::parseAndPlan(std::move(query), qec),
                                        HasSubstr("Missing parameter <end>"),
                                        parsedQuery::PathSearchException);
}

// __________________________________________________________________________
TEST(QueryPlanner, PathSearchMultipleEnds) {
  auto qec = ad_utility::testing::getQec("<x> <p> <y>. <y> <p> <z>");
  auto getId = ad_utility::testing::makeGetId(qec->getIndex());

  auto query =
      "PREFIX pathSearch: <https://qlever.cs.uni-freiburg.de/pathSearch/>"
      "SELECT ?start ?end ?path ?edge WHERE {"
      "SERVICE pathSearch: {"
      "_:path pathSearch:algorithm pathSearch:allPaths ;"
      "pathSearch:source <x> ;"
      "pathSearch:target <z> ;"
      "pathSearch:pathColumn ?path ;"
      "pathSearch:edgeColumn ?edge ;"
      "pathSearch:start ?start;"
      "pathSearch:end ?end1;"
      "pathSearch:end ?end2;"
      "{SELECT * WHERE {"
      "?start <p> ?end."
      "}}}}";
  AD_EXPECT_THROW_WITH_MESSAGE_AND_TYPE(
      h::parseAndPlan(std::move(query), qec),
      HasSubstr("parameter <end> has already been set "
                "to variable: '?end1'. New variable: '?end2'"),
      parsedQuery::MagicServiceException);
}

// __________________________________________________________________________
TEST(QueryPlanner, PathSearchStartNotVariable) {
  auto qec = ad_utility::testing::getQec("<x> <p> <y>. <y> <p> <z>");
  auto getId = ad_utility::testing::makeGetId(qec->getIndex());

  auto query =
      "PREFIX pathSearch: <https://qlever.cs.uni-freiburg.de/pathSearch/>"
      "SELECT ?start ?end ?path ?edge WHERE {"
      "SERVICE pathSearch: {"
      "_:path pathSearch:algorithm pathSearch:allPaths ;"
      "pathSearch:source <x> ;"
      "pathSearch:target <z> ;"
      "pathSearch:pathColumn ?path ;"
      "pathSearch:edgeColumn ?edge ;"
      "pathSearch:start <error>;"
      "pathSearch:end ?end;"
      "{SELECT * WHERE {"
      "?start <p> ?end."
      "}}}}";
  AD_EXPECT_THROW_WITH_MESSAGE_AND_TYPE(
      h::parseAndPlan(std::move(query), qec),
      HasSubstr("The value <error> for parameter <start>"),
      parsedQuery::MagicServiceException);
}

// __________________________________________________________________________
TEST(QueryPlanner, PathSearchPredicateNotIri) {
  auto qec = ad_utility::testing::getQec("<x> <p> <y>. <y> <p> <z>");
  auto getId = ad_utility::testing::makeGetId(qec->getIndex());

  auto query =
      "PREFIX pathSearch: <https://qlever.cs.uni-freiburg.de/pathSearch/>"
      "SELECT ?start ?end ?path ?edge WHERE {"
      "SERVICE pathSearch: {"
      "_:path ?algorithm pathSearch:allPaths ;"
      "pathSearch:source <x> ;"
      "pathSearch:target <z> ;"
      "pathSearch:pathColumn ?path ;"
      "pathSearch:edgeColumn ?edge ;"
      "pathSearch:start ?start;"
      "pathSearch:end ?end;"
      "{SELECT * WHERE {"
      "?start <p> ?end."
      "}}}}";
  AD_EXPECT_THROW_WITH_MESSAGE_AND_TYPE(h::parseAndPlan(std::move(query), qec),
                                        HasSubstr("Parameters must be IRIs"),
                                        parsedQuery::MagicServiceException);
}

// __________________________________________________________________________
TEST(QueryPlanner, PathSearchUnsupportedArgument) {
  auto qec = ad_utility::testing::getQec("<x> <p> <y>. <y> <p> <z>");
  auto getId = ad_utility::testing::makeGetId(qec->getIndex());

  auto query =
      "PREFIX pathSearch: <https://qlever.cs.uni-freiburg.de/pathSearch/>"
      "SELECT ?start ?end ?path ?edge WHERE {"
      "SERVICE pathSearch: {"
      "_:path pathSearch:algorithm pathSearch:allPaths ;"
      "<unsupportedArgument> ?error;"
      "pathSearch:source <x> ;"
      "pathSearch:target <z> ;"
      "pathSearch:pathColumn ?path ;"
      "pathSearch:edgeColumn ?edge ;"
      "pathSearch:start ?start;"
      "pathSearch:end ?end;"
      "{SELECT * WHERE {"
      "?start <p> ?end."
      "}}}}";
  AD_EXPECT_THROW_WITH_MESSAGE_AND_TYPE(
      h::parseAndPlan(std::move(query), qec),
      HasSubstr("Unsupported argument <unsupportedArgument> in PathSearch"),
      parsedQuery::PathSearchException);
}

// __________________________________________________________________________
TEST(QueryPlanner, PathSearchTwoVariablesForSource) {
  auto qec = ad_utility::testing::getQec("<x> <p> <y>. <y> <p> <z>");
  auto getId = ad_utility::testing::makeGetId(qec->getIndex());

  auto query =
      "PREFIX pathSearch: <https://qlever.cs.uni-freiburg.de/pathSearch/>"
      "SELECT ?start ?end ?path ?edge WHERE {"
      "SERVICE pathSearch: {"
      "_:path pathSearch:algorithm pathSearch:allPaths ;"
      "pathSearch:source ?source1 ;"
      "pathSearch:source ?source2 ;"
      "pathSearch:target <z> ;"
      "pathSearch:pathColumn ?path ;"
      "pathSearch:edgeColumn ?edge ;"
      "pathSearch:start ?start;"
      "pathSearch:end ?end;"
      "{SELECT * WHERE {"
      "?start <p> ?end."
      "}}}}";
  AD_EXPECT_THROW_WITH_MESSAGE_AND_TYPE(
      h::parseAndPlan(std::move(query), qec),
      HasSubstr("Only one variable is allowed per search side"),
      parsedQuery::PathSearchException);
}

// __________________________________________________________________________
TEST(QueryPlanner, PathSearchUnsupportedElement) {
  auto qec = ad_utility::testing::getQec("<x> <p> <y>. <y> <p> <z>");
  auto getId = ad_utility::testing::makeGetId(qec->getIndex());

  auto query =
      "PREFIX pathSearch: <https://qlever.cs.uni-freiburg.de/pathSearch/>"
      "SELECT ?start ?end ?path ?edge WHERE {"
      "SERVICE pathSearch: {"
      "_:path pathSearch:algorithm pathSearch:allPaths ;"
      "pathSearch:source ?source1 ;"
      "pathSearch:source ?source2 ;"
      "pathSearch:target <z> ;"
      "pathSearch:pathColumn ?path ;"
      "pathSearch:edgeColumn ?edge ;"
      "pathSearch:start ?start;"
      "pathSearch:end ?end;"
      "VALUES ?middle {<m1>}"
      "{SELECT * WHERE {"
      "?start <p> ?end."
      "}}}}";
  AD_EXPECT_THROW_WITH_MESSAGE_AND_TYPE(
      h::parseAndPlan(std::move(query), qec),
      HasSubstr("Unsupported element in pathSearch"),
      parsedQuery::PathSearchException);
}

// __________________________________________________________________________
TEST(QueryPlanner, PathSearchUnsupportedAlgorithm) {
  auto qec = ad_utility::testing::getQec("<x> <p> <y>. <y> <p> <z>");
  auto getId = ad_utility::testing::makeGetId(qec->getIndex());

  auto query =
      "PREFIX pathSearch: <https://qlever.cs.uni-freiburg.de/pathSearch/>"
      "SELECT ?start ?end ?path ?edge WHERE {"
      "SERVICE pathSearch: {"
      "_:path pathSearch:algorithm pathSearch:shortestPath ;"
      "pathSearch:source ?source1 ;"
      "pathSearch:source ?source2 ;"
      "pathSearch:target <z> ;"
      "pathSearch:pathColumn ?path ;"
      "pathSearch:edgeColumn ?edge ;"
      "pathSearch:start ?start;"
      "pathSearch:end ?end;"
      "{SELECT * WHERE {"
      "?start <p> ?end."
      "}}}}";
  AD_EXPECT_THROW_WITH_MESSAGE_AND_TYPE(
      h::parseAndPlan(std::move(query), qec),
      HasSubstr("Unsupported algorithm in pathSearch"),
      parsedQuery::PathSearchException);
}

// __________________________________________________________________________
TEST(QueryPlanner, PathSearchWrongArgumentCartesian) {
  auto qec = ad_utility::testing::getQec("<x> <p> <y>. <y> <p> <z>");
  auto getId = ad_utility::testing::makeGetId(qec->getIndex());

  auto query =
      "PREFIX pathSearch: <https://qlever.cs.uni-freiburg.de/pathSearch/>"
      "SELECT ?start ?end ?path ?edge WHERE {"
      "SERVICE pathSearch: {"
      "_:path pathSearch:algorithm pathSearch:allPaths ;"
      "pathSearch:source ?source1 ;"
      "pathSearch:source ?source2 ;"
      "pathSearch:target <z> ;"
      "pathSearch:pathColumn ?path ;"
      "pathSearch:edgeColumn ?edge ;"
      "pathSearch:start ?start;"
      "pathSearch:end ?end;"
      "pathSearch:cartesian <false>;"
      "{SELECT * WHERE {"
      "?start <p> ?end."
      "}}}}";
  AD_EXPECT_THROW_WITH_MESSAGE_AND_TYPE(
      h::parseAndPlan(std::move(query), qec),
      HasSubstr("The parameter <cartesian> expects a boolean"),
      parsedQuery::PathSearchException);
}

// __________________________________________________________________________
TEST(QueryPlanner, PathSearchWrongArgumentNumPathsPerTarget) {
  auto qec = ad_utility::testing::getQec("<x> <p> <y>. <y> <p> <z>");
  auto getId = ad_utility::testing::makeGetId(qec->getIndex());

  auto query =
      "PREFIX pathSearch: <https://qlever.cs.uni-freiburg.de/pathSearch/>"
      "SELECT ?start ?end ?path ?edge WHERE {"
      "SERVICE pathSearch: {"
      "_:path pathSearch:algorithm pathSearch:allPaths ;"
      "pathSearch:source ?source1 ;"
      "pathSearch:source ?source2 ;"
      "pathSearch:target <z> ;"
      "pathSearch:pathColumn ?path ;"
      "pathSearch:edgeColumn ?edge ;"
      "pathSearch:start ?start;"
      "pathSearch:end ?end;"
      "pathSearch:numPathsPerTarget <five>;"
      "{SELECT * WHERE {"
      "?start <p> ?end."
      "}}}}";
  AD_EXPECT_THROW_WITH_MESSAGE_AND_TYPE(
      h::parseAndPlan(std::move(query), qec),
      HasSubstr("The parameter <numPathsPerTarget> expects an integer"),
      parsedQuery::PathSearchException);
}

// __________________________________________________________________________
TEST(QueryPlanner, PathSearchWrongArgumentAlgorithm) {
  auto qec = ad_utility::testing::getQec("<x> <p> <y>. <y> <p> <z>");
  auto getId = ad_utility::testing::makeGetId(qec->getIndex());

  auto query =
      "PREFIX pathSearch: <https://qlever.cs.uni-freiburg.de/pathSearch/>"
      "SELECT ?start ?end ?path ?edge WHERE {"
      "SERVICE pathSearch: {"
      "_:path pathSearch:algorithm 1 ;"
      "pathSearch:source ?source1 ;"
      "pathSearch:source ?source2 ;"
      "pathSearch:target <z> ;"
      "pathSearch:pathColumn ?path ;"
      "pathSearch:edgeColumn ?edge ;"
      "pathSearch:start ?start;"
      "pathSearch:end ?end;"
      "{SELECT * WHERE {"
      "?start <p> ?end."
      "}}}}";
  AD_EXPECT_THROW_WITH_MESSAGE_AND_TYPE(
      h::parseAndPlan(std::move(query), qec),
      HasSubstr("The <algorithm> value has to be an IRI"),
      parsedQuery::PathSearchException);
}

// __________________________________________________________________________
TEST(QueryPlanner, SpatialJoinService) {
  auto scan = h::IndexScanFromStrings;
  using V = Variable;
  auto S2 = SpatialJoinAlgorithm::S2_GEOMETRY;
  auto Basel = SpatialJoinAlgorithm::BASELINE;
  auto BBox = SpatialJoinAlgorithm::BOUNDING_BOX;
  PayloadVariables emptyPayload{};

  // Simple base cases
  h::expect(
      "PREFIX spatialSearch: <https://qlever.cs.uni-freiburg.de/spatialSearch/>"
      "SELECT * WHERE {"
      "?x <p> ?y."
      "SERVICE spatialSearch: {"
      "_:config spatialSearch:algorithm spatialSearch:s2 ;"
      "spatialSearch:left ?y ;"
      "spatialSearch:right ?b ;"
      "spatialSearch:maxDistance 1 . "
      "{ ?a <p> ?b } }}",
      h::SpatialJoin(1, -1, V{"?y"}, V{"?b"}, std::nullopt, emptyPayload, S2,
                     scan("?x", "<p>", "?y"), scan("?a", "<p>", "?b")));
  h::expect(
      "PREFIX spatialSearch: <https://qlever.cs.uni-freiburg.de/spatialSearch/>"
      "SELECT * WHERE {"
      "?x <p> ?y."
      "SERVICE spatialSearch: {"
      "_:config spatialSearch:left ?y ;"
      "spatialSearch:right ?b ;"
      "spatialSearch:maxDistance 1 . "
      "{ ?a <p> ?b } }}",
      h::SpatialJoin(1, -1, V{"?y"}, V{"?b"}, std::nullopt, emptyPayload, S2,
                     scan("?x", "<p>", "?y"), scan("?a", "<p>", "?b")));
  h::expect(
      "PREFIX spatialSearch: <https://qlever.cs.uni-freiburg.de/spatialSearch/>"
      "SELECT * WHERE {"
      "?x <p> ?y."
      "SERVICE spatialSearch: {"
      "_:config spatialSearch:algorithm spatialSearch:baseline ;"
      "spatialSearch:left ?y ;"
      "spatialSearch:right ?b ;"
      "spatialSearch:maxDistance 1 . "
      "{ ?a <p> ?b } }}",
      h::SpatialJoin(1, -1, V{"?y"}, V{"?b"}, std::nullopt, emptyPayload, Basel,
                     scan("?x", "<p>", "?y"), scan("?a", "<p>", "?b")));
  h::expect(
      "PREFIX spatialSearch: <https://qlever.cs.uni-freiburg.de/spatialSearch/>"
      "SELECT * WHERE {"
      "?x <p> ?y."
      "SERVICE spatialSearch: {"
      "_:config spatialSearch:algorithm spatialSearch:boundingBox ;"
      "spatialSearch:left ?y ;"
      "spatialSearch:right ?b ;"
      "spatialSearch:maxDistance 100 . "
      "{ ?a <p> ?b } }}",
      h::SpatialJoin(100, -1, V{"?y"}, V{"?b"}, std::nullopt, emptyPayload,
                     BBox, scan("?x", "<p>", "?y"), scan("?a", "<p>", "?b")));
  h::expect(
      "PREFIX spatialSearch: <https://qlever.cs.uni-freiburg.de/spatialSearch/>"
      "SELECT * WHERE {"
      "?x <p> ?y."
      "SERVICE spatialSearch: {"
      "_:config spatialSearch:algorithm spatialSearch:s2 ;"
      "spatialSearch:left ?y ;"
      "spatialSearch:right ?b ;"
      "spatialSearch:maxDistance 100 ;"
      "spatialSearch:numNearestNeighbors 2 ;"
      "spatialSearch:bindDistance ?dist ."
      "{ ?a <p> ?b } }}",
      h::SpatialJoin(100, 2, V{"?y"}, V{"?b"}, V{"?dist"}, emptyPayload, S2,
                     scan("?x", "<p>", "?y"), scan("?a", "<p>", "?b")));
  h::expect(
      "PREFIX spatialSearch: <https://qlever.cs.uni-freiburg.de/spatialSearch/>"
      "SELECT * WHERE {"
      "?x <p> ?y."
      "SERVICE spatialSearch: {"
      "_:config spatialSearch:algorithm spatialSearch:s2 ;"
      "spatialSearch:right ?b ;"
      "spatialSearch:numNearestNeighbors 5 . "
      "_:config spatialSearch:left ?y ."
      "{ ?a <p> ?b } }}",
      h::SpatialJoin(-1, 5, V{"?y"}, V{"?b"}, std::nullopt, emptyPayload, S2,
                     scan("?x", "<p>", "?y"), scan("?a", "<p>", "?b")));
}

TEST(QueryPlanner, SpatialJoinServicePayloadVars) {
  // Test the <payload> option which allows selecting columns from the graph
  // pattern inside the service.

  auto scan = h::IndexScanFromStrings;
  using V = Variable;
  auto S2 = SpatialJoinAlgorithm::S2_GEOMETRY;
  using PV = PayloadVariables;

  h::expect(
      "PREFIX spatialSearch: <https://qlever.cs.uni-freiburg.de/spatialSearch/>"
      "SELECT * WHERE {"
      "?x <p> ?y."
      "SERVICE spatialSearch: {"
      "_:config spatialSearch:algorithm spatialSearch:s2 ;"
      "spatialSearch:right ?b ;"
      "spatialSearch:bindDistance ?dist ;"
      "spatialSearch:numNearestNeighbors 5 . "
      "_:config spatialSearch:left ?y ."
      "_:config spatialSearch:payload ?a ."
      "{ ?a <p> ?b } }}",
      h::SpatialJoin(-1, 5, V{"?y"}, V{"?b"}, V{"?dist"},
                     PV{std::vector<V>{V{"?a"}}}, S2, scan("?x", "<p>", "?y"),
                     scan("?a", "<p>", "?b")));
  h::expect(
      "PREFIX spatialSearch: <https://qlever.cs.uni-freiburg.de/spatialSearch/>"
      "SELECT * WHERE {"
      "?x <p> ?y."
      "SERVICE spatialSearch: {"
      "_:config spatialSearch:algorithm spatialSearch:s2 ;"
      "spatialSearch:right ?b ;"
      "spatialSearch:bindDistance ?dist ;"
      "spatialSearch:numNearestNeighbors 5 . "
      "_:config spatialSearch:left ?y ."
      "_:config spatialSearch:payload ?a , ?a2 ."
      "{ ?a <p> ?a2 . ?a2 <p> ?b } }}",
      h::SpatialJoin(
          -1, 5, V{"?y"}, V{"?b"}, V{"?dist"},
          PV{std::vector<V>{V{"?a"}, V{"?a2"}}}, S2, scan("?x", "<p>", "?y"),
          h::Join(scan("?a", "<p>", "?a2"), scan("?a2", "<p>", "?b"))));

  // Right variable and duplicates are possible (silently deduplicated during
  // query result computation)
  h::expect(
      "PREFIX spatialSearch: <https://qlever.cs.uni-freiburg.de/spatialSearch/>"
      "SELECT * WHERE {"
      "?x <p> ?y."
      "SERVICE spatialSearch: {"
      "_:config spatialSearch:algorithm spatialSearch:s2 ;"
      "spatialSearch:right ?b ;"
      "spatialSearch:bindDistance ?dist ;"
      "spatialSearch:numNearestNeighbors 5 . "
      "_:config spatialSearch:left ?y ."
      "_:config spatialSearch:payload ?a, ?a, ?b, ?a2 ."
      "{ ?a <p> ?a2 . ?a2 <p> ?b } }}",
      h::SpatialJoin(
          -1, 5, V{"?y"}, V{"?b"}, V{"?dist"},
          PV{std::vector<V>{V{"?a"}, V{"?a"}, V{"?b"}, V{"?a2"}}}, S2,
          scan("?x", "<p>", "?y"),
          h::Join(scan("?a", "<p>", "?a2"), scan("?a2", "<p>", "?b"))));

  // Selecting all payload variables using "all"
  h::expect(
      "PREFIX spatialSearch: <https://qlever.cs.uni-freiburg.de/spatialSearch/>"
      "SELECT * WHERE {"
      "?x <p> ?y."
      "SERVICE spatialSearch: {"
      "_:config spatialSearch:algorithm spatialSearch:s2 ;"
      "spatialSearch:right ?b ;"
      "spatialSearch:bindDistance ?dist ;"
      "spatialSearch:numNearestNeighbors 5 . "
      "_:config spatialSearch:left ?y ."
      "_:config spatialSearch:payload <all> ."
      "{ ?a <p> ?a2 . ?a2 <p> ?b } }}",
      h::SpatialJoin(
          -1, 5, V{"?y"}, V{"?b"}, V{"?dist"}, PayloadVariables::all(), S2,
          scan("?x", "<p>", "?y"),
          h::Join(scan("?a", "<p>", "?a2"), scan("?a2", "<p>", "?b"))));
  h::expect(
      "PREFIX spatialSearch: <https://qlever.cs.uni-freiburg.de/spatialSearch/>"
      "SELECT * WHERE {"
      "?x <p> ?y."
      "SERVICE spatialSearch: {"
      "_:config spatialSearch:algorithm spatialSearch:s2 ;"
      "spatialSearch:right ?b ;"
      "spatialSearch:bindDistance ?dist ;"
      "spatialSearch:numNearestNeighbors 5 . "
      "_:config spatialSearch:left ?y ."
      "_:config spatialSearch:payload spatialSearch:all ."
      "{ ?a <p> ?a2 . ?a2 <p> ?b } }}",
      h::SpatialJoin(
          -1, 5, V{"?y"}, V{"?b"}, V{"?dist"}, PayloadVariables::all(), S2,
          scan("?x", "<p>", "?y"),
          h::Join(scan("?a", "<p>", "?a2"), scan("?a2", "<p>", "?b"))));

  // All and explicitly named ones just select all
  h::expect(
      "PREFIX spatialSearch: <https://qlever.cs.uni-freiburg.de/spatialSearch/>"
      "SELECT * WHERE {"
      "?x <p> ?y."
      "SERVICE spatialSearch: {"
      "_:config spatialSearch:algorithm spatialSearch:s2 ;"
      "spatialSearch:right ?b ;"
      "spatialSearch:bindDistance ?dist ;"
      "spatialSearch:numNearestNeighbors 5 . "
      "_:config spatialSearch:left ?y ."
      "_:config spatialSearch:payload <all> ."
      "_:config spatialSearch:payload ?a ."
      "{ ?a <p> ?a2 . ?a2 <p> ?b } }}",
      h::SpatialJoin(
          -1, 5, V{"?y"}, V{"?b"}, V{"?dist"}, PayloadVariables::all(), S2,
          scan("?x", "<p>", "?y"),
          h::Join(scan("?a", "<p>", "?a2"), scan("?a2", "<p>", "?b"))));
}

TEST(QueryPlanner, SpatialJoinServiceMaxDistOutside) {
  auto scan = h::IndexScanFromStrings;
  using V = Variable;
  auto S2 = SpatialJoinAlgorithm::S2_GEOMETRY;

  // If only maxDistance is used but not numNearestNeighbors, the right variable
  // must not come from inside the SERVICE
  h::expect(
      "PREFIX spatialSearch: <https://qlever.cs.uni-freiburg.de/spatialSearch/>"
      "SELECT * WHERE {"
      "?a <p> ?b ."
      "?x <p> ?y ."
      "SERVICE spatialSearch: {"
      "_:config spatialSearch:algorithm spatialSearch:s2 ;"
      "spatialSearch:left ?y ;"
      "spatialSearch:right ?b ;"
      "spatialSearch:maxDistance 1 . "
      " } }",
      h::SpatialJoin(1, -1, V{"?y"}, V{"?b"}, std::nullopt,
                     // Payload variables have the default all instead of empty
                     // in this case
                     PayloadVariables::all(), S2, scan("?x", "<p>", "?y"),
                     scan("?a", "<p>", "?b")));

  // If the user explicitly states that they want all payload variables (which
  // is enforced and the default anyway), this should also work
  h::expect(
      "PREFIX spatialSearch: <https://qlever.cs.uni-freiburg.de/spatialSearch/>"
      "SELECT * WHERE {"
      "?a <p> ?b ."
      "?x <p> ?y ."
      "SERVICE spatialSearch: {"
      "_:config spatialSearch:algorithm spatialSearch:s2 ;"
      "spatialSearch:left ?y ;"
      "spatialSearch:right ?b ;"
      "spatialSearch:maxDistance 1 ; "
      "spatialSearch:payload spatialSearch:all ."
      " } }",
      h::SpatialJoin(1, -1, V{"?y"}, V{"?b"}, std::nullopt,
                     PayloadVariables::all(), S2, scan("?x", "<p>", "?y"),
                     scan("?a", "<p>", "?b")));

  // Nearest neighbors search requires the right child to be defined inside the
  // service
  AD_EXPECT_THROW_WITH_MESSAGE(
      h::expect("PREFIX spatialSearch: "
                "<https://qlever.cs.uni-freiburg.de/spatialSearch/>"
                "SELECT * WHERE {"
                "?a <p> ?b ."
                "?x <p> ?y ."
                "SERVICE spatialSearch: {"
                "_:config spatialSearch:algorithm spatialSearch:s2 ;"
                "spatialSearch:left ?y ;"
                "spatialSearch:right ?b ;"
                "spatialSearch:maxDistance 1 ; "
                "spatialSearch:numNearestNeighbors 5 ."
                "}}",
                ::testing::_),
      ::testing::ContainsRegex(
          "must have its right "
          "variable declared inside the service using a graph pattern"));

  // The user may not select specific payload variables if the right join table
  // is declared outside because this would mess up the query semantics and may
  // not have deterministic results on different inputs because of query planner
  // decisions
  AD_EXPECT_THROW_WITH_MESSAGE(
      h::expect("PREFIX spatialSearch: "
                "<https://qlever.cs.uni-freiburg.de/spatialSearch/>"
                "SELECT * WHERE {"
                "?a <p> ?b ."
                "?x <p> ?y ."
                "SERVICE spatialSearch: {"
                "_:config spatialSearch:algorithm spatialSearch:s2 ;"
                "spatialSearch:left ?y ;"
                "spatialSearch:right ?b ;"
                "spatialSearch:maxDistance 1 ; "
                "spatialSearch:payload ?a ."
                "}}",
                ::testing::_),
      ::testing::ContainsRegex(
          "right variable for the spatial search is declared outside the "
          "SERVICE, but the <payload> parameter was set"));
}

TEST(QueryPlanner, SpatialJoinMultipleServiceSharedLeft) {
  // Test two spatial join SERVICEs that share a common ?left variable

  auto scan = h::IndexScanFromStrings;
  using V = Variable;
  auto S2 = SpatialJoinAlgorithm::S2_GEOMETRY;
  using PV = PayloadVariables;

  h::expect(
      "SELECT * WHERE {"
      "?x <p> ?y ."
      "?y <max-distance-in-meters:100> ?b ."
      "?ab <p1> ?b ."
      "?y <max-distance-in-meters:500> ?c ."
      "?ac <p2> ?c ."
      "}",
      // Use two matchers using AnyOf here because the query planner may add the
      // children one way or the other depending on cost estimates. Both
      // versions are semantically correct.
      ::testing::AnyOf(
          h::SpatialJoin(100, -1, V{"?y"}, V{"?b"}, std::nullopt, PV::all(), S2,
                         h::SpatialJoin(500, -1, V{"?y"}, V{"?c"}, std::nullopt,
                                        PV::all(), S2, scan("?x", "<p>", "?y"),
                                        scan("?ac", "<p2>", "?c")),
                         scan("?ab", "<p1>", "?b")),
          h::SpatialJoin(500, -1, V{"?y"}, V{"?c"}, std::nullopt, PV::all(), S2,
                         h::SpatialJoin(100, -1, V{"?y"}, V{"?b"}, std::nullopt,
                                        PV::all(), S2, scan("?x", "<p>", "?y"),
                                        scan("?ab", "<p1>", "?b")),
                         scan("?ac", "<p2>", "?c"))));
  h::expect(
      "PREFIX spatialSearch: <https://qlever.cs.uni-freiburg.de/spatialSearch/>"
      "SELECT * WHERE {"
      "?x <p> ?y ."
      "SERVICE spatialSearch: {"
      "  _:config spatialSearch:algorithm spatialSearch:s2 ;"
      "    spatialSearch:left ?y ;"
      "    spatialSearch:right ?b ;"
      "    spatialSearch:numNearestNeighbors 5 ; "
      "    spatialSearch:bindDistance ?db ."
      "  { ?ab <p1> ?b } "
      "}"
      "SERVICE spatialSearch: {"
      "  _:config spatialSearch:algorithm spatialSearch:s2 ;"
      "    spatialSearch:left ?y ;"
      "    spatialSearch:right ?c ;"
      "    spatialSearch:numNearestNeighbors 5 ; "
      "    spatialSearch:maxDistance 500 ; "
      "    spatialSearch:payload ?ac ; "
      "    spatialSearch:bindDistance ?dc ."
      "  { ?ac <p2> ?c }"
      " }"
      "}",
      // Use two matchers using AnyOf here because the query planner may add the
      // children one way or the other depending on cost estimates. Both
      // versions are semantically correct.
      ::testing::AnyOf(
          h::SpatialJoin(500, 5, V{"?y"}, V{"?c"}, V{"?dc"},
                         PV{std::vector<V>{V{"?ac"}}}, S2,
                         h::SpatialJoin(-1, 5, V{"?y"}, V{"?b"}, V{"?db"}, PV{},
                                        S2, scan("?x", "<p>", "?y"),
                                        scan("?ab", "<p1>", "?b")),
                         scan("?ac", "<p2>", "?c")),
          h::SpatialJoin(-1, 5, V{"?y"}, V{"?b"}, V{"?db"}, PV{}, S2,
                         h::SpatialJoin(500, 5, V{"?y"}, V{"?c"}, V{"?dc"},
                                        PV{std::vector<V>{V{"?ac"}}}, S2,
                                        scan("?x", "<p>", "?y"),
                                        scan("?ac", "<p2>", "?c")),
                         scan("?ab", "<p1>", "?b"))));
}

TEST(QueryPlanner, SpatialJoinMissingConfig) {
  // Tests with incomplete config
  AD_EXPECT_THROW_WITH_MESSAGE(
      h::expect("PREFIX spatialSearch: "
                "<https://qlever.cs.uni-freiburg.de/spatialSearch/>"
                "SELECT * WHERE {"
                "?x <p> ?y ."
                "SERVICE spatialSearch: {"
                "_:config spatialSearch:right ?b ;"
                "spatialSearch:maxDistance 5 . "
                " { ?a <p> ?b . }"
                "}}",
                ::testing::_),
      ::testing::ContainsRegex("Missing parameter <left>"));
  AD_EXPECT_THROW_WITH_MESSAGE(
      h::expect("PREFIX spatialSearch: "
                "<https://qlever.cs.uni-freiburg.de/spatialSearch/>"
                "SELECT * WHERE {"
                "?x <p> ?y ."
                "SERVICE spatialSearch: {"
                "_:config spatialSearch:right ?b ;"
                "spatialSearch:numNearestNeighbors 5 . "
                " { ?a <p> ?b . }"
                "}}",
                ::testing::_),
      ::testing::ContainsRegex("Missing parameter <left>"));
  AD_EXPECT_THROW_WITH_MESSAGE(
      h::expect("PREFIX spatialSearch: "
                "<https://qlever.cs.uni-freiburg.de/spatialSearch/>"
                "SELECT * WHERE {"
                "?x <p> ?y ."
                "SERVICE spatialSearch: {"
                "_:config spatialSearch:left ?y ;"
                "spatialSearch:maxDistance 5 . "
                " { ?a <p> ?b . }"
                "}}",
                ::testing::_),
      ::testing::ContainsRegex("Missing parameter <right>"));
  AD_EXPECT_THROW_WITH_MESSAGE(
      h::expect("PREFIX spatialSearch: "
                "<https://qlever.cs.uni-freiburg.de/spatialSearch/>"
                "SELECT * WHERE {"
                "?x <p> ?y ."
                "SERVICE spatialSearch: {"
                "_:config spatialSearch:left ?y ;"
                "spatialSearch:numNearestNeighbors 5 . "
                " { ?a <p> ?b . }"
                "}}",
                ::testing::_),
      ::testing::ContainsRegex("Missing parameter <right>"));
  AD_EXPECT_THROW_WITH_MESSAGE(
      h::expect("PREFIX spatialSearch: "
                "<https://qlever.cs.uni-freiburg.de/spatialSearch/>"
                "SELECT * WHERE {"
                "?x <p> ?y ."
                "SERVICE spatialSearch: {"
                "_:config spatialSearch:left ?y ;"
                " spatialSearch:right ?b ."
                " { ?a <p> ?b . }"
                "}}",
                ::testing::_),
      ::testing::ContainsRegex(
          "Neither <numNearestNeighbors> nor <maxDistance> were provided"));
}

TEST(QueryPlanner, SpatialJoinInvalidOperationsInService) {
  // Test that unallowed operations inside the SERVICE statement throw
  AD_EXPECT_THROW_WITH_MESSAGE(
      h::expect("PREFIX spatialSearch: "
                "<https://qlever.cs.uni-freiburg.de/spatialSearch/>"
                "SELECT * WHERE {"
                "?x <p> ?y."
                "SERVICE spatialSearch: {"
                "_:config spatialSearch:algorithm spatialSearch:s2 ;"
                "spatialSearch:left ?y ;"
                "spatialSearch:right ?b ;"
                "spatialSearch:maxDistance 1 . "
                "{ ?a <p> ?b }"
                "SERVICE <http://example.com/> { ?a <something> <else> }"
                " }}",
                ::testing::_),
      ::testing::ContainsRegex("Unsupported element in spatialQuery"));
}

TEST(QueryPlanner, SpatialJoinServiceMultipleGraphPatterns) {
  // Test that the SERVICE statement may only contain at most one graph pattern
  AD_EXPECT_THROW_WITH_MESSAGE(
      h::expect("PREFIX spatialSearch: "
                "<https://qlever.cs.uni-freiburg.de/spatialSearch/>"
                "SELECT * WHERE {"
                "?x <p> ?y."
                "SERVICE spatialSearch: {"
                "_:config spatialSearch:algorithm spatialSearch:s2 ;"
                "spatialSearch:left ?y ;"
                "spatialSearch:right ?b ;"
                "spatialSearch:maxDistance 1 . "
                "{ ?a <p> ?b }"
                "{ ?a <p2> ?c } }}",
                ::testing::_),
      ::testing::ContainsRegex("A magic SERVICE query must not contain more "
                               "than one nested group graph pattern"));
}

TEST(QueryPlanner, SpatialJoinIncorrectConfigValues) {
  // Tests with mistakes in the config
  AD_EXPECT_THROW_WITH_MESSAGE(
      h::expect("PREFIX spatialSearch: "
                "<https://qlever.cs.uni-freiburg.de/spatialSearch/>"
                "SELECT * WHERE {"
                "?x <p> ?y ."
                "SERVICE spatialSearch: {"
                "_:config spatialSearch:right ?b ;"
                "spatialSearch:left ?y ;"
                "spatialSearch:maxDistance \"5\" . "
                " { ?a <p> ?b . }"
                "}}",
                ::testing::_),
      ::testing::ContainsRegex("<maxDistance> expects an integer"));
  AD_EXPECT_THROW_WITH_MESSAGE(
      h::expect("PREFIX spatialSearch: "
                "<https://qlever.cs.uni-freiburg.de/spatialSearch/>"
                "SELECT * WHERE {"
                "?x <p> ?y ."
                "SERVICE spatialSearch: {"
                "_:config spatialSearch:right ?b ;"
                "spatialSearch:left ?y ;"
                "spatialSearch:maxDistance 5 ;"
                "spatialSearch:numNearestNeighbors \"1\" ."
                " { ?a <p> ?b . }"
                "}}",
                ::testing::_),
      ::testing::ContainsRegex("<numNearestNeighbors> expects an integer"));
  AD_EXPECT_THROW_WITH_MESSAGE(
      h::expect("PREFIX spatialSearch: "
                "<https://qlever.cs.uni-freiburg.de/spatialSearch/>"
                "SELECT * WHERE {"
                "?x <p> ?y ."
                "SERVICE spatialSearch: {"
                "_:config spatialSearch:right ?b ;"
                "spatialSearch:left ?y ;"
                "spatialSearch:maxDistance 5 ;"
                "spatialSearch:algorithm \"1\" ."
                " { ?a <p> ?b . }"
                "}}",
                ::testing::_),
      ::testing::ContainsRegex("parameter <algorithm> needs an IRI"));
  AD_EXPECT_THROW_WITH_MESSAGE(
      h::expect("PREFIX spatialSearch: "
                "<https://qlever.cs.uni-freiburg.de/spatialSearch/>"
                "SELECT * WHERE {"
                "?x <p> ?y ."
                "SERVICE spatialSearch: {"
                "_:config spatialSearch:right ?b ;"
                "spatialSearch:left ?y ;"
                "spatialSearch:maxDistance 5 ;"
                "spatialSearch:algorithm <http://example.com/some-nonsense> ."
                " { ?a <p> ?b . }"
                "}}",
                ::testing::_),
      ::testing::ContainsRegex("<algorithm> does not refer to a supported "
                               "spatial search algorithm"));
  AD_EXPECT_THROW_WITH_MESSAGE(
      h::expect("PREFIX spatialSearch: "
                "<https://qlever.cs.uni-freiburg.de/spatialSearch/>"
                "SELECT * WHERE {"
                "?x <p> ?y ."
                "SERVICE spatialSearch: {"
                "_:config spatialSearch:right ?b ;"
                "spatialSearch:left ?y ;"
                "spatialSearch:maxDistance 5 ;"
                "<http://example.com/some-nonsense> 123 ."
                " { ?a <p> ?b . }"
                "}}",
                ::testing::_),
      ::testing::ContainsRegex("Unsupported argument"));
  AD_EXPECT_THROW_WITH_MESSAGE(
      h::expect("PREFIX spatialSearch: "
                "<https://qlever.cs.uni-freiburg.de/spatialSearch/>"
                "SELECT * WHERE {"
                "?x <p> ?y ."
                "SERVICE spatialSearch: {"
                "_:config spatialSearch:right ?b ;"
                "spatialSearch:left ?y ;"
                "spatialSearch:maxDistance 5 ;"
                "spatialSearch:bindDistance 123 ."
                " { ?a <p> ?b . }"
                "}}",
                ::testing::_),
      ::testing::ContainsRegex("<bindDistance> has to be a variable"));
  AD_EXPECT_THROW_WITH_MESSAGE(
      h::expect("PREFIX spatialSearch: "
                "<https://qlever.cs.uni-freiburg.de/spatialSearch/>"
                "SELECT * WHERE {"
                "?x <p> ?y ."
                "SERVICE spatialSearch: {"
                "_:config spatialSearch:right ?b ;"
                "spatialSearch:left ?y ;"
                "spatialSearch:maxDistance 5 ;"
                "spatialSearch:payload 123 ."
                " { ?a <p> ?b . }"
                "}}",
                ::testing::_),
      ::testing::ContainsRegex("<payload> parameter must be either a variable "
                               "to be selected or <all>"));
  AD_EXPECT_THROW_WITH_MESSAGE(
      h::expect("PREFIX spatialSearch: "
                "<https://qlever.cs.uni-freiburg.de/spatialSearch/>"
                "SELECT * WHERE {"
                "?x <p> ?y ."
                "SERVICE spatialSearch: {"
                "_:config spatialSearch:right ?b ;"
                "spatialSearch:left ?y ;"
                "spatialSearch:maxDistance 5 ;"
                "spatialSearch:payload <http://some.iri.that.is.not.all> ."
                " { ?a <p> ?b . }"
                "}}",
                ::testing::_),
      ::testing::ContainsRegex("<payload> parameter must be either a variable "
                               "to be selected or <all>"));
  AD_EXPECT_THROW_WITH_MESSAGE(
      h::expect("PREFIX spatialSearch: "
                "<https://qlever.cs.uni-freiburg.de/spatialSearch/>"
                "SELECT * WHERE {"
                "?x <p> ?y ."
                "SERVICE spatialSearch: {"
                "_:config spatialSearch:right ?b ;"
                "spatialSearch:left ?y ;"
                "spatialSearch:maxDistance 5 ;"
                "spatialSearch:bindDistance ?dist_a ;"
                "spatialSearch:bindDistance ?dist_b ."
                " { ?a <p> ?b . }"
                "}}",
                ::testing::_),
      ::testing::ContainsRegex("<bindDistance> has already been set"));
  AD_EXPECT_THROW_WITH_MESSAGE(
      h::expect("PREFIX spatialSearch: "
                "<https://qlever.cs.uni-freiburg.de/spatialSearch/>"
                "SELECT * WHERE {"
                "?x <p> ?y ."
                "SERVICE spatialSearch: {"
                "_:config spatialSearch:right 123 ;"
                "spatialSearch:left ?y ;"
                "spatialSearch:maxDistance 5 ."
                " { ?a <p> ?b . }"
                "}}",
                ::testing::_),
      ::testing::ContainsRegex("<right> has to be a variable"));
  AD_EXPECT_THROW_WITH_MESSAGE(
      h::expect("PREFIX spatialSearch: "
                "<https://qlever.cs.uni-freiburg.de/spatialSearch/>"
                "SELECT * WHERE {"
                "?x <p> ?y ."
                "SERVICE spatialSearch: {"
                "_:config spatialSearch:right ?b ;"
                "spatialSearch:left \"abc\" ;"
                "spatialSearch:maxDistance 5 ."
                " { ?a <p> ?b . }"
                "}}",
                ::testing::_),
      ::testing::ContainsRegex("<left> has to be a variable"));
}

TEST(QueryPlanner, SpatialJoinLegacyPredicateSupport) {
  auto scan = h::IndexScanFromStrings;
  using V = Variable;
  auto S2 = SpatialJoinAlgorithm::S2_GEOMETRY;

  // For maxDistance the special predicate remains supported
  h::expect(
      "SELECT * WHERE {"
      "?a <p> ?b ."
      "?y <max-distance-in-meters:1> ?b ."
      "?x <p> ?y ."
      " }",
      h::SpatialJoin(1, -1, V{"?y"}, V{"?b"}, std::nullopt,
                     PayloadVariables::all(), S2, scan("?x", "<p>", "?y"),
                     scan("?a", "<p>", "?b")));
  h::expect(
      "SELECT * WHERE {"
      "?a <p> ?b ."
      "?y <max-distance-in-meters:5000> ?b ."
      "?x <p> ?y ."
      " }",
      h::SpatialJoin(5000, -1, V{"?y"}, V{"?b"}, std::nullopt,
                     PayloadVariables::all(), S2, scan("?x", "<p>", "?y"),
                     scan("?a", "<p>", "?b")));

  // Test that invalid triples throw an error
  AD_EXPECT_THROW_WITH_MESSAGE(
      h::expect("SELECT ?x ?y WHERE {"
                "?x <p> ?y."
                "?a <p> ?b."
                "?y <max-distance-in-meters:1> ?b ."
                "?y <a> ?b}",
                ::testing::_),
      ::testing::ContainsRegex(
          "Currently, if both sides of a SpatialJoin are variables, then the"
          "SpatialJoin must be the only connection between these variables"));

  AD_EXPECT_THROW_WITH_MESSAGE(
      h::expect("SELECT ?x ?y WHERE {"
                "?y <p> ?b."
                "?y <max-distance-in-meters:1> ?b }",
                ::testing::_),
      ::testing::ContainsRegex(
          "Currently, if both sides of a SpatialJoin are variables, then the"
          "SpatialJoin must be the only connection between these variables"));

  EXPECT_ANY_THROW(
      h::expect("SELECT ?x ?y WHERE {"
                "?x <p> ?y."
                "?y <max-distance-in-meters:1> <a> }",
                ::testing::_));

  EXPECT_ANY_THROW(
      h::expect("SELECT ?x ?y WHERE {"
                "?x <p> ?y."
                "<a> <max-distance-in-meters:1> ?y }",
                ::testing::_));

  AD_EXPECT_THROW_WITH_MESSAGE(h::expect("SELECT ?x ?y WHERE {"
                                         "?x <p> ?y."
                                         "?a <p> ?b."
                                         "?y <max-distance-in-meters:-1> ?b }",
                                         ::testing::_),
                               ::testing::ContainsRegex("unknown triple"));

  // Test that the nearest neighbors special predicate is still accepted but
  // produces a warning
  h::expect(
      "SELECT ?x ?y WHERE {"
      "?x <p> ?y."
      "?a <p> ?b."
      "?y <nearest-neighbors:2:500> ?b }",
      h::QetWithWarnings(
          {"special predicate <nearest-neighbors:...> is deprecated"},
          h::SpatialJoin(500, 2, V{"?y"}, V{"?b"}, std::nullopt,
                         PayloadVariables::all(), S2, scan("?x", "<p>", "?y"),
                         scan("?a", "<p>", "?b"))));
  h::expect(
      "SELECT ?x ?y WHERE {"
      "?x <p> ?y."
      "?a <p> ?b."
      "?y <nearest-neighbors:20> ?b }",
      h::QetWithWarnings(
          {"special predicate <nearest-neighbors:...> is deprecated"},
          h::SpatialJoin(-1, 20, V{"?y"}, V{"?b"}, std::nullopt,
                         PayloadVariables::all(), S2, scan("?x", "<p>", "?y"),
                         scan("?a", "<p>", "?b"))));

  AD_EXPECT_THROW_WITH_MESSAGE(h::expect("SELECT ?x ?y WHERE {"
                                         "?x <p> ?y."
                                         "?a <p> ?b."
                                         "?y <nearest-neighbors:1:-200> ?b }",
                                         ::testing::_),
                               ::testing::ContainsRegex("unknown triple"));

  AD_EXPECT_THROW_WITH_MESSAGE(h::expect("SELECT ?x ?y WHERE {"
                                         "?x <p> ?y."
                                         "?a <p> ?b."
                                         "?y <nearest-neighbors:0:-1> ?b }",
                                         ::testing::_),
                               ::testing::ContainsRegex("unknown triple"));

  EXPECT_ANY_THROW(
      h::expect("SELECT ?x ?y WHERE {"
                "?x <p> ?y."
                "?a <p> ?b."
                "?y <nearest-neighbors:2:500> ?b ."
                "?y <a> ?b}",
                ::testing::_));

  EXPECT_ANY_THROW(
      h::expect("SELECT ?x ?y WHERE {"
                "?y <p> ?b."
                "?y <nearest-neighbors:1> ?b }",
                ::testing::_));

  EXPECT_ANY_THROW(
      h::expect("SELECT ?x ?y WHERE {"
                "?x <p> ?y."
                "?y <nearest-neighbors:2:500> <a> }",
                ::testing::_));

  EXPECT_ANY_THROW(
      h::expect("SELECT ?x ?y WHERE {"
                "?x <p> ?y."
                "<a> <nearest-neighbors:2:500> ?y }",
                ::testing::_));

  EXPECT_ANY_THROW(
      h::expect("SELECT ?x ?y WHERE {"
                "?x <p> ?y."
                "?a <p> ?b."
                "?y <nearest-neighbors:> ?b }",
                ::testing::_));

  EXPECT_ANY_THROW(
      h::expect("SELECT ?x ?y WHERE {"
                "?x <p> ?y."
                "?a <p> ?b."
                "?y <nearest-neighbors:-50:500> ?b }",
                ::testing::_));

  EXPECT_ANY_THROW(
      h::expect("SELECT ?x ?y WHERE {"
                "?x <p> ?y."
                "?a <p> ?b."
                "?y <nearest-neighbors:1:-200> ?b }",
                ::testing::_));

  EXPECT_ANY_THROW(
      h::expect("SELECT ?x ?y WHERE {"
                "?x <p> ?y."
                "?a <p> ?b."
                "?y <nearest-neighbors:0:-1> ?b }",
                ::testing::_));
}

TEST(QueryPlanner, SpatialJoinLegacyMaxDistanceParsing) {
  // test if the SpatialJoin operation parses the maximum distance correctly
  auto testMaxDistance = [](std::string distanceIRI, long long distance,
                            bool shouldThrow) {
    auto qec = ad_utility::testing::getQec();
    TripleComponent subject{Variable{"?subject"}};
    TripleComponent object{Variable{"?object"}};
    SparqlTriple triple{subject, distanceIRI, object};
    if (shouldThrow) {
      ASSERT_ANY_THROW(
          parsedQuery::SpatialQuery{triple}.toSpatialJoinConfiguration());
    } else {
      auto config =
          parsedQuery::SpatialQuery{triple}.toSpatialJoinConfiguration();
      std::shared_ptr<QueryExecutionTree> spatialJoinOperation =
          ad_utility::makeExecutionTree<SpatialJoin>(qec, config, std::nullopt,
                                                     std::nullopt);
      std::shared_ptr<Operation> op = spatialJoinOperation->getRootOperation();
      SpatialJoin* spatialJoin = static_cast<SpatialJoin*>(op.get());
      ASSERT_TRUE(spatialJoin->getMaxDist().has_value());
      ASSERT_EQ(spatialJoin->getMaxDist(), distance);
      ASSERT_FALSE(spatialJoin->getMaxResults().has_value());
    }
  };

  testMaxDistance("<max-distance-in-meters:1000>", 1000, false);

  testMaxDistance("<max-distance-in-meters:0>", 0, false);

  testMaxDistance("<max-distance-in-meters:20000000>", 20000000, false);

  testMaxDistance("<max-distance-in-meters:123456789>", 123456789, false);

  // the following distance is slightly bigger than earths circumference.
  // This distance should still be representable
  testMaxDistance("<max-distance-in-meters:45000000000>", 45000000000, false);

  // distance must be positive
  testMaxDistance("<max-distance-in-meters:-10>", -10, true);

  // some words start with an upper case
  testMaxDistance("<max-Distance-In-Meters:1000>", 1000, true);

  // wrong keyword for the spatialJoin operation
  testMaxDistance("<maxDistanceInMeters:1000>", 1000, true);

  // "M" in meters is upper case
  testMaxDistance("<max-distance-in-Meters:1000>", 1000, true);

  // two > at the end
  testMaxDistance("<maxDistanceInMeters:1000>>", 1000, true);

  // distance must be given as integer
  testMaxDistance("<maxDistanceInMeters:oneThousand>", 1000, true);

  // distance must be given as integer
  testMaxDistance("<maxDistanceInMeters:1000.54>>", 1000, true);

  // missing > at the end
  testMaxDistance("<maxDistanceInMeters:1000", 1000, true);

  // prefix before correct iri
  testMaxDistance("<asdfmax-distance-in-meters:1000>", 1000, true);

  // suffix after correct iri
  testMaxDistance("<max-distance-in-metersjklö:1000>", 1000, true);

  // suffix after correct iri
  testMaxDistance("<max-distance-in-meters:qwer1000>", 1000, true);

  // suffix after number.
  // Note that the usual stoll function would return
  // 1000 instead of throwing an exception. To fix this mistake, a for loop
  // has been added to the parsing, which checks, if each character (which
  // should be converted to a number) is a digit
  testMaxDistance("<max-distance-in-meters:1000asff>", 1000, true);

  // prefix before <
  testMaxDistance("yxcv<max-distance-in-metersjklö:1000>", 1000, true);

  // suffix after >
  testMaxDistance("<max-distance-in-metersjklö:1000>dfgh", 1000, true);
}

// __________________________________________________________________________
TEST(QueryPlanner, BindAtBeginningOfQuery) {
  h::expect(
      "SELECT * WHERE {"
      " BIND (3 + 5 AS ?x) }",
      h::Bind(h::NeutralElement(), "3 + 5", Variable{"?x"}));
}

// __________________________________________________________________________
TEST(QueryPlanner, TextIndexScanForWord) {
  auto qec = ad_utility::testing::getQec(
      "<a> <p> \"this text contains some words and is part of the test\" . <a> "
      "<p> \"testEntity\" . <a> <p> \"picking the right text can be a hard "
      "test\" . <a> <p> \"sentence for multiple words tests\" . "
      "<a> <p> \"testing and picking\"",
      true, true, true, 16_B, true);
  auto wordScan = h::TextIndexScanForWord;

  h::expect("SELECT * WHERE { ?text ql:contains-word \"test*\" }",
            wordScan(Var{"?text"}, "test*"), qec);

  h::expect("SELECT * WHERE { ?text2 ql:contains-word \"test\" }",
            wordScan(Var{"?text2"}, "test"), qec);

  h::expect(
      "SELECT * WHERE { ?text2 ql:contains-word \"multiple words* test\" }",
      h::UnorderedJoins(wordScan(Var{"?text2"}, "test"),
                        wordScan(Var{"?text2"}, "words*"),
                        wordScan(Var{"?text2"}, "multiple")),
      qec);

  AD_EXPECT_THROW_WITH_MESSAGE(
      SparqlParser::parseQuery(
          "SELECT * WHERE { ?text ql:contains-word <test> . }"),
      ::testing::ContainsRegex(
          "ql:contains-word has to be followed by a string in quotes"));
}

// __________________________________________________________________________
TEST(QueryPlanner, TextIndexScanForEntity) {
  auto qec = ad_utility::testing::getQec(
      "<a> <p> \"this text contains some words and is part of the test\" . <a> "
      "<p> <testEntity> . <a> <p> \"picking the right text can be a hard "
      "test\" . <a> <p> \"only this text contains the word opti \" . "
      "<a> <p> \"testing and picking\"",
      true, true, true, 16_B, true);

  auto wordScan = h::TextIndexScanForWord;
  auto entityScan = h::TextIndexScanForEntity;
  h::expect(
      "SELECT * WHERE { ?text ql:contains-entity ?scientist . ?text "
      "ql:contains-word \"test*\" }",
      h::Join(wordScan(Var{"?text"}, "test*"),
              entityScan(Var{"?text"}, Var{"?scientist"}, "test*")),
      qec);

  h::expect(
      "SELECT * WHERE { ?text ql:contains-entity <testEntity> . ?text "
      "ql:contains-word \"test\" }",
      h::Join(wordScan(Var{"?text"}, "test"),
              entityScan(Var{"?text"}, "<testEntity>", "test")),
      qec);

  // Test case sensitivity
  h::expect(
      "SELECT * WHERE { ?text ql:contains-entity <testEntity> . ?text "
      "ql:contains-word \"TeST\" }",
      h::Join(wordScan(Var{"?text"}, "test"),
              entityScan(Var{"?text"}, "<testEntity>", "test")),
      qec);

  // NOTE: It is important that the TextIndexScanForEntity uses "opti", because
  // we also want to test here if the QueryPlanner assigns the optimal word to
  // the Operation.
  h::expect(
      "SELECT * WHERE { ?text ql:contains-word \"picking*\" . ?text "
      "ql:contains-entity <testEntity> . ?text ql:contains-word "
      "\"opti\" . ?text ql:contains-word \"testi*\"}",
      h::UnorderedJoins(entityScan(Var{"?text"}, "<testEntity>", "opti"),
                        wordScan(Var{"?text"}, "testi*"),
                        wordScan(Var{"?text"}, "opti"),
                        wordScan(Var{"?text"}, "picking*")),
      qec);

  ParsedQuery pq = SparqlParser::parseQuery(
      "SELECT * WHERE { ?text ql:contains-entity ?scientist . }");
  QueryPlanner qp = makeQueryPlanner();
  AD_EXPECT_THROW_WITH_MESSAGE(
      qp.createExecutionTree(pq),
      ::testing::ContainsRegex(
          "Missing ql:contains-word statement. A ql:contains-entity statement "
          "always also needs corresponding ql:contains-word statement."));
}

TEST(QueryPlanner, TextLimit) {
  auto qec = ad_utility::testing::getQec(
      "<a> <p> \"this text contains some words and is part of the test\" . <a> "
      "<p> <testEntity> . <a> <p> \"picking the right text can be a hard "
      "test\" . <a> <p> \"only this text contains the word opti \" . "
      "<a> <p> \"testing and picking\"",
      true, true, true, 16_B, true);

  auto wordScan = h::TextIndexScanForWord;
  auto entityScan = h::TextIndexScanForEntity;

  // Only contains word
  h::expect("SELECT * WHERE { ?text ql:contains-word \"test*\" } TEXTLIMIT 10",
            wordScan(Var{"?text"}, "test*"), qec);

  // Contains fixed entity
  h::expect(
      "SELECT * WHERE { ?text ql:contains-word \"test*\" . ?text "
      "ql:contains-entity <testEntity> } TEXTLIMIT 10",
      h::TextLimit(10,
                   h::Join(wordScan(Var{"?text"}, "test*"),
                           entityScan(Var{"?text"}, "<testEntity>", "test*")),
                   Var{"?text"}, vector<Variable>{},
                   vector<Variable>{
                       Var{"?text"}.getEntityScoreVariable("<testEntity>")}),
      qec);

  // Contains entity
  h::expect(
      "SELECT * WHERE { ?text ql:contains-entity ?scientist . ?text "
      "ql:contains-word \"test*\" } TEXTLIMIT 10",
      h::TextLimit(
          10,
          h::Join(wordScan(Var{"?text"}, "test*"),
                  entityScan(Var{"?text"}, Var{"?scientist"}, "test*")),
          Var{"?text"}, vector<Variable>{Var{"?scientist"}},
          vector<Variable>{
              Var{"?text"}.getEntityScoreVariable(Var{"?scientist"})}),
      qec);

  // Contains entity and fixed entity
  h::expect(
      "SELECT * WHERE { ?text ql:contains-entity ?scientist . ?text "
      "ql:contains-word \"test*\" . ?text ql:contains-entity <testEntity>} "
      "TEXTLIMIT 5",
      h::TextLimit(5,
                   h::UnorderedJoins(
                       wordScan(Var{"?text"}, "test*"),
                       entityScan(Var{"?text"}, Var{"?scientist"}, "test*"),
                       entityScan(Var{"?text"}, "<testEntity>", "test*")),
                   Var{"?text"}, vector<Variable>{Var{"?scientist"}},
                   vector<Variable>{
                       Var{"?text"}.getEntityScoreVariable(Var{"?scientist"}),
                       Var{"?text"}.getEntityScoreVariable("<testEntity>")}),
      qec);

  // Contains two entities
  h::expect(
      "SELECT * WHERE { ?text ql:contains-entity ?scientist . ?text "
      "ql:contains-word \"test*\" . ?text ql:contains-entity ?scientist2} "
      "TEXTLIMIT 5",
      h::TextLimit(
          5,
          h::UnorderedJoins(
              wordScan(Var{"?text"}, "test*"),
              entityScan(Var{"?text"}, Var{"?scientist"}, "test*"),
              entityScan(Var{"?text"}, Var{"?scientist2"}, "test*")),
          Var{"?text"}, vector<Variable>{Var{"?scientist"}, Var{"?scientist2"}},
          vector<Variable>{
              Var{"?text"}.getEntityScoreVariable(Var{"?scientist"}),
              Var{"?text"}.getEntityScoreVariable(Var{"?scientist2"})}),
      qec);

  // Contains two text variables. Also checks if the textlimit at an efficient
  // place in the query
  h::expect(
      "SELECT * WHERE { ?text1 ql:contains-entity ?scientist1 . ?text1 "
      "ql:contains-word \"test*\" . ?text2 ql:contains-word \"test*\" . ?text2 "
      "ql:contains-entity ?author1 . ?text2 ql:contains-entity ?author2 } "
      "TEXTLIMIT 5",
      h::CartesianProductJoin(
          h::TextLimit(
              5,
              h::Join(wordScan(Var{"?text1"}, "test*"),
                      entityScan(Var{"?text1"}, Var{"?scientist1"}, "test*")),
              Var{"?text1"}, vector<Variable>{Var{"?scientist1"}},
              vector<Variable>{
                  Var{"?text1"}.getEntityScoreVariable(Var{"?scientist1"})}),
          h::TextLimit(
              5,
              h::UnorderedJoins(
                  wordScan(Var{"?text2"}, "test*"),
                  entityScan(Var{"?text2"}, Var{"?author1"}, "test*"),
                  entityScan(Var{"?text2"}, Var{"?author2"}, "test*")),
              Var{"?text2"}, vector<Variable>{Var{"?author1"}, Var{"?author2"}},
              vector<Variable>{
                  Var{"?text2"}.getEntityScoreVariable(Var{"?author1"}),
                  Var{"?text2"}.getEntityScoreVariable(Var{"?author2"})})),
      qec);
}

TEST(QueryPlanner, NonDistinctVariablesInTriple) {
  auto eq = [](std::string_view l, std::string_view r) {
    return absl::StrCat(l, "=", r);
  };

  h::expect("SELECT * WHERE {?s ?p ?s}",
            h::Filter(eq(internalVar(0), "?s"),
                      h::IndexScanFromStrings(internalVar(0), "?p", "?s")));
  h::expect("SELECT * WHERE {?s ?s ?o}",
            h::Filter(eq(internalVar(0), "?s"),
                      h::IndexScanFromStrings(internalVar(0), "?s", "?o")));
  h::expect("SELECT * WHERE {?s ?p ?p}",
            h::Filter(eq(internalVar(0), "?p"),
                      h::IndexScanFromStrings("?s", "?p", internalVar(0))));
  h::expect("SELECT * WHERE {?s ?s ?s}",
            h::Filter(eq(internalVar(1), "?s"),
                      h::Filter(eq(internalVar(0), "?s"),
                                h::IndexScanFromStrings(internalVar(1), "?s",
                                                        internalVar(0)))));
  h::expect("SELECT * WHERE {?s <is-a> ?s}",
            h::Filter(eq(internalVar(0), "?s"),
                      h::IndexScanFromStrings("?s", "<is-a>", internalVar(0))));
  h::expect("SELECT * WHERE {<s> ?p ?p}",
            h::Filter(eq(internalVar(0), "?p"),
                      h::IndexScanFromStrings("<s>", "?p", internalVar(0))));
  h::expect("SELECT * WHERE {?s ?s <o>}",
            h::Filter(eq(internalVar(0), "?s"),
                      h::IndexScanFromStrings(internalVar(0), "?s", "<o>")));
}

TEST(QueryPlanner, emptyGroupGraphPattern) {
  h::expect("SELECT * WHERE {}", h::NeutralElement());
  h::expect("SELECT * WHERE { {} }", h::NeutralElement());
  h::expect("SELECT * WHERE { {} {} }",
            h::CartesianProductJoin(h::NeutralElement(), h::NeutralElement()));
  h::expect("SELECT * WHERE { {} UNION {} }",
            h::Union(h::NeutralElement(), h::NeutralElement()));
  h::expect("SELECT * WHERE { {} { SELECT * WHERE {}}}",
            h::CartesianProductJoin(h::NeutralElement(), h::NeutralElement()));
}

// __________________________________________________________________________
TEST(QueryPlanner, TooManyTriples) {
  std::string query = "SELECT * WHERE {";
  for (size_t i = 0; i < 65; i++) {
    query = absl::StrCat(query, " ?x <p> ?y .");
  }
  query = absl::StrCat(query, "}");
  ParsedQuery pq = SparqlParser::parseQuery(query);
  QueryPlanner qp = makeQueryPlanner();
  AD_EXPECT_THROW_WITH_MESSAGE(
      qp.createExecutionTree(pq),
      ::testing::ContainsRegex("At most 64 triples allowed at the moment."));
}

// ___________________________________________________________________________
TEST(QueryPlanner, CountAvailablePredicates) {
  h::expect(
      "SELECT ?p (COUNT(DISTINCT ?s) as ?cnt) WHERE { ?s ?p ?o} GROUP BY ?p",
      h::CountAvailablePredicates(
          0, Var{"?p"}, Var{"?cnt"},
          h::IndexScanFromStrings("?s", HAS_PATTERN_PREDICATE, "?p")));
  h::expect(
      "SELECT ?p (COUNT(DISTINCT ?s) as ?cnt) WHERE { ?s ql:has-predicate "
      "?p} "
      "GROUP BY ?p",
      h::CountAvailablePredicates(
          0, Var{"?p"}, Var{"?cnt"},
          h::IndexScanFromStrings("?s", HAS_PATTERN_PREDICATE, "?p")));
  // TODO<joka921> Add a test for the case with subtrees with and without
  // rewriting of triples.
}

// Check that a MINUS operation that only refers to unbound variables is deleted
// by the query planner.
TEST(QueryPlanner, UnboundMinusIgnored) {
  h::expect("SELECT * WHERE {MINUS{?x <is-a> ?y}}", h::NeutralElement());
  h::expect("SELECT * WHERE { ?a <is-a> ?b MINUS{?x <is-a> ?y}}",
            h::IndexScanFromStrings("?a", "<is-a>", "?b"));
}

// ___________________________________________________________________________
TEST(QueryPlanner, SimpleMinus) {
  h::expect("SELECT * WHERE { ?a <is-a> ?b MINUS{?a <is-a> ?b}}",
            h::Minus(h::IndexScanFromStrings("?a", "<is-a>", "?b"),
                     h::IndexScanFromStrings("?a", "<is-a>", "?b")));
}

// ___________________________________________________________________________
TEST(QueryPlanner, CancellationCancelsQueryPlanning) {
  auto cancellationHandle =
      std::make_shared<ad_utility::CancellationHandle<>>();

  QueryPlanner qp{ad_utility::testing::getQec(), cancellationHandle};
  auto pq = SparqlParser::parseQuery("SELECT * WHERE { ?x ?y ?z }");

  cancellationHandle->cancel(ad_utility::CancellationState::MANUAL);

  AD_EXPECT_THROW_WITH_MESSAGE_AND_TYPE(qp.createExecutionTree(pq),
                                        HasSubstr("Query planning"),
                                        ad_utility::CancellationException);
}

// ___________________________________________________________________________
TEST(QueryPlanner, DatasetClause) {
  auto scan = h::IndexScanFromStrings;
  using Graphs = ad_utility::HashSet<std::string>;
  h::expect("SELECT * FROM <x> FROM <y> WHERE { ?x ?y ?z}",
            scan("?x", "?y", "?z", {}, Graphs{"<x>", "<y>"}));

  h::expect("SELECT * FROM <x> FROM <y> { SELECT * {?x ?y ?z}}",
            scan("?x", "?y", "?z", {}, Graphs{"<x>", "<y>"}));

  h::expect("SELECT * FROM <x> WHERE { GRAPH <z> {?x ?y ?z}}",
            scan("?x", "?y", "?z", {}, Graphs{"<z>"}));

  auto g1 = Graphs{"<g1>"};
  auto g2 = Graphs{"<g2>"};
  h::expect(
      "SELECT * FROM <g1> { <a> ?p <x>. {<b> ?p <y>} GRAPH <g2> { <c> ?p <z> "
      "{SELECT * {<d> ?p <z2>}}} <e> ?p <z3> }",
      h::UnorderedJoins(
          scan("<a>", "?p", "<x>", {}, g1), scan("<b>", "?p", "<y>", {}, g1),
          scan("<c>", "?p", "<z>", {}, g2), scan("<d>", "?p", "<z2>", {}, g2),
          scan("<e>", "?p", "<z3>", {}, g1)));

  auto g12 = Graphs{"<g1>", "<g2>"};
  auto varG = std::vector{Variable{"?g"}};
  std::vector<ColumnIndex> graphCol{ADDITIONAL_COLUMN_GRAPH_ID};
  h::expect(
      "SELECT * FROM <x> FROM NAMED <g1> FROM NAMED <g2> WHERE { GRAPH ?g {<a> "
      "<b> <c>}}",
      scan("<a>", "<b>", "<c>", {}, g12, varG, graphCol));

  h::expect("SELECT * FROM <x> WHERE { GRAPH ?g {<a> <b> <c>}}",
            scan("<a>", "<b>", "<c>", {}, std::nullopt, varG, graphCol));

  // `GROUP BY` inside a `GRAPH ?g` clause.
  // We use the `UnorderedJoins` matcher, because the index scan has to be
  // resorted by the graph column.
  h::expect(
      "SELECT * FROM <g1> FROM NAMED <g2> { GRAPH ?g "
      "{ "
      "{SELECT ?p {<d> ?p <z2>} GROUP BY ?p}"
      "} }",
      h::GroupBy({Variable{"?p"}, Variable{"?g"}}, {},
                 h::UnorderedJoins(
                     scan("<d>", "?p", "<z2>", {}, g2, varG, graphCol))));

  // A complex example with graph variables.
  h::expect(
      "SELECT * FROM <g1> FROM NAMED <g2> { <a> ?p <x>. {<b> ?p <y>} GRAPH ?g "
      "{ <c> ?p <z> "
      "{SELECT * {<d> ?p <z2>}}"
      "{SELECT ?p {<d> ?p <z2>} GROUP BY ?p}"
      "} <e> ?p <z3> }",
      h::UnorderedJoins(
          scan("<a>", "?p", "<x>", {}, g1), scan("<b>", "?p", "<y>", {}, g1),
          scan("<c>", "?p", "<z>", {}, g2, varG, graphCol),
          scan("<d>", "?p", "<z2>", {}, g2, varG, graphCol),
          h::GroupBy({Variable{"?p"}, Variable{"?g"}}, {},
                     h::UnorderedJoins(
                         scan("<d>", "?p", "<z2>", {}, g2, varG, graphCol))),
          scan("<e>", "?p", "<z3>", {}, g1)));
  // We currently don't support repeating the graph variable inside the
  // graph clause
  AD_EXPECT_THROW_WITH_MESSAGE(
      h::expect("SELECT * { GRAPH ?x {?x <b> <c>}}", ::testing::_),
      AllOf(HasSubstr("used as the graph specifier"),
            HasSubstr("may not appear in the body")));
}

// _____________________________________________________________________________
TEST(QueryPlanner, WarningsOnUnboundVariables) {
  using enum ::OrderBy::AscOrDesc;
  // Unbound variable in ORDER BY.
  h::expect(
      "SELECT * {} ORDER BY ?x",
      h::QetWithWarnings({"?x was used by ORDER BY"}, h::NeutralElement()));
  h::expect(
      "SELECT * { ?x <is-a> <y> } ORDER BY ?x ?y ",
      h::QetWithWarnings({"?y was used by ORDER BY"},
                         h::OrderBy({{Variable{"?x"}, Asc}}, ::testing::_)));

  // Unbound variable in GROUP BY.
  h::expect("SELECT ?x {} GROUP BY ?x",
            h::QetWithWarnings({"?x was used by GROUP BY"},
                               h::GroupBy({}, {}, h::NeutralElement())));
  h::expect("SELECT ?x ?y { ?x <is-a> <y> } GROUP BY ?x ?y ",
            h::QetWithWarnings(
                {"?y was used by GROUP BY"},
                h::GroupBy({Variable{"?x"}}, {},
                           h::IndexScanFromStrings("?x", "<is-a>", "<y>"))));

  // Unbound variable in BIND.
  h::expect(
      "SELECT ?x {BIND (?a as ?x)}",
      h::QetWithWarnings({"?a was used in the expression of a BIND"},
                         h::Bind(h::NeutralElement(), "?a", Variable{"?x"})));

  // Unbound variable in Subquery.
  h::expect("SELECT ?x { {SELECT * {BIND (?a as ?x)}} ?x <p> ?o}",
            h::QetWithWarnings({"?a was used in the expression of a BIND"},
                               testing::_));
}

// ___________________________________________________________________________
TEST(QueryPlanner, Describe) {
  // Note: We deliberately don't test the contents of the actual DESCRIBE
  // clause, because they have been extensively tested already in
  // `SparqlAntlrParserTest.cpp` where we have access to proper matchers for
  // them.
  h::expect("DESCRIBE <x>", h::Describe(::testing::_, h::NeutralElement()));
  h::expect("DESCRIBE ?x", h::Describe(::testing::_, h::NeutralElement()));
  h::expect(
      "Describe ?y { ?y <p> <o>}",
      h::Describe(::testing::_, h::IndexScanFromStrings("?y", "<p>", "<o>")));
  h::expect(
      "Describe ?y FROM <g> { ?y <p> <o>}",
      h::Describe(::testing::_, h::IndexScanFromStrings(
                                    "?y", "<p>", "<o>", {},
                                    ad_utility::HashSet<std::string>{"<g>"})));
}

// ____________________________________________________________________________
TEST(QueryPlanner, GroupByRedundantParensAndVariables) {
  auto matcher = h::GroupBy({Variable{"?x"}}, {},
                            h::IndexScanFromStrings("?x", "?y", "?z"));
  h::expect("SELECT ?x { ?x ?y ?z} GROUP BY (?x)", matcher);
  h::expect("SELECT ?x { ?x ?y ?z} GROUP BY ?x ?x", matcher);
  h::expect("SELECT ?x { ?x ?y ?z} GROUP BY ?x ?x (?x)", matcher);
}

// ____________________________________________________________________________
TEST(QueryPlanner, Exists) {
  auto xyz = h::IndexScanFromStrings("?x", "?y", "?z");
  auto abc = h::IndexScanFromStrings("?a", "?b", "?c");
  auto def = h::IndexScanFromStrings("?d", "?e", "?f");
  auto ghi = h::IndexScanFromStrings("?g", "?h", "?i");
  using V = Variable;

  // Simple tests for EXISTS with FILTER, BIND, and GROUP BY.
  h::expect("SELECT * { ?x ?y ?z FILTER EXISTS {?a ?b ?c} }",
            h::Filter("EXISTS {?a ?b ?c}", h::ExistsJoin(xyz, abc)));
  h::expect("SELECT * { ?x ?y ?z BIND(EXISTS {?a ?b ?c} as ?bound) }",
            h::Bind(h::ExistsJoin(xyz, abc), "EXISTS {?a ?b ?c}",
                    Variable("?bound")));
  h::expect(
      "SELECT ?x (SAMPLE(EXISTS{?a ?b ?c}) as ?s) { ?x ?y ?z } GROUP BY ?x",
      h::GroupBy({V{"?x"}}, {"(SAMPLE(EXISTS{?a ?b ?c}) as ?s)"},
                 h::ExistsJoin(xyz, abc)));

  // Similar tests, but with multiple EXISTS clauses
  auto existsAbcDef = h::ExistsJoin(h::ExistsJoin(xyz, abc), def);
  h::expect(
      "SELECT * { ?x ?y ?z FILTER (EXISTS {?a ?b ?c} || EXISTS {?d ?e ?f})}",
      h::Filter("EXISTS {?a ?b ?c} || EXISTS {?d ?e ?f}", existsAbcDef));
  ;
  h::expect(
      "SELECT * { ?x ?y ?z BIND(EXISTS {?a ?b ?c} || EXISTS {?d ?e ?f} as "
      "?bound)}",
      h::Bind(existsAbcDef, "EXISTS {?a ?b ?c} || EXISTS {?d ?e ?f}",
              Variable("?bound")));

  h::expect(
      "SELECT ?x (SAMPLE(EXISTS {?a ?b ?c} || EXISTS {?d ?e ?f}) as ?s) "
      "(SAMPLE(EXISTS{?g ?h ?i}) as ?t) { ?x ?y ?z } GROUP BY ?x",
      h::GroupBy({V{"?x"}},
                 {"(SAMPLE(EXISTS {?a ?b ?c} || EXISTS {?d ?e ?f}) as ?s)",
                  "(SAMPLE(EXISTS{?g ?h ?i}) as ?t)"},
                 h::ExistsJoin(existsAbcDef, ghi)));

  // Test the interaction of FROM with EXISTS.
  using H = ad_utility::HashSet<std::string>;
  auto xyzg = h::IndexScanFromStrings("?x", "?y", "?z", {}, H{"<g>"});
  auto abcg = h::IndexScanFromStrings("?a", "?b", "?c", {}, H{"<g>"});

  // Various uses of FILTER EXISTS.
  auto existsJoin = h::ExistsJoin(xyzg, abcg);
  auto filter = h::Filter("EXISTS {?a ?b ?c}", existsJoin);
  h::expect("SELECT * FROM <g> { ?x ?y ?z FILTER EXISTS {?a ?b ?c}}", filter);
  h::expect("ASK FROM <g> { ?x ?y ?z FILTER EXISTS {?a ?b ?c}}", filter);
  h::expect(
      "CONSTRUCT {<a> <b> <c>} FROM <g> { ?x ?y ?z FILTER EXISTS {?a ?b ?c}}",
      filter);
  h::expect("Describe ?x FROM <g> { ?x ?y ?z FILTER EXISTS {?a ?b ?c}}",
            h::Describe(::testing::_, filter));

  // Test the interaction of FROM NAMES with EXISTS
  auto varG = std::vector{Variable{"?g"}};
  std::vector<ColumnIndex> graphCol{ADDITIONAL_COLUMN_GRAPH_ID};
  auto uvcg =
      h::IndexScanFromStrings("?u", "?v", "?c", {}, H{"<g2>"}, varG, graphCol);
  existsJoin = h::ExistsJoin(xyzg, h::UnorderedJoins(abcg, uvcg));
  filter = h::Filter("EXISTS {?a ?b ?c. GRAPH ?g { ?u ?v ?c}}", existsJoin);
  h::expect(
      "SELECT * FROM <g> FROM NAMED <g2> { ?x ?y ?z FILTER EXISTS {?a ?b ?c. "
      "GRAPH ?g { ?u ?v ?c}}}",
      filter);
}

// _____________________________________________________________________________
TEST(QueryPlanner, ensureGeneratedInternalVariablesDontClash) {
  h::expect("SELECT * { SELECT ?s { ?s <a> [] } ORDER BY RAND() }",
            h::OrderBy({std::pair{Var{"?_QLever_internal_variable_1"},
                                  OrderBy::AscOrDesc::Asc}},
                       h::Bind(h::IndexScanFromStrings(
                                   "?s", "<a>", "?_QLever_internal_variable_0"),
                               "RAND()", Var{"?_QLever_internal_variable_1"})));
}

// _____________________________________________________________________________
TEST(QueryPlanner, FilterOnNeutralElement) {
  h::expect("SELECT * { FILTER(false) }",
            h::Filter("false", h::NeutralElement()));
  h::expect("SELECT * { FILTER(true) }",
            h::Filter("true", h::NeutralElement()));

  h::expect("SELECT * { { SELECT * WHERE { FILTER(false) } } VALUES ?x { 1 } }",
            h::CartesianProductJoin(h::Filter("false", h::NeutralElement()),
                                    h::ValuesClause("VALUES (?x) { (1) }")));
}

// _____________________________________________________________________________
TEST(QueryPlanner, ContainsWordInGraphClause) {
  {
    auto qp = makeQueryPlanner();
    auto query = SparqlParser::parseQuery(
        "SELECT * { GRAPH ?g { ?s "
        "<http://qlever.cs.uni-freiburg.de/builtin-functions/contains-word> "
        "\"Test\" } }");
    AD_EXPECT_THROW_WITH_MESSAGE_AND_TYPE(
        qp.createExecutionTree(query),
        ::testing::HasSubstr(
            "contains-word is not allowed inside GRAPH clauses "
            "or in queries with FROM/FROM NAMED clauses."),
        ad_utility::Exception);
  }
  {
    auto qp = makeQueryPlanner();
    auto query = SparqlParser::parseQuery(
        "SELECT * { GRAPH <my-iri> { ?s "
        "<http://qlever.cs.uni-freiburg.de/builtin-functions/contains-word> "
        "\"Test\" } }");
    AD_EXPECT_THROW_WITH_MESSAGE_AND_TYPE(
        qp.createExecutionTree(query),
        ::testing::HasSubstr(
            "contains-word is not allowed inside GRAPH clauses "
            "or in queries with FROM/FROM NAMED clauses."),
        ad_utility::Exception);
  }
  {
    auto qp = makeQueryPlanner();
    auto query = SparqlParser::parseQuery(
        "SELECT * FROM <my-iri> WHERE { ?s "
        "<http://qlever.cs.uni-freiburg.de/builtin-functions/contains-word> "
        "\"Test\" }");
    AD_EXPECT_THROW_WITH_MESSAGE_AND_TYPE(
        qp.createExecutionTree(query),
        ::testing::HasSubstr(
            "contains-word is not allowed inside GRAPH clauses "
            "or in queries with FROM/FROM NAMED clauses."),
        ad_utility::Exception);
  }
}

// _____________________________________________________________________________
TEST(QueryPlanner, UnconnectedComponentsInGraphClause) {
  h::expect("SELECT * WHERE { GRAPH ?g { ?s1 ?p1 ?o1 . ?s2 ?p2 ?o2 } }",
            h::Join(h::Sort(h::IndexScanFromStrings("?s1", "?p1", "?o1", {}, {},
                                                    {Variable{"?g"}}, {3})),
                    h::Sort(h::IndexScanFromStrings("?s2", "?p2", "?o2", {}, {},
                                                    {Variable{"?g"}}, {3}))));
  // Sanity check case without a GRAPH clause
  h::expect(
      "SELECT * WHERE { ?s1 ?p1 ?o1 . ?s2 ?p2 ?o2 }",
      h::CartesianProductJoin(h::IndexScanFromStrings("?s1", "?p1", "?o1"),
                              h::IndexScanFromStrings("?s2", "?p2", "?o2")));
}

// _____________________________________________________________________________
TEST(QueryPlanner, testDistributiveJoinInUnion) {
  auto* qec = ad_utility::testing::getQec();
  TransitivePathSide left1{std::nullopt, 0,
                           Variable("?_QLever_internal_variable_qp_0"), 0};
  TransitivePathSide left2{std::nullopt, 0,
                           Variable("?_QLever_internal_variable_qp_7"), 0};
  TransitivePathSide right{std::nullopt, 1, Variable("?type"), 1};
  std::string query =
      "SELECT * WHERE {\n"
      "  <Q11629> <P279>/(<P279>*|<P31>*) | <P31>/(<P279>*|<P31>*) ?type .\n"
      "}";

  h::expectWithGivenBudgets(
      std::move(query),
      h::Union(
          h::Union(
              h::TransitivePath(
                  left1, right, 0, std::numeric_limits<size_t>::max(),
                  h::IndexScanFromStrings("<Q11629>", "<P279>",
                                          "?_QLever_internal_variable_qp_0"),
                  h::IndexScanFromStrings("?_QLever_internal_variable_qp_2",
                                          "<P279>",
                                          "?_QLever_internal_variable_qp_3")),
              h::TransitivePath(
                  left1, right, 0, std::numeric_limits<size_t>::max(),
                  h::IndexScanFromStrings("<Q11629>", "<P279>",
                                          "?_QLever_internal_variable_qp_0"),
                  h::IndexScanFromStrings("?_QLever_internal_variable_qp_4",
                                          "<P31>",
                                          "?_QLever_internal_variable_qp_5"))),
          h::Union(
              h::TransitivePath(
                  left2, right, 0, std::numeric_limits<size_t>::max(),
                  h::IndexScanFromStrings("<Q11629>", "<P31>",
                                          "?_QLever_internal_variable_qp_7"),
                  h::IndexScanFromStrings("?_QLever_internal_variable_qp_9",
                                          "<P279>",
                                          "?_QLever_internal_variable_qp_10")),
              h::TransitivePath(
                  left2, right, 0, std::numeric_limits<size_t>::max(),
                  h::IndexScanFromStrings("<Q11629>", "<P31>",
                                          "?_QLever_internal_variable_qp_7"),
                  h::IndexScanFromStrings(
                      "?_QLever_internal_variable_qp_11", "<P31>",
                      "?_QLever_internal_variable_qp_12")))),
      qec, {4, 16, 64'000'000});

  TransitivePathSide left3{std::nullopt, 0, Variable("?s"), 0};
  TransitivePathSide right2{std::nullopt, 1, Variable("?y"), 1};

  h::expectWithGivenBudgets(
      "SELECT * WHERE { ?s <P31> ?o . { ?s <P279>+ ?y } UNION { VALUES ?x { 1 "
      "} }}",
      h::Union(
          h::TransitivePath(left3, right2, 1,
                            std::numeric_limits<size_t>::max(),
                            h::IndexScanFromStrings("?s", "<P31>", "?o"),
                            h::IndexScanFromStrings(
                                "?_QLever_internal_variable_qp_0", "<P279>",
                                "?_QLever_internal_variable_qp_1")),
          h::CartesianProductJoin(h::IndexScanFromStrings("?s", "<P31>", "?o"),
                                  h::ValuesClause("VALUES (?x) { (1) }"))),
      qec, {4, 16, 64'000'000});

  h::expectWithGivenBudgets(
      "SELECT * WHERE { { VALUES ?x { 1 } } UNION { ?s <P279>+ ?y } . "
      "?s <P31> ?o }",
      h::Union(
          h::CartesianProductJoin(h::ValuesClause("VALUES (?x) { (1) }"),
                                  h::IndexScanFromStrings("?s", "<P31>", "?o")),
          h::TransitivePath(std::move(left3), std::move(right2), 1,
                            std::numeric_limits<size_t>::max(),
                            h::IndexScanFromStrings("?s", "<P31>", "?o"),
                            h::IndexScanFromStrings(
                                "?_QLever_internal_variable_qp_0", "<P279>",
                                "?_QLever_internal_variable_qp_1"))),
      qec, {4, 16, 64'000'000});
}

// _____________________________________________________________________________
TEST(QueryPlanner, ensurePlanningIsSkippedWhenNoTransitivePathIsPresent) {
  auto qp = makeQueryPlanner();
  {
    auto query = SparqlParser::parseQuery(
        "SELECT * WHERE { ?x <P31> ?o ."
        "{ VALUES ?x { 1 } } UNION { VALUES ?x { 1 } }}");
    auto plans = qp.createExecutionTrees(query);
    ASSERT_EQ(plans.size(), 1);
    EXPECT_TRUE(
        std::dynamic_pointer_cast<Join>(plans.at(0)._qet->getRootOperation()));
  }
  {
    auto query = SparqlParser::parseQuery(
        "SELECT * WHERE { ?x <P31> ?o . "
        "{ { VALUES ?x { 1 } } UNION { VALUES ?x { 1 } } } "
        "UNION "
        "{ { VALUES ?x { 1 } } UNION { VALUES ?x { 1 } } } }");
    auto plans = qp.createExecutionTrees(query);
    ASSERT_EQ(plans.size(), 1);
    EXPECT_TRUE(
        std::dynamic_pointer_cast<Join>(plans.at(0)._qet->getRootOperation()));
  }
}

// _____________________________________________________________________________
TEST(QueryPlanner, ensurePlanningIsSkippedWhenTransitivePathIsAlreadyBound) {
  auto qp = makeQueryPlanner();
  auto query = SparqlParser::parseQuery(
      "SELECT * { { VALUES ?x { 1 } } UNION { ?s <P279>+ 1 } . ?s <P31> ?o }");
  auto plans = qp.createExecutionTrees(query);
  ASSERT_EQ(plans.size(), 1);
  EXPECT_TRUE(
      std::dynamic_pointer_cast<Join>(plans.at(0)._qet->getRootOperation()));
}

// _____________________________________________________________________________
TEST(QueryPlanner, testDistributiveJoinInUnionRecursive) {
  auto* qec = ad_utility::testing::getQec(
      "<a> <P279> <b> . <c> <P279> <d> . <e> <P279> <f> . <g> <P279> <h> ."
      " <i> <P279> <j> . <a> <P31> <b> . <c> <P31> <d> . <e> <P31> <f> ."
      " <g> <P31> <h> . <i> <P31> <j> .");
  TransitivePathSide left1{std::nullopt, 2,
                           Variable("?_QLever_internal_variable_qp_0"), 0};
  TransitivePathSide left2{std::nullopt, 0,
                           Variable("?_QLever_internal_variable_qp_4"), 0};
  TransitivePathSide left3{std::nullopt, 0,
                           Variable("?_QLever_internal_variable_qp_13"), 0};
  TransitivePathSide right1{std::nullopt, 1, Variable("?type"), 1};
  TransitivePathSide right2{std::nullopt, 1,
                            Variable("?_QLever_internal_variable_qp_3"), 1};
  TransitivePathSide right3{std::nullopt, 1,
                            Variable("?_QLever_internal_variable_qp_12"), 1};
  std::string query =
      "SELECT * WHERE {\n"
      "  <Q11629> "
      "  <P279>/((<P279>/(<P279>*|<P31>*))*|(<P31>/(<P279>*|<P31>*))*)"
      "  ?type .\n"
      "}";

  h::expectWithGivenBudgets(
      std::move(query),
      h::Union(
          h::TransitivePath(
              left1, right1, 0, std::numeric_limits<size_t>::max(),
              h::IndexScanFromStrings("<Q11629>", "<P279>",
                                      "?_QLever_internal_variable_qp_0"),
              h::Union(h::Sort(h::TransitivePath(
                           left2, right2, 0, std::numeric_limits<size_t>::max(),
                           h::IndexScanFromStrings(
                               "?_QLever_internal_variable_qp_2", "<P279>",
                               "?_QLever_internal_variable_qp_4"),
                           h::IndexScanFromStrings(
                               "?_QLever_internal_variable_qp_6", "<P279>",
                               "?_QLever_internal_variable_qp_7"))),
                       h::Sort(h::TransitivePath(
                           left2, right2, 0, std::numeric_limits<size_t>::max(),
                           h::IndexScanFromStrings(
                               "?_QLever_internal_variable_qp_2", "<P279>",
                               "?_QLever_internal_variable_qp_4"),
                           h::IndexScanFromStrings(
                               "?_QLever_internal_variable_qp_8", "<P31>",
                               "?_QLever_internal_variable_qp_9"))))),
          h::TransitivePath(
              left1, right1, 0, std::numeric_limits<size_t>::max(),
              h::IndexScanFromStrings("<Q11629>", "<P279>",
                                      "?_QLever_internal_variable_qp_0"),
              h::Union(h::Sort(h::TransitivePath(
                           left3, right3, 0, std::numeric_limits<size_t>::max(),
                           h::IndexScanFromStrings(
                               "?_QLever_internal_variable_qp_11", "<P31>",
                               "?_QLever_internal_variable_qp_13"),
                           h::IndexScanFromStrings(
                               "?_QLever_internal_variable_qp_15", "<P279>",
                               "?_QLever_internal_variable_qp_16"))),
                       h::Sort(h::TransitivePath(
                           left3, right3, 0, std::numeric_limits<size_t>::max(),
                           h::IndexScanFromStrings(
                               "?_QLever_internal_variable_qp_11", "<P31>",
                               "?_QLever_internal_variable_qp_13"),
                           h::IndexScanFromStrings(
                               "?_QLever_internal_variable_qp_17", "<P31>",
                               "?_QLever_internal_variable_qp_18")))))),
      qec, {4, 16, 64'000'000});
}
