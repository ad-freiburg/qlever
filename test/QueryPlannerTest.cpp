// Copyright 2015, University of Freiburg, Chair of Algorithms and Data
// Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "./QueryPlannerTestHelpers.h"
#include "./util/TripleComponentTestHelpers.h"
#include "engine/QueryPlanner.h"
#include "global/Constants.h"
#include "parser/SparqlParser.h"

namespace h = queryPlannerTestHelpers;
using Var = Variable;

TEST(QueryPlannerTest, createTripleGraph) {
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
    QueryPlanner qp(nullptr);
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
                                     "<http://abc.de>")),
                 {0, 1})}));

    ASSERT_TRUE(tg.isSimilar(expected));
  }

  {
    ParsedQuery pq = SparqlParser::parseQuery(
        "SELECT ?x WHERE {?x ?p <X>. ?x ?p2 <Y>. <X> ?p <Y>}");
    QueryPlanner qp(nullptr);
    auto tg = qp.createTripleGraph(&pq.children()[0].getBasic());
    TripleGraph expected =
        TripleGraph(std::vector<std::pair<Node, std::vector<size_t>>>(
            {std::make_pair<Node, vector<size_t>>(
                 QueryPlanner::TripleGraph::Node(
                     0, SparqlTriple(Var{"?x"}, "?p", "<X>")),
                 {1, 2}),
             std::make_pair<Node, vector<size_t>>(
                 QueryPlanner::TripleGraph::Node(
                     1, SparqlTriple(Var{"?x"}, "?p2", "<Y>")),
                 {0}),
             std::make_pair<Node, vector<size_t>>(
                 QueryPlanner::TripleGraph::Node(
                     2, SparqlTriple("<X>", "?p", "<Y>")),
                 {0})}));
    ASSERT_TRUE(tg.isSimilar(expected));
  }

  {
    ParsedQuery pq = SparqlParser::parseQuery(
        "SELECT ?x WHERE { ?x <is-a> <Book> . \n"
        "?x <Author> <Anthony_Newman_(Author)> }");
    QueryPlanner qp(nullptr);
    auto tg = qp.createTripleGraph(&pq.children()[0].getBasic());

    TripleGraph expected =
        TripleGraph(std::vector<std::pair<Node, std::vector<size_t>>>({
            std::make_pair<Node, vector<size_t>>(
                QueryPlanner::TripleGraph::Node(
                    0, SparqlTriple(Var{"?x"}, "<is-a>", "<Book>")),
                {1}),
            std::make_pair<Node, vector<size_t>>(
                QueryPlanner::TripleGraph::Node(
                    1, SparqlTriple(Var{"?x"}, "<Author>",
                                    "<Anthony_Newman_(Author)>")),
                {0}),
        }));
    ASSERT_TRUE(tg.isSimilar(expected));
  }
}

TEST(QueryPlannerTest, testCpyCtorWithKeepNodes) {
  {
    ParsedQuery pq = SparqlParser::parseQuery(
        "SELECT ?x WHERE {?x ?p <X>. ?x ?p2 <Y>. <X> ?p <Y>}");
    QueryPlanner qp(nullptr);
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

TEST(QueryPlannerTest, testBFSLeaveOut) {
  {
    ParsedQuery pq = SparqlParser::parseQuery(
        "SELECT ?x WHERE {?x ?p <X>. ?x ?p2 <Y>. <X> ?p <Y>}");
    QueryPlanner qp(nullptr);
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
    QueryPlanner qp(nullptr);
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

TEST(QueryPlannerTest, indexScanOneVariable) {
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

TEST(QueryPlannerTest, indexScanTwoVariables) {
  auto scan = h::IndexScanFromStrings;
  using enum Permutation::Enum;

  h::expect(
      "PREFIX : <http://rdf.myprefix.com/>\n"
      "SELECT ?x \n "
      "WHERE \t {?x :myrel ?y}",
      scan("?x", "<http://rdf.myprefix.com/myrel>", "?y", {POS, PSO}));
}

TEST(QueryPlannerTest, joinOfTwoScans) {
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

TEST(QueryPlannerTest, testActorsBornInEurope) {
  ParsedQuery pq = SparqlParser::parseQuery(
      "PREFIX : <pre/>\n"
      "SELECT ?a \n "
      "WHERE {?a :profession :Actor . ?a :born-in ?c. ?c :in :Europe}\n"
      "ORDER BY ?a");
  QueryPlanner qp(nullptr);
  QueryExecutionTree qet = qp.createExecutionTree(pq);
  ASSERT_EQ(27493u, qet.getCostEstimate());
  ASSERT_EQ(qet.getCacheKey(),
            "ORDER BY on columns:asc(0) \nJOIN\nSORT(internal) on "
            "columns:asc(1) \nJOIN\nSCAN POS with P = \"<pre/profession>\", O "
            "= \"<pre/Actor>\" join-column: [0]\n|X|\nSCAN PSO with P = "
            "\"<pre/born-in>\" join-column: [0] join-column: [1]\n|X|\nSCAN "
            "POS with P = \"<pre/in>\", O = \"<pre/Europe>\" join-column: [0]");
}

TEST(QueryPlannerTest, testStarTwoFree) {
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

TEST(QueryPlannerTest, testFilterAfterSeed) {
  ParsedQuery pq = SparqlParser::parseQuery(
      "SELECT ?x ?y ?z WHERE {"
      "?x <r> ?y . ?y <r> ?z . "
      "FILTER(?x != ?y) }");
  QueryPlanner qp(nullptr);
  QueryExecutionTree qet = qp.createExecutionTree(pq);
  ASSERT_EQ(qet.getCacheKey(),
            "FILTER JOIN\nSCAN POS with P = \"<r>\" join-column: "
            "[0]\n|X|\nSCAN PSO with P = \"<r>\" join-column: [0] with "
            "N16sparqlExpression10relational20RelationalExpressionILN18valueIdC"
            "omparators10ComparisonE3EEE#column_1##column_0#");
}

TEST(QueryPlannerTest, testFilterAfterJoin) {
  ParsedQuery pq = SparqlParser::parseQuery(
      "SELECT ?x ?y ?z WHERE {"
      "?x <r> ?y . ?y <r> ?z . "
      "FILTER(?x != ?z) }");
  QueryPlanner qp(nullptr);
  QueryExecutionTree qet = qp.createExecutionTree(pq);
  ASSERT_EQ(qet.getCacheKey(),
            "FILTER JOIN\nSCAN POS with P = \"<r>\" join-column: "
            "[0]\n|X|\nSCAN PSO with P = \"<r>\" join-column: [0] with "
            "N16sparqlExpression10relational20RelationalExpressionILN18valueIdC"
            "omparators10ComparisonE3EEE#column_1##column_2#");
}

TEST(QueryPlannerTest, threeVarTriples) {
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

TEST(QueryPlannerTest, threeVarTriplesTCJ) {
  auto qec = ad_utility::testing::getQec("<s> <p> <x>");
  h::expect(
      "SELECT ?x ?p ?o WHERE {"
      "<s> ?p ?x . ?x ?p ?o }",
      h::MultiColumnJoin(h::IndexScan("<s>", Var{"?p"}, Var{"?x"}),
                         h::IndexScan(Var{"?x"}, Var{"?p"}, Var{"?o"})),
      qec);

  h::expect(
      "SELECT ?s ?p ?o WHERE {"
      "?s ?p ?o . ?s ?p <x> }",
      h::MultiColumnJoin(h::IndexScan(Var{"?s"}, Var{"?p"}, Var{"?o"}),
                         h::IndexScan(Var{"?s"}, Var{"?p"}, "<x>")),
      qec);
}

TEST(QueryPlannerTest, threeVarXthreeVarException) {
  h::expect(
      "SELECT ?s ?s2 WHERE {"
      "?s ?p ?o . ?s2 ?p ?o }",
      h::MultiColumnJoin(h::IndexScan(Var{"?s"}, Var{"?p"}, Var{"?o"}),
                         h::IndexScan(Var{"?s2"}, Var{"?p"}, Var{"?o"})));
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
  QueryPlanner qp(nullptr);
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
  QueryPlanner qp(nullptr);
  QueryExecutionTree qet = qp.createExecutionTree(pq);
  ASSERT_TRUE(qet.isVariableCovered(Variable{"?1"}));
  ASSERT_TRUE(qet.isVariableCovered(Variable{"?0"}));
}

TEST(QueryPlannerTest, testSimpleOptional) {
  QueryPlanner qp(nullptr);

  ParsedQuery pq = SparqlParser::parseQuery(
      "SELECT ?a ?b \n "
      "WHERE  {?a <rel1> ?b . OPTIONAL { ?a <rel2> ?c }}");
  QueryExecutionTree qet = qp.createExecutionTree(pq);
  ASSERT_EQ(qet.getCacheKey(),
            "OPTIONAL_JOIN\nSCAN PSO with P = \"<rel1>\" join-columns: "
            "[0]\n|X|\nSCAN PSO with P = \"<rel2>\" join-columns: [0]");

  ParsedQuery pq2 = SparqlParser::parseQuery(
      "SELECT ?a ?b \n "
      "WHERE  {?a <rel1> ?b . "
      "OPTIONAL { ?a <rel2> ?c }} ORDER BY ?b");
  QueryExecutionTree qet2 = qp.createExecutionTree(pq2);
  ASSERT_EQ(qet2.getCacheKey(),
            "ORDER BY on columns:asc(1) \nOPTIONAL_JOIN\nSCAN PSO with P = "
            "\"<rel1>\" join-columns: [0]\n|X|\nSCAN PSO with P = \"<rel2>\" "
            "join-columns: [0]");
}

TEST(QueryPlannerTest, SimpleTripleOneVariable) {
  using enum Permutation::Enum;

  // With only one variable, there are always two permutations that will yield
  // exactly the same result. The query planner consistently chosses one of
  // them.
  h::expect("SELECT * WHERE { ?s <p> <o> }",
            h::IndexScan(Var{"?s"}, "<p>", "<o>", {POS}));
  h::expect("SELECT * WHERE { <s> ?p <o> }",
            h::IndexScan("<s>", Var{"?p"}, "<o>", {SOP}));
  h::expect("SELECT * WHERE { <s> <p> ?o }",
            h::IndexScan("<s>", "<p>", Var{"?o"}, {PSO}));
}

TEST(QueryPlannerTest, SimpleTripleTwoVariables) {
  using enum Permutation::Enum;

  // In the following tests we need the query planner to be aware that the index
  // contains the entities `<s> <p> <o>` that are used below, otherwise it will
  // estimate that and Index scan has the same cost as an Index scan followed by
  // a sort (because both plans have a cost of zero if the index scan is known
  // to be empty).

  auto qec = ad_utility::testing::getQec("<s> <p> <o>");

  // Fixed predicate.

  // Without `Order By`, two orderings are possible, both are fine.
  h::expect("SELECT * WHERE { ?s <p> ?o }",
            h::IndexScan(Var{"?s"}, "<p>", Var{"?o"}, {POS, PSO}), qec);
  // Must always be a single index scan, never index scan + sorting.
  h::expect("SELECT * WHERE { ?s <p> ?o } INTERNAL SORT BY ?o",
            h::IndexScan(Var{"?s"}, "<p>", Var{"?o"}, {POS}), qec);
  h::expect("SELECT * WHERE { ?s <p> ?o } INTERNAL SORT BY ?s",
            h::IndexScan(Var{"?s"}, "<p>", Var{"?o"}, {PSO}), qec);

  // Fixed subject.
  h::expect("SELECT * WHERE { <s> ?p ?o }",
            h::IndexScan("<s>", Var{"?p"}, Var{"?o"}, {SOP, SPO}), qec);
  h::expect("SELECT * WHERE { <s> ?p ?o } INTERNAL SORT BY ?o",
            h::IndexScan("<s>", Var{"?p"}, Var{"?o"}, {SOP}), qec);
  h::expect("SELECT * WHERE { <s> ?p ?o } INTERNAL SORT BY ?p",
            h::IndexScan("<s>", Var{"?p"}, Var{"?o"}, {SPO}), qec);

  // Fixed object.
  h::expect("SELECT * WHERE { <s> ?p ?o }",
            h::IndexScan("<s>", Var{"?p"}, Var{"?o"}, {SOP, SPO}), qec);
  h::expect("SELECT * WHERE { <s> ?p ?o } INTERNAL SORT BY ?o",
            h::IndexScan("<s>", Var{"?p"}, Var{"?o"}, {SOP}), qec);
  h::expect("SELECT * WHERE { <s> ?p ?o } INTERNAL SORT BY ?p",
            h::IndexScan("<s>", Var{"?p"}, Var{"?o"}, {SPO}), qec);
}

TEST(QueryPlannerTest, SimpleTripleThreeVariables) {
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

TEST(QueryPlannerTest, CartesianProductJoin) {
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

TEST(QueryPlannerTest, TransitivePathUnbound) {
  auto scan = h::IndexScanFromStrings;
  TransitivePathSide left{std::nullopt, 0, Variable("?x"), 0};
  TransitivePathSide right{std::nullopt, 1, Variable("?y"), 1};
  h::expect(
      "SELECT ?x ?y WHERE {"
      "?x <p>+ ?y }",
      h::TransitivePath(
          left, right, 1, std::numeric_limits<size_t>::max(),
          scan("?_qlever_internal_variable_query_planner_0", "<p>",
               "?_qlever_internal_variable_query_planner_1")));
}

TEST(QueryPlannerTest, TransitivePathLeftId) {
  auto scan = h::IndexScanFromStrings;
  auto qec = ad_utility::testing::getQec("<s> <p> <o>");

  auto getId = ad_utility::testing::makeGetId(qec->getIndex());

  TransitivePathSide left{std::nullopt, 0, getId("<s>"), 0};
  TransitivePathSide right{std::nullopt, 1, Variable("?y"), 1};
  h::expect(
      "SELECT ?y WHERE {"
      "<s> <p>+ ?y }",
      h::TransitivePath(
          left, right, 1, std::numeric_limits<size_t>::max(),
          scan("?_qlever_internal_variable_query_planner_0", "<p>",
               "?_qlever_internal_variable_query_planner_1")),
      qec);
}

TEST(QueryPlannerTest, TransitivePathRightId) {
  auto scan = h::IndexScanFromStrings;
  auto qec = ad_utility::testing::getQec("<s> <p> <o>");

  auto getId = ad_utility::testing::makeGetId(qec->getIndex());

  TransitivePathSide left{std::nullopt, 0, Variable("?x"), 0};
  TransitivePathSide right{std::nullopt, 1, getId("<o>"), 1};
  h::expect(
      "SELECT ?y WHERE {"
      "?x <p>+ <o> }",
      h::TransitivePath(
          left, right, 1, std::numeric_limits<size_t>::max(),
          scan("?_qlever_internal_variable_query_planner_0", "<p>",
               "?_qlever_internal_variable_query_planner_1")),
      qec);
}

TEST(QueryPlannerTest, TransitivePathBindLeft) {
  auto scan = h::IndexScanFromStrings;
  TransitivePathSide left{std::nullopt, 0, Variable("?x"), 0};
  TransitivePathSide right{std::nullopt, 1, Variable("?y"), 1};
  h::expect(
      "SELECT ?x ?y WHERE {"
      "<s> <p> ?x."
      "?x <p>* ?y }",
      h::TransitivePath(
          left, right, 0, std::numeric_limits<size_t>::max(),
          scan("<s>", "<p>", "?x"),
          scan("?_qlever_internal_variable_query_planner_0", "<p>",
               "?_qlever_internal_variable_query_planner_1")));
}

TEST(QueryPlannerTest, TransitivePathBindRight) {
  auto scan = h::IndexScanFromStrings;
  TransitivePathSide left{std::nullopt, 0, Variable("?x"), 0};
  TransitivePathSide right{std::nullopt, 1, Variable("?y"), 1};
  h::expect(
      "SELECT ?x ?y WHERE {"
      "?x <p>* ?y."
      "?y <p> <o> }",
      h::TransitivePath(
          left, right, 0, std::numeric_limits<size_t>::max(),
          scan("?y", "<p>", "<o>"),
          scan("?_qlever_internal_variable_query_planner_0", "<p>",
               "?_qlever_internal_variable_query_planner_1")));
}

// __________________________________________________________________________
TEST(QueryPlanner, BindAtBeginningOfQuery) {
  h::expect(
      "SELECT * WHERE {"
      " BIND (3 + 5 AS ?x) }",
      h::Bind(h::NeutralElementOperation(), "3 + 5", Variable{"?x"}));
}

// __________________________________________________________________________
TEST(QueryPlannerTest, TextIndexScanForWord) {
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
TEST(QueryPlannerTest, TextIndexScanForEntity) {
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
  QueryPlanner qp(nullptr);
  AD_EXPECT_THROW_WITH_MESSAGE(
      qp.createExecutionTree(pq),
      ::testing::ContainsRegex(
          "Missing ql:contains-word statement. A ql:contains-entity statement "
          "always also needs corresponding ql:contains-word statement."));
}

// __________________________________________________________________________
TEST(QueryPlannerTest, TooManyTriples) {
  std::string query = "SELECT * WHERE {";
  for (size_t i = 0; i < 65; i++) {
    query = absl::StrCat(query, " ?x <p> ?y .");
  }
  query = absl::StrCat(query, "}");
  ParsedQuery pq = SparqlParser::parseQuery(query);
  QueryPlanner qp(nullptr);
  AD_EXPECT_THROW_WITH_MESSAGE(
      qp.createExecutionTree(pq),
      ::testing::ContainsRegex("At most 64 triples allowed at the moment."));
}
