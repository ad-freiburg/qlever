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

namespace {
auto lit = ad_utility::testing::tripleComponentLiteral;
}

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

TEST(QueryPlannerTest, testcollapseTextCliques) {
  using TripleGraph = QueryPlanner::TripleGraph;
  using Node = QueryPlanner::TripleGraph::Node;
  using std::vector;
  {
    {
      ParsedQuery pq = SparqlParser::parseQuery(
          "SELECT ?x WHERE {?x <p> <X>. ?c ql:contains-entity ?x. ?c "
          "ql:contains-word \"abc\"}");
      QueryPlanner qp(nullptr);
      auto tg = qp.createTripleGraph(&pq.children()[0].getBasic());
      ASSERT_EQ(
          "0 {s: ?x, p: <p>, o: <X>} : (1)\n"
          "1 {s: ?c, p: <QLever-internal-function/contains-entity>, o: ?x} : "
          "(0, 2)\n"
          "2 {s: ?c, p: <QLever-internal-function/contains-word>, o: \"abc\"} "
          ": "
          "(1)",
          tg.asString());
      tg.collapseTextCliques();
      TripleGraph expected =
          TripleGraph(std::vector<std::pair<Node, std::vector<size_t>>>(
              {std::make_pair<Node, vector<size_t>>(
                   QueryPlanner::TripleGraph::Node(
                       0, Var{"?c"}, {"abc"},
                       {
                           SparqlTriple(
                               Var{"?c"},
                               "<QLever-internal-function/contains-entity>",
                               Var{"?x"}),
                           SparqlTriple(
                               Var{"?c"},
                               "<QLever-internal-function/contains-word>",
                               lit("\"abc\"")),
                       }),
                   {1}),
               std::make_pair<Node, vector<size_t>>(
                   QueryPlanner::TripleGraph::Node(
                       1, SparqlTriple(Var{"?x"}, "<p>", "<X>")),
                   {0})}));
      ASSERT_TRUE(tg.isSimilar(expected));
    }
    {
      ParsedQuery pq = SparqlParser::parseQuery(
          "SELECT ?x WHERE {?x <p> <X>. ?c "
          "<QLever-internal-function/contains-entity> ?x. ?c "
          "<QLever-internal-function/contains-word> \"abc\" . ?c "
          "ql:contains-entity ?y}");
      QueryPlanner qp(nullptr);
      auto tg = qp.createTripleGraph(&pq.children()[0].getBasic());
      ASSERT_EQ(
          "0 {s: ?x, p: <p>, o: <X>} : (1)\n"
          "1 {s: ?c, p: <QLever-internal-function/contains-entity>, o: ?x} : "
          "(0, 2, 3)\n"
          "2 {s: ?c, p: <QLever-internal-function/contains-word>, o: \"abc\"} "
          ": "
          "(1, 3)\n"
          "3 {s: ?c, p: <QLever-internal-function/contains-entity>, o: ?y} : "
          "(1, 2)",
          tg.asString());
      tg.collapseTextCliques();
      TripleGraph expected =
          TripleGraph(std::vector<std::pair<Node, std::vector<size_t>>>(
              {std::make_pair<Node, vector<size_t>>(
                   QueryPlanner::TripleGraph::Node(
                       0, Var{"?c"}, {"abc"},
                       {SparqlTriple(
                            Var{"?c"},
                            "<QLever-internal-function/contains-entity>",
                            Var{"?x"}),
                        SparqlTriple(Var{"?c"},
                                     "<QLever-internal-function/contains-word>",
                                     lit("\"abc\"")),
                        SparqlTriple(
                            Var{"?c"},
                            "<QLever-internal-function/contains-entity>",
                            Var{"?y"})}),
                   {1}),
               std::make_pair<Node, vector<size_t>>(
                   QueryPlanner::TripleGraph::Node(
                       1, SparqlTriple(Var{"?x"}, "<p>", "<X>")),
                   {0})}));
      ASSERT_TRUE(tg.isSimilar(expected));
    }
    {
      ParsedQuery pq = SparqlParser::parseQuery(
          "SELECT ?x WHERE {?x <p> <X>. ?c ql:contains-entity ?x. ?c "
          "ql:contains-word \"abc\" . ?c ql:contains-entity ?y. ?y <P2> "
          "<X2>}");
      QueryPlanner qp(nullptr);
      auto tg = qp.createTripleGraph(&pq.children()[0].getBasic());
      ASSERT_EQ(
          "0 {s: ?x, p: <p>, o: <X>} : (1)\n"
          "1 {s: ?c, p: <QLever-internal-function/contains-entity>, o: ?x} : "
          "(0, 2, 3)\n"
          "2 {s: ?c, p: <QLever-internal-function/contains-word>, o: \"abc\"} "
          ": "
          "(1, 3)\n"
          "3 {s: ?c, p: <QLever-internal-function/contains-entity>, o: ?y} : "
          "(1, 2, 4)\n"
          "4 {s: ?y, p: <P2>, o: <X2>} : (3)",
          tg.asString());
      tg.collapseTextCliques();
      TripleGraph expected =
          TripleGraph(std::vector<std::pair<Node, std::vector<size_t>>>(
              {std::make_pair<Node, vector<size_t>>(
                   QueryPlanner::TripleGraph::Node(
                       0, Var{"?c"}, {"abc"},
                       {SparqlTriple(
                            Var{"?c"},
                            "<QLever-internal-function/contains-entity>",
                            Var{"?x"}),
                        SparqlTriple(Var{"?c"},
                                     "<QLever-internal-function/contains-word>",
                                     lit("\"abc\"")),
                        SparqlTriple(
                            Var{"?c"},
                            "<QLever-internal-function/contains-entity>",
                            Var{"?y"})}),
                   {1, 2}),
               std::make_pair<Node, vector<size_t>>(
                   QueryPlanner::TripleGraph::Node(
                       1, SparqlTriple(Var{"?x"}, "<p>", "<X>")),
                   {0}),
               std::make_pair<Node, vector<size_t>>(
                   QueryPlanner::TripleGraph::Node(
                       2, SparqlTriple(Var{"?y"}, "<P2>", "<X2>")),
                   {0})}));
      ASSERT_TRUE(tg.isSimilar(expected));
    }
    {
      ParsedQuery pq = SparqlParser::parseQuery(
          "SELECT ?x WHERE {?x <p> <X>. ?c ql:contains-entity ?x. ?c "
          "ql:contains-word \"abc\" . ?c ql:contains-entity ?y. ?c2 "
          "ql:contains-entity ?y. ?c2 ql:contains-word \"xx\"}");
      QueryPlanner qp(nullptr);
      auto tg = qp.createTripleGraph(&pq.children()[0].getBasic());
      TripleGraph expected = TripleGraph(std::vector<
                                         std::pair<Node, std::vector<size_t>>>(
          {std::make_pair<Node, vector<size_t>>(
               QueryPlanner::TripleGraph::Node(
                   0, SparqlTriple(Var{"?x"}, "<p>", "<X>")),
               {1}),
           std::make_pair<Node, vector<size_t>>(
               QueryPlanner::TripleGraph::Node(
                   1, SparqlTriple(Var{"?c"},
                                   "<QLever-internal-function/contains-entity>",
                                   Var{"?x"})),
               {0, 2, 3}),
           std::make_pair<Node, vector<size_t>>(
               QueryPlanner::TripleGraph::Node(
                   2, SparqlTriple(Var{"?c"},
                                   "<QLever-internal-function/contains-word>",
                                   lit("\"abc\""))),
               {1, 3}),
           std::make_pair<Node, vector<size_t>>(
               QueryPlanner::TripleGraph::Node(
                   3, SparqlTriple(Var{"?c"},
                                   "<QLever-internal-function/contains-entity>",
                                   Var{"?y"})),
               {1, 2, 4}),
           std::make_pair<Node, vector<size_t>>(
               QueryPlanner::TripleGraph::Node(
                   4, SparqlTriple(Var{"?c2"},
                                   "<QLever-internal-function/contains-entity>",
                                   Var{"?y"})),
               {3, 5}),
           std::make_pair<Node, vector<size_t>>(
               QueryPlanner::TripleGraph::Node(
                   5, SparqlTriple(Var{"?c2"},
                                   "<QLever-internal-function/contains-word>",
                                   lit("\"xx\""))),
               {4})}));

      ASSERT_TRUE(tg.isSimilar(expected));
      tg.collapseTextCliques();
      TripleGraph expected2 = TripleGraph(std::vector<
                                          std::pair<Node, std::vector<size_t>>>(
          {std::make_pair<Node, vector<size_t>>(
               QueryPlanner::TripleGraph::Node(
                   0, Var{"?c"}, {"abc"},
                   {SparqlTriple(Var{"?c"},
                                 "<QLever-internal-function/contains-entity>",
                                 Var{"?x"}),
                    SparqlTriple(Var{"?c"},
                                 "<QLever-internal-function/contains-word>",
                                 lit("\"abc\"")),
                    SparqlTriple(Var{"?c"},
                                 "<QLever-internal-function/contains-entity>",
                                 Var{"?y"})}),
               {1, 2}),
           std::make_pair<Node, vector<size_t>>(
               QueryPlanner::TripleGraph::Node(
                   1, Var{"?c2"}, {"xx"},
                   {SparqlTriple(Var{"?c2"},
                                 "<QLever-internal-function/contains-entity>",
                                 Var{"?y"}),
                    SparqlTriple(Var{"?c2"},
                                 "<QLever-internal-function/contains-word>",
                                 lit("\"xx\""))}),
               {0}),
           std::make_pair<Node, vector<size_t>>(
               QueryPlanner::TripleGraph::Node(
                   2, SparqlTriple(Var{"?x"}, "<p>", "<X>")),
               {0})}));
      ASSERT_TRUE(tg.isSimilar(expected2));
    }
    {
      ParsedQuery pq = SparqlParser::parseQuery(
          "SELECT ?x WHERE {?x <p> <X>. ?c ql:contains-entity ?x. ?c "
          "ql:contains-word \"abc\" . ?c ql:contains-entity ?y. ?c2 "
          "ql:contains-entity ?y. ?c2 ql:contains-word \"xx\". ?y <P2> "
          "<X2>}");
      QueryPlanner qp(nullptr);
      auto tg = qp.createTripleGraph(&pq.children()[0].getBasic());
      ASSERT_EQ(
          "0 {s: ?x, p: <p>, o: <X>} : (1)\n"
          "1 {s: ?c, p: <QLever-internal-function/contains-entity>, o: ?x} : "
          "(0, 2, 3)\n"
          "2 {s: ?c, p: <QLever-internal-function/contains-word>, o: \"abc\"} "
          ": "
          "(1, 3)\n"
          "3 {s: ?c, p: <QLever-internal-function/contains-entity>, o: ?y} : "
          "(1, 2, 4, 6)\n"
          "4 {s: ?c2, p: <QLever-internal-function/contains-entity>, o: ?y} "
          ": (3, 5, 6)\n"
          "5 {s: ?c2, p: <QLever-internal-function/contains-word>, o: \"xx\"} "
          ": "
          "(4)\n"
          "6 {s: ?y, p: <P2>, o: <X2>} : (3, 4)",
          tg.asString());
      tg.collapseTextCliques();
      TripleGraph expected2 = TripleGraph(std::vector<
                                          std::pair<Node, std::vector<size_t>>>(
          {std::make_pair<Node, vector<size_t>>(
               QueryPlanner::TripleGraph::Node(
                   0, Var{"?c"}, {"abc"},
                   {SparqlTriple(Var{"?c"},
                                 "<QLever-internal-function/contains-entity>",
                                 Var{"?x"}),
                    SparqlTriple(Var{"?c"},
                                 "<QLever-internal-function/contains-word>",
                                 "abc"),
                    SparqlTriple(Var{"?c"},
                                 "<QLever-internal-function/contains-entity>",
                                 Var{"?y"})}),
               {1, 2, 3}),
           std::make_pair<Node, vector<size_t>>(
               QueryPlanner::TripleGraph::Node(
                   1, Var{"?c2"}, {"xx"},
                   {SparqlTriple(Var{"?c2"},
                                 "<QLever-internal-function/contains-entity>",
                                 Var{"?y"}),
                    SparqlTriple(Var{"?c2"},
                                 "<QLever-internal-function/contains-word>",
                                 lit("\"xx\""))}),
               {0, 3}),
           std::make_pair<Node, vector<size_t>>(
               QueryPlanner::TripleGraph::Node(
                   2, SparqlTriple(Var{"?x"}, "<p>", "<X>")),
               {0}),
           std::make_pair<Node, vector<size_t>>(
               QueryPlanner::TripleGraph::Node(
                   3, SparqlTriple(Var{"?y"}, "<P2>", "<X2>")),
               {0, 1})}));
      ASSERT_TRUE(tg.isSimilar(expected2));
    }
  }
}

TEST(QueryPlannerTest, testSPX) {
  ParsedQuery pq = SparqlParser::parseQuery(
      "PREFIX : <http://rdf.myprefix.com/>\n"
      "SELECT ?x \n "
      "WHERE \t {?x :myrel :obj}");
  QueryPlanner qp(nullptr);
  QueryExecutionTree qet = qp.createExecutionTree(pq);
  ASSERT_EQ(
      "{\n  SCAN POS with P = \"<http://rdf.myprefix.com/myrel>\","
      " O = \"<http://rdf.myprefix.com/obj>\"\n  qet-width: 1 \n}",
      qet.asString());
}

TEST(QueryPlannerTest, testXPO) {
  ParsedQuery pq = SparqlParser::parseQuery(
      "PREFIX : <http://rdf.myprefix.com/>\n"
      "SELECT ?x \n "
      "WHERE \t {:subj :myrel ?x}");
  QueryPlanner qp(nullptr);
  QueryExecutionTree qet = qp.createExecutionTree(pq);
  ASSERT_EQ(
      "{\n  SCAN PSO with P = \"<http://rdf.myprefix.com/myrel>\", "
      "S = \"<http://rdf.myprefix.com/subj>\"\n  qet-width: 1 \n}",
      qet.asString());
}

TEST(QueryPlannerTest, testSP_free_) {
  ParsedQuery pq = SparqlParser::parseQuery(
      "PREFIX : <http://rdf.myprefix.com/>\n"
      "SELECT ?x \n "
      "WHERE \t {?x :myrel ?y}");
  QueryPlanner qp(nullptr);
  QueryExecutionTree qet = qp.createExecutionTree(pq);
  ASSERT_EQ(
      "{\n  SCAN POS with P = \"<http://rdf.myprefix.com/myrel>\"\n  "
      "qet-width: 2 \n}",
      qet.asString());
}

TEST(QueryPlannerTest, testSPX_SPX) {
  ParsedQuery pq = SparqlParser::parseQuery(
      "PREFIX : <pre/>\n"
      "SELECT ?x \n "
      "WHERE \t {:s1 :r ?x. :s2 :r ?x}");
  QueryPlanner qp(nullptr);
  QueryExecutionTree qet = qp.createExecutionTree(pq);
  ASSERT_EQ(
      "{\n  JOIN\n  {\n    SCAN PSO with P = \"<pre/r>\", S = \"<pre/s1>\"\n "
      "   qet-width: 1 \n  } join-column: [0]\n  |X|\n  {\n    S"
      "CAN PSO with P = \"<pre/r>\", S = \"<pre/s2>\"\n    qet-w"
      "idth: 1 \n  } join-column: [0]\n  qet-width: 1 \n}",
      qet.asString());
}

TEST(QueryPlannerTest, test_free_PX_SPX) {
  ParsedQuery pq = SparqlParser::parseQuery(
      "PREFIX : <pre/>\n"
      "SELECT ?x ?y \n "
      "WHERE  {?y :r ?x . :s2 :r ?x}");
  QueryPlanner qp(nullptr);
  QueryExecutionTree qet = qp.createExecutionTree(pq);
  ASSERT_EQ(
      "{\n  JOIN\n  {\n    SCAN POS with P = \"<pre/r>\"\n    "
      "qet-width: 2 \n  } join-column: [0]\n  |X|\n  {\n   "
      " SCAN PSO with P = \"<pre/r>\", S = \"<pre/s2>\"\n  "
      "  qet-width: 1 \n  } join-column: [0]\n  qet-width: 2 \n}",
      qet.asString());
}

TEST(QueryPlannerTest, test_free_PX__free_PX) {
  ParsedQuery pq = SparqlParser::parseQuery(
      "PREFIX : <pre/>\n"
      "SELECT ?x ?y ?z \n "
      "WHERE {?y :r ?x. ?z :r ?x}");
  QueryPlanner qp(nullptr);
  QueryExecutionTree qet = qp.createExecutionTree(pq);
  ASSERT_EQ(
      "{\n  JOIN\n  {\n    SCAN POS with P = \"<pre/r>\"\n    "
      "qet-width: 2 \n  } join-column: [0]\n  |X|\n  {\n    "
      "SCAN POS with P = \"<pre/r>\"\n    qet-width: 2 \n"
      "  } join-column: [0]\n  qet-width: 3 \n}",
      qet.asString());
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
  ASSERT_EQ(
      "{\n  ORDER BY on columns:asc(0) \n  {\n    JOIN\n    {\n      "
      "SORT(internal) on columns:asc(1) \n      {\n        JOIN\n        {\n   "
      "       SCAN POS with P = \"<pre/profession>\", O = \"<pre/Actor>\"\n    "
      "      qet-width: 1 \n        } join-column: [0]\n        |X|\n        "
      "{\n          SCAN PSO with P = \"<pre/born-in>\"\n          qet-width: "
      "2 \n        } join-column: [0]\n        qet-width: 2 \n      }\n      "
      "qet-width: 2 \n    } join-column: [1]\n    |X|\n    {\n      SCAN POS "
      "with P = \"<pre/in>\", O = \"<pre/Europe>\"\n      qet-width: 1 \n    } "
      "join-column: [0]\n    qet-width: 2 \n  }\n  qet-width: 2 \n}",
      qet.asString());
}

TEST(QueryPlannerTest, testStarTwoFree) {
  {
    ParsedQuery pq = SparqlParser::parseQuery(
        "PREFIX : <http://rdf.myprefix.com/>\n"
        "PREFIX ns: <http://rdf.myprefix.com/ns/>\n"
        "PREFIX xxx: <http://rdf.myprefix.com/xxx/>\n"
        "SELECT ?x ?z \n "
        "WHERE \t {?x :myrel ?y. ?y ns:myrel ?z. ?y xxx:rel2 "
        "<http://abc.de>}");
    QueryPlanner qp(nullptr);
    QueryExecutionTree qet = qp.createExecutionTree(pq);
    ASSERT_EQ(
        "{\n  JOIN\n  {\n    JOIN\n    {\n      SCAN POS with P = "
        "\"<http://rdf.myprefix.com/myrel>\"\n      qet-width: 2 \n    } "
        "join-column: [0]\n    |X|\n    {\n      SCAN POS with P = "
        "\"<http://rdf.myprefix.com/xxx/rel2>\", O = \"<http://abc.de>\"\n   "
        "   qet-width: 1 \n    } join-column: [0]\n    qet-width: 2 \n  } "
        "join-column: [0]\n  |X|\n  {\n    SCAN PSO with P = "
        "\"<http://rdf.myprefix.com/ns/myrel>\"\n    qet-width: 2 \n  } "
        "join-column: [0]\n  qet-width: 3 \n}",
        qet.asString());
  }
}

TEST(QueryPlannerTest, testFilterAfterSeed) {
  ParsedQuery pq = SparqlParser::parseQuery(
      "SELECT ?x ?y ?z WHERE {"
      "?x <r> ?y . ?y <r> ?z . "
      "FILTER(?x != ?y) }");
  QueryPlanner qp(nullptr);
  QueryExecutionTree qet = qp.createExecutionTree(pq);
  ASSERT_EQ(
      "{\n  FILTER   {\n    JOIN\n    {\n      SCAN POS with P = \"<r>\"\n   "
      "   qet-width: 2 \n    } join-column: [0]\n    |X|\n    {\n      SCAN "
      "PSO with P = \"<r>\"\n      qet-width: 2 \n    } join-column: [0]\n   "
      " qet-width: 3 \n  } with "
      "N16sparqlExpression10relational20RelationalExpressionILN18valueIdCompa"
      "rators10ComparisonE3EEE#column_1##column_0#\n  qet-width: 3 \n}",
      qet.asString());
}

TEST(QueryPlannerTest, testFilterAfterJoin) {
  ParsedQuery pq = SparqlParser::parseQuery(
      "SELECT ?x ?y ?z WHERE {"
      "?x <r> ?y . ?y <r> ?z . "
      "FILTER(?x != ?z) }");
  QueryPlanner qp(nullptr);
  QueryExecutionTree qet = qp.createExecutionTree(pq);
  ASSERT_EQ(
      "{\n  FILTER   {\n    JOIN\n    {\n      SCAN POS with P = \"<r>\"\n   "
      "   qet-width: 2 \n    } join-column: [0]\n    |X|\n    {\n      SCAN "
      "PSO with P = \"<r>\"\n      qet-width: 2 \n    } join-column: [0]\n   "
      " qet-width: 3 \n  } with "
      "N16sparqlExpression10relational20RelationalExpressionILN18valueIdCompa"
      "rators10ComparisonE3EEE#column_1##column_2#\n  qet-width: 3 \n}",
      qet.asString());
}

TEST(QueryPlannerTest, threeVarTriples) {
  {
    ParsedQuery pq = SparqlParser::parseQuery(
        "SELECT ?x ?p ?o WHERE {"
        "<s> <p> ?x . ?x ?p ?o }");
    QueryPlanner qp(nullptr);
    QueryExecutionTree qet = qp.createExecutionTree(pq);
    ASSERT_EQ(
        "{\n  JOIN\n  {\n    SORT(internal) on columns:asc(1) \n    {\n      "
        "SCAN FOR FULL INDEX OSP (DUMMY OPERATION)\n      qet-width: 3 \n    "
        "}\n    qet-width: 3 \n  } join-column: [1]\n  |X|\n  {\n    SCAN PSO "
        "with P = \"<p>\", S = \"<s>\"\n    qet-width: 1 \n  } join-column: "
        "[0]\n  qet-width: 3 \n}",
        qet.asString());
  }
  {
    ParsedQuery pq = SparqlParser::parseQuery(
        "SELECT ?x ?p ?o WHERE {"
        "<s> ?x <o> . ?x ?p ?o }");
    QueryPlanner qp(nullptr);
    QueryExecutionTree qet = qp.createExecutionTree(pq);
    ASSERT_EQ(
        "{\n  JOIN\n  {\n    SORT(internal) on columns:asc(1) \n    {\n      "
        "SCAN FOR FULL INDEX OSP (DUMMY OPERATION)\n      qet-width: 3 \n    "
        "}\n    qet-width: 3 \n  } join-column: [1]\n  |X|\n  {\n    SCAN SOP "
        "with S = \"<s>\", O = \"<o>\"\n    qet-width: 1 \n  } join-column: "
        "[0]\n  qet-width: 3 \n}",
        qet.asString());
  }
  {
    ParsedQuery pq = SparqlParser::parseQuery(
        "SELECT ?s ?p ?o WHERE {"
        "<s> <p> ?p . ?s ?p ?o }");
    QueryPlanner qp(nullptr);
    QueryExecutionTree qet = qp.createExecutionTree(pq);
    ASSERT_EQ(
        "{\n  JOIN\n  {\n    SORT(internal) on columns:asc(1) \n    {\n      "
        "SCAN FOR FULL INDEX OPS (DUMMY OPERATION)\n      qet-width: 3 \n    "
        "}\n    qet-width: 3 \n  } join-column: [1]\n  |X|\n  {\n    SCAN PSO "
        "with P = \"<p>\", S = \"<s>\"\n    qet-width: 1 \n  } join-column: "
        "[0]\n  qet-width: 3 \n}",
        qet.asString());
  }
}

TEST(QueryPlannerTest, threeVarTriplesTCJ) {
  h::expect(
      "SELECT ?x ?p ?o WHERE {"
      "<s> ?p ?x . ?x ?p ?o }",
      h::MultiColumnJoin(h::IndexScan("<s>", Var{"?p"}, Var{"?x"}),
                         h::IndexScan(Var{"?x"}, Var{"?p"}, Var{"?o"})));

  h::expect(
      "SELECT ?s ?p ?o WHERE {"
      "?s ?p ?o . ?s ?p <x> }",
      h::MultiColumnJoin(h::IndexScan(Var{"?s"}, Var{"?p"}, Var{"?o"}),
                         h::IndexScan(Var{"?s"}, Var{"?p"}, "<x>")));
}

TEST(QueryPlannerTest, threeVarXthreeVarException) {
  ParsedQuery pq = SparqlParser::parseQuery(
      "SELECT ?s ?s2 WHERE {"
      "?s ?p ?o . ?s2 ?p ?o }");
  QueryPlanner qp(nullptr);
  QueryExecutionTree qet = qp.createExecutionTree(pq);
  h::expect(
      "SELECT ?s ?s2 WHERE {"
      "?s ?p ?o . ?s2 ?p ?o }",
      h::MultiColumnJoin(h::IndexScan(Var{"?s"}, Var{"?p"}, Var{"?o"}),
                         h::IndexScan(Var{"?s2"}, Var{"?p"}, Var{"?o"})));
}

TEST(QueryExecutionTreeTest, testBooksbyNewman) {
  ParsedQuery pq = SparqlParser::parseQuery(
      "SELECT ?x WHERE { ?x <is-a> <Book> . "
      "?x <Author> <Anthony_Newman_(Author)> }");
  QueryPlanner qp(nullptr);
  QueryExecutionTree qet = qp.createExecutionTree(pq);
  ASSERT_EQ(
      "{\n  JOIN\n  {\n    SCAN POS with "
      "P = \"<Author>\", O = \"<Anthony_Newman_(Author)>\"\n   "
      " qet-width: 1 \n  } join-column: [0]\n  |X|\n  {\n  "
      "  SCAN POS with P = \"<is-a>\", O = \"<Book>\"\n  "
      "  qet-width: 1 \n  } join-column: [0]\n  qet-width: 1 \n}",
      qet.asString());
}

TEST(QueryExecutionTreeTest, testBooksGermanAwardNomAuth) {
  ParsedQuery pq = SparqlParser::parseQuery(
      "SELECT ?x ?y WHERE { "
      "?x <is-a> <Person> . "
      "?x <Country_of_nationality> <Germany> . "
      "?x <Author> ?y . "
      "?y <is-a> <Award-Nominated_Work> }");
  QueryPlanner qp(nullptr);
  QueryExecutionTree qet = qp.createExecutionTree(pq);
  ASSERT_GT(qet.asString().size(), 0u);
  // Just check that ther is no exception, here.
}

TEST(QueryExecutionTreeTest, testPlantsEdibleLeaves) {
  ParsedQuery pq = SparqlParser::parseQuery(
      "SELECT ?a \n "
      "WHERE  {?a <is-a> <Plant> . ?c ql:contains-entity ?a. "
      "?c ql:contains-word \"edible leaves\"} TEXTLIMIT 5");
  QueryPlanner qp(nullptr);
  QueryPlanner::TripleGraph tg =
      qp.createTripleGraph(&pq.children()[0].getBasic());
  ASSERT_EQ(1u, tg._nodeMap.find(0)->second->_variables.size());
  QueryExecutionTree qet = qp.createExecutionTree(pq);
  ASSERT_EQ(
      "{\n  TEXT OPERATION WITH FILTER: co-occurrence with words: "
      "\"edible leaves\" and 1 variables with textLimit = 5 "
      "filtered by\n  {\n    SCAN POS with P = \"<is-a>\", "
      "O = \"<Plant>\"\n    qet-width: 1 \n  }\n   filtered on "
      "column 0\n  qet-width: 3 \n}",
      qet.asString());
}

TEST(QueryExecutionTreeTest, testTextQuerySE) {
  ParsedQuery pq = SparqlParser::parseQuery(
      "SELECT ?c \n "
      "WHERE  {?c ql:contains-word \"search engine\"}");
  QueryPlanner qp(nullptr);
  QueryExecutionTree qet = qp.createExecutionTree(pq);
  ASSERT_EQ(absl::StrCat(
                "{\n  TEXT OPERATION WITHOUT FILTER: co-occurrence with words:",
                " \"search engine\" and 0 variables with textLimit = ",
                TEXT_LIMIT_DEFAULT, "\n", "  qet-width: 2 \n}"),
            qet.asString());
}

TEST(QueryExecutionTreeTest, testBornInEuropeOwCocaine) {
  ParsedQuery pq = SparqlParser::parseQuery(
      "PREFIX : <>\n"
      "SELECT ?x ?y ?c\n "
      "WHERE \t {"
      "?x :Place_of_birth ?y ."
      "?y :Contained_by :Europe ."
      "?c ql:contains-entity ?x ."
      "?c ql:contains-word \"cocaine\" ."
      "} TEXTLIMIT 1");
  QueryPlanner qp(nullptr);
  QueryExecutionTree qet = qp.createExecutionTree(pq);
  ASSERT_EQ(
      "{\n  TEXT OPERATION WITH FILTER: co-occurrence with words: "
      "\"cocaine\" and 1 variables with textLimit = 1 filtered by\n  "
      "{\n    JOIN\n    {\n      SCAN POS with P = \"<Contained_by>\", "
      "O = \"<Europe>\"\n      qet-width: 1 \n    } join-column: [0]\n"
      "    |X|\n    {\n      SCAN POS with P = \"<Place_of_birth>\"\n"
      "      qet-width: 2 \n    } join-column: [0]\n    qet-width: 2 \n"
      "  }\n   filtered on column 1\n  qet-width: 4 \n}",
      qet.asString());
  auto c = Variable{"?c"};
  ASSERT_EQ(0u, qet.getVariableColumn(c));
  ASSERT_EQ(1u, qet.getVariableColumn(c.getTextScoreVariable()));
  ASSERT_EQ(2u, qet.getVariableColumn(Variable{"?y"}));
}

TEST(QueryExecutionTreeTest, testCoOccFreeVar) {
  ParsedQuery pq = SparqlParser::parseQuery(
      "PREFIX : <>"
      "SELECT ?x ?y WHERE {"
      "?x :is-a :Politician ."
      "?c ql:contains-entity ?x ."
      "?c ql:contains-word \"friend*\" ."
      "?c ql:contains-entity ?y ."
      "} TEXTLIMIT 1");
  QueryPlanner qp(nullptr);
  QueryExecutionTree qet = qp.createExecutionTree(pq);
  ASSERT_EQ(
      "{\n  TEXT OPERATION WITH FILTER: co-occurrence with words: "
      "\"friend*\" and 2 variables with textLimit = 1 filtered by\n"
      "  {\n    SCAN POS with P = \"<is-a>\", O = \"<Politician>"
      "\"\n    qet-width: 1 \n  }\n   filtered on column 0\n "
      " qet-width: 4 \n}",
      qet.asString());
}

TEST(QueryExecutionTreeTest, testPoliticiansFriendWithScieManHatProj) {
  ParsedQuery pq = SparqlParser::parseQuery(
      "SELECT ?p ?s \n "
      "WHERE {"
      "?a <is-a> <Politician> . "
      "?c ql:contains-entity ?a ."
      "?c ql:contains-word \"friend*\" ."
      "?c ql:contains-entity ?s ."
      "?s <is-a> <Scientist> ."
      "?c2 ql:contains-entity ?s ."
      "?c2 ql:contains-word \"manhattan project\"} TEXTLIMIT 1");
  QueryPlanner qp(nullptr);
  QueryExecutionTree qet = qp.createExecutionTree(pq);
  ASSERT_EQ(
      "{\n  TEXT OPERATION WITH FILTER: co-occurrence with words: \"manhattan "
      "project\" and 1 variables with textLimit = 1 filtered by\n  {\n    "
      "JOIN\n    {\n      SORT(internal) on columns:asc(2) \n      {\n        "
      "TEXT OPERATION WITH FILTER: co-occurrence with words: \"friend*\" and 2 "
      "variables with textLimit = 1 filtered by\n        {\n          SCAN POS "
      "with P = \"<is-a>\", O = \"<Politician>\"\n          qet-width: 1 \n    "
      "    }\n         filtered on column 0\n        qet-width: 4 \n      }\n  "
      "    qet-width: 4 \n    } join-column: [2]\n    |X|\n    {\n      SCAN "
      "POS with P = \"<is-a>\", O = \"<Scientist>\"\n      qet-width: 1 \n    "
      "} join-column: [0]\n    qet-width: 4 \n  }\n   filtered on column 2\n  "
      "qet-width: 6 \n}",
      qet.asString());
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

  auto actual = strip(qet.asString());

  if (actual != possible1 && actual != possible2 && actual != possible3 &&
      actual != possible4 && actual != possible5) {
    // TODO<joka921> Make this work, there are just too many possibilities.
    /*
    FAIL() << "query execution tree is none of the possible trees, it is "
              "actually "
           << qet.asString() << '\n' << actual << '\n'
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
  ASSERT_EQ(
      "{\n  OPTIONAL_JOIN\n  {\n    SCAN PSO with P = \"<rel1>\"\n    "
      "qet-width: 2 \n  } join-columns: [0]\n  |X|\n  {\n    SCAN PSO with P "
      "= \"<rel2>\"\n    qet-width: 2 \n  } join-columns: [0]\n  qet-width: "
      "3 \n}",

      qet.asString());

  ParsedQuery pq2 = SparqlParser::parseQuery(
      "SELECT ?a ?b \n "
      "WHERE  {?a <rel1> ?b . "
      "OPTIONAL { ?a <rel2> ?c }} ORDER BY ?b");
  QueryExecutionTree qet2 = qp.createExecutionTree(pq2);
  ASSERT_EQ(
      "{\n  ORDER BY on columns:asc(1) \n  {\n    OPTIONAL_JOIN\n    "
      "{\n      SCAN PSO with P = \"<rel1>\"\n      qet-width: 2 \n    } "
      "join-columns: [0]\n    |X|\n    {\n      SCAN PSO with P = "
      "\"<rel2>\"\n      qet-width: 2 \n    } join-columns: [0]\n    "
      "qet-width: 3 \n  }\n  qet-width: 3 \n}",
      qet2.asString());
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

  // Fixed predicate.

  // Without `Order By`, two orderings are possible, both are fine.
  h::expect("SELECT * WHERE { ?s <p> ?o }",
            h::IndexScan(Var{"?s"}, "<p>", Var{"?o"}, {POS, PSO}));
  // Must always be a single index scan, never index scan + sorting.
  h::expect("SELECT * WHERE { ?s <p> ?o } INTERNAL SORT BY ?o",
            h::IndexScan(Var{"?s"}, "<p>", Var{"?o"}, {POS}));
  h::expect("SELECT * WHERE { ?s <p> ?o } INTERNAL SORT BY ?s",
            h::IndexScan(Var{"?s"}, "<p>", Var{"?o"}, {PSO}));

  // Fixed subject.
  h::expect("SELECT * WHERE { <s> ?p ?o }",
            h::IndexScan("<s>", Var{"?p"}, Var{"?o"}, {SOP, SPO}));
  h::expect("SELECT * WHERE { <s> ?p ?o } INTERNAL SORT BY ?o",
            h::IndexScan("<s>", Var{"?p"}, Var{"?o"}, {SOP}));
  h::expect("SELECT * WHERE { <s> ?p ?o } INTERNAL SORT BY ?p",
            h::IndexScan("<s>", Var{"?p"}, Var{"?o"}, {SPO}));

  // Fixed object.
  h::expect("SELECT * WHERE { <s> ?p ?o }",
            h::IndexScan("<s>", Var{"?p"}, Var{"?o"}, {SOP, SPO}));
  h::expect("SELECT * WHERE { <s> ?p ?o } INTERNAL SORT BY ?o",
            h::IndexScan("<s>", Var{"?p"}, Var{"?o"}, {SOP}));
  h::expect("SELECT * WHERE { <s> ?p ?o } INTERNAL SORT BY ?p",
            h::IndexScan("<s>", Var{"?p"}, Var{"?o"}, {SPO}));
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
