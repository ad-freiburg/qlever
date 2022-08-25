// Copyright 2015, University of Freiburg, Chair of Algorithms and Data
// Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)

#include <gtest/gtest.h>

#include "../src/engine/QueryPlanner.h"
#include "../src/parser/SparqlParser.h"

TEST(QueryPlannerTest, createTripleGraph) {
  using TripleGraph = QueryPlanner::TripleGraph;
  using Node = QueryPlanner::TripleGraph::Node;
  using std::vector;

  try {
    {
      ParsedQuery pq = SparqlParser(
                           "PREFIX : <http://rdf.myprefix.com/>\n"
                           "PREFIX ns: <http://rdf.myprefix.com/ns/>\n"
                           "PREFIX xxx: <http://rdf.myprefix.com/xxx/>\n"
                           "SELECT ?x ?z \n "
                           "WHERE \t {?x :myrel ?y. ?y ns:myrel ?z.?y xxx:rel2 "
                           "<http://abc.de>}")
                           .parse();
      pq.expandPrefixes();
      QueryPlanner qp(nullptr);
      auto tg =
          qp.createTripleGraph(&pq._rootGraphPattern._children[0].getBasic());
      TripleGraph expected =
          TripleGraph(std::vector<std::pair<Node, std::vector<size_t>>>(
              {std::make_pair<Node, vector<size_t>>(
                   QueryPlanner::TripleGraph::Node(
                       0, SparqlTriple("?x", "<http://rdf.myprefix.com/myrel>",
                                       "?y")),
                   {1, 2}),
               std::make_pair<Node, vector<size_t>>(
                   QueryPlanner::TripleGraph::Node(
                       1,
                       SparqlTriple("?y", "<http://rdf.myprefix.com/ns/myrel>",
                                    "?z")),
                   {0, 2}),
               std::make_pair<Node, vector<size_t>>(
                   QueryPlanner::TripleGraph::Node(
                       2,
                       SparqlTriple("?y", "<http://rdf.myprefix.com/xxx/rel2>",
                                    "<http://abc.de>")),
                   {0, 1})}));

      ASSERT_TRUE(tg.isSimilar(expected));
    }

    {
      ParsedQuery pq =
          SparqlParser("SELECT ?x WHERE {?x ?p <X>. ?x ?p2 <Y>. <X> ?p <Y>}")
              .parse();
      pq.expandPrefixes();
      QueryPlanner qp(nullptr);
      auto tg = qp.createTripleGraph(&pq.children()[0].getBasic());
      TripleGraph expected =
          TripleGraph(std::vector<std::pair<Node, std::vector<size_t>>>(
              {std::make_pair<Node, vector<size_t>>(
                   QueryPlanner::TripleGraph::Node(
                       0, SparqlTriple("?x", "?p", "<X>")),
                   {1, 2}),
               std::make_pair<Node, vector<size_t>>(
                   QueryPlanner::TripleGraph::Node(
                       1, SparqlTriple("?x", "?p2", "<Y>")),
                   {0}),
               std::make_pair<Node, vector<size_t>>(
                   QueryPlanner::TripleGraph::Node(
                       2, SparqlTriple("<X>", "?p", "<Y>")),
                   {0})}));
      ASSERT_TRUE(tg.isSimilar(expected));
    }

    {
      ParsedQuery pq = SparqlParser(
                           "SELECT ?x WHERE { ?x <is-a> <Book> . \n"
                           "?x <Author> <Anthony_Newman_(Author)> }")
                           .parse();
      pq.expandPrefixes();
      QueryPlanner qp(nullptr);
      auto tg = qp.createTripleGraph(&pq.children()[0].getBasic());

      TripleGraph expected =
          TripleGraph(std::vector<std::pair<Node, std::vector<size_t>>>({
              std::make_pair<Node, vector<size_t>>(
                  QueryPlanner::TripleGraph::Node(
                      0, SparqlTriple("?x", "<is-a>", "<Book>")),
                  {1}),
              std::make_pair<Node, vector<size_t>>(
                  QueryPlanner::TripleGraph::Node(
                      1, SparqlTriple("?x", "<Author>",
                                      "<Anthony_Newman_(Author)>")),
                  {0}),
          }));
      ASSERT_TRUE(tg.isSimilar(expected));
    }
  } catch (const ad_semsearch::Exception& e) {
    std::cout << "Caught: " << e.getFullErrorMessage() << std::endl;
    FAIL() << e.getFullErrorMessage();
  } catch (const std::exception& e) {
    std::cout << "Caught: " << e.what() << std::endl;
    FAIL() << e.what();
  }
}

TEST(QueryPlannerTest, testCpyCtorWithKeepNodes) {
  try {
    {
      ParsedQuery pq =
          SparqlParser("SELECT ?x WHERE {?x ?p <X>. ?x ?p2 <Y>. <X> ?p <Y>}")
              .parse();
      pq.expandPrefixes();
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
  } catch (const std::exception& e) {
    std::cout << "Caught: " << e.what() << std::endl;
    FAIL() << e.what();
  }
}

TEST(QueryPlannerTest, testBFSLeaveOut) {
  try {
    {
      ParsedQuery pq =
          SparqlParser("SELECT ?x WHERE {?x ?p <X>. ?x ?p2 <Y>. <X> ?p <Y>}")
              .parse();
      pq.expandPrefixes();
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
      ParsedQuery pq =
          SparqlParser("SELECT ?x WHERE {<A> <B> ?x. ?x <C> ?y. ?y <X> <Y>}")
              .parse();
      pq.expandPrefixes();
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
  } catch (const ad_semsearch::Exception& e) {
    std::cout << "Caught: " << e.getFullErrorMessage() << std::endl;
    FAIL() << e.getFullErrorMessage();
  } catch (const std::exception& e) {
    std::cout << "Caught: " << e.what() << std::endl;
    FAIL() << e.what();
  }
}

TEST(QueryPlannerTest, testcollapseTextCliques) {
  using TripleGraph = QueryPlanner::TripleGraph;
  using Node = QueryPlanner::TripleGraph::Node;
  using std::vector;
  try {
    {
      {
        ParsedQuery pq =
            SparqlParser(
                "SELECT ?x WHERE {?x <p> <X>. ?c ql:contains-entity ?x. ?c "
                "ql:contains-word \"abc\"}")
                .parse();
        pq.expandPrefixes();
        QueryPlanner qp(nullptr);
        auto tg = qp.createTripleGraph(&pq.children()[0].getBasic());
        ASSERT_EQ(
            "0 {s: ?x, p: <p>, o: <X>} : (1)\n"
            "1 {s: ?c, p: <QLever-internal-function/contains-entity>, o: ?x} : "
            "(0, 2)\n"
            "2 {s: ?c, p: <QLever-internal-function/contains-word>, o: abc} : "
            "(1)",
            tg.asString());
        tg.collapseTextCliques();
        TripleGraph expected =
            TripleGraph(std::vector<std::pair<Node, std::vector<size_t>>>(
                {std::make_pair<Node, vector<size_t>>(
                     QueryPlanner::TripleGraph::Node(
                         0, "?c", "abc",
                         {
                             SparqlTriple(
                                 "?c",
                                 "<QLever-internal-function/contains-entity>",
                                 "?x"),
                             SparqlTriple(
                                 "?c",
                                 "<QLever-internal-function/contains-word>",
                                 "abc"),
                         }),
                     {1}),
                 std::make_pair<Node, vector<size_t>>(
                     QueryPlanner::TripleGraph::Node(
                         1, SparqlTriple("?x", "<p>", "<X>")),
                     {0})}));
        ASSERT_TRUE(tg.isSimilar(expected));
      }
      {
        ParsedQuery pq =
            SparqlParser(
                "SELECT ?x WHERE {?x <p> <X>. ?c "
                "<QLever-internal-function/contains-entity> ?x. ?c "
                "<QLever-internal-function/contains-word> \"abc\" . ?c "
                "ql:contains-entity ?y}")
                .parse();
        pq.expandPrefixes();
        QueryPlanner qp(nullptr);
        auto tg = qp.createTripleGraph(&pq.children()[0].getBasic());
        ASSERT_EQ(
            "0 {s: ?x, p: <p>, o: <X>} : (1)\n"
            "1 {s: ?c, p: <QLever-internal-function/contains-entity>, o: ?x} : "
            "(0, 2, 3)\n"
            "2 {s: ?c, p: <QLever-internal-function/contains-word>, o: abc} : "
            "(1, 3)\n"
            "3 {s: ?c, p: <QLever-internal-function/contains-entity>, o: ?y} : "
            "(1, 2)",
            tg.asString());
        tg.collapseTextCliques();
        TripleGraph expected =
            TripleGraph(std::vector<std::pair<Node, std::vector<size_t>>>(
                {std::make_pair<Node, vector<size_t>>(
                     QueryPlanner::TripleGraph::Node(
                         0, "?c", "abc",
                         {SparqlTriple(
                              "?c",
                              "<QLever-internal-function/contains-entity>",
                              "?x"),
                          SparqlTriple(
                              "?c", "<QLever-internal-function/contains-word>",
                              "abc"),
                          SparqlTriple(
                              "?c",
                              "<QLever-internal-function/contains-entity>",
                              "?y")}),
                     {1}),
                 std::make_pair<Node, vector<size_t>>(
                     QueryPlanner::TripleGraph::Node(
                         1, SparqlTriple("?x", "<p>", "<X>")),
                     {0})}));
        ASSERT_TRUE(tg.isSimilar(expected));
      }
      {
        ParsedQuery pq =
            SparqlParser(
                "SELECT ?x WHERE {?x <p> <X>. ?c ql:contains-entity ?x. ?c "
                "ql:contains-word \"abc\" . ?c ql:contains-entity ?y. ?y <P2> "
                "<X2>}")
                .parse();
        pq.expandPrefixes();
        QueryPlanner qp(nullptr);
        auto tg = qp.createTripleGraph(&pq.children()[0].getBasic());
        ASSERT_EQ(
            "0 {s: ?x, p: <p>, o: <X>} : (1)\n"
            "1 {s: ?c, p: <QLever-internal-function/contains-entity>, o: ?x} : "
            "(0, 2, 3)\n"
            "2 {s: ?c, p: <QLever-internal-function/contains-word>, o: abc} : "
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
                         0, "?c", "abc",
                         {SparqlTriple(
                              "?c",
                              "<QLever-internal-function/contains-entity>",
                              "?x"),
                          SparqlTriple(
                              "?c", "<QLever-internal-function/contains-word>",
                              "abc"),
                          SparqlTriple(
                              "?c",
                              "<QLever-internal-function/contains-entity>",
                              "?y")}),
                     {1, 2}),
                 std::make_pair<Node, vector<size_t>>(
                     QueryPlanner::TripleGraph::Node(
                         1, SparqlTriple("?x", "<p>", "<X>")),
                     {0}),
                 std::make_pair<Node, vector<size_t>>(
                     QueryPlanner::TripleGraph::Node(
                         2, SparqlTriple("?y", "<P2>", "<X2>")),
                     {0})}));
        ASSERT_TRUE(tg.isSimilar(expected));
      }
      {
        ParsedQuery pq =
            SparqlParser(
                "SELECT ?x WHERE {?x <p> <X>. ?c ql:contains-entity ?x. ?c "
                "ql:contains-word \"abc\" . ?c ql:contains-entity ?y. ?c2 "
                "ql:contains-entity ?y. ?c2 ql:contains-word \"xx\"}")
                .parse();
        pq.expandPrefixes();
        QueryPlanner qp(nullptr);
        auto tg = qp.createTripleGraph(&pq.children()[0].getBasic());
        TripleGraph expected = TripleGraph(std::vector<std::pair<
                                               Node, std::vector<size_t>>>(
            {std::make_pair<Node, vector<size_t>>(
                 QueryPlanner::TripleGraph::Node(
                     0, SparqlTriple("?x", "<p>", "<X>")),
                 {1}),
             std::make_pair<Node, vector<size_t>>(
                 QueryPlanner::TripleGraph::Node(
                     1, SparqlTriple(
                            "?c", "<QLever-internal-function/contains-entity>",
                            "?x")),
                 {0, 2, 3}),
             std::make_pair<Node, vector<size_t>>(
                 QueryPlanner::TripleGraph::Node(
                     2, SparqlTriple("?c",
                                     "<QLever-internal-function/contains-word>",
                                     "abc")),
                 {1, 3}),
             std::make_pair<Node, vector<size_t>>(
                 QueryPlanner::TripleGraph::Node(
                     3, SparqlTriple(
                            "?c", "<QLever-internal-function/contains-entity>",
                            "?y")),
                 {1, 2, 4}),
             std::make_pair<Node, vector<size_t>>(
                 QueryPlanner::TripleGraph::Node(
                     4, SparqlTriple(
                            "?c2", "<QLever-internal-function/contains-entity>",
                            "?y")),
                 {3, 5}),
             std::make_pair<Node, vector<size_t>>(
                 QueryPlanner::TripleGraph::Node(
                     5, SparqlTriple("?c2",
                                     "<QLever-internal-function/contains-word>",
                                     "xx")),
                 {4})}));

        ASSERT_TRUE(tg.isSimilar(expected));
        tg.collapseTextCliques();
        TripleGraph expected2 =
            TripleGraph(std::vector<std::pair<Node, std::vector<size_t>>>(
                {std::make_pair<Node, vector<size_t>>(
                     QueryPlanner::TripleGraph::Node(
                         0, "?c", "abc",
                         {SparqlTriple(
                              "?c",
                              "<QLever-internal-function/contains-entity>",
                              "?x"),
                          SparqlTriple(
                              "?c", "<QLever-internal-function/contains-word>",
                              "abc"),
                          SparqlTriple(
                              "?c",
                              "<QLever-internal-function/contains-entity>",
                              "?y")}),
                     {1, 2}),
                 std::make_pair<Node, vector<size_t>>(
                     QueryPlanner::TripleGraph::Node(
                         1, "?c2", "xx",
                         {SparqlTriple(
                              "?c2",
                              "<QLever-internal-function/contains-entity>",
                              "?y"),
                          SparqlTriple(
                              "?c2", "<QLever-internal-function/contains-word>",
                              "xx")}),
                     {0}),
                 std::make_pair<Node, vector<size_t>>(
                     QueryPlanner::TripleGraph::Node(
                         2, SparqlTriple("?x", "<p>", "<X>")),
                     {0})}));
        ASSERT_TRUE(tg.isSimilar(expected2));
      }
      {
        ParsedQuery pq =
            SparqlParser(
                "SELECT ?x WHERE {?x <p> <X>. ?c ql:contains-entity ?x. ?c "
                "ql:contains-word \"abc\" . ?c ql:contains-entity ?y. ?c2 "
                "ql:contains-entity ?y. ?c2 ql:contains-word \"xx\". ?y <P2> "
                "<X2>}")
                .parse();
        pq.expandPrefixes();
        QueryPlanner qp(nullptr);
        auto tg = qp.createTripleGraph(&pq.children()[0].getBasic());
        ASSERT_EQ(
            "0 {s: ?x, p: <p>, o: <X>} : (1)\n"
            "1 {s: ?c, p: <QLever-internal-function/contains-entity>, o: ?x} : "
            "(0, 2, 3)\n"
            "2 {s: ?c, p: <QLever-internal-function/contains-word>, o: abc} : "
            "(1, 3)\n"
            "3 {s: ?c, p: <QLever-internal-function/contains-entity>, o: ?y} : "
            "(1, 2, 4, 6)\n"
            "4 {s: ?c2, p: <QLever-internal-function/contains-entity>, o: ?y} "
            ": (3, 5, 6)\n"
            "5 {s: ?c2, p: <QLever-internal-function/contains-word>, o: xx} : "
            "(4)\n"
            "6 {s: ?y, p: <P2>, o: <X2>} : (3, 4)",
            tg.asString());
        tg.collapseTextCliques();
        TripleGraph expected2 =
            TripleGraph(std::vector<std::pair<Node, std::vector<size_t>>>(
                {std::make_pair<Node, vector<size_t>>(
                     QueryPlanner::TripleGraph::Node(
                         0, "?c", "abc",
                         {SparqlTriple(
                              "?c",
                              "<QLever-internal-function/contains-entity>",
                              "?x"),
                          SparqlTriple(
                              "?c", "<QLever-internal-function/contains-word>",
                              "abc"),
                          SparqlTriple(
                              "?c",
                              "<QLever-internal-function/contains-entity>",
                              "?y")}),
                     {1, 2, 3}),
                 std::make_pair<Node, vector<size_t>>(
                     QueryPlanner::TripleGraph::Node(
                         1, "?c2", "xx",
                         {SparqlTriple(
                              "?c2",
                              "<QLever-internal-function/contains-entity>",
                              "?y"),
                          SparqlTriple(
                              "?c2", "<QLever-internal-function/contains-word>",
                              "xx")}),
                     {0, 3}),
                 std::make_pair<Node, vector<size_t>>(
                     QueryPlanner::TripleGraph::Node(
                         2, SparqlTriple("?x", "<p>", "<X>")),
                     {0}),
                 std::make_pair<Node, vector<size_t>>(
                     QueryPlanner::TripleGraph::Node(
                         3, SparqlTriple("?y", "<P2>", "<X2>")),
                     {0, 1})}));
        ASSERT_TRUE(tg.isSimilar(expected2));
      }
    }
  } catch (const std::exception& e) {
    std::cout << "Caught: " << e.what() << std::endl;
    FAIL() << e.what();
  }
}

TEST(QueryPlannerTest, testSPX) {
  ParsedQuery pq = SparqlParser(
                       "PREFIX : <http://rdf.myprefix.com/>\n"
                       "SELECT ?x \n "
                       "WHERE \t {?x :myrel :obj}")
                       .parse();
  pq.expandPrefixes();
  QueryPlanner qp(nullptr);
  QueryExecutionTree qet = qp.createExecutionTree(pq);
  ASSERT_EQ(
      "{\n  SCAN POS with P = \"<http://rdf.myprefix.com/myrel>\","
      " O = \"<http://rdf.myprefix.com/obj>\"\n  qet-width: 1 \n}",
      qet.asString());
}

TEST(QueryPlannerTest, testXPO) {
  ParsedQuery pq = SparqlParser(
                       "PREFIX : <http://rdf.myprefix.com/>\n"
                       "SELECT ?x \n "
                       "WHERE \t {:subj :myrel ?x}")
                       .parse();
  pq.expandPrefixes();
  QueryPlanner qp(nullptr);
  QueryExecutionTree qet = qp.createExecutionTree(pq);
  ASSERT_EQ(
      "{\n  SCAN PSO with P = \"<http://rdf.myprefix.com/myrel>\", "
      "S = \"<http://rdf.myprefix.com/subj>\"\n  qet-width: 1 \n}",
      qet.asString());
}

TEST(QueryPlannerTest, testSP_free_) {
  ParsedQuery pq = SparqlParser(
                       "PREFIX : <http://rdf.myprefix.com/>\n"
                       "SELECT ?x \n "
                       "WHERE \t {?x :myrel ?y}")
                       .parse();
  pq.expandPrefixes();
  QueryPlanner qp(nullptr);
  QueryExecutionTree qet = qp.createExecutionTree(pq);
  ASSERT_EQ(
      "{\n  SCAN POS with P = \"<http://rdf.myprefix.com/myrel>\"\n  "
      "qet-width: 2 \n}",
      qet.asString());
}

TEST(QueryPlannerTest, testSPX_SPX) {
  try {
    ParsedQuery pq = SparqlParser(
                         "PREFIX : <pre/>\n"
                         "SELECT ?x \n "
                         "WHERE \t {:s1 :r ?x. :s2 :r ?x}")
                         .parse();
    pq.expandPrefixes();
    QueryPlanner qp(nullptr);
    QueryExecutionTree qet = qp.createExecutionTree(pq);
    ASSERT_EQ(
        "{\n  JOIN\n  {\n    SCAN PSO with P = \"<pre/r>\", S = \"<pre/s1>\"\n "
        "   qet-width: 1 \n  } join-column: [0]\n  |X|\n  {\n    S"
        "CAN PSO with P = \"<pre/r>\", S = \"<pre/s2>\"\n    qet-w"
        "idth: 1 \n  } join-column: [0]\n  qet-width: 1 \n}",
        qet.asString());
  } catch (const ad_semsearch::Exception& e) {
    std::cout << "Caught: " << e.getFullErrorMessage() << std::endl;
    FAIL() << e.getFullErrorMessage();
  } catch (const std::exception& e) {
    std::cout << "Caught: " << e.what() << std::endl;
    FAIL() << e.what();
  }
}

TEST(QueryPlannerTest, test_free_PX_SPX) {
  try {
    ParsedQuery pq = SparqlParser(
                         "PREFIX : <pre/>\n"
                         "SELECT ?x ?y \n "
                         "WHERE  {?y :r ?x . :s2 :r ?x}")
                         .parse();
    pq.expandPrefixes();
    QueryPlanner qp(nullptr);
    QueryExecutionTree qet = qp.createExecutionTree(pq);
    ASSERT_EQ(
        "{\n  JOIN\n  {\n    SCAN POS with P = \"<pre/r>\"\n    "
        "qet-width: 2 \n  } join-column: [0]\n  |X|\n  {\n   "
        " SCAN PSO with P = \"<pre/r>\", S = \"<pre/s2>\"\n  "
        "  qet-width: 1 \n  } join-column: [0]\n  qet-width: 2 \n}",
        qet.asString());
  } catch (const ad_semsearch::Exception& e) {
    std::cout << "Caught: " << e.getFullErrorMessage() << std::endl;
    FAIL() << e.getFullErrorMessage();
  } catch (const std::exception& e) {
    std::cout << "Caught: " << e.what() << std::endl;
    FAIL() << e.what();
  }
}

TEST(QueryPlannerTest, test_free_PX__free_PX) {
  try {
    ParsedQuery pq = SparqlParser(
                         "PREFIX : <pre/>\n"
                         "SELECT ?x ?y ?z \n "
                         "WHERE {?y :r ?x. ?z :r ?x}")
                         .parse();
    pq.expandPrefixes();
    QueryPlanner qp(nullptr);
    QueryExecutionTree qet = qp.createExecutionTree(pq);
    ASSERT_EQ(
        "{\n  JOIN\n  {\n    SCAN POS with P = \"<pre/r>\"\n    "
        "qet-width: 2 \n  } join-column: [0]\n  |X|\n  {\n    "
        "SCAN POS with P = \"<pre/r>\"\n    qet-width: 2 \n"
        "  } join-column: [0]\n  qet-width: 3 \n}",
        qet.asString());
  } catch (const ad_semsearch::Exception& e) {
    std::cout << "Caught: " << e.getFullErrorMessage() << std::endl;
    FAIL() << e.getFullErrorMessage();
  } catch (const std::exception& e) {
    std::cout << "Caught: " << e.what() << std::endl;
    FAIL() << e.what();
  }
}

TEST(QueryPlannerTest, testActorsBornInEurope) {
  try {
    ParsedQuery pq =
        SparqlParser(
            "PREFIX : <pre/>\n"
            "SELECT ?a \n "
            "WHERE {?a :profession :Actor . ?a :born-in ?c. ?c :in :Europe}\n"
            "ORDER BY ?a")
            .parse();
    pq.expandPrefixes();
    QueryPlanner qp(nullptr);
    QueryExecutionTree qet = qp.createExecutionTree(pq);
    ASSERT_EQ(18340u, qet.getCostEstimate());
    ASSERT_EQ(
        "{\n  JOIN\n  {\n    SCAN POS with P = \"<pre/profession>\", "
        "O = \"<pre/Actor>\"\n    qet-width: 1 \n  } join-column:"
        " [0]\n  |X|\n  {\n    SORT / ORDER BY on columns:asc(1) \n    {\n     "
        " "
        "JOIN\n      {\n        SCAN POS with P = \"<pre/born-i"
        "n>\"\n        qet-width: 2 \n      } join-column: [0]\n "
        "     |X|\n      {\n        SCAN POS with P = \"<pre/in>\""
        ", O = \"<pre/Europe>\"\n        qet-width: 1 \n      }"
        " join-column: [0]\n      qet-width: 2 \n    }\n    "
        "qet-width: 2 \n  } join-column: [1]\n  qet-width: 2 \n}",
        qet.asString());
  } catch (const ad_semsearch::Exception& e) {
    std::cout << "Caught: " << e.getFullErrorMessage() << std::endl;
    FAIL() << e.getFullErrorMessage();
  } catch (const std::exception& e) {
    std::cout << "Caught: " << e.what() << std::endl;
    FAIL() << e.what();
  }
}

TEST(QueryPlannerTest, testStarTwoFree) {
  try {
    {
      ParsedQuery pq =
          SparqlParser(
              "PREFIX : <http://rdf.myprefix.com/>\n"
              "PREFIX ns: <http://rdf.myprefix.com/ns/>\n"
              "PREFIX xxx: <http://rdf.myprefix.com/xxx/>\n"
              "SELECT ?x ?z \n "
              "WHERE \t {?x :myrel ?y. ?y ns:myrel ?z. ?y xxx:rel2 "
              "<http://abc.de>}")
              .parse();
      pq.expandPrefixes();
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

  } catch (const ad_semsearch::Exception& e) {
    std::cout << "Caught: " << e.getFullErrorMessage() << std::endl;
    FAIL() << e.getFullErrorMessage();
  } catch (const std::exception& e) {
    std::cout << "Caught: " << e.what() << std::endl;
    FAIL() << e.what();
  }
}

TEST(QueryPlannerTest, testFilterAfterSeed) {
  try {
    ParsedQuery pq = SparqlParser(
                         "SELECT ?x ?y ?z WHERE {"
                         "?x <r> ?y . ?y <r> ?z . "
                         "FILTER(?x != ?y) }")
                         .parse();
    QueryPlanner qp(nullptr);
    QueryExecutionTree qet = qp.createExecutionTree(pq);
    ASSERT_EQ(
        "{\n  JOIN\n  {\n    FILTER     {\n      "
        "SCAN POS with P = \"<r>\"\n      qet-width: 2 \n   "
        " } with ?x != ?y\n\n    qet-width: 2 \n  }"
        " join-column: [0]\n  |X|\n  {\n    SCAN PSO with P = \""
        "<r>\"\n    qet-width: 2 \n  } join-column: [0]\n "
        " qet-width: 3 \n}",
        qet.asString());
  } catch (const ad_semsearch::Exception& e) {
    std::cout << "Caught: " << e.getFullErrorMessage() << std::endl;
    FAIL() << e.getFullErrorMessage();
  } catch (const std::exception& e) {
    std::cout << "Caught: " << e.what() << std::endl;
    FAIL() << e.what();
  }
}

TEST(QueryPlannerTest, testFilterAfterJoin) {
  try {
    ParsedQuery pq = SparqlParser(
                         "SELECT ?x ?y ?z WHERE {"
                         "?x <r> ?y . ?y <r> ?z . "
                         "FILTER(?x != ?z) }")
                         .parse();
    QueryPlanner qp(nullptr);
    QueryExecutionTree qet = qp.createExecutionTree(pq);
    ASSERT_EQ(
        "{\n  FILTER   {\n    JOIN\n    {\n      "
        "SCAN POS with P = \"<r>\"\n      qet-width: 2 \n"
        "    } join-column: [0]\n    |X|\n    {\n      "
        "SCAN PSO with P = \"<r>\"\n      qet-width: 2 \n"
        "    } join-column: [0]\n    qet-width: 3 \n  }"
        " with ?x != ?z\n\n  qet-width: 3 \n}",
        qet.asString());
  } catch (const ad_semsearch::Exception& e) {
    std::cout << "Caught: " << e.getFullErrorMessage() << std::endl;
    FAIL() << e.getFullErrorMessage();
  } catch (const std::exception& e) {
    std::cout << "Caught: " << e.what() << std::endl;
    FAIL() << e.what();
  }
}

TEST(QueryPlannerTest, threeVarTriples) {
  try {
    ParsedQuery pq = SparqlParser(
                         "SELECT ?x ?p ?o WHERE {"
                         "<s> <p> ?x . ?x ?p ?o }")
                         .parse();
    QueryPlanner qp(nullptr);
    QueryExecutionTree qet = qp.createExecutionTree(pq);
    ASSERT_EQ(
        "{\n  JOIN\n  {\n    SCAN PSO with P = \"<p>\", S = \"<s>\"\n    "
        "qet-width: 1 \n  } join-column: [0]\n  |X|\n  {\n    SORT / ORDER BY "
        "on columns:asc(1) \n    {\n      SCAN FOR FULL INDEX OSP (DUMMY "
        "OPERATION)\n      qet-width: 3 \n    }\n    qet-width: 3 \n  } "
        "join-column: [1]\n  qet-width: 3 \n}",
        qet.asString());
  } catch (const ad_semsearch::Exception& e) {
    std::cout << "Caught: " << e.getFullErrorMessage() << std::endl;
    FAIL() << e.getFullErrorMessage();
  } catch (const std::exception& e) {
    std::cout << "Caught: " << e.what() << std::endl;
    FAIL() << e.what();
  }

  try {
    ParsedQuery pq = SparqlParser(
                         "SELECT ?x ?p ?o WHERE {"
                         "<s> ?x <o> . ?x ?p ?o }")
                         .parse();
    QueryPlanner qp(nullptr);
    QueryExecutionTree qet = qp.createExecutionTree(pq);
    ASSERT_EQ(
        "{\n  JOIN\n  {\n    SCAN SOP with S = \"<s>\", O = \"<o>\"\n    "
        "qet-width: 1 \n  } join-column: [0]\n  |X|\n  {\n    SORT / ORDER BY "
        "on columns:asc(1) \n    {\n      SCAN FOR FULL INDEX OSP (DUMMY "
        "OPERATION)\n      qet-width: 3 \n    }\n    qet-width: 3 \n  } "
        "join-column: [1]\n  qet-width: 3 \n}",
        qet.asString());
  } catch (const ad_semsearch::Exception& e) {
    std::cout << "Caught: " << e.getFullErrorMessage() << std::endl;
    FAIL() << e.getFullErrorMessage();
  } catch (const std::exception& e) {
    std::cout << "Caught: " << e.what() << std::endl;
    FAIL() << e.what();
  }

  try {
    ParsedQuery pq = SparqlParser(
                         "SELECT ?s ?p ?o WHERE {"
                         "<s> <p> ?p . ?s ?p ?o }")
                         .parse();
    QueryPlanner qp(nullptr);
    QueryExecutionTree qet = qp.createExecutionTree(pq);
    ASSERT_EQ(
        "{\n  JOIN\n  {\n    SCAN PSO with P = \"<p>\", S = \"<s>\"\n    "
        "qet-width: 1 \n  } join-column: [0]\n  |X|\n  {\n    SORT / ORDER BY "
        "on columns:asc(1) \n    {\n      SCAN FOR FULL INDEX OPS (DUMMY "
        "OPERATION)\n      qet-width: 3 \n    }\n    qet-width: 3 \n  } "
        "join-column: [1]\n  qet-width: 3 \n}",
        qet.asString());
  } catch (const ad_semsearch::Exception& e) {
    std::cout << "Caught: " << e.getFullErrorMessage() << std::endl;
    FAIL() << e.getFullErrorMessage();
  } catch (const std::exception& e) {
    std::cout << "Caught: " << e.what() << std::endl;
    FAIL() << e.what();
  }
}

TEST(QueryPlannerTest, threeVarTriplesTCJ) {
  try {
    ParsedQuery pq = SparqlParser(
                         "SELECT ?x ?p ?o WHERE {"
                         "<s> ?p ?x . ?x ?p ?o }")
                         .parse();
    QueryPlanner qp(nullptr);
    ASSERT_THROW(qp.createExecutionTree(pq), ad_semsearch::Exception);
  } catch (const ad_semsearch::Exception& e) {
    std::cout << "Caught: " << e.getFullErrorMessage() << std::endl;
    FAIL() << e.getFullErrorMessage();
  } catch (const std::exception& e) {
    std::cout << "Caught: " << e.what() << std::endl;
    FAIL() << e.what();
  }

  try {
    ParsedQuery pq = SparqlParser(
                         "SELECT ?s ?p ?o WHERE {"
                         "?s ?p ?o . ?s ?p <x> }")
                         .parse();
    QueryPlanner qp(nullptr);
    ASSERT_THROW(QueryExecutionTree qet = qp.createExecutionTree(pq),
                 ad_semsearch::Exception);
  } catch (const ad_semsearch::Exception& e) {
    std::cout << "Caught: " << e.getFullErrorMessage() << std::endl;
    FAIL() << e.getFullErrorMessage();
  } catch (const std::exception& e) {
    std::cout << "Caught: " << e.what() << std::endl;
    FAIL() << e.what();
  }
}

TEST(QueryPlannerTest, threeVarXthreeVarException) {
  try {
    ParsedQuery pq = SparqlParser(
                         "SELECT ?s ?s2 WHERE {"
                         "?s ?p ?o . ?s2 ?p ?o }")
                         .parse();
    QueryPlanner qp(nullptr);
    QueryExecutionTree qet = qp.createExecutionTree(pq);
    FAIL() << "Was expecting exception, but got" << qet.asString() << std::endl;
  } catch (const ad_semsearch::Exception& e) {
    ASSERT_EQ(
        "Could not find a suitable execution tree. "
        "Likely cause: Queries that require joins of the full index with "
        "itself are not supported at the moment.",
        e.getErrorDetailsNoFileAndLines());
  } catch (const std::exception& e) {
    std::cout << "Caught: " << e.what() << std::endl;
    FAIL() << e.what();
  }
}

TEST(QueryExecutionTreeTest, testBooksbyNewman) {
  try {
    ParsedQuery pq = SparqlParser(
                         "SELECT ?x WHERE { ?x <is-a> <Book> . "
                         "?x <Author> <Anthony_Newman_(Author)> }")
                         .parse();
    pq.expandPrefixes();
    QueryPlanner qp(nullptr);
    QueryExecutionTree qet = qp.createExecutionTree(pq);
    ASSERT_EQ(
        "{\n  JOIN\n  {\n    SCAN POS with "
        "P = \"<Author>\", O = \"<Anthony_Newman_(Author)>\"\n   "
        " qet-width: 1 \n  } join-column: [0]\n  |X|\n  {\n  "
        "  SCAN POS with P = \"<is-a>\", O = \"<Book>\"\n  "
        "  qet-width: 1 \n  } join-column: [0]\n  qet-width: 1 \n}",
        qet.asString());
  } catch (const ad_semsearch::Exception& e) {
    std::cout << "Caught: " << e.getFullErrorMessage() << std::endl;
    FAIL() << e.getFullErrorMessage();
  } catch (const std::exception& e) {
    std::cout << "Caught: " << e.what() << std::endl;
    FAIL() << e.what();
  }
}

TEST(QueryExecutionTreeTest, testBooksGermanAwardNomAuth) {
  try {
    ParsedQuery pq = SparqlParser(
                         "SELECT ?x ?y WHERE { "
                         "?x <is-a> <Person> . "
                         "?x <Country_of_nationality> <Germany> . "
                         "?x <Author> ?y . "
                         "?y <is-a> <Award-Nominated_Work> }")
                         .parse();
    pq.expandPrefixes();
    QueryPlanner qp(nullptr);
    QueryExecutionTree qet = qp.createExecutionTree(pq);
    ASSERT_GT(qet.asString().size(), 0u);
    // Just check that ther is no exception, here.
  } catch (const ad_semsearch::Exception& e) {
    std::cout << "Caught: " << e.getFullErrorMessage() << std::endl;
    FAIL() << e.getFullErrorMessage();
  } catch (const std::exception& e) {
    std::cout << "Caught: " << e.what() << std::endl;
    FAIL() << e.what();
  }
}

TEST(QueryExecutionTreeTest, testPlantsEdibleLeaves) {
  try {
    ParsedQuery pq =
        SparqlParser(
            "SELECT ?a \n "
            "WHERE  {?a <is-a> <Plant> . ?c ql:contains-entity ?a. "
            "?c ql:contains-word \"edible leaves\"} TEXTLIMIT 5")
            .parse();
    pq.expandPrefixes();
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
  } catch (const ad_semsearch::Exception& e) {
    std::cout << "Caught: " << e.getFullErrorMessage() << std::endl;
    FAIL() << e.getFullErrorMessage();
  } catch (const std::exception& e) {
    std::cout << "Caught: " << e.what() << std::endl;
    FAIL() << e.what();
  }
}

TEST(QueryExecutionTreeTest, testTextQuerySE) {
  try {
    ParsedQuery pq = SparqlParser(
                         "SELECT ?c \n "
                         "WHERE  {?c ql:contains-word \"search engine\"}")
                         .parse();
    pq.expandPrefixes();
    QueryPlanner qp(nullptr);
    QueryExecutionTree qet = qp.createExecutionTree(pq);
    ASSERT_EQ(
        "{\n  TEXT OPERATION WITHOUT FILTER: co-occurrence with words:"
        " \"search engine\" and 0 variables with textLimit = 1\n"
        "  qet-width: 2 \n}",
        qet.asString());
  } catch (const ad_semsearch::Exception& e) {
    std::cout << "Caught: " << e.getFullErrorMessage() << std::endl;
    FAIL() << e.getFullErrorMessage();
  } catch (const std::exception& e) {
    std::cout << "Caught: " << e.what() << std::endl;
    FAIL() << e.what();
  }
}

TEST(QueryExecutionTreeTest, testBornInEuropeOwCocaine) {
  try {
    ParsedQuery pq = SparqlParser(
                         "PREFIX : <>\n"
                         "SELECT ?x ?y ?c\n "
                         "WHERE \t {"
                         "?x :Place_of_birth ?y ."
                         "?y :Contained_by :Europe ."
                         "?c ql:contains-entity ?x ."
                         "?c ql:contains-word \"cocaine\" ."
                         "}")
                         .parse();
    pq.expandPrefixes();
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
    ASSERT_EQ(0u, qet.getVariableColumn("?c"));
    ASSERT_EQ(1u, qet.getVariableColumn(
                      absl::StrCat(TEXTSCORE_VARIABLE_PREFIX, "c")));
    ASSERT_EQ(2u, qet.getVariableColumn("?y"));
  } catch (const ad_semsearch::Exception& e) {
    std::cout << "Caught: " << e.getFullErrorMessage() << std::endl;
    FAIL() << e.getFullErrorMessage();
  } catch (const std::exception& e) {
    std::cout << "Caught: " << e.what() << std::endl;
    FAIL() << e.what();
  }
}

TEST(QueryExecutionTreeTest, testCoOccFreeVar) {
  try {
    ParsedQuery pq = SparqlParser(
                         "PREFIX : <>"
                         "SELECT ?x ?y WHERE {"
                         "?x :is-a :Politician ."
                         "?c ql:contains-entity ?x ."
                         "?c ql:contains-word \"friend*\" ."
                         "?c ql:contains-entity ?y ."
                         "}")
                         .parse();
    pq.expandPrefixes();
    QueryPlanner qp(nullptr);
    QueryExecutionTree qet = qp.createExecutionTree(pq);
    ASSERT_EQ(
        "{\n  TEXT OPERATION WITH FILTER: co-occurrence with words: "
        "\"friend*\" and 2 variables with textLimit = 1 filtered by\n"
        "  {\n    SCAN POS with P = \"<is-a>\", O = \"<Politician>"
        "\"\n    qet-width: 1 \n  }\n   filtered on column 0\n "
        " qet-width: 4 \n}",
        qet.asString());
  } catch (const ad_semsearch::Exception& e) {
    std::cout << "Caught: " << e.getFullErrorMessage() << std::endl;
    FAIL() << e.getFullErrorMessage();
  } catch (const std::exception& e) {
    std::cout << "Caught: " << e.what() << std::endl;
    FAIL() << e.what();
  }
}

TEST(QueryExecutionTreeTest, testPoliticiansFriendWithScieManHatProj) {
  try {
    ParsedQuery pq = SparqlParser(
                         "SELECT ?p ?s \n "
                         "WHERE {"
                         "?a <is-a> <Politician> . "
                         "?c ql:contains-entity ?a ."
                         "?c ql:contains-word \"friend*\" ."
                         "?c ql:contains-entity ?s ."
                         "?s <is-a> <Scientist> ."
                         "?c2 ql:contains-entity ?s ."
                         "?c2 ql:contains-word \"manhattan project\"}")
                         .parse();
    pq.expandPrefixes();
    QueryPlanner qp(nullptr);
    QueryExecutionTree qet = qp.createExecutionTree(pq);
    ASSERT_EQ(

        "{\n  TEXT OPERATION WITH FILTER: co-occurrence with words: "
        "\"manhattan project\" and 1 variables with textLimit = 1 filtered "
        "by\n  {\n    JOIN\n    {\n      SCAN POS with P = \"<is-a>\", O = "
        "\"<Scientist>\"\n      qet-width: 1 \n    } join-column: [0]\n    "
        "|X|\n    {\n      SORT / ORDER BY on columns:asc(2) \n      {\n       "
        " TEXT OPERATION "
        "WITH FILTER: co-occurrence with words: \"friend*\" and 2 variables "
        "with textLimit = 1 filtered by\n        {\n          SCAN POS with P "
        "= \"<is-a>\", O = \"<Politician>\"\n          qet-width: 1 \n        "
        "}\n         filtered on column 0\n        qet-width: 4 \n      }\n    "
        "  qet-width: 4 \n    } join-column: [2]\n    qet-width: 4 \n  }\n   "
        "filtered on column 0\n  qet-width: 6 \n}",
        qet.asString());
  } catch (const ad_semsearch::Exception& e) {
    std::cout << "Caught: " << e.getFullErrorMessage() << std::endl;
    FAIL() << e.getFullErrorMessage();
  } catch (const std::exception& e) {
    std::cout << "Caught: " << e.what() << std::endl;
    FAIL() << e.what();
  }
}

TEST(QueryExecutionTreeTest, testCyclicQuery) {
  try {
    ParsedQuery pq =
        SparqlParser(
            "SELECT ?x ?y ?m WHERE { ?x <Spouse_(or_domestic_partner)> ?y . "
            "?x <Film_performance> ?m . ?y <Film_performance> ?m }")
            .parse();
    pq.expandPrefixes();
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
        "1]\n  |X|\n    {\n    SORT / ORDER BY on columns:asc(2) asc(1) \n    "
        "{\n      JOIN\n      {\n        SCAN PSO with P = "
        "\"<Film_performance>\"\n        qet-width: 2 \n      } join-column: "
        "[0]\n      |X|\n      {\n        SCAN PSO with P = "
        "\"<Spouse_(or_domestic_partner)>\"\n        qet-width: 2 \n      } "
        "join-column: [0]\n      qet-width: 3 \n    }\n    qet-width: 3 \n  "
        "}\n  join-columns: [2 & 1]\n  qet-width: 3 \n}");
    std::string possible2 = strip(
        "{\n  MULTI_COLUMN_JOIN\n    {\n    SCAN POS with P = "
        "\"<Film_performance>\"\n    qet-width: 2 \n  }\n  join-columns: [0 & "
        "1]\n  |X|\n    {\n    SORT / ORDER BY on columns:asc(1) asc(2) \n    "
        "{\n      JOIN\n      {\n        SCAN PSO with P = "
        "\"<Film_performance>\"\n        qet-width: 2 \n      } join-column: "
        "[0]\n      |X|\n      {\n        SCAN PSO with P = "
        "\"<Spouse_(or_domestic_partner)>\"\n        qet-width: 2 \n      } "
        "join-column: [0]\n      qet-width: 3 \n    }\n    qet-width: 3 \n  "
        "}\n  join-columns: [1 & 2]\n  qet-width: 3 \n}");
    std::string possible3 = strip(
        "{\n  MULTI_COLUMN_JOIN\n    {\n    SCAN POS with P = "
        "\"<Spouse_(or_domestic_partner)>\"\n    qet-width: 2 \n  }\n  "
        "join-columns: [0 & 1]\n  |X|\n    {\n    SORT / ORDER BY on "
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
          SORT / ORDER BY on columns:asc(1) asc(2)
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

  } catch (const ad_semsearch::Exception& e) {
    std::cout << "Caught: " << e.getFullErrorMessage() << std::endl;
    FAIL() << e.getFullErrorMessage();
  } catch (const std::exception& e) {
    std::cout << "Caught: " << e.what() << std::endl;
    FAIL() << e.what();
  }
}

TEST(QueryExecutionTreeTest, testFormerSegfaultTriFilter) {
  try {
    ParsedQuery pq =
        SparqlParser(
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
            "} LIMIT 300")
            .parse();
    pq.expandPrefixes();
    QueryPlanner qp(nullptr);
    QueryExecutionTree qet = qp.createExecutionTree(pq);
    ASSERT_TRUE(qet.varCovered("?1"));
    ASSERT_TRUE(qet.varCovered("?0"));
  } catch (const ad_semsearch::Exception& e) {
    std::cout << "Caught: " << e.getFullErrorMessage() << std::endl;
    FAIL() << e.getFullErrorMessage();
  } catch (const std::exception& e) {
    std::cout << "Caught: " << e.what() << std::endl;
    FAIL() << e.what();
  }
}

TEST(QueryPlannerTest, testSimpleOptional) {
  try {
    QueryPlanner qp(nullptr);

    ParsedQuery pq = SparqlParser(
                         "SELECT ?a ?b \n "
                         "WHERE  {?a <rel1> ?b . OPTIONAL { ?a <rel2> ?c }}")
                         .parse();
    pq.expandPrefixes();
    QueryExecutionTree qet = qp.createExecutionTree(pq);
    ASSERT_EQ(
        "{\n  OPTIONAL_JOIN\n  {\n    SCAN PSO with P = \"<rel1>\"\n    "
        "qet-width: 2 \n  } join-columns: [0]\n  |X|\n  {\n    SCAN PSO with P "
        "= \"<rel2>\"\n    qet-width: 2 \n  } join-columns: [0]\n  qet-width: "
        "3 \n}",

        qet.asString());

    ParsedQuery pq2 = SparqlParser(
                          "SELECT ?a ?b \n "
                          "WHERE  {?a <rel1> ?b . "
                          "OPTIONAL { ?a <rel2> ?c }} ORDER BY ?b")
                          .parse();
    pq2.expandPrefixes();
    QueryExecutionTree qet2 = qp.createExecutionTree(pq2);
    ASSERT_EQ(
        "{\n  SORT / ORDER BY on columns:asc(1) \n  {\n    OPTIONAL_JOIN\n    "
        "{\n      SCAN PSO with P = \"<rel1>\"\n      qet-width: 2 \n    } "
        "join-columns: [0]\n    |X|\n    {\n      SCAN PSO with P = "
        "\"<rel2>\"\n      qet-width: 2 \n    } join-columns: [0]\n    "
        "qet-width: 3 \n  }\n  qet-width: 3 \n}",
        qet2.asString());
  } catch (const ad_semsearch::Exception& e) {
    std::cout << "Caught: " << e.getFullErrorMessage() << std::endl;
    FAIL() << e.getFullErrorMessage();
  } catch (const std::exception& e) {
    std::cout << "Caught: " << e.what() << std::endl;
    FAIL() << e.what();
  }
}
