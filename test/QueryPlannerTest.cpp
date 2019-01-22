// Copyright 2015, University of Freiburg, Chair of Algorithms and Data
// Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)

#include <gtest/gtest.h>

#include "../src/engine/QueryPlanner.h"
#include "../src/parser/SparqlParser.h"

TEST(QueryPlannerTest, createTripleGraph) {
  try {
    {
      ParsedQuery pq = SparqlParser::parse(
          "PREFIX : <http://rdf.myprefix.com/>\n"
          "PREFIX ns: <http://rdf.myprefix.com/ns/>\n"
          "PREFIX xxx: <http://rdf.myprefix.com/xxx/>\n"
          "SELECT ?x ?z \n "
          "WHERE \t {?x :myrel ?y. ?y ns:myrel ?z.?y xxx:rel2 "
          "<http://abc.de>}");
      pq.expandPrefixes();
      QueryPlanner qp(nullptr);
      auto tg = qp.createTripleGraph(&pq._rootGraphPattern);
      ASSERT_EQ(
          "0 {s: ?x, p: <http://rdf.myprefix.com/myrel>, o: ?y} : (1, 2)\n"
          "1 {s: ?y, p: <http://rdf.myprefix.com/ns/myrel>, o: ?z} : (0, 2)\n"
          "2 {s: ?y, p: <http://rdf.myprefix.com/xxx/rel2>, o: "
          "<http://abc.de>} : (0, 1)",
          tg.asString());
    }

    {
      ParsedQuery pq = SparqlParser::parse(
          "SELECT ?x WHERE {?x ?p <X>. ?x ?p2 <Y>. <X> ?p <Y>}");
      pq.expandPrefixes();
      QueryPlanner qp(nullptr);
      auto tg = qp.createTripleGraph(&pq._rootGraphPattern);
      ASSERT_EQ(
          "0 {s: ?x, p: ?p, o: <X>} : (1, 2)\n"
          "1 {s: ?x, p: ?p2, o: <Y>} : (0)\n"
          "2 {s: <X>, p: ?p, o: <Y>} : (0)",
          tg.asString());
    }

    {
      ParsedQuery pq = SparqlParser::parse(
          "SELECT ?x WHERE { ?x <is-a> <Book> . \n"
          "?x <Author> <Anthony_Newman_(Author)> }");
      pq.expandPrefixes();
      QueryPlanner qp(nullptr);
      auto tg = qp.createTripleGraph(&pq._rootGraphPattern);
      ASSERT_EQ(
          "0 {s: ?x, p: <is-a>, o: <Book>} : (1)\n"
          "1 {s: ?x, p: <Author>, o: <Anthony_Newman_(Author)>} : (0)",
          tg.asString());
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
      ParsedQuery pq = SparqlParser::parse(
          "SELECT ?x WHERE {?x ?p <X>. ?x ?p2 <Y>. <X> ?p <Y>}");
      pq.expandPrefixes();
      QueryPlanner qp(nullptr);
      auto tg = qp.createTripleGraph(&pq._rootGraphPattern);
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
      ParsedQuery pq = SparqlParser::parse(
          "SELECT ?x WHERE {?x ?p <X>. ?x ?p2 <Y>. <X> ?p <Y>}");
      pq.expandPrefixes();
      QueryPlanner qp(nullptr);
      auto tg = qp.createTripleGraph(&pq._rootGraphPattern);
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
      ParsedQuery pq = SparqlParser::parse(
          "SELECT ?x WHERE {<A> <B> ?x. ?x <C> ?y. ?y <X> <Y>}");
      pq.expandPrefixes();
      QueryPlanner qp(nullptr);
      auto tg = qp.createTripleGraph(&pq._rootGraphPattern);
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
  try {
    {
      {
        ParsedQuery pq = SparqlParser::parse(
            "SELECT ?x WHERE {?x <p> <X>. ?c ql:contains-entity ?x. ?c "
            "ql:contains-word abc}");
        pq.expandPrefixes();
        QueryPlanner qp(nullptr);
        auto tg = qp.createTripleGraph(&pq._rootGraphPattern);
        ASSERT_EQ(
            "0 {s: ?x, p: <p>, o: <X>} : (1)\n"
            "1 {s: ?c, p: <QLever-internal-function/contains-entity>, o: ?x} : "
            "(0, 2)\n"
            "2 {s: ?c, p: <QLever-internal-function/contains-word>, o: abc} : "
            "(1)",
            tg.asString());
        tg.collapseTextCliques();
        ASSERT_EQ(
            "0 {TextOP for ?c, wordPart: \"abc\"} : (1)\n"
            "1 {s: ?x, p: <p>, o: <X>} : (0)",
            tg.asString());
        ASSERT_EQ(2ul, tg._nodeMap[0]->_variables.size());
        ASSERT_EQ(1ul, tg._nodeMap[1]->_variables.size());
      }
      {
        ParsedQuery pq = SparqlParser::parse(
            "SELECT ?x WHERE {?x <p> <X>. ?c "
            "<QLever-internal-function/contains-entity> ?x. ?c "
            "<QLever-internal-function/contains-word> abc . ?c "
            "ql:contains-entity ?y}");
        pq.expandPrefixes();
        QueryPlanner qp(nullptr);
        auto tg = qp.createTripleGraph(&pq._rootGraphPattern);
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
        ASSERT_EQ(
            "0 {TextOP for ?c, wordPart: \"abc\"} : (1)\n"
            "1 {s: ?x, p: <p>, o: <X>} : (0)",
            tg.asString());
        ASSERT_EQ(3ul, tg._nodeMap[0]->_variables.size());
        ASSERT_EQ(1ul, tg._nodeMap[1]->_variables.size());
      }
      {
        ParsedQuery pq = SparqlParser::parse(
            "SELECT ?x WHERE {?x <p> <X>. ?c ql:contains-entity ?x. ?c "
            "ql:contains-word abc . ?c ql:contains-entity ?y. ?y <P2> <X2>}");
        pq.expandPrefixes();
        QueryPlanner qp(nullptr);
        auto tg = qp.createTripleGraph(&pq._rootGraphPattern);
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
        ASSERT_EQ(
            "0 {TextOP for ?c, wordPart: \"abc\"} : (1, 2)\n"
            "1 {s: ?x, p: <p>, o: <X>} : (0)\n"
            "2 {s: ?y, p: <P2>, o: <X2>} : (0)",
            tg.asString());
        ASSERT_EQ(3ul, tg._nodeMap[0]->_variables.size());
        ASSERT_EQ(1ul, tg._nodeMap[1]->_variables.size());
        ASSERT_EQ(1ul, tg._nodeMap[2]->_variables.size());
      }
      {
        ParsedQuery pq = SparqlParser::parse(
            "(SELECT ?x WHERE {?x <p> <X>. ?c ql:contains-entity ?x. ?c "
            "ql:contains-word \"abc\" . ?c ql:contains-entity ?y. ?c2 "
            "ql:contains-entity ?y. ?c2 ql:contains-word \"xx\"})");
        pq.expandPrefixes();
        QueryPlanner qp(nullptr);
        auto tg = qp.createTripleGraph(&pq._rootGraphPattern);
        ASSERT_EQ(
            "0 {s: ?x, p: <p>, o: <X>} : (1)\n"
            "1 {s: ?c, p: <QLever-internal-function/contains-entity>, o: ?x} : "
            "(0, 2, 3)\n"
            "2 {s: ?c, p: <QLever-internal-function/contains-word>, o: abc} : "
            "(1, 3)\n"
            "3 {s: ?c, p: <QLever-internal-function/contains-entity>, o: ?y} : "
            "(1, 2, 4)\n"
            "4 {s: ?c2, p: <QLever-internal-function/contains-entity>, o: ?y} "
            ": (3, 5)\n"
            "5 {s: ?c2, p: <QLever-internal-function/contains-word>, o: xx} : "
            "(4)",
            tg.asString());
        tg.collapseTextCliques();
        ASSERT_EQ(
            "0 {TextOP for ?c, wordPart: \"abc\"} : (1, 2)\n"
            "1 {TextOP for ?c2, wordPart: \"xx\"} : (0)\n"
            "2 {s: ?x, p: <p>, o: <X>} : (0)",
            tg.asString());
        ASSERT_EQ(3ul, tg._nodeMap[0]->_variables.size());
        ASSERT_EQ(2ul, tg._nodeMap[1]->_variables.size());
        ASSERT_EQ(1ul, tg._nodeMap[2]->_variables.size());
      }
      {
        ParsedQuery pq = SparqlParser::parse(
            "SELECT ?x WHERE {?x <p> <X>. ?c ql:contains-entity ?x. ?c "
            "ql:contains-word abc . ?c ql:contains-entity ?y. ?c2 "
            "ql:contains-entity ?y. ?c2 ql:contains-word xx. ?y <P2> <X2>}");
        pq.expandPrefixes();
        QueryPlanner qp(nullptr);
        auto tg = qp.createTripleGraph(&pq._rootGraphPattern);
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
        ASSERT_EQ(
            "0 {TextOP for ?c, wordPart: \"abc\"} : (1, 2, 3)\n"
            "1 {TextOP for ?c2, wordPart: \"xx\"} : (0, 3)\n"
            "2 {s: ?x, p: <p>, o: <X>} : (0)\n"
            "3 {s: ?y, p: <P2>, o: <X2>} : (0, 1)",
            tg.asString());
        ASSERT_EQ(3ul, tg._nodeMap[0]->_variables.size());
        ASSERT_EQ(2ul, tg._nodeMap[1]->_variables.size());
        ASSERT_EQ(1ul, tg._nodeMap[2]->_variables.size());
        ASSERT_EQ(1ul, tg._nodeMap[3]->_variables.size());
      }
    }
  } catch (const std::exception& e) {
    std::cout << "Caught: " << e.what() << std::endl;
    FAIL() << e.what();
  }
}

TEST(QueryPlannerTest, testSPX) {
  ParsedQuery pq = SparqlParser::parse(
      "PREFIX : <http://rdf.myprefix.com/>\n"
      "SELECT ?x \n "
      "WHERE \t {?x :myrel :obj}");
  pq.expandPrefixes();
  QueryPlanner qp(nullptr);
  QueryExecutionTree qet = qp.createExecutionTree(pq);
  ASSERT_EQ(
      "{\n  SCAN POS with P = \"<http://rdf.myprefix.com/myrel>\","
      " O = \"<http://rdf.myprefix.com/obj>\"\n  qet-width: 1 \n}",
      qet.asString());
}

TEST(QueryPlannerTest, testXPO) {
  ParsedQuery pq = SparqlParser::parse(
      "PREFIX : <http://rdf.myprefix.com/>\n"
      "SELECT ?x \n "
      "WHERE \t {:subj :myrel ?x}");
  pq.expandPrefixes();
  QueryPlanner qp(nullptr);
  QueryExecutionTree qet = qp.createExecutionTree(pq);
  ASSERT_EQ(
      "{\n  SCAN PSO with P = \"<http://rdf.myprefix.com/myrel>\", "
      "S = \"<http://rdf.myprefix.com/subj>\"\n  qet-width: 1 \n}",
      qet.asString());
}

TEST(QueryPlannerTest, testSP_free_) {
  ParsedQuery pq = SparqlParser::parse(
      "PREFIX : <http://rdf.myprefix.com/>\n"
      "SELECT ?x \n "
      "WHERE \t {?x :myrel ?y}");
  pq.expandPrefixes();
  QueryPlanner qp(nullptr);
  QueryExecutionTree qet = qp.createExecutionTree(pq);
  ASSERT_EQ(
      "{\n  SCAN PSO with P = \"<http://rdf.myprefix.com/myrel>\"\n  "
      "qet-width: 2 \n}",
      qet.asString());
}

TEST(QueryPlannerTest, testSPX_SPX) {
  try {
    ParsedQuery pq = SparqlParser::parse(
        "PREFIX : <pre/>\n"
        "SELECT ?x \n "
        "WHERE \t {:s1 :r ?x. :s2 :r ?x}");
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
    ParsedQuery pq = SparqlParser::parse(
        "PREFIX : <pre/>\n"
        "SELECT ?x ?y \n "
        "WHERE  {?y :r ?x . :s2 :r ?x}");
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
    ParsedQuery pq = SparqlParser::parse(
        "PREFIX : <pre/>\n"
        "SELECT ?x ?y ?z \n "
        "WHERE {?y :r ?x. ?z :r ?x}");
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
    ParsedQuery pq = SparqlParser::parse(
        "PREFIX : <pre/>\n"
        "SELECT ?a \n "
        "WHERE {?a :profession :Actor . ?a :born-in ?c. ?c :in :Europe}\n"
        "ORDER BY ?a");
    pq.expandPrefixes();
    QueryPlanner qp(nullptr);
    QueryExecutionTree qet = qp.createExecutionTree(pq);
    ASSERT_EQ(
        "{\n  JOIN\n  {\n    SCAN POS with P = \"<pre/profession>\", "
        "O = \"<pre/Actor>\"\n    qet-width: 1 \n  } join-column:"
        " [0]\n  |X|\n  {\n    SORT on column:1\n    {\n      "
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
      ParsedQuery pq = SparqlParser::parse(
          "PREFIX : <http://rdf.myprefix.com/>\n"
          "PREFIX ns: <http://rdf.myprefix.com/ns/>\n"
          "PREFIX xxx: <http://rdf.myprefix.com/xxx/>\n"
          "SELECT ?x ?z \n "
          "WHERE \t {?x :myrel ?y. ?y ns:myrel ?z. ?y xxx:rel2 "
          "<http://abc.de>}");
      pq.expandPrefixes();
      QueryPlanner qp(nullptr);
      QueryExecutionTree qet = qp.createExecutionTree(pq);
      ASSERT_EQ(
          "{\n  JOIN\n  {\n    JOIN\n    {\n      SCAN POS with P = \""
          "<http://rdf.myprefix.com/myrel>\"\n      qet-width: 2 \n "
          "   } join-column: [0]\n    |X|\n    {\n      SCAN PSO wit"
          "h P = \"<http://rdf.myprefix.com/ns/myrel>\"\n      qet-"
          "width: 2 \n    } join-column: [0]\n    qet-width: 3 \n  "
          "} join-column: [0]\n  |X|\n  {\n    SCAN POS with P = "
          "\"<http://rdf.myprefix.com/xxx/rel2>\", O = \"<http://a"
          "bc.de>\"\n    qet-width: 1 \n  } join-column: [0]\n  qet"
          "-width: 3 \n}",
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
    ParsedQuery pq = SparqlParser::parse(
        "SELECT ?x ?y ?z WHERE {"
        "?x <r> ?y . ?y <r> ?z . "
        "FILTER(?x != ?y) }");
    QueryPlanner qp(nullptr);
    QueryExecutionTree qet = qp.createExecutionTree(pq);
    ASSERT_EQ(
        "{\n  JOIN\n  {\n    FILTER     {\n      "
        "SCAN POS with P = \"<r>\"\n      qet-width: 2 \n   "
        " }\n     with ?x != ?y\n    qet-width: 2 \n  }"
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
    ParsedQuery pq = SparqlParser::parse(
        "SELECT ?x ?y ?z WHERE {"
        "?x <r> ?y . ?y <r> ?z . "
        "FILTER(?x != ?z) }");
    QueryPlanner qp(nullptr);
    QueryExecutionTree qet = qp.createExecutionTree(pq);
    ASSERT_EQ(
        "{\n  FILTER   {\n    JOIN\n    {\n      "
        "SCAN POS with P = \"<r>\"\n      qet-width: 2 \n"
        "    } join-column: [0]\n    |X|\n    {\n      "
        "SCAN PSO with P = \"<r>\"\n      qet-width: 2 \n"
        "    } join-column: [0]\n    qet-width: 3 \n  }\n"
        "   with ?x != ?z\n  qet-width: 3 \n}",
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
    ParsedQuery pq = SparqlParser::parse(
        "SELECT ?x ?p ?o WHERE {"
        "<s> <p> ?x . ?x ?p ?o }");
    QueryPlanner qp(nullptr);
    QueryExecutionTree qet = qp.createExecutionTree(pq);
    ASSERT_EQ(
        "{\n  JOIN\n  {\n    SCAN FOR FULL INDEX SPO (DUMMY OPERATION)\n   "
        " qet-width: 3 \n  } join-column: [0]\n  |X|\n  {\n    SCAN"
        " PSO with P = \"<p>\", S = \"<s>\"\n    qet-width: 1 \n  }"
        " join-column: [0]\n  qet-width: 3 \n}",
        qet.asString());
  } catch (const ad_semsearch::Exception& e) {
    std::cout << "Caught: " << e.getFullErrorMessage() << std::endl;
    FAIL() << e.getFullErrorMessage();
  } catch (const std::exception& e) {
    std::cout << "Caught: " << e.what() << std::endl;
    FAIL() << e.what();
  }

  try {
    ParsedQuery pq = SparqlParser::parse(
        "SELECT ?x ?p ?o WHERE {"
        "<s> ?x <o> . ?x ?p ?o }");
    QueryPlanner qp(nullptr);
    QueryExecutionTree qet = qp.createExecutionTree(pq);
    ASSERT_EQ(
        "{\n  JOIN\n  {\n    SCAN FOR FULL INDEX SPO (DUMMY OP"
        "ERATION)\n    qet-width: 3 \n  } join-column: [0]"
        "\n  |X|\n  {\n    SCAN SOP with S = \"<s>\", O = \"<o>\"\n "
        "   qet-width: 1 \n  } join-column: [0]\n  qet-width: 3 \n}",
        qet.asString());
  } catch (const ad_semsearch::Exception& e) {
    std::cout << "Caught: " << e.getFullErrorMessage() << std::endl;
    FAIL() << e.getFullErrorMessage();
  } catch (const std::exception& e) {
    std::cout << "Caught: " << e.what() << std::endl;
    FAIL() << e.what();
  }

  try {
    ParsedQuery pq = SparqlParser::parse(
        "SELECT ?s ?p ?o WHERE {"
        "<s> <p> ?p . ?s ?p ?o }");
    QueryPlanner qp(nullptr);
    QueryExecutionTree qet = qp.createExecutionTree(pq);
    ASSERT_EQ(
        "{\n  JOIN\n  {\n    SCAN FOR FULL INDEX PSO (DUMMY OPERATION)\n"
        "    qet-width: 3 \n  } join-column: [0]\n  |X|\n  {\n "
        "   SCAN PSO with P = \"<p>\", S = \"<s>\"\n  "
        "  qet-width: 1 \n  } join-column: [0]\n  qet-width: 3 \n}",
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
    ParsedQuery pq = SparqlParser::parse(
        "SELECT ?x ?p ?o WHERE {"
        "<s> ?p ?x . ?x ?p ?o }");
    QueryPlanner qp(nullptr);
    QueryExecutionTree qet = qp.createExecutionTree(pq);
    ASSERT_EQ(
        "{\n  TWO_COLUMN_JOIN\n    {\n    "
        "SCAN FOR FULL INDEX SPO (DUMMY OPERATION)\n    qet-width: 3 \n  }"
        "\n  join-columns: [0 & 1]\n  |X|\n    {\n    SCAN SOP with S ="
        " \"<s>\"\n    qet-width: 2 \n  }\n  join-columns: [0 & 1]\n "
        " qet-width: 3 \n}",
        qet.asString());
  } catch (const ad_semsearch::Exception& e) {
    std::cout << "Caught: " << e.getFullErrorMessage() << std::endl;
    FAIL() << e.getFullErrorMessage();
  } catch (const std::exception& e) {
    std::cout << "Caught: " << e.what() << std::endl;
    FAIL() << e.what();
  }

  try {
    ParsedQuery pq = SparqlParser::parse(
        "SELECT ?s ?p ?o WHERE {"
        "?s ?p ?o . ?s ?p <x> }");
    QueryPlanner qp(nullptr);
    QueryExecutionTree qet = qp.createExecutionTree(pq);
    ASSERT_EQ(
        "{\n  TWO_COLUMN_JOIN\n    {\n    SCAN FOR FULL INDEX SPO"
        " (DUMMY OPERATION)\n    qet-width: 3 \n  }\n  join-"
        "columns: [0 & 1]\n  |X|\n    {\n    SCAN OSP with "
        "O = \"<x>\"\n    qet-width: 2 \n  }\n  join-columns"
        ": [0 & 1]\n  qet-width: 3 \n}",
        qet.asString());
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
    ParsedQuery pq = SparqlParser::parse(
        "SELECT ?s ?s2 WHERE {"
        "?s ?p ?o . ?s2 ?p ?o }");
    QueryPlanner qp(nullptr);
    QueryExecutionTree qet = qp.createExecutionTree(pq);
    FAIL() << "Was expecting exception" << std::endl;
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
    ParsedQuery pq = SparqlParser::parse(
        "SELECT ?x WHERE { ?x <is-a> <Book> . "
        "?x <Author> <Anthony_Newman_(Author)> }");
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
    ParsedQuery pq = SparqlParser::parse(
        "SELECT ?x ?y WHERE { "
        "?x <is-a> <Person> . "
        "?x <Country_of_nationality> <Germany> . "
        "?x <Author> ?y . "
        "?y <is-a> <Award-Nominated_Work> }");
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
    ParsedQuery pq = SparqlParser::parse(
        "SELECT ?a \n "
        "WHERE  {?a <is-a> <Plant> . ?c ql:contains-entity ?a. "
        "?c ql:contains-word \"edible leaves\"} TEXTLIMIT 5");
    pq.expandPrefixes();
    QueryPlanner qp(nullptr);
    QueryPlanner::TripleGraph tg = qp.createTripleGraph(&pq._rootGraphPattern);
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
    ParsedQuery pq = SparqlParser::parse(
        "SELECT TEXT(?c) \n "
        "WHERE  {?c ql:contains-word \"search engine\"}");
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
    ParsedQuery pq = SparqlParser::parse(
        "PREFIX : <>\n"
        "SELECT ?x ?y TEXT(?c)\n "
        "WHERE \t {"
        "?x :Place_of_birth ?y ."
        "?y :Contained_by :Europe ."
        "?c ql:contains-entity ?x ."
        "?c ql:contains-word \"cocaine\" ."
        "}");
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
    ASSERT_EQ(1u, qet.getVariableColumn("SCORE(?c)"));
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
    ParsedQuery pq = SparqlParser::parse(
        "PREFIX : <>"
        "SELECT ?x ?y WHERE {"
        "?x :is-a :Politician ."
        "?c ql:contains-entity ?x ."
        "?c ql:contains-word \"friend*\" ."
        "?c ql:contains-entity ?y ."
        "}");
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
    ParsedQuery pq = SparqlParser::parse(
        "SELECT ?p ?s \n "
        "WHERE {"
        "?a <is-a> <Politician> . "
        "?c ql:contains-entity ?a ."
        "?c ql:contains-word \"friend*\" ."
        "?c ql:contains-entity ?s ."
        "?s <is-a> <Scientist> ."
        "?c2 ql:contains-entity ?s ."
        "?c2 ql:contains-word \"manhattan project\"}");
    pq.expandPrefixes();
    QueryPlanner qp(nullptr);
    QueryExecutionTree qet = qp.createExecutionTree(pq);
    ASSERT_EQ(
        "{\n  JOIN\n  {\n    SCAN POS with P = \"<is-a>\", O = "
        "\"<Scientist>\"\n    qet-width: 1 \n  } join-column: [0]\n  |X|\n  "
        "{\n    SORT on column:4\n    {\n      TEXT OPERATION WITH FILTER: "
        "co-occurrence with words: \"manhattan project\" and 1 variables with "
        "textLimit = 1 filtered by\n      {\n        TEXT OPERATION WITH "
        "FILTER: co-occurrence with words: \"friend*\" and 2 variables with "
        "textLimit = 1 filtered by\n        {\n          SCAN POS with P = "
        "\"<is-a>\", O = \"<Politician>\"\n          qet-width: 1 \n        "
        "}\n         filtered on column 0\n        qet-width: 4 \n      }\n    "
        "   filtered on column 2\n      qet-width: 6 \n    }\n    qet-width: 6 "
        "\n  } join-column: [4]\n  qet-width: 6 \n}",
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
    ParsedQuery pq = SparqlParser::parse(
        "SELECT ?x ?y ?m WHERE { ?x <Spouse_(or_domestic_partner)> ?y . "
        "?x <Film_performance> ?m . ?y <Film_performance> ?m }");
    pq.expandPrefixes();
    QueryPlanner qp(nullptr);
    QueryExecutionTree qet = qp.createExecutionTree(pq);
    ASSERT_EQ(
        "{\n  TWO_COLUMN_JOIN\n    {\n    ORDER_BY\n    {\n      JOIN\n"
        "      {\n        SCAN PSO with P = \"<Film_performance>\"\n"
        "        qet-width: 2 \n      } join-column: [0]\n      |X|\n"
        "      {\n        "
        "SCAN PSO with P = \"<Spouse_(or_domestic_partner)>\"\n    "
        "    qet-width: 2 \n      } join-column: [0]\n "
        "     qet-width: 3 \n    } order on asc(2) asc(1) \n"
        "    qet-width: 3 \n  }\n  join-columns: [2 & 1]\n  |X|\n"
        "    {\n    SCAN PSO with P = \"<Film_performance>\"\n"
        "    qet-width: 2 \n  }\n  join-columns: [0 & 1]\n"
        "  qet-width: 3 \n}",
        qet.asString());
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
    ParsedQuery pq = SparqlParser::parse(
        "PREFIX fb: <http://rdf.freebase.com/ns/>\n"
        "SELECT DISTINCT ?1 ?0 WHERE {\n"
        "fb:m.0fkvn fb:government.government_office_category.officeholders ?0 "
        ".\n"
        "?0 fb:government.government_position_held.jurisdiction_of_office "
        "fb:m.0vmt .\n"
        "?0 fb:government.government_position_held.office_holder ?1 .\n"
        "FILTER (?1 != fb:m.0fkvn) .\n"
        "FILTER (?1 != fb:m.0vmt) .\n"
        "FILTER (?1 != fb:m.018mts)"
        "} LIMIT 300");
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

    ParsedQuery pq = SparqlParser::parse(
        "SELECT ?a ?b \n "
        "WHERE  {?a <rel1> ?b . OPTIONAL { ?a <rel2> ?c }}");
    pq.expandPrefixes();
    QueryExecutionTree qet = qp.createExecutionTree(pq);
    ASSERT_EQ(
        "{\n"
        "  OPTIONAL_JOIN\n"
        "  {\n"
        "    SCAN PSO with P = \"<rel1>\"\n"
        "    qet-width: 2 \n"
        "  } join-columns: [0]\n"
        "  |X|\n"
        "  {\n"
        "    SCAN PSO with P = \"<rel2>\"\n"
        "    qet-width: 2 \n"
        "  } join-columns: [0]\n"
        "  qet-width: 3 \n"
        "}",
        qet.asString());

    ParsedQuery pq2 = SparqlParser::parse(
        "SELECT ?a ?b \n "
        "WHERE  {?a <rel1> ?b . "
        "OPTIONAL { ?a <rel2> ?c }} ORDER BY ?b");
    pq2.expandPrefixes();
    QueryExecutionTree qet2 = qp.createExecutionTree(pq2);
    ASSERT_EQ(
        "{\n"
        "  SORT on column:1\n"
        "  {\n"
        "    OPTIONAL_JOIN\n"
        "    {\n"
        "      SCAN PSO with P = \"<rel1>\"\n"
        "      qet-width: 2 \n"
        "    } join-columns: [0]\n"
        "    |X|\n"
        "    {\n"
        "      SCAN PSO with P = \"<rel2>\"\n"
        "      qet-width: 2 \n"
        "    } join-columns: [0]\n"
        "    qet-width: 3 \n"
        "  }\n"
        "  qet-width: 3 \n"
        "}",
        qet2.asString());
  } catch (const ad_semsearch::Exception& e) {
    std::cout << "Caught: " << e.getFullErrorMessage() << std::endl;
    FAIL() << e.getFullErrorMessage();
  } catch (const std::exception& e) {
    std::cout << "Caught: " << e.what() << std::endl;
    FAIL() << e.what();
  }
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
