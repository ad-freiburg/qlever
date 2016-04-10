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
              "WHERE \t {?x :myrel ?y. ?y ns:myrel ?z.?y xxx:rel2 <http://abc.de>}");
      pq.expandPrefixes();
      QueryPlanner qp(nullptr);
      auto tg = qp.createTripleGraph(pq);
      ASSERT_EQ(
          "0 {s: ?x, p: <http://rdf.myprefix.com/myrel>, o: ?y} : (1, 2)\n"
              "1 {s: ?y, p: <http://rdf.myprefix.com/ns/myrel>, o: ?z} : (0, 2)\n"
              "2 {s: ?y, p: <http://rdf.myprefix.com/xxx/rel2>, o: <http://abc.de>} : (0, 1)",
          tg.asString());
    }

    {
      ParsedQuery pq = SparqlParser::parse(
          "SELECT ?x WHERE {?x ?p <X>. ?x ?p2 <Y>. <X> ?p <Y>}");
      pq.expandPrefixes();
      QueryPlanner qp(nullptr);
      auto tg = qp.createTripleGraph(pq);
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
      auto tg = qp.createTripleGraph(pq);
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
      auto tg = qp.createTripleGraph(pq);
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
        ASSERT_EQ(
            "0 {s: ?x, p: ?p, o: <X>} : ()",
            tgnew.asString());
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
      auto tg = qp.createTripleGraph(pq);
      ASSERT_EQ(3u, tg._adjLists.size());
      unordered_set<size_t> lo;
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
      auto tg = qp.createTripleGraph(pq);
      unordered_set<size_t> lo;
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

TEST(QueryPlannerTest, testSplitAtContextVars) {
  try {
    {
      ParsedQuery pq = SparqlParser::parse(
          "SELECT ?x WHERE {?x <p> <X>. ?x <in-context> ?c. ?c <in-context> abc}");
      pq.expandPrefixes();
      QueryPlanner qp(nullptr);
      auto tg = qp.createTripleGraph(pq);
      ASSERT_EQ(
          "0 {s: ?x, p: <p>, o: <X>} : (1)\n"
              "1 {s: ?x, p: <in-context>, o: ?c} : (0, 2)\n"
              "2 {s: ?c, p: <in-context>, o: abc} : (1)",
          tg.asString());
      unordered_map<string, vector<size_t>> cvarToNodeIds;
      vector<std::pair<QueryPlanner::TripleGraph, vector<SparqlFilter>>> tgs;
      vector<SparqlFilter> filtersWithCvars;
      tg.splitAtText(pq._filters, tgs, cvarToNodeIds, filtersWithCvars);
      ASSERT_EQ(0u, filtersWithCvars.size());
      ASSERT_EQ(1u, tgs.size());
      ASSERT_EQ(1u, cvarToNodeIds.size());
      ASSERT_EQ(2u, cvarToNodeIds["?c"].size());
      ASSERT_EQ(1u, cvarToNodeIds["?c"][0]);
      ASSERT_EQ(2u, cvarToNodeIds["?c"][1]);
      ASSERT_EQ(0u, tgs[0].second.size());
      ASSERT_EQ("0 {s: ?x, p: <p>, o: <X>} : ()", tgs[0].first.asString());
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
  ASSERT_EQ("{SCAN POS with P = \"<http://rdf.myprefix.com/myrel>\", "
                "O = \"<http://rdf.myprefix.com/obj>\" | width: 1}",
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
  ASSERT_EQ("{SCAN PSO with P = \"<http://rdf.myprefix.com/myrel>\", "
                "S = \"<http://rdf.myprefix.com/subj>\" | width: 1}",
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
  ASSERT_EQ("{SCAN PSO with P = \"<http://rdf.myprefix.com/myrel>\" | width: "
                "2}",
            qet.asString());
}

TEST(QueryPlannerTest, test_free_PX) {
  ParsedQuery pq = SparqlParser::parse(
      "PREFIX : <http://rdf.myprefix.com/>\n"
          "SELECT ?x \n "
          "WHERE \t {?y :myrel ?x}");
  pq.expandPrefixes();
  QueryPlanner qp(nullptr);
  QueryExecutionTree qet = qp.createExecutionTree(pq);
  EXPECT_TRUE("{SCAN POS with P = \"<http://rdf.myprefix.com/myrel>\" | width: "
                  "2}" == qet.asString() ||
              "{SCAN PSO with P = \"<http://rdf.myprefix.com/myrel>\" | width: "
                  "2}" == qet.asString());
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
    ASSERT_EQ("{JOIN(\n"
                  "\t{SCAN PSO with P = \"<pre/r>\", S = \"<pre/s1>\" | width: 1} [0]\n"
                  "\t|X|\n"
                  "\t{SCAN PSO with P = \"<pre/r>\", S = \"<pre/s2>\" | width: 1} [0]\n"
                  ") | width: 1}",
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
            "WHERE \t {?y :r ?x . :s2 :r ?x}");
    pq.expandPrefixes();
    QueryPlanner qp(nullptr);
    QueryExecutionTree qet = qp.createExecutionTree(pq);
    ASSERT_EQ("{JOIN(\n"
                  "\t{SCAN POS with P = \"<pre/r>\" | width: 2} [0]\n"
                  "\t|X|\n"
                  "\t{SCAN PSO with P = \"<pre/r>\", S = \"<pre/s2>\" | width: 1} [0]\n"
                  ") | width: 2}",
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
            "WHERE \t {?y :r ?x. ?z :r ?x}");
    pq.expandPrefixes();
    QueryPlanner qp(nullptr);
    QueryExecutionTree qet = qp.createExecutionTree(pq);
    ASSERT_EQ("{JOIN(\n"
                  "\t{SCAN POS with P = \"<pre/r>\" | width: 2} [0]\n"
                  "\t|X|\n"
                  "\t{SCAN POS with P = \"<pre/r>\" | width: 2} [0]\n"
                  ") | width: 3}",
              qet.asString());
  } catch (const ad_semsearch::Exception& e) {
    std::cout << "Caught: " << e.getFullErrorMessage() << std::endl;
    FAIL() << e.getFullErrorMessage();
  } catch (const std::exception& e) {
    std::cout << "Caught: " << e.what() << std::endl;
    FAIL() << e.what();
  }
}

TEST(QueryPlannerTest, testSpielbergMovieActors) {
  try {
    ParsedQuery pq = SparqlParser::parse(
        "PREFIX : <pre/>\n"
            "SELECT ?a \n "
            "WHERE \t {?a :acted-in ?m. ?m :directed-by :SS}");
    pq.expandPrefixes();
    QueryPlanner qp(nullptr);
    QueryExecutionTree qet = qp.createExecutionTree(pq);
    EXPECT_TRUE(("{JOIN(\n"
                     "\t{SCAN POS with P = \"<pre/acted-in>\" | width: 2} [0]\n"
                     "\t|X|\n"
                     "\t{SCAN POS with P = \"<pre/directed-by>\", "
                     "O = \"<pre/SS>\" | width: 1} [0]\n"
                     ") | width: 2}"
                 == qet.asString()) ||
                ("{JOIN(\n"
                     "\t{SCAN POS with P = \"<pre/directed-by>\", "
                     "\t|X|\n"
                     "\t{SCAN POS with P = \"<pre/acted-in>\" | width: 2} [0]\n"
                     "O = \"<pre/SS>\" | width: 1} [0]\n"
                     ") | width: 2}"
                 == qet.asString()));
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
            "WHERE \t {?a :profession :Actor . ?a :born-in ?c. ?c :in :Europe}\n"
            "ORDER BY ?a");
    pq.expandPrefixes();
    QueryPlanner qp(nullptr);
    QueryExecutionTree qet = qp.createExecutionTree(pq);
    ASSERT_EQ("{JOIN(\n"
                  "\t{SCAN POS with P = \"<pre/profession>\", "
                  "O = \"<pre/Actor>\" | width: 1} [0]\n"
                  "\t|X|\n"
                  "\t{SORT {JOIN(\n"
                  "\t{SCAN POS with P = \"<pre/born-in>\" | width: 2} [0]\n"
                  "\t|X|\n"
                  "\t{SCAN POS with P = \"<pre/in>\", O = \"<pre/Europe>\" | "
                  "width: 1} [0]\n"
                  ") | width: 2} on 1 | width: 2} [1]\n"
                  ") | width: 2}",
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
              "WHERE \t {?x :myrel ?y. ?y ns:myrel ?z.?y xxx:rel2 <http://abc.de>}");
      pq.expandPrefixes();
      QueryPlanner qp(nullptr);
      QueryExecutionTree qet = qp.createExecutionTree(pq);
      ASSERT_EQ("{JOIN(\n\t"
                    "{JOIN(\n\t"
                    "{SCAN POS with P = \"<http://rdf.myprefix.com/xxx/rel2>\", O = \"<http://abc.de>\" | width: 1} [0]\n\t"
                    "|X|\n\t"
                    "{SCAN PSO with P = \"<http://rdf.myprefix.com/ns/myrel>\" | width: 2} [0]\n"
                    ") | width: 2} [0]\n\t"
                    "|X|\n\t"
                    "{SCAN POS with P = \"<http://rdf.myprefix.com/myrel>\" | width: 2} [0]\n"
                    ") | width: 3}", qet.asString());
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
    ParsedQuery pq = SparqlParser::parse("SELECT ?x ?y ?z WHERE {"
                                             "?x <r> ?y . ?y <r> ?z . "
                                             "FILTER(?x != ?y) }");
    QueryPlanner qp(nullptr);
    QueryExecutionTree qet = qp.createExecutionTree(pq);
    ASSERT_EQ("{JOIN(\n\t"
                  "{FILTER {SCAN POS with P = \"<r>\" | width: 2} with col 1 != col 0 | width: 2} [0]"
                  "\n\t|X|\n\t"
                  "{SCAN PSO with P = \"<r>\" | width: 2} [0]"
                  "\n) | width: 3}", qet.asString());
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
    ParsedQuery pq = SparqlParser::parse("SELECT ?x ?y ?z WHERE {"
                                             "?x <r> ?y . ?y <r> ?z . "
                                             "FILTER(?x != ?z) }");
    QueryPlanner qp(nullptr);
    QueryExecutionTree qet = qp.createExecutionTree(pq);
    ASSERT_EQ("{FILTER {JOIN(\n\t"
                  "{SCAN POS with P = \"<r>\" | width: 2} [0]"
                  "\n\t|X|\n\t"
                  "{SCAN PSO with P = \"<r>\" | width: 2} [0]"
                  "\n) | width: 3} with col 1 != col 2 | width: 3}",
              qet.asString());
  } catch (const ad_semsearch::Exception& e) {
    std::cout << "Caught: " << e.getFullErrorMessage() << std::endl;
    FAIL() << e.getFullErrorMessage();
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
    ASSERT_EQ("{JOIN(\n"
                  "\t{SCAN POS with P = \"<Author>\", "
                  "O = \"<Anthony_Newman_(Author)>\" | width: 1} [0]\n"
                  "\t|X|\n"
                  "\t{SCAN POS with P = \"<is-a>\", O = \"<Book>\" | width: 1} [0]\n"
                  ") | width: 1}",
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
            "WHERE \t {?a <is-a> <Plant> . ?a <in-context> ?c. "
            "?c <in-context> edible leaves} TEXTLIMIT 5");
    pq.expandPrefixes();
    QueryPlanner qp(nullptr);
    QueryPlanner::TripleGraph tg = qp.createTripleGraph(pq);
    ASSERT_EQ(1u, tg._nodeMap.find(0)->second->_variables.size());
    QueryExecutionTree qet = qp.createExecutionTree(pq);
    ASSERT_EQ("{JOIN(\n"
                  "\t{SCAN POS with P = \"<is-a>\", "
                  "O = \"<Plant>\" | width: 1} [0]\n"
                  "\t|X|\n"
                  "\t{SORT {TEXT OPERATION FOR ENTITIES: "
                  "co-occurrence with words: "
                  "\"edible leaves\" with textLimit = 5 | width: 3} on 0 "
                  "| width: 3} [0]\n) | width: 3}",
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
            "WHERE \t {?c <in-context> search engine}");
    pq.expandPrefixes();
    QueryPlanner qp(nullptr);
    QueryExecutionTree qet = qp.createExecutionTree(pq);
    ASSERT_EQ("{TEXT OPERATION FOR CONTEXTS: "
                  "co-occurrence with words: "
                  "\"search engine\" with textLimit = 1 | width: 2}",
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
            "WHERE \t {"
            "?a <is-a> <Politician> . "
            "?a <in-context> ?c ."
            "?c <in-context> friend* ."
            "?c <in-context> ?s ."
            "?s <is-a> <Scientist> ."
            "?s <in-context> ?c2 ."
            "?c2 <in-context> manhattan project}");
    pq.expandPrefixes();
    QueryPlanner qp(nullptr);
    QueryExecutionTree qet = qp.createExecutionTree(pq);
    ASSERT_EQ("{JOIN(\n\t"
                  "{SCAN POS with P = \"<is-a>\", O = \"<Politician>\" | width: 1} [0]\n\t"
                  "|X|\n\t"
                  "{TEXT OPERATION FOR ENTITIES: co-occurrence with words: \"friend*\"\n\t"
                  "and {JOIN(\n\t"
                  "{SCAN POS with P = \"<is-a>\", O = \"<Scientist>\" | width: 1} [0]\n\t"
                  "|X|\n\t"
                  "{SORT {TEXT OPERATION FOR ENTITIES: "
                  "co-occurrence with words: \"manhattan project\" "
                  "with textLimit = 1 | width: 3} on 0 | width: 3} [0]\n) "
                  "| width: 3} [0] with textLimit = 1 | width: 6} [0]\n) "
                  "| width: 6}",
              qet.asString());

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
            "?x :in-context ?c ."
            "?c :in-context friend* ."
            "?y :in-context ?c ."
            "}");
    pq.expandPrefixes();
    QueryPlanner qp(nullptr);
    QueryExecutionTree qet = qp.createExecutionTree(pq);
    ASSERT_EQ("{TEXT OPERATION FOR ENTITIES: co-occurrence with words: "
                  "\"friend*\"\n"
                  "\tand {SCAN POS with P = \"<is-a>\", O = \"<Politician>\" "
                  "| width: 1} [0] with textLimit = 1 | width: 4}",
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
            "?x :in-context ?c ."
            "?c :in-context cocaine ."
            "}");
    pq.expandPrefixes();
    QueryPlanner qp(nullptr);
    QueryExecutionTree qet = qp.createExecutionTree(pq);
    ASSERT_EQ("{JOIN(\n\t"
                  "{SCAN POS with P = \"<Contained_by>\", O = \"<Europe>\" | width: 1} [0]"
                  "\n\t|X|\n\t"
                  "{SORT {JOIN(\n\t"
                  "{SCAN PSO with P = \"<Place_of_birth>\" | width: 2} [0]"
                  "\n\t|X|\n\t"
                  "{SORT {TEXT OPERATION FOR ENTITIES: co-occurrence with words: "
                  "\"cocaine\" with textLimit = 1 | width: 3} on 0 "
                  "| width: 3} [0]\n) | width: 4} "
                  "on 1 | width: 4} [1]\n) | width: 4}",
              qet.asString());
    ASSERT_EQ(0u, qet.getVariableColumn("?y"));
  } catch (const ad_semsearch::Exception& e) {
    std::cout << "Caught: " << e.getFullErrorMessage() << std::endl;
    FAIL() << e.getFullErrorMessage();
  } catch (const std::exception& e) {
    std::cout << "Caught: " << e.what() << std::endl;
    FAIL() << e.what();
  }
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}