// Copyright 2015, University of Freiburg, Chair of Algorithms and Data
// Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)

#include "gtest/gtest.h"

#include "../src/engine/QueryGraph.h"
#include "../src/parser/SparqlParser.h"


TEST(QueryExecutionTreeTest, testSPX) {
  ParsedQuery pq = SparqlParser::parse(
      "PREFIX : <http://rdf.myprefix.com/>\n"
          "SELECT ?x \n "
          "WHERE \t {?x :myrel :obj}");
  pq.expandPrefixes();
  QueryGraph qg;
  qg.createFromParsedQuery(pq);
  const QueryExecutionTree& qet = qg.getExecutionTree();
  ASSERT_EQ("{SCAN POS with P = \"<http://rdf.myprefix.com/myrel>\", "
      "O = \"<http://rdf.myprefix.com/obj>\" | width: 1}",
      qet.asString());
}

TEST(QueryExecutionTreeTest, testXPO) {
  ParsedQuery pq = SparqlParser::parse(
      "PREFIX : <http://rdf.myprefix.com/>\n"
          "SELECT ?x \n "
          "WHERE \t {:subj :myrel ?x}");
  pq.expandPrefixes();
  QueryGraph qg;
  qg.createFromParsedQuery(pq);
  const QueryExecutionTree& qet = qg.getExecutionTree();
  ASSERT_EQ("{SCAN PSO with P = \"<http://rdf.myprefix.com/myrel>\", "
      "S = \"<http://rdf.myprefix.com/subj>\" | width: 1}",
      qet.asString());
}

TEST(QueryExecutionTreeTest, testSP_free_) {
  ParsedQuery pq = SparqlParser::parse(
      "PREFIX : <http://rdf.myprefix.com/>\n"
          "SELECT ?x \n "
          "WHERE \t {?x :myrel ?y}");
  pq.expandPrefixes();
  QueryGraph qg;
  qg.createFromParsedQuery(pq);
  const QueryExecutionTree& qet = qg.getExecutionTree();
  ASSERT_EQ("{SCAN PSO with P = \"<http://rdf.myprefix.com/myrel>\" | width: "
      "2}",
      qet.asString());
}

TEST(QueryExecutionTreeTest, test_free_PX) {
  ParsedQuery pq = SparqlParser::parse(
      "PREFIX : <http://rdf.myprefix.com/>\n"
          "SELECT ?x \n "
          "WHERE \t {?y :myrel ?x}");
  pq.expandPrefixes();
  QueryGraph qg;
  qg.createFromParsedQuery(pq);
  const QueryExecutionTree& qet = qg.getExecutionTree();
  ASSERT_EQ("{SCAN POS with P = \"<http://rdf.myprefix.com/myrel>\" | width: "
      "2}",
      qet.asString());
}

TEST(QueryExecutionTreeTest, testSPX_SPX) {
  try {
    ParsedQuery pq = SparqlParser::parse(
        "PREFIX : <pre/>\n"
            "SELECT ?x \n "
            "WHERE \t {:s1 :r ?x. :s2 :r ?x}");
    pq.expandPrefixes();
    QueryGraph qg;
    qg.createFromParsedQuery(pq);
    const QueryExecutionTree& qet = qg.getExecutionTree();
    ASSERT_EQ("{JOIN(\n"
        "\t{SCAN PSO with P = \"<pre/r>\", S = \"<pre/s1>\" | width: 1} [0]\n"
        "\t|X|\n"
        "\t{SCAN PSO with P = \"<pre/r>\", S = \"<pre/s2>\" | width: 1} [0]\n"
        ") | width: 1}",
       qet.asString());
  } catch (const ad_semsearch::Exception& e) {
    std::cout << "Caught: " << e.getFullErrorMessage() << std::endl;
  } catch (const std::exception& e) {
    std::cout << "Caught: " << e.what() << std::endl;
  }
}

TEST(QueryExecutionTreeTest, test_free_PX_SPX) {
  try {
    ParsedQuery pq = SparqlParser::parse(
        "PREFIX : <pre/>\n"
            "SELECT ?x ?y \n "
            "WHERE \t {?y :r ?x. :s2 :r ?x}");
    pq.expandPrefixes();
    QueryGraph qg;
    qg.createFromParsedQuery(pq);
    const QueryExecutionTree& qet = qg.getExecutionTree();
    ASSERT_EQ("{JOIN(\n"
        "\t{SCAN POS with P = \"<pre/r>\" | width: 2} [0]\n"
        "\t|X|\n"
        "\t{SCAN PSO with P = \"<pre/r>\", S = \"<pre/s2>\" | width: 1} [0]\n"
        ") | width: 2}",
        qet.asString());
  } catch (const ad_semsearch::Exception& e) {
    std::cout << "Caught: " << e.getFullErrorMessage() << std::endl;
  } catch (const std::exception& e) {
    std::cout << "Caught: " << e.what() << std::endl;
  }
}

TEST(QueryExecutionTreeTest, test_free_PX__free_PX) {
  try {
    ParsedQuery pq = SparqlParser::parse(
        "PREFIX : <pre/>\n"
            "SELECT ?x ?y ?z \n "
            "WHERE \t {?y :r ?x. ?z :r ?x}");
    pq.expandPrefixes();
    QueryGraph qg;
    qg.createFromParsedQuery(pq);
    const QueryExecutionTree& qet = qg.getExecutionTree();
    ASSERT_EQ("{JOIN(\n"
        "\t{SCAN POS with P = \"<pre/r>\" | width: 2} [0]\n"
        "\t|X|\n"
        "\t{SCAN POS with P = \"<pre/r>\" | width: 2} [0]\n"
        ") | width: 3}",
        qet.asString());
  } catch (const ad_semsearch::Exception& e) {
    std::cout << "Caught: " << e.getFullErrorMessage() << std::endl;
  } catch (const std::exception& e) {
    std::cout << "Caught: " << e.what() << std::endl;
  }
}

TEST(QueryExecutionTreeTest, testSpielbergMovieActors) {
  try {
    ParsedQuery pq = SparqlParser::parse(
        "PREFIX : <pre/>\n"
            "SELECT ?a \n "
            "WHERE \t {?a :acted-in ?m. ?m :directed-by :SS}");
    pq.expandPrefixes();
    QueryGraph qg;
    qg.createFromParsedQuery(pq);
    const QueryExecutionTree& qet = qg.getExecutionTree();
    ASSERT_EQ("{JOIN(\n"
        "\t{SCAN POS with P = \"<pre/acted-in>\" | width: 2} [0]\n"
        "\t|X|\n"
        "\t{SCAN POS with P = \"<pre/directed-by>\", "
        "O = \"<pre/SS>\" | width: 1} [0]\n"
        ") | width: 2}",
        qet.asString());
  } catch (const ad_semsearch::Exception& e) {
    std::cout << "Caught: " << e.getFullErrorMessage() << std::endl;
  } catch (const std::exception& e) {
    std::cout << "Caught: " << e.what() << std::endl;
  }
}

TEST(QueryExecutionTreeTest, testActorsBornInEurope) {
  try {
    ParsedQuery pq = SparqlParser::parse(
        "PREFIX : <pre/>\n"
            "SELECT ?a \n "
            "WHERE \t {?a :profession :Actor. ?a :born-in ?c. ?c :in :Europe}");
    pq.expandPrefixes();
    QueryGraph qg;
    qg.createFromParsedQuery(pq);
    const QueryExecutionTree& qet = qg.getExecutionTree();
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
  } catch (const std::exception& e) {
    std::cout << "Caught: " << e.what() << std::endl;
  }
}


int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}