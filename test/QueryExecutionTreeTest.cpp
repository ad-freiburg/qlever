// Copyright 2015, University of Freiburg, Chair of Algorithms and Data
// Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)

#include <gtest/gtest.h>

#include "../src/engine/QueryGraph.h"
#include "../src/parser/SparqlParser.h"


TEST(QueryExecutionTreeTest, testSPX) {
  ParsedQuery pq = SparqlParser::parse(
      "PREFIX : <http://rdf.myprefix.com/>\n"
          "SELECT ?x \n "
          "WHERE  {?x :myrel :obj}");
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
          "WHERE  {:subj :myrel ?x}");
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
          "WHERE  {?x :myrel ?y}");
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
          "WHERE  {?y :myrel ?x}");
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
            "WHERE  {:s1 :r ?x. :s2 :r ?x}");
    pq.expandPrefixes();
    QueryGraph qg;
    qg.createFromParsedQuery(pq);
    const QueryExecutionTree& qet = qg.getExecutionTree();
    ASSERT_EQ("{JOIN(\n"
                  "{SCAN PSO with P = \"<pre/r>\", S = \"<pre/s1>\" | width: 1} [0]\n"
                  "|X|\n"
                  "{SCAN PSO with P = \"<pre/r>\", S = \"<pre/s2>\" | width: 1} [0]\n"
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

TEST(QueryExecutionTreeTest, test_free_PX_SPX) {
  try {
    ParsedQuery pq = SparqlParser::parse(
        "PREFIX : <pre/>\n"
            "SELECT ?x ?y \n "
            "WHERE  {?y :r ?x . :s2 :r ?x}");
    pq.expandPrefixes();
    QueryGraph qg;
    qg.createFromParsedQuery(pq);
    const QueryExecutionTree& qet = qg.getExecutionTree();
    ASSERT_EQ("{JOIN(\n"
                  "{SCAN POS with P = \"<pre/r>\" | width: 2} [0]\n"
                  "|X|\n"
                  "{SCAN PSO with P = \"<pre/r>\", S = \"<pre/s2>\" | width: 1} [0]\n"
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

TEST(QueryExecutionTreeTest, test_free_PX__free_PX) {
  try {
    ParsedQuery pq = SparqlParser::parse(
        "PREFIX : <pre/>\n"
            "SELECT ?x ?y ?z \n "
            "WHERE  {?y :r ?x. ?z :r ?x}");
    pq.expandPrefixes();
    QueryGraph qg;
    qg.createFromParsedQuery(pq);
    const QueryExecutionTree& qet = qg.getExecutionTree();
    ASSERT_EQ("{JOIN(\n"
                  "{SCAN POS with P = \"<pre/r>\" | width: 2} [0]\n"
                  "|X|\n"
                  "{SCAN POS with P = \"<pre/r>\" | width: 2} [0]\n"
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

TEST(QueryExecutionTreeTest, testSpielbergMovieActors) {
  try {
    ParsedQuery pq = SparqlParser::parse(
        "PREFIX : <pre/>\n"
            "SELECT ?a \n "
            "WHERE  {?a :acted-in ?m. ?m :directed-by :SS}");
    pq.expandPrefixes();
    QueryGraph qg;
    qg.createFromParsedQuery(pq);
    const QueryExecutionTree& qet = qg.getExecutionTree();
    ASSERT_EQ("{JOIN(\n"
                  "{SCAN POS with P = \"<pre/acted-in>\" | width: 2} [0]\n"
                  "|X|\n"
                  "{SCAN POS with P = \"<pre/directed-by>\", "
                  "O = \"<pre/SS>\" | width: 1} [0]\n"
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



TEST(QueryExecutionTreeTest, testActorsBornInEurope) {
  try {
    ParsedQuery pq = SparqlParser::parse(
        "PREFIX : <pre/>\n"
            "SELECT ?a \n "
            "WHERE  {?a :profession :Actor . ?a :born-in ?c. ?c :in :Europe}");
    pq.expandPrefixes();
    QueryGraph qg;
    qg.createFromParsedQuery(pq);
    const QueryExecutionTree& qet = qg.getExecutionTree();
    ASSERT_EQ("{JOIN(\n"
                  "{SCAN POS with P = \"<pre/profession>\", "
                  "O = \"<pre/Actor>\" | width: 1} [0]\n"
                  "|X|\n"
                  "{SORT {JOIN(\n"
                  "{SCAN POS with P = \"<pre/born-in>\" | width: 2} [0]\n"
                  "|X|\n"
                  "{SCAN POS with P = \"<pre/in>\", O = \"<pre/Europe>\" | "
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

TEST(QueryExecutionTreeTest, testPlantsEdibleLeaves) {
  try {
    ParsedQuery pq = SparqlParser::parse(
        "SELECT ?a \n "
            "WHERE  {?a <is-a> <Plant> . ?a <in-context> ?c. "
            "?c <in-context> edible leaves} TEXTLIMIT 5");
    pq.expandPrefixes();
    QueryGraph qg;
    qg.createFromParsedQuery(pq);
    const QueryExecutionTree& qet = qg.getExecutionTree();
    ASSERT_EQ("{JOIN(\n"
                  "{SCAN POS with P = \"<is-a>\", "
                  "O = \"<Plant>\" | width: 1} [0]\n"
                  "|X|\n"
                  "{SORT {TEXT OPERATION FOR ENTITIES: "
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
            "WHERE  {?c <in-context> search engine}");
    pq.expandPrefixes();
    QueryGraph qg;
    qg.createFromParsedQuery(pq);
    const QueryExecutionTree& qet = qg.getExecutionTree();
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
            "WHERE  {"
            "?a <is-a> <Politician> . "
            "?a <in-context> ?c ."
            "?c <in-context> friend* ."
            "?c <in-context> ?s ."
            "?s <is-a> <Scientist> ."
            "?s <in-context> ?c2 ."
            "?c2 <in-context> manhattan project}");
    pq.expandPrefixes();
    QueryGraph qg;
    qg.createFromParsedQuery(pq);
    const QueryExecutionTree& qet = qg.getExecutionTree();
    ASSERT_EQ("{JOIN(\n"
      "{SCAN POS with P = \"<is-a>\", O = \"<Politician>\" | width: 1} [0]\n"
      "|X|\n"
      "{TEXT OPERATION FOR ENTITIES: co-occurrence with words: \"friend*\"\n"
      "and {JOIN(\n"
      "{SCAN POS with P = \"<is-a>\", O = \"<Scientist>\" | width: 1} [0]\n"
      "|X|\n"
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
    QueryGraph qg;
    qg.createFromParsedQuery(pq);
    const QueryExecutionTree& qet = qg.getExecutionTree();
    ASSERT_EQ("{TEXT OPERATION FOR ENTITIES: co-occurrence with words: "
                  "\"friend*\"\n"
                  "and {SCAN POS with P = \"<is-a>\", O = \"<Politician>\" "
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
            "WHERE  {"
            "?x :Place_of_birth ?y ."
            "?y :Contained_by :Europe ."
            "?x :in-context ?c ."
            "?c :in-context cocaine ."
            "}");
    pq.expandPrefixes();
    QueryGraph qg;
    qg.createFromParsedQuery(pq);
    const QueryExecutionTree& qet = qg.getExecutionTree();
    ASSERT_EQ("{JOIN(\n"
    "{SCAN POS with P = \"<Contained_by>\", O = \"<Europe>\" | width: 1} [0]"
                  "\n|X|\n"
    "{SORT {JOIN(\n"
      "{SCAN PSO with P = \"<Place_of_birth>\" | width: 2} [0]"
                  "\n|X|\n"
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