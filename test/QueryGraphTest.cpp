// Copyright 2015, University of Freiburg, Chair of Algorithms and Data
// Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)

#include <gtest/gtest.h>

#include "../src/engine/QueryGraph.h"
#include "../src/parser/SparqlParser.h"


TEST(QueryGraphTest, testAddNode) {
  try {
    Index index;
    Engine engine;
    QueryExecutionContext qec(index, engine);
    QueryGraph qg(&qec);
    ASSERT_EQ("", qg.asString());
    qg.addNode("?one");
    ASSERT_EQ("(?one):", qg.asString());
    qg.addNode("?two");
    ASSERT_EQ("(?one):\n(?two):", qg.asString());
  } catch (const std::exception& e) {
    std::cout << e.what() << std::endl;
  }
}

TEST(QueryGraphTest, testAddEdge) {
  QueryGraph qg;
  ASSERT_EQ("", qg.asString());

  qg.addNode("?0");
  qg.addNode("?1");
  qg.addNode("?2");
  qg.addNode("?3");
  ASSERT_EQ("(?0):\n(?1):\n(?2):\n(?3):", qg.asString());

  qg.addEdge(1, 2, "rel1");
  ASSERT_EQ("(?0):\n(?1):{2,rel1}\n(?2):{1,rel1_r}\n(?3):", qg.asString());

  qg.addEdge(1, 3, "rel2");
  ASSERT_EQ("(?0):\n(?1):{2,rel1},{3,rel2}\n(?2):{1,rel1_r}\n(?3):{1,rel2_r}",
      qg.asString());

  qg.addEdge(3, 2, "rel1");
  ASSERT_EQ("(?0):\n(?1):{2,rel1},{3,rel2}\n(?2):{1,rel1_r},{3,rel1_r}\n"
      "(?3):{1,rel2_r},{2,rel1}", qg.asString());
}

TEST(QueryGraphTest, testCollapseNode) {
  QueryGraph qg;
  qg.addNode("?0");
  qg.addNode("?1");
  qg.addNode("?2");
  qg.addNode("?3");
  qg.addEdge(1, 2, "rel1");
  qg.addEdge(1, 3, "rel2");
  qg.addEdge(2, 1, "rel1");

  ASSERT_EQ("(?0):\n"
      "(?1):{2,rel1},{3,rel2},{2,rel1_r}\n"
      "(?2):{1,rel1_r},{1,rel1}\n"
      "(?3):{1,rel2_r}", qg.asString());

  qg.collapseNode(3);
  ASSERT_EQ("(?0):\n"
      "(?1):{2,rel1},{2,rel1_r}\n"
      "(?2):{1,rel1_r},{1,rel1}\n"
      "(?3):", qg.asString());

  qg.addNode("X");
  qg.addEdge(2, 4, "relX");
  ASSERT_EQ("(?0):\n"
      "(?1):{2,rel1},{2,rel1_r}\n"
      "(?2):{1,rel1_r},{1,rel1},{4,relX}\n"
      "(?3):\n"
      "(X_0):{2,relX_r}", qg.asString());

  qg.collapseNode(4);
  ASSERT_EQ("(?0):\n"
      "(?1):{2,rel1},{2,rel1_r}\n"
      "(?2):{1,rel1_r},{1,rel1}\n"
      "(?3):\n"
      "(X_0):", qg.asString());
}

TEST(QueryGraphTest, testCreate) {
  ParsedQuery pq = SparqlParser::parse(
      "PREFIX : <http://rdf.myprefix.com/>\n"
          "PREFIX ns: <http://rdf.myprefix.com/ns/>\n"
          "PREFIX xxx: <http://rdf.myprefix.com/xxx/>\n"
          "SELECT ?x ?z \n "
          "WHERE \t {?x :myrel ?y. ?y ns:myrel ?z.?y xxx:rel2 <http://abc.de>"
          ".?z xxx:rel2 <http://abc.de>}");
  pq.expandPrefixes();
  QueryGraph qg;
  qg.createFromParsedQuery(pq);

  ASSERT_EQ(
      "(?x):{1,<http://rdf.myprefix.com/myrel>}\n"
          "(?y):{0,<http://rdf.myprefix.com/myrel>_r},"
          "{2,<http://rdf.myprefix.com/ns/myrel>},"
          "{3,<http://rdf.myprefix.com/xxx/rel2>}\n"
          "(?z):{1,<http://rdf.myprefix.com/ns/myrel>_r},"
          "{4,<http://rdf.myprefix.com/xxx/rel2>}\n"
          "(<http://abc.de>_0):{1,<http://rdf.myprefix.com/xxx/rel2>_r}\n"
          "(<http://abc.de>_1):{2,<http://rdf.myprefix.com/xxx/rel2>_r}",
      qg.asString());
};


TEST(QueryGraphTest, testCollapseByHand) {
  ParsedQuery pq = SparqlParser::parse(
      "PREFIX : <http://rdf.myprefix.com/>\n"
          "PREFIX ns: <http://rdf.myprefix.com/ns/>\n"
          "PREFIX xxx: <http://rdf.myprefix.com/xxx/>\n"
          "SELECT ?x ?z \n "
          "WHERE \t {?x :myrel ?y. ?y ns:myrel ?z.?y xxx:rel2 <http://abc.de>}");
  pq.expandPrefixes();
  QueryGraph qg;
  qg.createFromParsedQuery(pq);

  ASSERT_EQ(
      "(?x):{1,<http://rdf.myprefix.com/myrel>}\n"
          "(?y):{0,<http://rdf.myprefix.com/myrel>_r},"
          "{2,<http://rdf.myprefix.com/ns/myrel>},"
          "{3,<http://rdf.myprefix.com/xxx/rel2>}\n"
          "(?z):{1,<http://rdf.myprefix.com/ns/myrel>_r}\n"
          "(<http://abc.de>_0):{1,<http://rdf.myprefix.com/xxx/rel2>_r}",
      qg.asString());

  qg.collapseNode(3);

  ASSERT_EQ(
      "(?x):{1,<http://rdf.myprefix.com/myrel>}\n"
          "(?y):{0,<http://rdf.myprefix.com/myrel>_r},"
          "{2,<http://rdf.myprefix.com/ns/myrel>}\n"
          "(?z):{1,<http://rdf.myprefix.com/ns/myrel>_r}\n"
          "(<http://abc.de>_0):",
      qg.asString());

  qg.collapseNode(0);
  ASSERT_EQ(
      "(?x):\n"
          "(?y):{2,<http://rdf.myprefix.com/ns/myrel>}\n"
          "(?z):{1,<http://rdf.myprefix.com/ns/myrel>_r}\n"
          "(<http://abc.de>_0):",
      qg.asString());

  qg.collapseNode(2);
  ASSERT_EQ(
      "(?x):\n"
          "(?y):\n"
          "(?z):\n"
          "(<http://abc.de>_0):",
      qg.asString());
};

TEST(QueryGraphTest, testCollapseAndCreateExecutionTree) {
  {
    ParsedQuery pq = SparqlParser::parse(
        "PREFIX : <http://rdf.myprefix.com/>\n"
            "PREFIX ns: <http://rdf.myprefix.com/ns/>\n"
            "PREFIX xxx: <http://rdf.myprefix.com/xxx/>\n"
            "SELECT ?x ?z \n "
            "WHERE \t {?x :myrel ?y. ?y ns:myrel ?z.?y xxx:rel2 <http://abc.de>}");
    pq.expandPrefixes();
    QueryGraph qg;
    qg.createFromParsedQuery(pq);
    QueryGraph::Node* root = qg.collapseAndCreateExecutionTree();
    ASSERT_NE(root, nullptr);
    ASSERT_EQ("(?y)", root->asString());
  }

  {
    ParsedQuery pq = SparqlParser::parse(
        "PREFIX : <http://rdf.myprefix.com/>\n"
            "SELECT ?x \n "
            "WHERE \t {?x :myrel ?y}");
    pq.expandPrefixes();
    QueryGraph qg;
    qg.createFromParsedQuery(pq);
    QueryGraph::Node* root = qg.collapseAndCreateExecutionTree();
    ASSERT_NE(root, nullptr);
    ASSERT_EQ("(?x)", root->asString());
  }

  {
    ParsedQuery pq = SparqlParser::parse(
        "PREFIX : <http://rdf.myprefix.com/>\n"
            "SELECT ?x \n "
            "WHERE \t {?y :myrel ?x}");
    pq.expandPrefixes();
    QueryGraph qg;
    qg.createFromParsedQuery(pq);
    QueryGraph::Node* root = qg.collapseAndCreateExecutionTree();
    ASSERT_NE(root, nullptr);
    ASSERT_EQ("(?x)", root->asString());
  }
  {
    ParsedQuery pq = SparqlParser::parse(
        "PREFIX : <pre/>\n"
            "SELECT ?a \n "
            "WHERE \t {?a :profession :Actor. ?a :born-in ?c. ?c in :Europe}");
    pq.expandPrefixes();
    QueryGraph qg;
    qg.createFromParsedQuery(pq);
    QueryGraph::Node* root = qg.collapseAndCreateExecutionTree();
    ASSERT_NE(root, nullptr);
    ASSERT_EQ("(?a)", root->asString());
  }
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}