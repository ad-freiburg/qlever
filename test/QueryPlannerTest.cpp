// Copyright 2015, University of Freiburg, Chair of Algorithms and Data
// Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)

#include <gtest/gtest.h>

#include "../src/engine/QueryPlanner.h"
#include "../src/parser/SparqlParser.h"

TEST(QueryGraphTest, createExecutionTree) {
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
    ASSERT_EQ("xxx", qet.asString());
  }
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}