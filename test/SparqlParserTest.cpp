// Copyright 2014, University of Freiburg, Chair of Algorithms and Data
// Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)

#include "../src/parser/SparqlParser.h"
#include "gtest/gtest.h"

TEST(ParserTest, testParse) {
    ParsedQuery pq = SparqlParser::parse("SELECT ?x WHERE {?x ?y ?z}");
    ASSERT_GT(pq.asString().size(), 0);
    ASSERT_EQ(0, pq._prefixes.size());
    ASSERT_EQ(1, pq._selectedVariables.size());
    ASSERT_EQ(1, pq._whereClauseTriples.size());

    pq = SparqlParser::parse(
        "PREFIX : <http://rdf.myprefix.com/>\n"
        "PREFIX ns: <http://rdf.myprefix.com/ns/>\n"
        "PREFIX xxx: <http://rdf.myprefix.com/xxx/>\n"
        "SELECT ?x ?z \n "
        "WHERE \t {?x :myrel ?y. ?y ns:myrel ?z.?y nsx:rel2 <http://abc.de>}");
    ASSERT_EQ(3, pq._prefixes.size());
    ASSERT_EQ(2, pq._selectedVariables.size());
    ASSERT_EQ(3, pq._whereClauseTriples.size());

    ASSERT_EQ("", pq._prefixes[0]._prefix);
    ASSERT_EQ("<http://rdf.myprefix.com/>", pq._prefixes[0]._uri);
    ASSERT_EQ("ns", pq._prefixes[1]._prefix);
    ASSERT_EQ("<http://rdf.myprefix.com/ns/>", pq._prefixes[1]._uri);
    ASSERT_EQ("?x", pq._selectedVariables[0]);
    ASSERT_EQ("?z", pq._selectedVariables[1]);
    ASSERT_EQ("?x", pq._whereClauseTriples[0]._s);
    ASSERT_EQ(":myrel", pq._whereClauseTriples[0]._p);
    ASSERT_EQ("?y", pq._whereClauseTriples[0]._o);
    ASSERT_EQ("?y", pq._whereClauseTriples[1]._s);
    ASSERT_EQ("ns:myrel", pq._whereClauseTriples[1]._p);
    ASSERT_EQ("?z", pq._whereClauseTriples[1]._o);
    ASSERT_EQ("?y", pq._whereClauseTriples[2]._s);
    ASSERT_EQ("nsx:rel2", pq._whereClauseTriples[2]._p);
    ASSERT_EQ("<http://abc.de>", pq._whereClauseTriples[2]._o);

    pq = SparqlParser::parse(
    "PREFIX : <http://rdf.myprefix.com/>\n"
        "PREFIX ns: <http://rdf.myprefix.com/ns/>\n"
        "PREFIX xxx: <http://rdf.myprefix.com/xxx/>\n"
        "SELECT ?x ?z \n "
        "WHERE \t {\n?x :myrel ?y. ?y ns:myrel ?z.\n?y nsx:rel2 <http://abc.de>\n}");
    ASSERT_EQ(3, pq._prefixes.size());
    ASSERT_EQ(2, pq._selectedVariables.size());
    ASSERT_EQ(3, pq._whereClauseTriples.size());

    ASSERT_EQ("", pq._prefixes[0]._prefix);
    ASSERT_EQ("<http://rdf.myprefix.com/>", pq._prefixes[0]._uri);
    ASSERT_EQ("ns", pq._prefixes[1]._prefix);
    ASSERT_EQ("<http://rdf.myprefix.com/ns/>", pq._prefixes[1]._uri);
    ASSERT_EQ("?x", pq._selectedVariables[0]);
    ASSERT_EQ("?z", pq._selectedVariables[1]);
    ASSERT_EQ("?x", pq._whereClauseTriples[0]._s);
    ASSERT_EQ(":myrel", pq._whereClauseTriples[0]._p);
    ASSERT_EQ("?y", pq._whereClauseTriples[0]._o);
    ASSERT_EQ("?y", pq._whereClauseTriples[1]._s);
    ASSERT_EQ("ns:myrel", pq._whereClauseTriples[1]._p);
    ASSERT_EQ("?z", pq._whereClauseTriples[1]._o);
    ASSERT_EQ("?y", pq._whereClauseTriples[2]._s);
    ASSERT_EQ("nsx:rel2", pq._whereClauseTriples[2]._p);
    ASSERT_EQ("<http://abc.de>", pq._whereClauseTriples[2]._o);
};

TEST(ParserTest, testExpandPrefixes) {
    ParsedQuery pq = SparqlParser::parse(
        "PREFIX : <http://rdf.myprefix.com/>\n"
        "PREFIX ns: <http://rdf.myprefix.com/ns/>\n"
        "PREFIX xxx: <http://rdf.myprefix.com/xxx/>\n"
        "SELECT ?x ?z \n "
        "WHERE \t {?x :myrel ?y. ?y ns:myrel ?z.?y nsx:rel2 <http://abc.de>}");
    pq.expandPrefixes();
    ASSERT_EQ(3, pq._prefixes.size());
    ASSERT_EQ(2, pq._selectedVariables.size());
    ASSERT_EQ(3, pq._whereClauseTriples.size());
    ASSERT_EQ("", pq._prefixes[0]._prefix);
    ASSERT_EQ("<http://rdf.myprefix.com/>", pq._prefixes[0]._uri);
    ASSERT_EQ("ns", pq._prefixes[1]._prefix);
    ASSERT_EQ("<http://rdf.myprefix.com/ns/>", pq._prefixes[1]._uri);
    ASSERT_EQ("?x", pq._selectedVariables[0]);
    ASSERT_EQ("?z", pq._selectedVariables[1]);
    ASSERT_EQ("?x", pq._whereClauseTriples[0]._s);
    ASSERT_EQ("<http://rdf.myprefix.com/myrel>", pq._whereClauseTriples[0]._p);
    ASSERT_EQ("?y", pq._whereClauseTriples[0]._o);
    ASSERT_EQ("?y", pq._whereClauseTriples[1]._s);
    ASSERT_EQ("<http://rdf.myprefix.com/ns/myrel>", pq._whereClauseTriples[1]
        ._p);
    ASSERT_EQ("?z", pq._whereClauseTriples[1]._o);
    ASSERT_EQ("?y", pq._whereClauseTriples[2]._s);
    ASSERT_EQ("nsx:rel2", pq._whereClauseTriples[2]._p);
    ASSERT_EQ("<http://abc.de>", pq._whereClauseTriples[2]._o);
}
int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}