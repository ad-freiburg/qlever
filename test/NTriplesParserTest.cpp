// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)

#include <gtest/gtest.h>

#include <cstdio>
#include <fstream>
#include <iostream>

#include "../src/parser/NTriplesParser.h"
#include "../src/util/Exception.h"

TEST(NTriplesParserTest, getLineTest) {
  try {
    // Like TSV
    {
      std::fstream f("_testtmp.nt", std::ios_base::out);
      f << "<a>\t<b>\t<c>\t.\n"
           "<a2>\t<b2>\t<c2>\t.";
      f.close();
      NTriplesParser p("_testtmp.nt");
      array<string, 3> a;
      ASSERT_TRUE(p.getLine(a));
      ASSERT_EQ("<a>", a[0]);
      ASSERT_EQ("<b>", a[1]);
      ASSERT_EQ("<c>", a[2]);
      ASSERT_TRUE(p.getLine(a));
      ASSERT_EQ("<a2>", a[0]);
      ASSERT_EQ("<b2>", a[1]);
      ASSERT_EQ("<c2>", a[2]);
      ASSERT_FALSE(p.getLine(a));
      remove("_testtmp.nt");
    }
    // With literals etc
    {
      std::fstream f("_testtmp.nt", std::ios_base::out);
      f << "<foo>\t<bar>\t<c>\t.\n"
           "<foo>    <Äö>\t\"this is some text. It goes\ton!\"\t.\n"
           "<a> <b> \"123\"^^<http://foo.bar/a> .\n";
      f.close();
      NTriplesParser p("_testtmp.nt");
      array<string, 3> a;
      ASSERT_TRUE(p.getLine(a));
      ASSERT_EQ("<foo>", a[0]);
      ASSERT_EQ("<bar>", a[1]);
      ASSERT_EQ("<c>", a[2]);
      ASSERT_TRUE(p.getLine(a));
      ASSERT_EQ("<foo>", a[0]);
      ASSERT_EQ("<Äö>", a[1]);
      ASSERT_EQ("\"this is some text. It goes\ton!\"", a[2]);
      ASSERT_TRUE(p.getLine(a));
      ASSERT_EQ("<a>", a[0]);
      ASSERT_EQ("<b>", a[1]);
      ASSERT_EQ("\"123\"^^<http://foo.bar/a>", a[2]);
      ASSERT_FALSE(p.getLine(a));
      remove("_testtmp.nt");
    }
    // blank nodes
    {
      std::fstream f("_testtmp.nt", std::ios_base::out);
      f << "<node1> <rel1> _:b.lank_node_id1 .\n"
           "_:b.lank_node_id1\t<rel2> \"goat cheese.\" .\n"
           "_:bla--nk_no.de_id.4 <relasd> _:b.lank_node_id1 .\n";
      f.close();
      NTriplesParser p("_testtmp.nt");
      array<string, 3> a;
      ASSERT_TRUE(p.getLine(a));
      ASSERT_EQ("<node1>", a[0]);
      ASSERT_EQ("<rel1>", a[1]);
      ASSERT_EQ("_:b.lank_node_id1", a[2]);
      ASSERT_TRUE(p.getLine(a));
      ASSERT_EQ("_:b.lank_node_id1", a[0]);
      ASSERT_EQ("<rel2>", a[1]);
      ASSERT_EQ("\"goat cheese.\"", a[2]);
      ASSERT_TRUE(p.getLine(a));
      ASSERT_EQ("_:bla--nk_no.de_id.4", a[0]);
      ASSERT_EQ("<relasd>", a[1]);
      ASSERT_EQ("_:b.lank_node_id1", a[2]);
      ASSERT_FALSE(p.getLine(a));
      remove("_testtmp.nt");
    }
  } catch (const ad_semsearch::Exception& e) {
    FAIL() << e.getFullErrorMessage();
  }
};

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
