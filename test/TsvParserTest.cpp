// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)

#include <gtest/gtest.h>

#include <cstdio>
#include <fstream>

#include "../src/parser/TsvParser.h"

TEST(TsvParserTest,
     getLineTest){// Without trailing newline
                  {std::fstream f("_testtmp.tsv", std::ios_base::out);
f << "a\tb\tc\t.\n"
     "a2\tb2\tc2\t.";
f.close();
TsvParser p("_testtmp.tsv");
array<string, 3> a;
ASSERT_TRUE(p.getLine(a));
ASSERT_EQ("a", a[0]);
ASSERT_EQ("b", a[1]);
ASSERT_EQ("c", a[2]);
ASSERT_TRUE(p.getLine(a));
ASSERT_EQ("a2", a[0]);
ASSERT_EQ("b2", a[1]);
ASSERT_EQ("c2", a[2]);
ASSERT_FALSE(p.getLine(a));
remove("_testtmp.tsv");
}
// With trailing newline
{
  std::fstream f("_testtmp.tsv", std::ios_base::out);
  f << "a\tb\tc\t.\n"
       "a2\tb2\tc2\t.\n";
  f.close();
  TsvParser p("_testtmp.tsv");
  array<string, 3> a;
  ASSERT_TRUE(p.getLine(a));
  ASSERT_EQ("a", a[0]);
  ASSERT_EQ("b", a[1]);
  ASSERT_EQ("c", a[2]);
  ASSERT_TRUE(p.getLine(a));
  ASSERT_EQ("a2", a[0]);
  ASSERT_EQ("b2", a[1]);
  ASSERT_EQ("c2", a[2]);
  ASSERT_FALSE(p.getLine(a));
  remove("_testtmp.tsv");
}
}
;

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
