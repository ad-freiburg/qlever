// Copyright 2023, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Andre Schlegel (May of 2023,
// schlegea@informatik.uni-freiburg.de)

#include <ANTLRInputStream.h>
#include <gtest/gtest.h>

#include "../test/util/ANTLR4Mockup/ANTLR4MockupLexer.h"
#include "util/ANTLRHelper.h"

/*
For the helper function `stringOnlyContainsSpecifiedTokens` in
`benchmark/infrastructure/util/ANTLRLexerHelper.h`.
*/
TEST(ANTLRE4UtilTests, stringOnlyContainsSpecifiedTokens) {
  // Everything is correct.
  ASSERT_TRUE(stringOnlyContainsSpecifiedTokens<ANTLR4MockupLexer>(
      "true -429 6.7 \"test\"",
      {static_cast<size_t>(ANTLR4MockupLexer::BOOL),
       static_cast<size_t>(ANTLR4MockupLexer::INTEGER),
       static_cast<size_t>(ANTLR4MockupLexer::FLOAT),
       static_cast<size_t>(ANTLR4MockupLexer::STRING)}));
  ASSERT_TRUE(stringOnlyContainsSpecifiedTokens<ANTLR4MockupLexer>(
      "true", {static_cast<size_t>(ANTLR4MockupLexer::BOOL)}));
  ASSERT_TRUE(stringOnlyContainsSpecifiedTokens<ANTLR4MockupLexer>(
      "6.7 6.7 6.7 6.7 6.7", {static_cast<size_t>(ANTLR4MockupLexer::FLOAT),
                              static_cast<size_t>(ANTLR4MockupLexer::FLOAT),
                              static_cast<size_t>(ANTLR4MockupLexer::FLOAT),
                              static_cast<size_t>(ANTLR4MockupLexer::FLOAT),
                              static_cast<size_t>(ANTLR4MockupLexer::FLOAT)}));

  // We got more indexes to check, than we have tokens.
  ASSERT_FALSE(stringOnlyContainsSpecifiedTokens<ANTLR4MockupLexer>(
      "true -429 6.7", {static_cast<size_t>(ANTLR4MockupLexer::BOOL),
                        static_cast<size_t>(ANTLR4MockupLexer::INTEGER),
                        static_cast<size_t>(ANTLR4MockupLexer::FLOAT),
                        static_cast<size_t>(ANTLR4MockupLexer::STRING)}));

  // We got more tokens, than we have indexes to check.
  ASSERT_FALSE(stringOnlyContainsSpecifiedTokens<ANTLR4MockupLexer>(
      "true -429 6.7", {static_cast<size_t>(ANTLR4MockupLexer::BOOL)}));

  // The first index is wrong.
  ASSERT_FALSE(stringOnlyContainsSpecifiedTokens<ANTLR4MockupLexer>(
      " -429 6.7", {static_cast<size_t>(ANTLR4MockupLexer::BOOL),
                    static_cast<size_t>(ANTLR4MockupLexer::FLOAT)}));

  // The last index is wrong.
  ASSERT_FALSE(stringOnlyContainsSpecifiedTokens<ANTLR4MockupLexer>(
      " -429 6.7", {static_cast<size_t>(ANTLR4MockupLexer::INTEGER),
                    static_cast<size_t>(ANTLR4MockupLexer::BOOL)}));

  // One index in the middle is wrong.
  ASSERT_FALSE(stringOnlyContainsSpecifiedTokens<ANTLR4MockupLexer>(
      "true -429 6.7", {static_cast<size_t>(ANTLR4MockupLexer::BOOL),
                        static_cast<size_t>(ANTLR4MockupLexer::STRING),
                        static_cast<size_t>(ANTLR4MockupLexer::FLOAT)}));

  // The indexes are all completly false.
  ASSERT_FALSE(stringOnlyContainsSpecifiedTokens<ANTLR4MockupLexer>(
      " -429 6.7 true", {static_cast<size_t>(ANTLR4MockupLexer::BOOL),
                         static_cast<size_t>(ANTLR4MockupLexer::INTEGER),
                         static_cast<size_t>(ANTLR4MockupLexer::FLOAT)}));

  // Call with empty string and wanted indexes.
  ASSERT_FALSE(stringOnlyContainsSpecifiedTokens<ANTLR4MockupLexer>(
      "", {static_cast<size_t>(ANTLR4MockupLexer::INTEGER),
           static_cast<size_t>(ANTLR4MockupLexer::BOOL)}));

  // Call with no string and no wanted indexes.
  ASSERT_TRUE(stringOnlyContainsSpecifiedTokens<ANTLR4MockupLexer>("", {}));

  // Call with string and no wanted indexes.
  ASSERT_FALSE(
      stringOnlyContainsSpecifiedTokens<ANTLR4MockupLexer>(" -429 6.7", {}));
}
