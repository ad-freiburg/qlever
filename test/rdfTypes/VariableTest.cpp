//  Copyright 2022, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include <gmock/gmock.h>
#include <unicode/uchar.h>

#include "../util/GTestHelpers.h"
#include "rdfTypes/Variable.h"
#include "util/HashSet.h"
#include "util/Serializer/ByteBufferSerializer.h"

// _____________________________________________________________________________
TEST(Variable, legalAndIllegalNames) {
  if constexpr (!ad_utility::areExpensiveChecksEnabled) {
    GTEST_SKIP()
        << "legality of variable names is only checked with expensive checks";
  }
  EXPECT_NO_THROW(Variable("?x", true));
  EXPECT_NO_THROW(Variable("$x", true));
  EXPECT_NO_THROW(Variable("?ql_matching_word_thür", true));

  // World emoji + chinese symbol for "world" (according to ChatGPT) are also
  // valid.
  EXPECT_NO_THROW(Variable("?hello_world_\U0001F30D\u4E16\u754C", true));

  // Asian scriptures are valid

  // No leading ? or $
  auto matcher = ::testing::HasSubstr("not a valid SPARQL variable");
  AD_EXPECT_THROW_WITH_MESSAGE(Variable("x", true), matcher);
  AD_EXPECT_THROW_WITH_MESSAGE(Variable("?x spaceInVar", true), matcher);
}

// _____________________________________________________________________________
TEST(Variable, DollarToQuestionMark) {
  Variable x{"?x"};
  Variable x2{"$x"};
  EXPECT_EQ(x.name(), "?x");
  EXPECT_EQ(x2.name(), "?x");
}

// _____________________________________________________________________________
TEST(Variable, ScoreAndMatchVariablesUnicode) {
  ad_utility::HashSet<Variable> vars;
  auto f = [&vars](Variable var) { vars.insert(std::move(var)); };

  // Test that the following variables all have valid names (otherwise the
  // constructor of `Variable` would throw), and that they are all unique.
  EXPECT_NO_THROW(f(Variable("?x").getWordScoreVariable("\U0001F600", false)));
  EXPECT_NO_THROW(f(Variable("?x").getWordScoreVariable("\U0001F600*", true)));
  EXPECT_NO_THROW(f(Variable("?x").getWordScoreVariable("äpfel", false)));
  EXPECT_NO_THROW(f(Variable("?x").getEntityScoreVariable("\U0001F600")));
  EXPECT_NO_THROW(f(Variable("?x").getEntityScoreVariable("äpfel")));
  EXPECT_NO_THROW(f(Variable("?x").getMatchingWordVariable("äpfel")));
  EXPECT_NO_THROW(f(Variable("?x").getMatchingWordVariable("\U0001F600")));

  // Examples of characters that are alphabetic according to Unicode, but are
  // not valid in SPARQL variables.
  EXPECT_NO_THROW(f(Variable("?x").getMatchingWordVariable("\U000000AA")));
  EXPECT_NO_THROW(f(Variable("?x").getMatchingWordVariable("\U000000B5")));
  EXPECT_NO_THROW(f(Variable("?x").getMatchingWordVariable("\U000000BA")));

  // Expect that all the created variables are unique.
  EXPECT_EQ(vars.size(), 10);

  // Ensure that underscores which are used to escape unsupported code points
  // are themselves escaped to ensure unique variable names.
  EXPECT_EQ(Variable("?x").getMatchingWordVariable("_").name(),
            "?ql_matchingword_x__95_");

  // Invalid UTF-8 will throw an exception.
  AD_EXPECT_THROW_WITH_MESSAGE(Variable("?x").getMatchingWordVariable("\255"),
                               ::testing::HasSubstr("Invalid UTF-8"));

  // Regression test for https://github.com/ad-freiburg/qlever/issues/2244
  EXPECT_NO_THROW(Variable("?x").getMatchingWordVariable("�"));
}

// _____________________________________________________________________________
TEST(Variable, ScoreAndMatchUnicodeExhaustive) {
#ifndef QLEVER_RUN_EXPENSIVE_TESTS
  GTEST_SKIP();
#endif
  size_t numErrors = 0;
  size_t numSuccesful = 0;
  std::string buffer;
  buffer.resize(U8_MAX_LENGTH);
  auto testChar = [&](UChar32 cp) {
    UBool isError = false;
    int32_t offset = 0;
    U8_APPEND(buffer.data(), offset, U8_MAX_LENGTH, cp, isError);
    if (isError) {
      throw std::runtime_error("Invalid UChar32 code point.");
    }
    std::string_view s{buffer.data(), buffer.data() + offset};
    try {
      Variable("?x").getMatchingWordVariable(s);
      numSuccesful++;
    } catch (...) {
      ++numErrors;
    }
  };

  for (UChar32 cp = 0; cp <= 0x10FFFF; ++cp) {
    if (cp >= 0xD800 && cp <= 0xDFFF) {
      continue;  // Skip surrogate pairs
    }

    if (u_isalnum(cp)) {
      testChar(cp);
    }
  }
  EXPECT_EQ(numErrors, 0) << numErrors << ' ' << numSuccesful;
}

// _____________________________________________________________________________
TEST(Variable, Serialization) {
  Variable v{"?x"};
  ad_utility::serialization::ByteBufferWriteSerializer writer;
  writer << v;
  ad_utility::serialization::ByteBufferReadSerializer reader(
      std::move(writer).data());
  Variable v2{"?somethingElse"};
  reader | v2;
  EXPECT_EQ(v2.name(), "?x");
}
