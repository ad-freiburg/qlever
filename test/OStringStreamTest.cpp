// Copyright 2021, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Robin Textor-Falconi (textorr@informatik.uni-freiburg.de)

#include <gtest/gtest.h>

#include "../src/util/ostringstream.h"

using namespace ad_utility::streams::implementation;

TEST(OStringStreamTest, TestViewIsCorrect) {
  const std::string testString = "Abc";
  ostringstream stream;
  stream << testString;
  ASSERT_EQ(stream.view(), testString);
}

TEST(OStringStreamTest, TestViewMultipleConcatenations) {
  const std::string testString = "Abc";
  ostringstream stream;
  stream << testString << testString;
  ASSERT_EQ(stream.view(), testString + testString);
}

TEST(OStringStreamTest, TestStrFunctionWorks) {
  const std::string testString = "Abc";
  ostringstream stream;
  stream << "Value should be deleted";
  stream.str(testString);
  ASSERT_EQ(stream.view(), testString);
}

TEST(CustomBufferTest, TestViewIsCorrect) {
  const std::string testString = "Abc";
  CustomBuffer buffer;
  buffer.str(testString);
  ASSERT_EQ(buffer.view(), testString);
}
