// Copyright 2021, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Robin Textor-Falconi (textorr@informatik.uni-freiburg.de)

#include <gtest/gtest.h>

#include "../src/util/streamable_generator.h"

using namespace ad_utility::stream_generator;

void throwException() { throw std::runtime_error("Test Exception"); }

stream_generator generateException() {
  throwException();
  co_return;
}

TEST(StreamableGeneratorTest, TestGeneratorExceptionResultsInException) {
  auto generator = generateException();
  ASSERT_TRUE(generator.hasNext());
  try {
    generator.next();
    FAIL() << "Generator should throw Exception";
  } catch (const std::exception& e) {
    ASSERT_STREQ(e.what(), "Test Exception");
  }
  ASSERT_FALSE(generator.hasNext());
}

stream_generator generateNothing() { co_return; }

TEST(StreamableGeneratorTest, TestEmptyGeneratorReturnsEmptyResult) {
  auto generator = generateNothing();
  ASSERT_TRUE(generator.hasNext());
  ASSERT_EQ(generator.next(), "");
  ASSERT_FALSE(generator.hasNext());
}

stream_generator generateMultipleElements() {
  co_yield std::string(1024, 'A');
  co_yield 1;
  co_yield "Abc";
}

TEST(StreamableGeneratorTest, TestGeneratorReturnsBufferedResults) {
  auto generator = generateMultipleElements();

  ASSERT_TRUE(generator.hasNext());
  ASSERT_EQ(generator.next(), std::string(1024, 'A'));

  ASSERT_TRUE(generator.hasNext());
  ASSERT_EQ(generator.next(), std::string("1Abc"));

  ASSERT_FALSE(generator.hasNext());
}
