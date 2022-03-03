// Copyright 2021, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Robin Textor-Falconi (textorr@informatik.uni-freiburg.de)

#include <gtest/gtest.h>

#include "../src/util/stream_generator.h"

using namespace ad_utility::streams;

constexpr size_t TEST_BUFFER_SIZE = 10;

stream_generator generateException() {
  throw std::runtime_error("Test Exception");
  co_return;
}

TEST(StreamableGeneratorTest, TestGeneratorExceptionResultsInException) {
  auto generator = generateException();
  try {
    generator.begin();
    FAIL() << "Generator should throw Exception";
  } catch (const std::exception& e) {
    ASSERT_STREQ(e.what(), "Test Exception");
  }
}

stream_generator generateNothing() { co_return; }

TEST(StreamableGeneratorTest, TestEmptyGeneratorReturnsEmptyResult) {
  auto generator = generateNothing();
  auto iterator = generator.begin();
  ASSERT_NE(iterator, generator.end());
  ASSERT_EQ(*iterator, "");
  iterator++;
  ASSERT_EQ(iterator, generator.end());
}

const std::string MAX_TEST_BUFFER_STRING(TEST_BUFFER_SIZE, 'A');

basic_stream_generator<TEST_BUFFER_SIZE> generateMultipleElements() {
  co_yield MAX_TEST_BUFFER_STRING;
  co_yield 1;
  co_yield "Abc";
}

TEST(StreamableGeneratorTest, TestGeneratorReturnsBufferedResults) {
  auto generator = generateMultipleElements();

  auto iterator = generator.begin();
  ASSERT_NE(iterator, generator.end());
  ASSERT_EQ(*iterator, MAX_TEST_BUFFER_STRING);
  iterator++;

  ASSERT_NE(iterator, generator.end());
  ASSERT_EQ(*iterator, std::string("1Abc"));

  ASSERT_EQ(iterator, generator.end());
}

TEST(StreamableGeneratorTest, TestGeneratorDefaultInitialisesWithNoOp) {
  stream_generator generator;
  auto iterator = generator.begin();
  ASSERT_NE(iterator, generator.end());
  ASSERT_EQ(*iterator, "");
  iterator++;
  ASSERT_EQ(iterator, generator.end());
}
