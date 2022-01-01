// Copyright 2021, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Robin Textor-Falconi (textorr@informatik.uni-freiburg.de)

#include <gtest/gtest.h>

#include "../src/util/streamable_generator.h"

using namespace ad_utility::stream_generator;

constexpr size_t TEST_BUFFER_SIZE = 10;

stream_generator generateException() {
  throw std::runtime_error("Test Exception");
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

basic_stream_generator<TEST_BUFFER_SIZE> generateMultipleElements() {
  co_yield std::string(TEST_BUFFER_SIZE, 'A');
  co_yield 1;
  co_yield "Abc";
}

TEST(StreamableGeneratorTest, TestGeneratorReturnsBufferedResults) {
  auto generator = generateMultipleElements();

  ASSERT_TRUE(generator.hasNext());
  ASSERT_EQ(generator.next(), std::string(TEST_BUFFER_SIZE, 'A'));

  ASSERT_TRUE(generator.hasNext());
  ASSERT_EQ(generator.next(), std::string("1Abc"));

  ASSERT_FALSE(generator.hasNext());
}

TEST(StreamableGeneratorTest, TestGeneratorDefaultInitialisesWithNoOp) {
  stream_generator generator;

  ASSERT_TRUE(generator.hasNext());
  std::string_view view = generator.next();
  ASSERT_TRUE(view.empty());
  ASSERT_FALSE(generator.hasNext());
}

TEST(StreamableGeneratorTest, TestGeneratorNextThrowsExceptionWhenDone) {
  auto generator = generateNothing();

  generator.next();
  try {
    generator.next();
    FAIL() << "next() should throw an exception";
  } catch (const std::exception& e) {
    ASSERT_STREQ(e.what(), "Coroutine is not active");
  }
}
