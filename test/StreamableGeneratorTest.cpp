// Copyright 2021, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Robin Textor-Falconi (textorr@informatik.uni-freiburg.de)

#include <gtest/gtest.h>

#include "util/stream_generator.h"

using namespace ad_utility::streams;

namespace {
constexpr size_t TEST_BUFFER_SIZE = 10;
}  // namespace

// _____________________________________________________________________________
TEST(StreamableGenerator, GeneratorExceptionResultsInException) {
  auto generator = []() -> stream_generator {
    throw std::runtime_error{"Test Exception"};
    co_return;
  }();
  try {
    generator.begin();
    FAIL() << "Generator should throw Exception";
  } catch (const std::exception& e) {
    ASSERT_STREQ(e.what(), "Test Exception");
  }
}

// _____________________________________________________________________________
TEST(StreamableGenerator, EmptyGeneratorReturnsEmptyResult) {
  auto generator = []() -> stream_generator { co_return; }();
  auto iterator = generator.begin();
  ASSERT_EQ(iterator, generator.end());
}

// _____________________________________________________________________________
TEST(StreamableGenerator, GeneratorReturnsBufferedResults) {
  auto generator = []() -> basic_stream_generator<TEST_BUFFER_SIZE> {
    co_yield std::string(TEST_BUFFER_SIZE, 'A');
    // Suppress false positive on GCC 11
    DISABLE_OVERREAD_WARNINGS
    co_yield '1';
    ENABLE_OVERREAD_WARNINGS
    co_yield "Abc";
  }();

  auto iterator = generator.begin();
  ASSERT_NE(iterator, generator.end());
  EXPECT_EQ(*iterator, "AAAAAAAAAA");
  ++iterator;

  ASSERT_NE(iterator, generator.end());
  EXPECT_EQ(*iterator, "1Abc");
  ++iterator;

  ASSERT_EQ(iterator, generator.end());
}

// _____________________________________________________________________________
TEST(StreamableGenerator, GeneratorReturnsBufferedResultsIfTooLarge) {
  auto generator = []() -> basic_stream_generator<TEST_BUFFER_SIZE> {
    co_yield std::string(TEST_BUFFER_SIZE / 2, 'A');
    co_yield std::string(TEST_BUFFER_SIZE, 'B');
    co_yield std::string(TEST_BUFFER_SIZE * 2, 'C');
  }();

  auto iterator = generator.begin();
  ASSERT_NE(iterator, generator.end());
  EXPECT_EQ(*iterator, "AAAAABBBBB");
  ++iterator;

  ASSERT_NE(iterator, generator.end());
  EXPECT_EQ(*iterator, "BBBBBCCCCC");
  ++iterator;

  ASSERT_NE(iterator, generator.end());
  EXPECT_EQ(*iterator, "CCCCCCCCCC");
  ++iterator;

  ASSERT_NE(iterator, generator.end());
  EXPECT_EQ(*iterator, "CCCCC");
  ++iterator;

  ASSERT_EQ(iterator, generator.end());
}

// _____________________________________________________________________________
TEST(StreamableGenerator, GeneratorDefaultInitialisesWithNoOp) {
  stream_generator generator;
  auto iterator = generator.begin();
  ASSERT_EQ(iterator, generator.end());
}
