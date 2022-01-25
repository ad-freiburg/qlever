// Copyright 2021, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Robin Textor-Falconi (textorr@informatik.uni-freiburg.de)

#include <gtest/gtest.h>

#include "../src/util/HttpServer/beast.h"
#include "../src/util/HttpServer/streamable_body.h"

using namespace ad_utility::httpUtils::httpStreams;
using ad_utility::stream_generator::stream_generator;

constexpr size_t BUFFER_SIZE = 1u << 20;

TEST(StreamableBodyTest, TestInitReturnsNoErrorCode) {
  stream_generator generator;
  boost::beast::http::header<false, boost::beast::http::fields> header;
  streamable_body::writer writer{header, generator};
  boost::system::error_code errorCode;

  writer.init(errorCode);
  ASSERT_EQ(errorCode, boost::system::error_code());
}

stream_generator generateException() {
  throw std::runtime_error("Test Exception");
  co_return;
}

TEST(StreamableBodyTest, TestGeneratorExceptionResultsInErrorCode) {
  auto generator = generateException();
  boost::beast::http::header<false, boost::beast::http::fields> header;
  streamable_body::writer writer{header, generator};
  boost::system::error_code errorCode;

  auto result = writer.get(errorCode);
  ASSERT_NE(errorCode, boost::system::error_code());
  ASSERT_TRUE(result == boost::none);
}

stream_generator generateNothing() { co_return; }

TEST(StreamableBodyTest, TestEmptyGeneratorReturnsEmptyResult) {
  auto generator = generateNothing();
  boost::beast::http::header<false, boost::beast::http::fields> header;
  streamable_body::writer writer{header, generator};
  boost::system::error_code errorCode;

  auto result = writer.get(errorCode);
  ASSERT_EQ(errorCode, boost::system::error_code());
  ASSERT_TRUE(result != boost::none);
  ASSERT_EQ(result->first.size(), 0u);
  ASSERT_FALSE(result->second);
}

stream_generator generateMultipleElements() {
  co_yield std::string(BUFFER_SIZE, 'A');
  co_yield 1;
  co_yield "Abc";
}

std::string_view toStringView(
    const streamable_body::writer::const_buffers_type& buffer) {
  return {static_cast<const char*>(buffer.data()), buffer.size()};
}

TEST(StreamableBodyTest, TestGeneratorReturnsBufferedResults) {
  auto generator = generateMultipleElements();
  boost::beast::http::header<false, boost::beast::http::fields> header;
  streamable_body::writer writer{header, generator};
  boost::system::error_code errorCode;

  auto result = writer.get(errorCode);
  ASSERT_EQ(errorCode, boost::system::error_code());
  ASSERT_TRUE(result != boost::none);
  ASSERT_EQ(toStringView(result->first), std::string(BUFFER_SIZE, 'A'));
  ASSERT_TRUE(result->second);

  auto result2 = writer.get(errorCode);
  ASSERT_EQ(errorCode, boost::system::error_code());
  ASSERT_TRUE(result2 != boost::none);
  ASSERT_EQ(toStringView(result2->first), std::string("1Abc"));
  ASSERT_FALSE(result2->second);
}
