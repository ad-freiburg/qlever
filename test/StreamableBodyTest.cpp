// Copyright 2021, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Robin Textor-Falconi (textorr@informatik.uni-freiburg.de)

#include <gtest/gtest.h>

#include "../src/util/HttpServer/beast.h"
#include "../src/util/HttpServer/streamable_body.h"

using namespace ad_utility::httpUtils::httpStreams;

TEST(StreamableBodyTest, TestInitReturnsNoErrorCode) {
  ad_utility::stream_generator::stream_generator generator;
  boost::beast::http::header<false, boost::beast::http::fields> header;
  streamable_body::writer writer{header, generator};
  boost::system::error_code errorCode;

  writer.init(errorCode);
  ASSERT_EQ(errorCode, boost::system::error_code());
}

void throwException() { throw std::runtime_error("Test Exception"); }

ad_utility::stream_generator::stream_generator generateException() {
  throwException();
  co_return;
}

TEST(StreamableBodyTest, TestGeneratorExceptionResultsInErrorCode) {
  auto generator = generateException();
  boost::beast::http::header<false, boost::beast::http::fields> header;
  streamable_body::writer writer{header, generator};
  boost::system::error_code errorCode;

  auto result = writer.get(errorCode);
  ASSERT_NE(errorCode, boost::system::error_code());
  ASSERT_EQ(result, boost::none);
}

ad_utility::stream_generator::stream_generator generateNothing() { co_return; }

TEST(StreamableBodyTest, TestEmptyGeneratorReturnsEmptyResult) {
  auto generator = generateNothing();
  boost::beast::http::header<false, boost::beast::http::fields> header;
  streamable_body::writer writer{header, generator};
  boost::system::error_code errorCode;

  auto result = writer.get(errorCode);
  ASSERT_EQ(errorCode, boost::system::error_code());
  ASSERT_NE(result, boost::none);
  ASSERT_EQ(std::get<0>(*result).size(), static_cast<size_t>(0));
  ASSERT_FALSE(std::get<1>(*result));
}

ad_utility::stream_generator::stream_generator generateMultipleElements() {
  co_yield std::string(1000, 'A');
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
  ASSERT_NE(result, boost::none);
  ASSERT_EQ(toStringView(std::get<0>(*result)), std::string(1000, 'A'));
  ASSERT_TRUE(std::get<1>(*result));

  auto result2 = writer.get(errorCode);
  ASSERT_EQ(errorCode, boost::system::error_code());
  ASSERT_NE(result, boost::none);
  ASSERT_EQ(toStringView(std::get<0>(*result2)), std::string("1Abc"));
  ASSERT_FALSE(std::get<1>(*result2));
}
