// Copyright 2022, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Robin Textor-Falconi (textorr@informatik.uni-freiburg.de)

#include <gtest/gtest.h>

#include "../src/util/HttpServer/ContentEncodingHelper.h"

using namespace ad_utility::content_encoding;
namespace http = boost::beast::http;
using ad_utility::content_encoding::CompressionMethod;

class ContentEncodingHelperFixture
    : public ::testing::TestWithParam<
          std::pair<CompressionMethod, std::string_view>> {};

TEST_P(ContentEncodingHelperFixture, HeaderIsNotSetCorrectly) {
  const auto& [compressionMethod, headerValue] = GetParam();

  http::header<false, http::fields> header;
  setContentEncodingHeaderForCompressionMethod(compressionMethod, header);

  ASSERT_EQ(header[http::field::content_encoding], headerValue);
}

TEST(ContentEncodingHelper, TestHeadersAreInsertedCorrectly) {
  http::header<false, http::fields> header;
  setContentEncodingHeaderForCompressionMethod(CompressionMethod::GZIP, header);
  setContentEncodingHeaderForCompressionMethod(CompressionMethod::DEFLATE,
                                               header);
  setContentEncodingHeaderForCompressionMethod(CompressionMethod::DEFLATE,
                                               header);

  auto [current, end] = header.equal_range(http::field::content_encoding);
  ASSERT_NE(current, end);
  ASSERT_EQ(current->value(), "gzip");
  current++;
  ASSERT_NE(current, end);
  ASSERT_EQ(current->value(), "deflate");
  current++;
  ASSERT_NE(current, end);
  ASSERT_EQ(current->value(), "deflate");
  current++;
  ASSERT_EQ(current, end);
}

auto getValuePairsForHeaderTest() {
  return ::testing::Values(
      // empty string_view means no such header is present
      std::pair{CompressionMethod::NONE, std::string_view{}},
      std::pair{CompressionMethod::DEFLATE, "deflate"},
      std::pair{CompressionMethod::GZIP, "gzip"});
}

INSTANTIATE_TEST_SUITE_P(CompressionMethodParameters,
                         ContentEncodingHelperFixture,
                         getValuePairsForHeaderTest());

TEST(ContentEncodingHelper, NoneHeaderIsIndentifiedCorrectly) {
  http::request<http::string_body> request;
  auto result = getCompressionMethodForRequest(request);

  ASSERT_EQ(result, CompressionMethod::NONE);
}

TEST(ContentEncodingHelper, GzipHeaderIsIndentifiedCorrectly) {
  http::request<http::string_body> request;
  request.set(http::field::accept_encoding, "gzip");
  auto result = getCompressionMethodForRequest(request);

  ASSERT_EQ(result, CompressionMethod::GZIP);
}

TEST(ContentEncodingHelper, DeflateHeaderIsIndentifiedCorrectly) {
  http::request<http::string_body> request;
  request.set(http::field::accept_encoding, "deflate");
  auto result = getCompressionMethodForRequest(request);

  ASSERT_EQ(result, CompressionMethod::DEFLATE);
}

TEST(ContentEncodingHelper, DeflateHeaderIsPreferredOverGzip) {
  http::request<http::string_body> request;
  request.set(http::field::accept_encoding, "gzip, deflate");
  auto result = getCompressionMethodForRequest(request);

  ASSERT_EQ(result, CompressionMethod::DEFLATE);
}

TEST(ContentEncodingHelper, DeflateHeaderIsPreferredOverGzipOnMultipleHeaders) {
  http::request<http::string_body> request;
  request.insert(http::field::accept_encoding, "gzip");
  request.insert(http::field::accept_encoding, "deflate");
  auto result = getCompressionMethodForRequest(request);

  ASSERT_EQ(result, CompressionMethod::DEFLATE);
}
