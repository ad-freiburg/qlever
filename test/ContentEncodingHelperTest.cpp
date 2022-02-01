// Copyright 2022, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Robin Textor-Falconi (textorr@informatik.uni-freiburg.de)

#include <gtest/gtest.h>

#include "../src/util/HttpServer/ContentEncodingHelper.h"
#include "../src/util/HttpServer/beast.h"

using namespace ad_utility::content_encoding;
namespace http = boost::beast::http;
using ad_utility::content_encoding::CompressionMethod;

TEST(ContentEncodingHelperTest, TestNoneHeaderIsNotSetCorrectly) {
  http::header<false, http::fields> header;
  setContentEncodingHeaderForCompressionMethod(CompressionMethod::NONE, header);

  // empty string_view means no such header is present
  ASSERT_EQ(header[http::field::content_encoding], std::string_view{});
}

TEST(ContentEncodingHelperTest, TestDeflateHeaderIsSetCorrectly) {
  http::header<false, http::fields> header;
  setContentEncodingHeaderForCompressionMethod(CompressionMethod::DEFLATE,
                                               header);

  ASSERT_EQ(header[http::field::content_encoding], "deflate");
}

TEST(ContentEncodingHelperTest, TestGzipHeaderIsSetCorrectly) {
  http::header<false, http::fields> header;
  setContentEncodingHeaderForCompressionMethod(CompressionMethod::GZIP, header);

  ASSERT_EQ(header[http::field::content_encoding], "gzip");
}

TEST(ContentEncodingHelperTest, TestNoHeaderIsIndentifiedCorrectly) {
  http::request<http::string_body> request;
  auto result = getCompressionMethodForRequest(request);

  ASSERT_EQ(result, CompressionMethod::NONE);
}

TEST(ContentEncodingHelperTest, TestGzipHeaderIsIndentifiedCorrectly) {
  http::request<http::string_body> request;
  request.set(http::field::accept_encoding, "gzip");
  auto result = getCompressionMethodForRequest(request);

  ASSERT_EQ(result, CompressionMethod::GZIP);
}

TEST(ContentEncodingHelperTest, TestDeflateHeaderIsIndentifiedCorrectly) {
  http::request<http::string_body> request;
  request.set(http::field::accept_encoding, "deflate");
  auto result = getCompressionMethodForRequest(request);

  ASSERT_EQ(result, CompressionMethod::DEFLATE);
}

TEST(ContentEncodingHelperTest, TestDeflateHeaderIsPreferredOverGzip) {
  http::request<http::string_body> request;
  request.set(http::field::accept_encoding, "gzip, deflate");
  auto result = getCompressionMethodForRequest(request);

  ASSERT_EQ(result, CompressionMethod::DEFLATE);
}
