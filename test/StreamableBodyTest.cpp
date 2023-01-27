// Copyright 2021, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Robin Textor-Falconi (textorr@informatik.uni-freiburg.de)

#include <gtest/gtest.h>

#include "util/http/beast.h"
#include "util/http/streamable_body.h"

using namespace ad_utility::httpUtils::httpStreams;
using ad_utility::streams::basic_stream_generator;
using ad_utility::streams::stream_generator;

namespace {
cppcoro::generator<std::string> toGenerator(stream_generator generator) {
  for (auto& value : generator) {
    co_yield value;
  }
}

std::string_view toStringView(
    const streamable_body::writer::const_buffers_type& buffer) {
  return {static_cast<const char*>(buffer.data()), buffer.size()};
}
}  // namespace

namespace boost {
// When using ASSERT_EQ/ASSERT_NE GTest always needs operator<< to print the
// arguments in case of a mismatch, which are not provided by default for
// the following boost parameters:
static std::ostream& operator<<(std::ostream& out, none_t) {
  out << "boost::none";
  return out;
}
static std::ostream& operator<<(
    std::ostream& out,
    const optional<std::pair<asio::const_buffer, bool>>& optionalBuffer) {
  if (!optionalBuffer.has_value()) {
    return out << boost::none;
  }
  const auto& [buffer, hasNext] = optionalBuffer.value();
  out << "Value: \"";
  out << toStringView(buffer);
  out << "\", hasNext: ";
  out << hasNext;
  return out;
}
}  // namespace boost

namespace {
constexpr size_t BUFFER_SIZE = 1u << 20;
}

TEST(StreamableBodyTest, TestInitReturnsNoErrorCode) {
  auto generator = toGenerator(stream_generator{});
  boost::beast::http::header<false, boost::beast::http::fields> header;
  streamable_body::writer writer{header, generator};
  boost::system::error_code errorCode;

  writer.init(errorCode);
  ASSERT_EQ(errorCode, boost::system::error_code());
}

namespace {
stream_generator generateException() {
  throw std::runtime_error("Test Exception");
  co_return;
}
}  // namespace

TEST(StreamableBodyTest, TestGeneratorExceptionResultsInErrorCode) {
  auto generator = toGenerator(generateException());
  boost::beast::http::header<false, boost::beast::http::fields> header;
  streamable_body::writer writer{header, generator};
  boost::system::error_code errorCode;

  auto result = writer.get(errorCode);
  ASSERT_NE(errorCode, boost::system::error_code());
  ASSERT_EQ(result, boost::none);
}

namespace {
stream_generator generateNothing() { co_return; }
}  // namespace

TEST(StreamableBodyTest, TestEmptyGeneratorReturnsEmptyResult) {
  auto generator = toGenerator(generateNothing());
  boost::beast::http::header<false, boost::beast::http::fields> header;
  streamable_body::writer writer{header, generator};
  boost::system::error_code errorCode;

  auto result = writer.get(errorCode);
  ASSERT_EQ(errorCode, boost::system::error_code());
  ASSERT_EQ(result, boost::none);
}

namespace {
stream_generator generateMultipleElements() {
  co_yield std::string(BUFFER_SIZE, 'A');
  co_yield 1;
  co_yield "Abc";
}
}  // namespace

TEST(StreamableBodyTest, TestGeneratorReturnsBufferedResults) {
  auto generator = toGenerator(generateMultipleElements());
  boost::beast::http::header<false, boost::beast::http::fields> header;
  streamable_body::writer writer{header, generator};
  boost::system::error_code errorCode;

  auto result = writer.get(errorCode);
  ASSERT_EQ(errorCode, boost::system::error_code());
  ASSERT_NE(result, boost::none);
  ASSERT_EQ(toStringView(result->first), std::string(BUFFER_SIZE, 'A'));
  ASSERT_TRUE(result->second);

  auto result2 = writer.get(errorCode);
  ASSERT_EQ(errorCode, boost::system::error_code());
  ASSERT_NE(result2, boost::none);
  ASSERT_EQ(toStringView(result2->first), std::string("1Abc"));
  ASSERT_TRUE(result2->second);

  auto result3 = writer.get(errorCode);
  ASSERT_EQ(errorCode, boost::system::error_code());
  ASSERT_EQ(result3, boost::none);
}
