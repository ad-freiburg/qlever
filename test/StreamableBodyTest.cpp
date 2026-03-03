// Copyright 2021, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Robin Textor-Falconi (textorr@informatik.uni-freiburg.de)

#include <gtest/gtest.h>

#include "util/http/beast.h"
#include "util/http/streamable_body.h"

using namespace ad_utility::httpUtils::httpStreams;

namespace {
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

// _____________________________________________________________________________
TEST(StreamableBody, InitReturnsNoErrorCode) {
  cppcoro::generator<std::string> generator{};
  boost::beast::http::header<false, boost::beast::http::fields> header;
  streamable_body::writer writer{header, generator};
  boost::system::error_code errorCode;

  writer.init(errorCode);
  ASSERT_EQ(errorCode, boost::system::error_code());
}

// _____________________________________________________________________________
TEST(StreamableBody, GeneratorExceptionResultsInErrorCode) {
  auto generator = []() -> cppcoro::generator<std::string> {
    throw std::runtime_error("Test Exception");
    co_return;
  }();
  boost::beast::http::header<false, boost::beast::http::fields> header;
  streamable_body::writer writer{header, generator};
  boost::system::error_code errorCode;

  auto result = writer.get(errorCode);
  ASSERT_NE(errorCode, boost::system::error_code());
  ASSERT_EQ(result, boost::none);
}

// _____________________________________________________________________________
TEST(StreamableBody, EmptyGeneratorReturnsEmptyResult) {
  auto generator = []() -> cppcoro::generator<std::string> { co_return; }();
  boost::beast::http::header<false, boost::beast::http::fields> header;
  streamable_body::writer writer{header, generator};
  boost::system::error_code errorCode;

  auto result = writer.get(errorCode);
  ASSERT_EQ(errorCode, boost::system::error_code());
  ASSERT_EQ(result, boost::none);
}

// _____________________________________________________________________________
TEST(StreamableBody, GeneratorReturnsBufferedResults) {
  auto generator = []() -> cppcoro::generator<std::string> {
    co_yield "AAAAAAAAAA";
    co_yield "1Abc";
  }();
  boost::beast::http::header<false, boost::beast::http::fields> header;
  streamable_body::writer writer{header, generator};
  boost::system::error_code errorCode;

  auto result = writer.get(errorCode);
  ASSERT_EQ(errorCode, boost::system::error_code());
  ASSERT_NE(result, boost::none);
  ASSERT_EQ(toStringView(result->first), "AAAAAAAAAA");
  ASSERT_TRUE(result->second);

  auto result2 = writer.get(errorCode);
  ASSERT_EQ(errorCode, boost::system::error_code());
  ASSERT_NE(result2, boost::none);
  ASSERT_EQ(toStringView(result2->first), "1Abc");
  ASSERT_TRUE(result2->second);

  auto result3 = writer.get(errorCode);
  ASSERT_EQ(errorCode, boost::system::error_code());
  ASSERT_EQ(result3, boost::none);
}
