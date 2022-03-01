// Copyright 2021, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Robin Textor-Falconi (textorr@informatik.uni-freiburg.de)

#include <gtest/gtest.h>

#include <memory>

#include "../src/util/HttpServer/beast.h"
#include "../src/util/HttpServer/streamable_body.h"

using namespace ad_utility::httpUtils::httpStreams;
using ad_utility::streams::basic_stream_generator;
using ad_utility::streams::stream_generator;
using ad_utility::streams::StringSupplier;

std::string_view toStringView(
    const streamable_body::writer::const_buffers_type& buffer) {
  return {static_cast<const char*>(buffer.data()), buffer.size()};
}

namespace boost {
// When using ASSERT_EQ/ASSERT_NE GTest always needs operator<< to print the
// arguments in case of a mismatch, which are not provided by default for
// the following boost parameters:
std::ostream& operator<<(std::ostream& out, none_t) {
  out << "boost::none";
  return out;
}
std::ostream& operator<<(
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

constexpr size_t BUFFER_SIZE = 1u << 20;

template <size_t BUFFER_SIZE>
std::unique_ptr<StringSupplier> toPtr(
    basic_stream_generator<BUFFER_SIZE>&& generator) {
  return std::make_unique<basic_stream_generator<BUFFER_SIZE>>(
      std::move(generator));
}

TEST(StreamableBodyTest, TestInitReturnsNoErrorCode) {
  auto generator = toPtr(stream_generator{});
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
  auto generator = toPtr(generateException());
  boost::beast::http::header<false, boost::beast::http::fields> header;
  streamable_body::writer writer{header, generator};
  boost::system::error_code errorCode;

  auto result = writer.get(errorCode);
  ASSERT_NE(errorCode, boost::system::error_code());
  ASSERT_EQ(result, boost::none);
}

stream_generator generateNothing() { co_return; }

TEST(StreamableBodyTest, TestEmptyGeneratorReturnsEmptyResult) {
  auto generator = toPtr(generateNothing());
  boost::beast::http::header<false, boost::beast::http::fields> header;
  streamable_body::writer writer{header, generator};
  boost::system::error_code errorCode;

  auto result = writer.get(errorCode);
  ASSERT_EQ(errorCode, boost::system::error_code());
  ASSERT_NE(result, boost::none);
  ASSERT_EQ(result->first.size(), 0u);
  ASSERT_FALSE(result->second);
}

stream_generator generateMultipleElements() {
  co_yield std::string(BUFFER_SIZE, 'A');
  co_yield 1;
  co_yield "Abc";
}

TEST(StreamableBodyTest, TestGeneratorReturnsBufferedResults) {
  auto generator = toPtr(generateMultipleElements());
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
  ASSERT_FALSE(result2->second);
}
