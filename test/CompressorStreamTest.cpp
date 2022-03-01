// Copyright 2022, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Robin Textor-Falconi (textorr@informatik.uni-freiburg.de)

#include <gmock/gmock.h>

#include "../src/util/CompressorStream.h"
#include "../src/util/Exception.h"

namespace io = boost::iostreams;

namespace http = boost::beast::http;
using ad_utility::content_encoding::CompressionMethod;
using ad_utility::streams::StringSupplier;
using ad_utility::streams::CompressorStream;

class CompressorStreamTestFixture1
    : public ::testing::TestWithParam<
          std::pair<CompressionMethod, std::string_view>> {};

std::unique_ptr<StringSupplier> generateNothing() {
  class NoOp : public StringSupplier {
   public:
    [[nodiscard]] constexpr bool hasNext() const override {
      return false;
    }
    [[nodiscard]] constexpr std::string_view next() override {
      return std::string_view{};
    }
  };
  return std::make_unique<NoOp>();
}

std::unique_ptr<StringSupplier> generateNChars(size_t n) {
  class FiniteStream : public StringSupplier {
    size_t _counter = 0;
    size_t _n;
   public:
    explicit FiniteStream(size_t n): _n{n} {}

    [[nodiscard]] bool hasNext() const override {
      return _counter < _n;
    }
    [[nodiscard]] constexpr std::string_view next() override {
      _counter++;
      return "A";
    }
  };
  return std::make_unique<FiniteStream>(n);
}

TEST_P(CompressorStreamTestFixture1,
       TestHeadersAreSetCorrectlyForCompressingGenerator) {
  const auto& [compressionMethod, headerValue] = GetParam();

  CompressorStream stream{generateNothing(), compressionMethod};
  http::header<false, http::fields> header;
  stream.prepareHttpHeaders(header);

  ASSERT_EQ(header[http::field::content_encoding], headerValue);
}

auto getValuePairsForHeaderTest() {
  return ::testing::Values(
      // empty string_view means no such header is present
      std::pair{CompressionMethod::NONE, std::string_view{}},
      std::pair{CompressionMethod::DEFLATE, "deflate"},
      std::pair{CompressionMethod::GZIP, "gzip"});
}

INSTANTIATE_TEST_SUITE_P(CompressionMethodParameters,
                         CompressorStreamTestFixture1,
                         getValuePairsForHeaderTest());

TEST(CompressorStream, TestHeadersAreInsertedCorrectly) {
  CompressorStream stream1{generateNothing(), CompressionMethod::GZIP};
  CompressorStream stream2{generateNothing(), CompressionMethod::DEFLATE};
  CompressorStream stream3{generateNothing(), CompressionMethod::DEFLATE};
  http::header<false, http::fields> header;
  stream1.prepareHttpHeaders(header);
  stream2.prepareHttpHeaders(header);
  stream3.prepareHttpHeaders(header);

  auto [ current, end ] = header.equal_range(http::field::content_encoding);
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

class CompressorStreamTestFixture2
    : public ::testing::TestWithParam<CompressionMethod> {
 public:
  [[nodiscard]] static std::string decompressData(
      std::string_view compressedData) {
    std::string result;
    io::filtering_ostream filterStream;
    if (GetParam() == CompressionMethod::GZIP) {
      filterStream.push(io::gzip_decompressor());
    } else if (GetParam() == CompressionMethod::DEFLATE) {
      filterStream.push(io::zlib_decompressor());
    } else {
      // Unsupported decompression
      AD_CHECK(false);
    }
    filterStream.push(io::back_inserter(result));

    filterStream.write(compressedData.data(),
                       static_cast<std::streamsize>(compressedData.size()));
    return result;
  }
};

TEST_P(CompressorStreamTestFixture2, TestGeneratorAppliesCompression) {
  CompressorStream stream{generateNChars(10), GetParam()};

  ASSERT_TRUE(stream.hasNext());
  auto compressedData = stream.next();

  ASSERT_EQ(decompressData(compressedData), "AAAAAAAAAA");

  ASSERT_FALSE(stream.hasNext());
}

using ad_utility::content_encoding::CompressionMethod;

INSTANTIATE_TEST_SUITE_P(CompressionMethodParameters,
                         CompressorStreamTestFixture2,
                         ::testing::Values(CompressionMethod::DEFLATE,
                                           CompressionMethod::GZIP));
