// Copyright 2022, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Robin Textor-Falconi (textorr@informatik.uni-freiburg.de)

#include <gmock/gmock.h>

#include "../src/util/CompressorStream.h"
#include "../src/util/Exception.h"

namespace io = boost::iostreams;

namespace http = boost::beast::http;
using ad_utility::content_encoding::CompressionMethod;
using ad_utility::streams::compressStream;

namespace {
cppcoro::generator<std::string> generateNChars(size_t n) {
  for (size_t i = 0; i < n; i++) {
    co_yield "A";
  }
}
}  // namespace

class CompressorStreamTestFixture
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
      AD_FAIL();
    }
    filterStream.push(io::back_inserter(result));

    filterStream.write(compressedData.data(),
                       static_cast<std::streamsize>(compressedData.size()));
    return result;
  }
};

TEST_P(CompressorStreamTestFixture, TestGeneratorAppliesCompression) {
  auto generator = compressStream(generateNChars(10), GetParam());

  auto iterator = generator.begin();

  ASSERT_NE(iterator, generator.end());
  auto compressedData = *iterator;

  ASSERT_EQ(decompressData(compressedData), "AAAAAAAAAA");

  ++iterator;

  ASSERT_EQ(iterator, generator.end());
}

using ad_utility::content_encoding::CompressionMethod;

INSTANTIATE_TEST_SUITE_P(CompressionMethodParameters,
                         CompressorStreamTestFixture,
                         ::testing::Values(CompressionMethod::DEFLATE,
                                           CompressionMethod::GZIP));
