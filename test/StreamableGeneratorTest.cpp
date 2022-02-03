// Copyright 2021, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Robin Textor-Falconi (textorr@informatik.uni-freiburg.de)

#include <gtest/gtest.h>

#include "../src/util/streamable_generator.h"

using namespace ad_utility::stream_generator;
namespace io = boost::iostreams;

constexpr size_t TEST_BUFFER_SIZE = 10;

stream_generator generateException() {
  throw std::runtime_error("Test Exception");
  co_return;
}

TEST(StreamableGeneratorTest, TestGeneratorExceptionResultsInException) {
  auto generator = generateException();
  ASSERT_TRUE(generator.hasNext());
  try {
    generator.next();
    FAIL() << "Generator should throw Exception";
  } catch (const std::exception& e) {
    ASSERT_STREQ(e.what(), "Test Exception");
  }
  ASSERT_FALSE(generator.hasNext());
}

stream_generator generateNothing() { co_return; }

TEST(StreamableGeneratorTest, TestEmptyGeneratorReturnsEmptyResult) {
  auto generator = generateNothing();
  ASSERT_TRUE(generator.hasNext());
  ASSERT_EQ(generator.next(), "");
  ASSERT_FALSE(generator.hasNext());
}

const std::string MAX_TEST_BUFFER_STRING(TEST_BUFFER_SIZE, 'A');

basic_stream_generator<TEST_BUFFER_SIZE> generateMultipleElements() {
  co_yield MAX_TEST_BUFFER_STRING;
  co_yield 1;
  co_yield "Abc";
}

TEST(StreamableGeneratorTest, TestGeneratorReturnsBufferedResults) {
  auto generator = generateMultipleElements();

  ASSERT_TRUE(generator.hasNext());
  ASSERT_EQ(generator.next(), MAX_TEST_BUFFER_STRING);

  ASSERT_TRUE(generator.hasNext());
  ASSERT_EQ(generator.next(), std::string("1Abc"));

  ASSERT_FALSE(generator.hasNext());
}

TEST(StreamableGeneratorTest, TestGeneratorDefaultInitialisesWithNoOp) {
  stream_generator generator;

  ASSERT_TRUE(generator.hasNext());
  std::string_view view = generator.next();
  ASSERT_TRUE(view.empty());
  ASSERT_FALSE(generator.hasNext());
}

TEST(StreamableGeneratorTest, TestGeneratorNextThrowsExceptionWhenDone) {
  auto generator = generateNothing();

  generator.next();
  try {
    generator.next();
    FAIL() << "next() should throw an exception";
  } catch (const std::exception& e) {
    ASSERT_STREQ(e.what(), "Coroutine is not active");
  }
}

class StreamableGeneratorTestFixture
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

TEST_P(StreamableGeneratorTestFixture, TestGeneratorAppliesCompression) {
  auto generator = generateMultipleElements();
  generator.setCompressionMethod(GetParam());

  ASSERT_TRUE(generator.hasNext());
  auto compressedData = generator.next();

  // compression introduces another buffer, which is why co_yield
  // is not suspended after TEST_BUFFER_SIZE bytes
  ASSERT_EQ(decompressData(compressedData), MAX_TEST_BUFFER_STRING + "1Abc");

  ASSERT_FALSE(generator.hasNext());
}

using ad_utility::content_encoding::CompressionMethod;

INSTANTIATE_TEST_SUITE_P(CompressionMethodParameters,
                         StreamableGeneratorTestFixture,
                         ::testing::Values(CompressionMethod::DEFLATE,
                                           CompressionMethod::GZIP));
