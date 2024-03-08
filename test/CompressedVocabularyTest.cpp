//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include <gtest/gtest.h>

#include "./VocabularyTestHelpers.h"
#include "index/VocabularyOnDisk.h"
#include "index/vocabulary/CompressedVocabulary.h"
#include "index/vocabulary/PrefixCompressor.h"
#include "index/vocabulary/VocabularyInMemory.h"

namespace {

using namespace vocabulary_test;
using namespace ad_utility::vocabulary;
// A stateless "compressor" that applies a trivial transformation to a string
struct DummyDecoder {
  static std::string decompress(std::string_view compressed) {
    std::string result{compressed};
    for (char& c : result) {
      c -= 2;
    }
    return result;
  }
  // This class has no state, but it still needs to be serialized.
  friend std::true_type allowTrivialSerialization(DummyDecoder, auto);
};

// A wrapper for the stateless dummy compression.
struct DummyCompressionWrapper
    : ad_utility::vocabulary::detail::DecoderMultiplexer<DummyDecoder> {
  using Base = ad_utility::vocabulary::detail::DecoderMultiplexer<DummyDecoder>;
  using Base::Base;

  static std::string compress(std::string_view uncompressed) {
    std::string result{uncompressed};
    for (auto& c : result) {
      c += 2;
    }
    return result;
  }

  static std::tuple<int, std::vector<std::string>, DummyDecoder> compressAll(
      const std::vector<std::string>& strings) {
    std::vector<std::string> result;
    for (const auto& string : strings) {
      result.push_back(compress(string));
    }
    return {0, std::move(result), DummyDecoder{}};
  }
};

// _______________________________________________________
TEST(CompressedVocabulary, CompressionIsActuallyApplied) {
  const std::vector<std::string> words{"alpha", "delta", "beta", "42",
                                       "31",    "0",     "al"};

  CompressedVocabulary<VocabularyInMemory, DummyCompressionWrapper> v;
  auto writer = v.makeDiskWriter("vocabtmp.txt");
  for (const auto& word : words) {
    writer(word);
  }
  writer.finish();

  VocabularyInMemory simple;
  simple.open("vocabtmp.txt.words");
  ad_utility::deleteFile("vocabtmp.txt.words");

  ASSERT_EQ(simple.size(), words.size());
  for (size_t i = 0; i < simple.size(); ++i) {
    ASSERT_NE(simple[i], words[i]);
    ASSERT_EQ(DummyDecoder::decompress(simple[i]), words[i]);
  }
}

// The generic tests from the vocabulary testing framework, templated on all the
// compressors that we have defined.

// Add additional compression wrappers to the following type list. They will
// then automatically be tested.
using Compressors =
    ::testing::Types<FsstSquaredCompressionWrapper, FsstCompressionWrapper,
                     PrefixCompressionWrapper, DummyCompressionWrapper>;

// _________________________________________________________________________
template <ad_utility::vocabulary::CompressionWrapper Compressor>
struct CompressedVocabularyF : public testing::Test {
  // Tests for the FSST-compressed vocabulary. These use the generic testing
  // framework that was set up for all the other vocabularies.
  static constexpr auto createCompressedVocabulary(
      const std::string& filename) {
    return [filename](const std::vector<std::string>& words) {
      // We deliberately set the blocksize to a very small number.
      CompressedVocabulary<VocabularyOnDisk, Compressor, 4> vocab;
      auto writer = vocab.makeDiskWriter(filename);
      for (const auto& word : words) {
        writer(word);
      }
      writer.finish();
      vocab.open(filename);
      return vocab;
    };
  }
};
TYPED_TEST_SUITE(CompressedVocabularyF, Compressors);

// _______________________________________________________
TYPED_TEST(CompressedVocabularyF, LowerUpperBoundStdLess) {
  testUpperAndLowerBoundWithStdLess(
      this->createCompressedVocabulary("lowerUpperBoundStdLessFsst"));
}

// _______________________________________________________
TYPED_TEST(CompressedVocabularyF, LowerUpperBoundNumeric) {
  testUpperAndLowerBoundWithNumericComparator(
      this->createCompressedVocabulary("lowerUpperBoundNumericFsst"));
}

// _______________________________________________________
TYPED_TEST(CompressedVocabularyF, AccessOperator) {
  testAccessOperatorForUnorderedVocabulary(
      this->createCompressedVocabulary("accessOperatorFsst"));
}

// _______________________________________________________
TYPED_TEST(CompressedVocabularyF, EmptyVocabulary) {
  testEmptyVocabulary(this->createCompressedVocabulary("accessOperatorFsst"));
}

}  // namespace
