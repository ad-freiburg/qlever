// Copyright 2022 - 2026, The QLever Authors, in particular:
//
// 2022 - 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
// 2026        Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR

#include <absl/strings/str_cat.h>
#include <gtest/gtest.h>

#include "VocabularyTestHelpers.h"
#include "backports/algorithm.h"
#include "backports/span.h"
#include "index/vocabulary/CompressedVocabulary.h"
#include "index/vocabulary/PrefixCompressor.h"
#include "index/vocabulary/VocabularyInMemory.h"
#include "index/vocabulary/VocabularyOnDisk.h"
#include "util/Exception.h"
#include "util/Serializer/ByteBufferSerializer.h"

namespace {

using namespace vocabulary_test;
using namespace ad_utility::vocabulary;
// A stateless "compressor" that applies a trivial transformation to a string
struct DummyDecoder {
  static size_t maxDecompressedSize(std::string_view compressed) {
    return compressed.size();
  }

  static size_t decompressInto(std::string_view compressed,
                               ql::span<char> out) {
    AD_CONTRACT_CHECK(out.size() >= compressed.size());
    for (auto&& [dest, src] :
         ::ranges::views::zip(out.subspan(0, compressed.size()), compressed)) {
      dest = static_cast<char>(src - 2);
    }
    return compressed.size();
  }

  static std::string decompress(std::string_view compressed) {
    std::string result{compressed};
    for (char& c : result) {
      c -= 2;
    }
    return result;
  }
  // This class has no state, but it still needs to be serialized.
  template <typename T>
  friend std::true_type allowTrivialSerialization(DummyDecoder, T);
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

// The compressed vocabulary must actually compress: identical uncompressed
// and on-disk sizes would mean the compression layer is a no-op.
// _______________________________________________________
TEST(CompressedVocabulary, CompressionIsActuallyApplied) {
  const std::vector<std::string> words{"alpha", "delta", "beta", "42",
                                       "31",    "0",     "al"};

  CompressedVocabulary<VocabularyInMemory, DummyCompressionWrapper> v;
  {
    auto writerPtr = v.makeDiskWriterPtr("vocabtmp.txt");
    auto& writer = *writerPtr;
    for (const auto& [i, word] : ::ranges::views::enumerate(words)) {
      ASSERT_EQ(writer(word, false), static_cast<uint64_t>(i));
    }
    writer.readableName() = "blabb";
    EXPECT_EQ(writer.readableName(), "blabb");
    // Test the case that the destructor implicitly calls `finish`.
    // The other unit tests have
  }

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
template <typename Compressor>
struct CompressedVocabularyF : public testing::Test {
  static_assert(ad_utility::vocabulary::CompressionWrapper<Compressor>);
  // Tests for the FSST-compressed vocabulary. These use the generic testing
  // framework that was set up for all the other vocabularies.
  static auto createCompressedVocabulary() {
    std::string filename = gtestCurrentTestName();
    return [filename =
                std::move(filename)](const std::vector<std::string>& words) {
      // We deliberately set the blocksize to a very small number.
      CompressedVocabulary<VocabularyOnDisk, Compressor, 4> vocab;
      auto writerPtr = vocab.makeDiskWriterPtr(filename);
      auto& writer = *writerPtr;
      for (const auto& word : words) {
        writer(word, false);
      }
      writer.finish();
      vocab.open(filename);
      return vocab;
    };
  }
};
TYPED_TEST_SUITE(CompressedVocabularyF, Compressors);

// The generic vocabulary framework tests, run for every compressor: lower and
// upper bound searches must behave identically to an uncompressed vocabulary,
// with both the standard string comparator and the numeric-index comparator.
// _______________________________________________________
TYPED_TEST(CompressedVocabularyF, LowerUpperBoundStdLess) {
  testUpperAndLowerBoundWithStdLess(this->createCompressedVocabulary());
}

// See above: same bounds contract, but driven through the numeric comparator
// that `ValueId`-based lookups use.
// _______________________________________________________
TYPED_TEST(CompressedVocabularyF, LowerUpperBoundNumeric) {
  testUpperAndLowerBoundWithNumericComparator(
      this->createCompressedVocabulary());
}

// Single-word access via `operator[]` must return exactly the stored word for
// every index (and nullopt behavior stays with the underlying vocabulary).
// _______________________________________________________
TYPED_TEST(CompressedVocabularyF, AccessOperator) {
  testAccessOperatorForUnorderedVocabulary(this->createCompressedVocabulary());
}

// `lookupBatch` must agree with the per-word `operator[]` for arbitrary index
// combinations: same words, same order as requested (duplicates included), and
// each returned view must alias the vocabulary's own storage. An empty index
// list is a contract violation and must throw.
// _____________________________________________________________________________
TYPED_TEST(CompressedVocabularyF, LookupBatchMatchesAccessOperator) {
  const std::vector<std::string> words{"alpha", "beta", "gamma", "delta",
                                       "epsilon"};
  auto vocab = this->createCompressedVocabulary()(words);
  const std::array<size_t, 7> indices{4, 1, 0, 3, 1, 2, 4};
  auto result = vocab.lookupBatch(indices);
  assertLookupResultMatchesVocabularyAtIndices(vocab, result, indices);
  AD_EXPECT_THROW_WITH_MESSAGE(vocab.lookupBatch(ql::span<const size_t>{}),
                               ::testing::HasSubstr("!indices.empty()"));
}

//______________________________________________________________________________
// A vocabulary containing the empty string word ("") must decompress correctly
// through `lookupBatch` without allocations or crashes across all compressors
// (exercising the `boundOnDecompressedWordSize == 0` fast path).
TYPED_TEST(CompressedVocabularyF, LookupBatchEmptyWordInVocabulary) {
  const std::vector<std::string> words{"alpha", "", "beta", "", "gamma"};
  auto vocab = this->createCompressedVocabulary()(words);
  const std::array<size_t, 6> indices{1, 0, 3, 2, 4, 1};
  auto result = vocab.lookupBatch(indices);
  assertLookupResultMatchesVocabularyAtIndices(vocab, result, indices);
  EXPECT_TRUE((*result)[0].empty());
  EXPECT_EQ((*result)[1], "alpha");
  EXPECT_TRUE((*result)[2].empty());
  EXPECT_EQ((*result)[3], "beta");
  EXPECT_EQ((*result)[4], "gamma");
  EXPECT_TRUE((*result)[5].empty());
}

// The generic framework's empty-vocabulary contract: lookups, iteration, and
// size must all behave on a vocabulary with zero words.
// _______________________________________________________
TYPED_TEST(CompressedVocabularyF, EmptyVocabulary) {
  testEmptyVocabulary(this->createCompressedVocabulary());
}

// Serialization round trip: write the compressed vocabulary to disk with the
// serializer, read it back, and verify every word survives identically.
// _______________________________________________________
TYPED_TEST(CompressedVocabularyF, WriteAndReadWithSerializer) {
  const std::vector<std::string> words{"alpha", "delta", "beta", "42",
                                       "31",    "0",     "al"};

  // Create vocabulary with small block size (4 words per block).
  // Use VocabularyInMemory as the underlying vocabulary.
  CompressedVocabulary<VocabularyInMemory, TypeParam, 4> vocab;
  std::string filename = gtestCurrentTestName();
  auto writerPtr = vocab.makeDiskWriterPtr(filename);
  auto& writer = *writerPtr;
  for (const auto& word : words) {
    writer(word, false);
  }
  writer.finish();
  vocab.open(filename);

  // Write using serializer.
  ad_utility::serialization::ByteBufferWriteSerializer writeSerializer;
  writeSerializer | vocab;
  const auto& blob = writeSerializer.data();
  ASSERT_FALSE(blob.empty());

  // Read using serializer into a different vocabulary.
  CompressedVocabulary<VocabularyInMemory, TypeParam, 4> readVocab;
  ad_utility::serialization::ByteBufferReadSerializer readSerializer{blob};
  readSerializer | readVocab;
  assertThatRangesAreEqual(vocab, readVocab);

  // Cleanup files.
  ad_utility::deleteFile(filename);
}

// Zero-copy deserialization: opening a vocabulary serialized by this same
// process must map/reference the existing buffers instead of decompressing
// everything anew, and lookups must still return the correct words.
// _______________________________________________________
TYPED_TEST(CompressedVocabularyF, ZeroCopyDeserialization) {
  const std::vector<std::string> words{"alpha", "delta", "beta", "42",
                                       "31",    "0",     "al"};

  // Create vocabulary with small block size (4 words per block) on top of an
  // in-memory (and hence zero-copy-capable) underlying vocabulary.
  CompressedVocabulary<VocabularyInMemory, TypeParam, 4> vocab;
  std::string filename = gtestCurrentTestName();
  auto writerPtr = vocab.makeDiskWriterPtr(filename);
  auto& writer = *writerPtr;
  for (const auto& word : words) {
    writer(word, false);
  }
  writer.finish();
  vocab.open(filename);

  // Write using an aligned serializer (required for zero-copy reads).
  ad_utility::serialization::AlignedByteBufferWriteSerializer writeSerializer;
  writeSerializer | vocab;

  // Read back the words as a non-owning, zero-copy view, and the (small)
  // decoders normally.
  ad_utility::serialization::AlignedByteBufferReadSerializer readSerializer{
      std::move(writeSerializer).data()};
  auto view =
      (CompressedVocabulary<VocabularyInMemory, TypeParam,
                            4>::fromZeroCopyDeserializer(readSerializer));
  assertThatRangesAreEqual(vocab, view);

  ad_utility::deleteFile(filename);
}

}  // namespace

// `scanAll` must yield every word in index order, spanning multiple decoder
// blocks (the fixture's small block size forces many blocks for 111 words).
// _____________________________________________________________________________
TYPED_TEST(CompressedVocabularyF, ScanAll) {
  auto createVocab = TestFixture::createCompressedVocabulary();
  std::vector<std::string> words;
  for (size_t i = 0; i < 111; ++i) {
    words.push_back(absl::StrCat("someWord", i, std::string(i % 13, 'y')));
  }
  // NOTE: The fixture uses a decoder block size of 4, so this vocabulary has
  // many decoder blocks that the scan has to span.
  auto vocab = createVocab(words);

  using ::testing::ElementsAreArray;
  EXPECT_THAT(scanAllToVector(vocab.scanAll()), ElementsAreArray(words));
  // Abandon a scan early; the destructor has to clean up properly.
  {
    auto range = vocab.scanAll();
    auto it = ql::ranges::begin(range);
    ASSERT_NE(it, ql::ranges::end(range));
    IndexAndWord indexAndWord = *it;
    EXPECT_EQ(indexAndWord.index_, 0);
    EXPECT_EQ(indexAndWord.word_, words.at(0));
  }
}

// _____________________________________________________________________________
// Regression test for a dangling-view bug this lookup path once had: an
// intermediate local `std::pmr::string` uses the small-string optimization
// regardless of its allocator, so for short words a saved `string_view`
// pointed into the destroyed local object instead of the arena.
//
// Detection strength:
//  - Under AddressSanitizer builds (CMAKE_BUILD_TYPE=Asan) this test is a
//    DETERMINISTIC detector: ASan poisons returned stack frames, so any read
//    through the dangling view is reported as stack-use-after-return.
//  - In normal builds it is a practical tripwire, not a proof: reading a
//    dangling view is UB, so we clobber the stack with sentinel bytes and
//    verify content byte-for-byte, which makes corruption overwhelmingly
//    likely but not formally guaranteed.
TYPED_TEST(CompressedVocabularyF, LookupBatchShortWordViewsStayValid) {
  // All words deliberately short (<= 15 chars): every one takes the SSO
  // path in a `pmr::string`-based implementation, and none would end up in
  // the monotonic buffer that owns the result's storage.
  std::vector<std::string> words;
  words.reserve(64);
  for (int i = 0; i < 64; ++i) {
    words.push_back(absl::StrCat("s", i));
  }
  auto vocab = this->createCompressedVocabulary()(words);

  std::vector<size_t> indices(words.size());
  for (size_t i = 0; i < words.size(); ++i) {
    indices[i] = i;
  }
  auto result = vocab.lookupBatch(indices);
  ASSERT_EQ(result->size(), indices.size());

  // Clobber the stack region a dangling SSO view would point into. Two deep
  // frames of sentinel bytes leave no plausible intact copy behind.
  auto churn = []() {
    volatile char sentinel[2048];
    ql::ranges::fill(sentinel, '#');
    static_cast<void>(sentinel);
  };
  churn();
  churn();

  for (size_t i = 0; i < indices.size(); ++i) {
    ASSERT_EQ((*result)[i], words[i]);
  }
}

// `scanAll` on an empty vocabulary must yield an empty (but valid) range,
// not an error or a dangling iterator.
// _____________________________________________________________________________
TYPED_TEST(CompressedVocabularyF, ScanAllEmptyVocabulary) {
  auto createVocab = TestFixture::createCompressedVocabulary();
  auto vocab = createVocab({});
  auto range = vocab.scanAll();
  EXPECT_EQ(ql::ranges::begin(range), ql::ranges::end(range));
}
