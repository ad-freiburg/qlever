//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include <absl/cleanup/cleanup.h>
#include <absl/strings/str_cat.h>
#include <gtest/gtest.h>

#include "VocabularyTestHelpers.h"
#include "backports/algorithm.h"
#include "index/vocabulary/CompressedVocabulary.h"
#include "index/vocabulary/PrefixCompressor.h"
#include "index/vocabulary/VocabularyInMemory.h"
#include "index/vocabulary/VocabularyInMemoryBinSearch.h"
#include "index/vocabulary/VocabularyOnDisk.h"
#include "util/Serializer/ByteBufferSerializer.h"

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

// _______________________________________________________
TYPED_TEST(CompressedVocabularyF, LowerUpperBoundStdLess) {
  testUpperAndLowerBoundWithStdLess(this->createCompressedVocabulary());
}

// _______________________________________________________
TYPED_TEST(CompressedVocabularyF, LowerUpperBoundNumeric) {
  testUpperAndLowerBoundWithNumericComparator(
      this->createCompressedVocabulary());
}

// _______________________________________________________
TYPED_TEST(CompressedVocabularyF, AccessOperator) {
  testAccessOperatorForUnorderedVocabulary(this->createCompressedVocabulary());
}

// _______________________________________________________
TYPED_TEST(CompressedVocabularyF, EmptyVocabulary) {
  testEmptyVocabulary(this->createCompressedVocabulary());
}

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
TYPED_TEST(CompressedVocabularyF, ScanAllEmptyVocabulary) {
  auto createVocab = TestFixture::createCompressedVocabulary();
  auto vocab = createVocab({});
  auto range = vocab.scanAll();
  EXPECT_EQ(ql::ranges::begin(range), ql::ranges::end(range));
}

namespace {

// A compressed vocabulary with "holes" (see `VocabularyInMemoryBinSearch`). The
// number of words per decoder block is deliberately small, so that the tests
// below span several blocks.
using CompressedVocabularyWithHoles =
    CompressedVocabulary<VocabularyInMemoryBinSearch,
                         FsstSquaredCompressionWrapper, 4>;

// For an underlying vocabulary with holes, the `WordWriter` has to take an
// explicit index for each word.
static_assert(std::is_same_v<
              CompressedVocabularyWithHoles::WordWriter,
              CompressedVocabularyWithHoles::DiskWriterWithExplicitIndices>);

// The words of the vocabulary with holes that the tests below use, sorted (as
// the underlying vocabulary requires sorted input at write time). NOTE: The
// numbers have a fixed width (so that the words are sorted also for more than
// ten words), and each word ends in a letter (so that the tests for
// `lower_bound` and `upper_bound` can make a word slightly larger or smaller
// without hitting one of the neighbouring words).
std::vector<std::string> wordsWithHoles() {
  std::vector<std::string> words;
  for (size_t i = 0; i < 11; ++i) {
    words.push_back(absl::StrCat("word", i / 10, i % 10, "m"));
  }
  return words;
}

// The (non-contiguous) vocabulary indices for `wordsWithHoles`, such that every
// third index is contained.
std::vector<uint64_t> indicesWithHoles() {
  std::vector<uint64_t> indices;
  for (size_t i = 0; i < wordsWithHoles().size(); ++i) {
    indices.push_back(3 * i + 1);
  }
  return indices;
}

// Delete the three files that the `DiskWriterWithExplicitIndices` for the given
// `filename` creates. Do not warn about files that were never created.
void deleteVocabularyFiles(const std::string& filename) {
  for (const auto& suffix : {".words", ".words.ids", ".codebooks"}) {
    ad_utility::deleteFile(absl::StrCat(filename, suffix), false);
  }
}

// Create a `CompressedVocabularyWithHoles` with the given `words` and
// `indices`, using the `DiskWriterWithExplicitIndices`. The suffixes of the two
// filenames are the ones that `CompressedVocabulary::open` expects.
CompressedVocabularyWithHoles createVocabularyWithHoles(
    const std::string& filename, const std::vector<std::string>& words,
    const std::vector<uint64_t>& indices) {
  AD_CORRECTNESS_CHECK(words.size() == indices.size());
  {
    CompressedVocabularyWithHoles::WordWriter writer{
        absl::StrCat(filename, ".words"), absl::StrCat(filename, ".codebooks")};
    for (size_t i = 0; i < words.size(); ++i) {
      EXPECT_EQ(writer(words.at(i), indices.at(i)), indices.at(i));
    }
    writer.finish();
    // Calling `finish` twice has no additional effect.
    writer.finish();
  }
  CompressedVocabularyWithHoles vocab;
  vocab.open(filename);
  return vocab;
}

// The `{index, word}` pairs that a vocabulary built from `wordsWithHoles` and
// `indicesWithHoles` is expected to contain.
std::vector<std::pair<uint64_t, std::string>> expectedIndicesAndWords() {
  std::vector<std::pair<uint64_t, std::string>> result;
  auto words = wordsWithHoles();
  auto indices = indicesWithHoles();
  for (size_t i = 0; i < words.size(); ++i) {
    result.emplace_back(indices.at(i), words.at(i));
  }
  return result;
}

}  // namespace

// _____________________________________________________________________________
TEST(CompressedVocabularyWithHoles, accessOperator) {
  std::string filename = gtestCurrentTestName();
  absl::Cleanup cleanup = [&filename] { deleteVocabularyFiles(filename); };
  auto words = wordsWithHoles();
  auto indices = indicesWithHoles();
  auto vocab = createVocabularyWithHoles(filename, words, indices);

  ASSERT_EQ(vocab.size(), words.size());
  // The words that are contained are decompressed with the decoder of the block
  // that they were compressed in (which is determined by their position, not by
  // their vocabulary index).
  for (size_t i = 0; i < words.size(); ++i) {
    EXPECT_EQ(vocab[indices.at(i)], words.at(i)) << "at position " << i;
  }

  // The indices that are not contained (the "holes") yield a placeholder.
  for (uint64_t index : {uint64_t{0}, uint64_t{2}, uint64_t{3}, uint64_t{35}}) {
    EXPECT_EQ(vocab[index],
              ad_utility::vocabulary::placeholderForMissingVocabIndex(index));
  }
}

// _____________________________________________________________________________
TEST(CompressedVocabularyWithHoles, lowerAndUpperBound) {
  std::string filename = gtestCurrentTestName();
  absl::Cleanup cleanup = [&filename] { deleteVocabularyFiles(filename); };
  auto words = wordsWithHoles();
  auto indices = indicesWithHoles();
  // `lower_bound` and `upper_bound` have to report the vocabulary indices (and
  // not the positions) of the words, and have to decompress the words with the
  // correct decoder across all block boundaries.
  testUpperAndLowerBoundWithStdLessFromWordsAndIds(
      createVocabularyWithHoles(filename, words, indices), words, indices);
}

// _____________________________________________________________________________
TEST(CompressedVocabularyWithHoles, endIndexAndGetPositionOfWord) {
  std::string filename = gtestCurrentTestName();
  absl::Cleanup cleanup = [&filename] { deleteVocabularyFiles(filename); };
  auto words = wordsWithHoles();
  auto indices = indicesWithHoles();
  auto vocab = createVocabularyWithHoles(filename, words, indices);

  // The "one past the end" index is one larger than the largest contained
  // index, and NOT `size()`.
  ASSERT_EQ(vocab.endIndex(), indices.back() + 1);
  ASSERT_NE(vocab.endIndex(), vocab.size());

  using Pair = std::pair<uint64_t, uint64_t>;
  auto getPositionOfWord = [&vocab](std::string_view word) {
    return vocab.getPositionOfWord(word, ql::ranges::less{});
  };

  // A word that is contained yields the half-open range consisting of exactly
  // its (non-contiguous) vocabulary index. This also has to work across the
  // boundaries of the decoder blocks.
  for (size_t position = 0; position < words.size(); ++position) {
    uint64_t index = indices.at(position);
    EXPECT_EQ(getPositionOfWord(words.at(position)), (Pair{index, index + 1}))
        << "at position " << position;
  }

  // A word that is not contained yields the empty range at the index of the
  // first word that is greater than it.
  EXPECT_EQ(getPositionOfWord("aaa"), (Pair{indices.at(0), indices.at(0)}));
  EXPECT_EQ(getPositionOfWord(absl::StrCat(words.at(0), "x")),
            (Pair{indices.at(1), indices.at(1)}));

  // A word that is greater than all contained words yields the empty range at
  // `endIndex()`. Using `size()` here would be a bug, because it is a valid
  // index of a word that sorts BEFORE the word that is looked up.
  EXPECT_EQ(getPositionOfWord("zzz"),
            (Pair{vocab.endIndex(), vocab.endIndex()}));
  EXPECT_GT(getPositionOfWord("zzz").first, indices.back());

  // In an empty vocabulary, every word yields the empty range at index 0.
  auto emptyVocab = createVocabularyWithHoles(filename, {}, {});
  EXPECT_EQ(emptyVocab.endIndex(), 0);
  EXPECT_EQ(emptyVocab.getPositionOfWord("alpha", ql::ranges::less{}),
            (Pair{0, 0}));
}

// _____________________________________________________________________________
TEST(CompressedVocabularyWithHoles, scanAll) {
  std::string filename = gtestCurrentTestName();
  absl::Cleanup cleanup = [&filename] { deleteVocabularyFiles(filename); };
  auto vocab =
      createVocabularyWithHoles(filename, wordsWithHoles(), indicesWithHoles());

  EXPECT_EQ(scanAllToIndexAndWordVector(vocab.scanAll()),
            expectedIndicesAndWords());
}

// _____________________________________________________________________________
TEST(CompressedVocabularyWithHoles, serialization) {
  std::string filename = gtestCurrentTestName();
  absl::Cleanup cleanup = [&filename] { deleteVocabularyFiles(filename); };
  auto vocab =
      createVocabularyWithHoles(filename, wordsWithHoles(), indicesWithHoles());

  // The generic serialization.
  ad_utility::serialization::ByteBufferWriteSerializer writeSerializer;
  writeSerializer << vocab;
  CompressedVocabularyWithHoles readVocab;
  ad_utility::serialization::ByteBufferReadSerializer readSerializer{
      std::move(writeSerializer).data()};
  readSerializer >> readVocab;
  EXPECT_EQ(scanAllToIndexAndWordVector(readVocab.scanAll()),
            expectedIndicesAndWords());

  // The zero-copy deserialization, which reads back the same layout.
  ad_utility::serialization::AlignedByteBufferWriteSerializer
      alignedWriteSerializer;
  alignedWriteSerializer << vocab;
  ad_utility::serialization::AlignedByteBufferReadSerializer
      alignedReadSerializer{std::move(alignedWriteSerializer).data()};
  auto view = CompressedVocabularyWithHoles::fromZeroCopyDeserializer(
      alignedReadSerializer);
  EXPECT_EQ(scanAllToIndexAndWordVector(view.scanAll()),
            expectedIndicesAndWords());
  EXPECT_EQ(view[1], "word00m");
  EXPECT_EQ(view[2],
            ad_utility::vocabulary::placeholderForMissingVocabIndex(2));
}

// _____________________________________________________________________________
TEST(CompressedVocabularyWithHoles, makeDiskWriterPtrThrows) {
  // A vocabulary with holes cannot be built via the `WordWriterBase` interface,
  // which cannot express the explicit indices.
  AD_EXPECT_THROW_WITH_MESSAGE(
      CompressedVocabularyWithHoles::makeDiskWriterPtr(gtestCurrentTestName()),
      ::testing::HasSubstr("cannot be built word by word"));
}
