// Copyright 2022 - 2026, The QLever Authors, in particular:
//
// 2022 - 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
// 2026        Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <absl/cleanup/cleanup.h>
#include <absl/strings/str_cat.h>
#include <gtest/gtest.h>

#include <array>

#include "../../util/DanglingViewTestHelpers.h"
#include "../../util/GTestHelpers.h"
#include "../../util/PmrStringSsoTestHelpers.h"
#include "VocabularyTestHelpers.h"
#include "backports/algorithm.h"
#include "backports/span.h"
#include "index/vocabulary/CompressedVocabulary.h"
#include "index/vocabulary/PrefixCompressor.h"
#include "index/vocabulary/VocabularyInMemory.h"
#include "index/vocabulary/VocabularyInMemoryBinSearch.h"
#include "index/vocabulary/VocabularyOnDisk.h"
#include "util/AllocatorWithLimit.h"
#include "util/Exception.h"
#include "util/MemorySize/MemorySize.h"
#include "util/Serializer/ByteBufferSerializer.h"

namespace {

using namespace vocabulary_test;
using namespace ad_utility::vocabulary;

// A stateless test "compressor" that applies a trivial transformation to a
// string. Satisfies the in-place decompression interface: `maxDecompressedSize`
// provides the output bound, while `decompressInto` writes into caller-provided
// storage and returns the number of bytes written.
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
    std::string result(compressed.size(), '\0');
    decompressInto(compressed, ql::span<char>{result.data(), result.size()});
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

// _____________________________________________________________________________
TEST(CompressedVocabulary, CompressionIsActuallyApplied) {
  const std::vector<std::string> words{"alpha", "delta", "beta", "42",
                                       "31",    "0",     "al"};

  CompressedVocabulary<VocabularyInMemory, DummyCompressionWrapper> v;
  std::string filename = gtestCurrentTestName();
  auto cleanup = makeVocabFileCleanup<decltype(v)>(filename);
  {
    auto writerPtr = v.makeDiskWriterPtr(filename);
    auto& writer = *writerPtr;
    for (const auto& [i, word] : ::ranges::views::enumerate(words)) {
      ASSERT_EQ(writer(word, false), static_cast<uint64_t>(i));
    }
    writer.readableName() = "blabb";
    EXPECT_EQ(writer.readableName(), "blabb");
    // Test the case that the destructor implicitly calls `finish`.
  }

  VocabularyInMemory simple;
  simple.open(absl::StrCat(filename, decltype(v)::wordsSuffix));

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

// _____________________________________________________________________________
template <typename Compressor>
struct CompressedVocabularyF : public testing::Test {
  static_assert(ad_utility::vocabulary::CompressionWrapper<Compressor>);
  // The vocabulary that `createCompressedVocabulary` below creates. It is
  // written to the file with the name of the current test.
  using Vocabulary = CompressedVocabulary<VocabularyOnDisk, Compressor, 4>;

  // Delete the files of that vocabulary. The vocabulary itself is a local
  // variable of the test body and hence already destroyed at this point.
  void TearDown() override {
    deleteVocabularyFiles<Vocabulary>(gtestCurrentTestName());
  }

  // Tests for the FSST-compressed vocabulary. These use the generic testing
  // framework that was set up for all the other vocabularies.
  static auto createCompressedVocabulary() {
    std::string filename = gtestCurrentTestName();
    return [filename =
                std::move(filename)](const std::vector<std::string>& words) {
      // We deliberately set the blocksize to a very small number.
      Vocabulary vocab;
      auto writerPtr = vocab.makeDiskWriterPtr(filename);
      writeWordsAndFinish(*writerPtr, words);
      vocab.open(filename);
      return vocab;
    };
  }
};

TYPED_TEST_SUITE(CompressedVocabularyF, Compressors);

// _____________________________________________________________________________
TYPED_TEST(CompressedVocabularyF, LowerUpperBoundStdLess) {
  testUpperAndLowerBoundWithStdLess(this->createCompressedVocabulary());
}

// _____________________________________________________________________________
TYPED_TEST(CompressedVocabularyF, LowerUpperBoundNumeric) {
  testUpperAndLowerBoundWithNumericComparator(
      this->createCompressedVocabulary());
}

// _____________________________________________________________________________
TYPED_TEST(CompressedVocabularyF, AccessOperator) {
  testAccessOperatorForUnorderedVocabulary(this->createCompressedVocabulary());
}

// _____________________________________________________________________________
// `lookupBatch` must agree with the per-word `operator[]` for arbitrary index
// combinations: same words, same order as requested (duplicates included), with
// the returned batch owning the lifetime of all decompressed string views. An
// empty index list is a contract violation and must throw.
TYPED_TEST(CompressedVocabularyF, LookupBatchMatchesAccessOperator) {
  const std::vector<std::string> words{"alpha", "beta", "gamma", "delta",
                                       "epsilon"};
  auto vocab = this->createCompressedVocabulary()(words);
  const std::array<size_t, 7> indices{4, 1, 0, 3, 1, 2, 4};
  const auto result = vocab.lookupBatch(indices);
  assertLookupResultMatchesVocabularyAtIndices(vocab, result, indices);
  AD_EXPECT_THROW_WITH_MESSAGE(vocab.lookupBatch(ql::span<const size_t>{}),
                               ::testing::HasSubstr("!indices.empty()"));
}

// _____________________________________________________________________________
// The builder overload charges the decoded words against the memory limit of
// the builder's allocator: a limit that is too small throws, a sufficient one
// yields the same words as the convenience overload.
TYPED_TEST(CompressedVocabularyF, LookupBatchRespectsMemoryLimit) {
  using namespace ad_utility::memory_literals;
  const std::vector<std::string> words{"alpha", "beta", "gamma", "delta",
                                       "epsilon"};
  auto vocab = this->createCompressedVocabulary()(words);
  const std::array<size_t, 5> indices{4, 1, 0, 3, 2};

  ArenaVocabBatchBuilder tooSmall(indices.size(),
                                  ad_utility::makeAllocatorWithLimit<Id>(8_B));
  EXPECT_THROW(vocab.lookupBatch(indices, tooSmall),
               ad_utility::detail::AllocationExceedsLimitException);

  ArenaVocabBatchBuilder sufficient(
      indices.size(), ad_utility::makeAllocatorWithLimit<Id>(1_MB));
  vocab.lookupBatch(indices, sufficient);
  const auto result = std::move(sufficient).finalize();
  assertLookupResultMatchesVocabularyAtIndices(vocab, result, indices);
}

// _____________________________________________________________________________
// A vocabulary containing the empty string word ("") must decompress correctly
// through `lookupBatch` without allocations or crashes across all compressors
// (exercising the `boundOnDecompressedWordSize == 0` fast path).
TYPED_TEST(CompressedVocabularyF, LookupBatchEmptyWordInVocabulary) {
  const std::vector<std::string> words{"alpha", "", "beta", "", "gamma"};
  auto vocab = this->createCompressedVocabulary()(words);
  const std::array<size_t, 6> indices{1, 0, 3, 2, 4, 1};
  const auto result = vocab.lookupBatch(indices);
  assertLookupResultMatchesVocabularyAtIndices(vocab, result, indices);
  EXPECT_TRUE(result[0].empty());
  EXPECT_EQ(result[1], "alpha");
  EXPECT_TRUE(result[2].empty());
  EXPECT_EQ(result[3], "beta");
  EXPECT_EQ(result[4], "gamma");
  EXPECT_TRUE(result[5].empty());
}

// _____________________________________________________________________________
// The generic framework's empty-vocabulary contract: lookups, iteration, and
// size must all behave on a vocabulary with zero words.
TYPED_TEST(CompressedVocabularyF, EmptyVocabulary) {
  testEmptyVocabulary(this->createCompressedVocabulary());
}

// _____________________________________________________________________________
// Serialization round trip: write the compressed vocabulary to disk with the
// serializer, read it back, and verify every word survives identically.
TYPED_TEST(CompressedVocabularyF, WriteAndReadWithSerializer) {
  const std::vector<std::string> words{"alpha", "delta", "beta", "42",
                                       "31",    "0",     "al"};

  // Create vocabulary with small block size (4 words per block).
  // Use VocabularyInMemory as the underlying vocabulary.
  CompressedVocabulary<VocabularyInMemory, TypeParam, 4> vocab;
  std::string filename = gtestCurrentTestName();
  auto cleanup = makeVocabFileCleanup<decltype(vocab)>(filename);
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
}

// _____________________________________________________________________________
// Zero-copy deserialization: opening a vocabulary serialized by this same
// process must map/reference the existing buffers instead of decompressing
// everything anew, and lookups must still return the correct words.
TYPED_TEST(CompressedVocabularyF, ZeroCopyDeserialization) {
  const std::vector<std::string> words{"alpha", "delta", "beta", "42",
                                       "31",    "0",     "al"};

  // Create vocabulary with small block size (4 words per block) on top of an
  // in-memory (and hence zero-copy-capable) underlying vocabulary.
  CompressedVocabulary<VocabularyInMemory, TypeParam, 4> vocab;
  std::string filename = gtestCurrentTestName();
  auto cleanup = makeVocabFileCleanup<decltype(vocab)>(filename);
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
}

}  // namespace

// _____________________________________________________________________________
// `scanAll` must yield every word in index order, spanning multiple decoder
// blocks (the fixture's small block size forces many blocks for 111 words).
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
  // Verify that this platform uses inline storage for `std::pmr::string`;
  // short words therefore use the Small String Optimization (SSO) and would
  // otherwise end up inside a destroyed stack object rather than the arena.
  requirePmrStringInlineStorage(15);

  // All words deliberately short (<= 15 chars): every one takes the SSO
  // path in a `pmr::string`-based implementation, and none would end up in
  // the monotonic buffer that owns the result's storage.
  std::vector<std::string> words;
  words.reserve(64);
  for (int i = 0; i < 64; ++i) {
    words.push_back(absl::StrCat("s", i));
  }
  // Enforce the test premise: all words must fit within standard library SSO
  // capacity (<= 15 bytes on 64-bit platforms).
  ASSERT_TRUE(ql::ranges::all_of(
      words, [](const auto& word) { return word.size() <= 15; }));
  auto vocab = this->createCompressedVocabulary()(words);

  const auto indices =
      ::ranges::to<std::vector>(ql::views::iota(size_t{0}, words.size()));
  const auto result = vocab.lookupBatch(indices);
  ASSERT_EQ(result.size(), indices.size());

  // Clobber the stack region a dangling SSO view would point into. Two deep
  // frames of sentinel bytes leave no plausible intact copy behind.
  auto churn = []() { clobberStack(); };
  churn();
  churn();

  for (size_t i = 0; i < indices.size(); ++i) {
    ASSERT_EQ(result[i], words[i]);
  }
}

// _____________________________________________________________________________
// `scanAll` on an empty vocabulary must yield an empty (but valid) range,
// not an error or a dangling iterator.
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

// An `absl::Cleanup` that deletes all the files that a
// `CompressedVocabularyWithHoles` with the given base `filename` consists of.
auto getFileCleanup(const std::string& filename) {
  return vocabulary_test::makeVocabFileCleanup<CompressedVocabularyWithHoles>(
      filename);
}

// The words of the vocabulary with holes that the tests below use, sorted (as
// the underlying vocabulary requires sorted input at write time).
//
// NOTE: The numbers have a fixed width (so that the words are sorted also for
// more than ten words), and each word ends in a letter (so that the tests for
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

// Create a `CompressedVocabularyWithHoles` with the given `words` and
// `indices`, using the `DiskWriterWithExplicitIndices`.
CompressedVocabularyWithHoles createVocabularyWithHoles(
    const std::string& filename, const std::vector<std::string>& words,
    const std::vector<uint64_t>& indices) {
  AD_CORRECTNESS_CHECK(words.size() == indices.size());
  {
    CompressedVocabularyWithHoles::WordWriter writer{filename};
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
  auto cleanup = getFileCleanup(filename);
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
// `lookupBatch` on a vocabulary with holes must agree with `operator[]`: same
// words in the requested order, with the placeholder for hole indices. The
// batch must not feed the plain-text placeholder to the decoder.
TEST(CompressedVocabularyWithHoles, lookupBatchMatchesAccessOperator) {
  std::string filename = gtestCurrentTestName();
  auto cleanup = getFileCleanup(filename);
  auto vocab =
      createVocabularyWithHoles(filename, wordsWithHoles(), indicesWithHoles());
  // Mix contained indices, holes, duplicates, and an index past the end.
  const std::vector<size_t> indices{0, 1, 2, 4, 5, 7, 1, 31, 32, 35};
  const auto result = vocab.lookupBatch(ql::span<const size_t>{indices});
  assertLookupResultMatchesVocabularyAtIndices(vocab, result, indices);
}

// _____________________________________________________________________________
TEST(CompressedVocabularyWithHoles, lowerAndUpperBound) {
  std::string filename = gtestCurrentTestName();
  auto cleanup = getFileCleanup(filename);
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
  auto cleanup = getFileCleanup(filename);
  auto words = wordsWithHoles();
  auto indices = indicesWithHoles();
  auto vocab = createVocabularyWithHoles(filename, words, indices);

  vocabulary_test::testEndIndexAndGetPositionOfWord(
      vocab, words, indices,
      {{"aaa", indices.at(0)},
       {absl::StrCat(words.at(0), "x"), indices.at(1)}});

  // In an empty vocabulary, every word yields the empty range at index 0.
  auto emptyVocab = createVocabularyWithHoles(filename, {}, {});
  EXPECT_EQ(emptyVocab.endIndex(), 0);
  EXPECT_EQ(emptyVocab.getPositionOfWord("alpha", ql::ranges::less{}),
            (std::pair<uint64_t, uint64_t>{0, 0}));
}

// _____________________________________________________________________________
TEST(CompressedVocabularyWithHoles, scanAll) {
  std::string filename = gtestCurrentTestName();
  auto cleanup = getFileCleanup(filename);
  auto vocab =
      createVocabularyWithHoles(filename, wordsWithHoles(), indicesWithHoles());

  EXPECT_EQ(scanAllToIndexAndWordVector(vocab.scanAll()),
            expectedIndicesAndWords());
}

// _____________________________________________________________________________
TEST(CompressedVocabularyWithHoles, serialization) {
  std::string filename = gtestCurrentTestName();
  auto cleanup = getFileCleanup(filename);
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

// _____________________________________________________________________________
TEST(CompressedVocabularyWithHoles, addWordAfterFinishThrows) {
  std::string filename = gtestCurrentTestName();
  auto cleanup = getFileCleanup(filename);
  CompressedVocabularyWithHoles::WordWriter writer{filename};
  auto words = wordsWithHoles();
  auto indices = indicesWithHoles();
  EXPECT_EQ(writer(words.at(0), indices.at(0)), indices.at(0));
  writer.finish();

  // Adding a word after `finish` was called is a contract violation, because
  // the word could no longer be written to disk.
  AD_EXPECT_THROW_WITH_MESSAGE(
      writer(words.at(1), indices.at(1)),
      ::testing::HasSubstr("Assertion `!finishWasCalled_` failed"));

  // The vocabulary that was written before the failed call is intact, and
  // contains only the single word that was added successfully.
  CompressedVocabularyWithHoles vocab;
  vocab.open(filename);
  EXPECT_EQ(vocab.size(), 1);
  EXPECT_EQ(vocab[indices.at(0)], words.at(0));
}

// _____________________________________________________________________________
TEST(CompressedVocabularyWithHoles, nonAscendingIndicesThrow) {
  std::string filename = gtestCurrentTestName();
  auto cleanup = getFileCleanup(filename);
  CompressedVocabularyWithHoles::WordWriter writer{filename};
  auto words = wordsWithHoles();
  auto indices = indicesWithHoles();
  // Write one word more than a single block holds, such that the check for
  // ascending indices is also tested across a block boundary (the buffer of
  // indices is cleared whenever a block is written).
  static constexpr size_t numWords = 5;
  for (size_t i = 0; i < numWords; ++i) {
    EXPECT_EQ(writer(words.at(i), indices.at(i)), indices.at(i));
  }

  // Adding a word with an index that is not strictly greater than the last
  // index is a contract violation.
  auto expectThrow = [&writer, &words](uint64_t index) {
    AD_EXPECT_THROW_WITH_MESSAGE(
        writer(words.at(numWords), index),
        ::testing::HasSubstr("strictly ascending order"));
  };
  expectThrow(indices.at(numWords - 1));
  expectThrow(indices.at(numWords - 1) - 1);
  expectThrow(0);

  // The failed calls have left the writer intact, so the words that were added
  // before them can still be written.
  writer.finish();
  CompressedVocabularyWithHoles vocab;
  vocab.open(filename);
  ASSERT_EQ(vocab.size(), numWords);
  for (size_t i = 0; i < numWords; ++i) {
    EXPECT_EQ(vocab[indices.at(i)], words.at(i)) << "at position " << i;
  }
}

// _____________________________________________________________________________
// A vocabulary containing the empty string word ("") must be scanned correctly
// across all compressors (exercising the `maxDecompressedSize == 0` fast path
// in `scanAll`'s buffered decode), and zero-length views must provide non-null
// data pointers.
TYPED_TEST(CompressedVocabularyF, ScanAllEmptyWordInVocabulary) {
  auto createVocab = TestFixture::createCompressedVocabulary();
  const std::vector<std::string> words{"alpha", "", "beta", "", "gamma"};
  auto vocab = createVocab(words);
  std::vector<std::string> scannedWords;
  for (const IndexAndWord& entry : vocab.scanAll()) {
    if (entry.word_.empty()) {
      EXPECT_NE(entry.word_.data(), nullptr);
    }
    scannedWords.emplace_back(entry.word_);
  }
  using ::testing::ElementsAreArray;
  EXPECT_THAT(scannedWords, ElementsAreArray(words));
}

// _____________________________________________________________________________
// Direct test of the documented lifetime semantics of `scanAll`: each yielded
// `string_view` points into the range object that is REUSED for
// the next element, so a previously yielded view must no longer hold the old
// word once the next element has been pulled. Because the buffer outlives the
// whole range (it lives in the range adaptor's closure), reading the stale
// view afterwards is well-defined memory access -- which makes the assertion
// deterministic rather than UB-dependent.
TYPED_TEST(CompressedVocabularyF, ScanAllViewInvalidAfterNextPull) {
  auto createVocab = TestFixture::createCompressedVocabulary();
  // The first word is longer than the second so the decode buffer allocated on
  // the first pull is guaranteed to have enough capacity for the second word
  // without reallocating, ensuring the buffer pointer remains stable and
  // well-defined while its contents are overwritten.
  const std::vector<std::string> words{"firstMuchLongerWordPreallocatingBuffer",
                                       "secondShort"};
  auto vocab = createVocab(words);

  auto range = vocab.scanAll();
  auto it = ql::ranges::begin(range);
  ASSERT_NE(it, ql::ranges::end(range));
  IndexAndWord first = *it;
  ASSERT_EQ(first.index_, 0u);
  ASSERT_EQ(first.word_, words[0]);
  const char* firstData = first.word_.data();

  ++it;
  ASSERT_NE(it, ql::ranges::end(range));
  IndexAndWord second = *it;
  ASSERT_EQ(second.index_, 1u);
  ASSERT_EQ(second.word_, words[1]);

  // The stale view must no longer represent the first word: the underlying
  // buffer was reused for the second decompression.
  EXPECT_EQ(first.word_.data(), firstData);
  EXPECT_NE(first.word_, words[0]);
}

// _____________________________________________________________________________
// `lookupBatch` with several decoder blocks: a small block size (2 words per
// block) forces multiple decoders, exercising the per-request decoder
// selection. Covers a single-element batch, a batch with repeated indices,
// and a mixed batch crossing block boundaries; results must match
// `operator[]` exactly and appear in request order, with all views staying
// valid while the returned result object lives.
TYPED_TEST(CompressedVocabularyF, LookupBatchAcrossDecoderBlocks) {
  const std::vector<std::string> words{"alpha", "beta",  "gamma", "delta",
                                       "epsi",  "zeta",  "eta",   "theta",
                                       "iota",  "kappa", "",      "lambda"};

  const std::string filename = std::string{gtestCurrentTestName()} + "-blocks";
  ad_utility::deleteFile(filename, false);
  CompressedVocabulary<VocabularyInMemory, TypeParam, 2> vocab;
  {
    auto writerPtr = vocab.makeDiskWriterPtr(filename);
    writeWordsAndFinish(*writerPtr, words);
  }
  vocab.open(filename);
  ASSERT_EQ(vocab.size(), words.size());

  // Single-element batch: boundary case with exactly one requested index.
  const std::array<size_t, 1> singleIdx{7};
  auto single = vocab.lookupBatch(singleIdx);
  ASSERT_EQ(single.size(), 1u);
  EXPECT_EQ(single[0], "theta");

  // Multi-block batch containing repeated indices and the empty-string word:
  // every entry must match the per-word `operator[]`, in request order.
  const std::array<size_t, 8> indices{10, 0, 11, 5, 5, 2, 10, 9};
  const auto result = vocab.lookupBatch(indices);
  assertLookupResultMatchesVocabularyAtIndices(vocab, result, indices);
  EXPECT_TRUE(result[0].empty());
  EXPECT_EQ(result[6], "");

  // All views yielded by the result must stay intact while the result object
  // is alive, even after unrelated allocations have run in between. The
  // append forces a real heap reallocation of `unrelatedAllocation` after
  // `lookupBatch` produced the views, so a dangling view would be exposed
  // by the comparison loop below.
  std::string unrelatedAllocation;
  unrelatedAllocation.append(64, 'x');
  ASSERT_EQ(unrelatedAllocation.size(), 64u);
  EXPECT_EQ(unrelatedAllocation, std::string(64, 'x'));
  for (size_t i = 0; i < indices.size(); ++i) {
    const size_t idx = indices[i];
    EXPECT_EQ(result[i], vocab[idx]) << "at vocabulary index " << idx;
  }

  ad_utility::deleteFile(filename);
}

// _____________________________________________________________________________
TEST(DecoderMultiplexer, DirectDecompressIntoAndMaxDecompressedSize) {
  std::vector<DummyDecoder> decoders{DummyDecoder{}, DummyDecoder{}};
  ad_utility::vocabulary::detail::DecoderMultiplexer<DummyDecoder> mux{
      std::move(decoders)};
  ASSERT_EQ(mux.numDecoders(), 2u);

  const std::string compressed = DummyCompressionWrapper::compress("testword");
  const size_t bound = mux.maxDecompressedSize(compressed, 0);
  EXPECT_EQ(bound, compressed.size());

  std::string outputBuffer(bound, '\0');
  std::string scratch;
  const size_t written = mux.decompressInto(
      compressed, 0, ql::span<char>{outputBuffer.data(), outputBuffer.size()},
      scratch);
  EXPECT_EQ(written, 8u);
  EXPECT_EQ(std::string_view(outputBuffer.data(), written), "testword");
  EXPECT_EQ(mux.decompress(compressed, 0), "testword");

  // An undersized output buffer must be rejected by the underlying decoder's
  // contract check (`out.size() >= compressed.size()`).
  ql::span<char> undersized{outputBuffer.data(), bound - 1};
  AD_EXPECT_THROW_WITH_MESSAGE(
      static_cast<void>(mux.decompressInto(compressed, 0, undersized, scratch)),
      ::testing::HasSubstr("out.size() >= compressed.size()"));

  // Out-of-range decoder indices must be rejected for all dispatching
  // methods rather than silently reading out of bounds.
  const size_t invalidIndex = mux.numDecoders();
  EXPECT_THROW(
      static_cast<void>(mux.maxDecompressedSize(compressed, invalidIndex)),
      std::out_of_range);
  EXPECT_THROW(
      static_cast<void>(mux.decompressInto(
          compressed, invalidIndex,
          ql::span<char>{outputBuffer.data(), outputBuffer.size()}, scratch)),
      std::out_of_range);
  EXPECT_THROW(static_cast<void>(mux.decompress(compressed, invalidIndex)),
               std::out_of_range);
}
