// Copyright 2024, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach <johannes.kalmbach@gmail.com>

#include <absl/cleanup/cleanup.h>
#include <gtest/gtest.h>

#include "./VocabularyTestHelpers.h"
#include "backports/algorithm.h"
#include "index/vocabulary/VocabularyInMemoryBinSearch.h"
#include "util/Forward.h"
#include "util/Serializer/ByteBufferSerializer.h"

namespace {
using namespace vocabulary_test;

// A common suffix for all files to reduce the probability of colliding file
// names, when other tests are run in parallel.
std::string suffix = ".vocabularyInMemoryBinSearchTest.dat";

// Store a VocabularyInMemoryBinSearch and read it back from file. For each
// instance of `VocabularyCreator` that exists at the same time, a different
// filename has to be chosen.
class VocabularyCreator {
 private:
  std::string vocabFilename_;

 public:
  explicit VocabularyCreator(std::string filename)
      : vocabFilename_{filename + suffix} {
    ad_utility::deleteFile(vocabFilename_, false);
  }
  ~VocabularyCreator() { ad_utility::deleteFile(vocabFilename_); }

  // Create and return a `VocabularyInMemoryBinSearch` from words and ids.
  // `words` and `ids` must have the same size. If `ids` is `nullopt`, then
  // ascending IDs starting at 0 will be automatically assigned to the words.
  auto createVocabularyImpl(
      const std::vector<std::string>& words,
      std::optional<std::vector<uint64_t>> ids = std::nullopt) {
    VocabularyInMemoryBinSearch vocabulary;
    {
      auto writer = VocabularyInMemoryBinSearch::WordWriter(vocabFilename_);
      if (ids.has_value()) {
        AD_CORRECTNESS_CHECK(ids.value().size() == words.size());
      }
      uint64_t idx = 0;
      for (auto& word : words) {
        size_t actualIdx = ids.has_value() ? ids.value().at(idx) : idx;
        EXPECT_EQ(writer(word, actualIdx), actualIdx);
        ++idx;
      }
      static std::atomic<unsigned> doFinish = 0;
      // In some tests, call `finish` explicitly, in others let the destructor
      // handle this.
      if (doFinish.fetch_add(1) % 2 == 0) {
        writer.finish();
      }
    }
    vocabulary.open(vocabFilename_);
    return vocabulary;
  }

  // Like `createVocabularyImpl`, but the resulting vocabulary will be destroyed
  // and re-initialized from disk before it is returned.
  auto createVocabularyFromDiskImpl(
      const std::vector<std::string>& words,
      std::optional<std::vector<uint64_t>> ids = std::nullopt) {
    { createVocabularyImpl(words, std::move(ids)); }
    VocabularyInMemoryBinSearch vocabulary;
    vocabulary.open(vocabFilename_);
    return vocabulary;
  }

  // Create and return a `VocabularyInMemoryBinSearch` from words. The ids will
  // be [0, .. words.size()).
  auto createVocabulary(const std::vector<std::string>& words) {
    return createVocabularyImpl(words);
  }

  // Create and return a `VocabularyInMemoryBinSearch` from words. The ids will
  // be [0, .. words.size()). Note: The resulting vocabulary will be destroyed
  // and re-initialized from disk before it is returned.
  auto createVocabularyFromDisk(const std::vector<std::string>& words) {
    return createVocabularyFromDiskImpl(words);
  }
};

auto createVocabulary(std::string filename) {
  return [c = VocabularyCreator{std::move(filename)}](auto&&... args) mutable {
    return c.createVocabulary(AD_FWD(args)...);
  };
}

auto createVocabularyFromDisk(std::string filename) {
  return [c = VocabularyCreator{std::move(filename)}](auto&&... args) mutable {
    return c.createVocabularyFromDisk(AD_FWD(args)...);
  };
}

}  // namespace

TEST(VocabularyInMemoryBinSearch, LowerUpperBoundStdLess) {
  testUpperAndLowerBoundWithStdLess(
      createVocabulary("lowerUpperBoundStdLess1"));
  testUpperAndLowerBoundWithStdLess(
      createVocabularyFromDisk("lowerUpperBoundStdLess2"));
}

TEST(VocabularyInMemoryBinSearch, LowerUpperBoundNumeric) {
  testUpperAndLowerBoundWithNumericComparator(
      createVocabulary("lowerUpperBoundNumeric1"));
  testUpperAndLowerBoundWithNumericComparator(
      createVocabularyFromDisk("lowerUpperBoundNumeric2"));
}

TEST(VocabularyInMemoryBinSearch, AccessOperator) {
  testAccessOperatorForUnorderedVocabulary(createVocabulary("AccessOperator1"));
  testAccessOperatorForUnorderedVocabulary(
      createVocabularyFromDisk("AccessOperator2"));
}

TEST(VocabularyInMemoryBinSearch, AccessOperatorWithNonContiguousIds) {
  testAccessOperatorForUnorderedVocabulary(
      createVocabulary("AccessOperatorWithNonContiguousIds1"));
  testAccessOperatorForUnorderedVocabulary(
      createVocabularyFromDisk("AccessOperatorWithNonContiguousIds2"));
}

TEST(VocabularyInMemoryBinSearch, ErrorOnNonAscendingIds) {
  std::vector<std::string> words{"game", "4", "nobody"};
  std::vector<uint64_t> ids{2, 4, 3};
  VocabularyCreator creator1{"ErrorOnNonAscendingIds1"};
  ASSERT_THROW(creator1.createVocabularyImpl(words, ids),
               ad_utility::Exception);
  VocabularyCreator creator2{"ErrorOnNonAscendingIds2"};
  ASSERT_THROW(creator2.createVocabularyFromDiskImpl(words, ids),
               ad_utility::Exception);
}

TEST(VocabularyInMemoryBinSearch, EmptyVocabulary) {
  testEmptyVocabulary(createVocabulary("EmptyVocabulary"));
}

namespace {

// The words and the (non-contiguous, i.e. with "holes") vocabulary indices that
// the tests below use. The words are sorted, as the vocabulary requires sorted
// input at write time.
const std::vector<std::string> wordsWithHoles{"alpha", "beta", "delta",
                                              "gamma"};
const std::vector<uint64_t> indicesWithHoles{0, 3, 4, 9};

// The vocabulary indices that are NOT contained in a vocabulary that was built
// from `wordsWithHoles` and `indicesWithHoles`, i.e. its "holes".
const std::vector<uint64_t> missingIndices{1, 2, 5, 6, 7, 8, 10, 12345};

// Create a `VocabularyInMemoryBinSearch` with the given `words` and `indices`
// via a `WordWriter` that writes to `filename`. The vocabulary keeps all its
// contents in RAM, so the caller may delete the files as soon as this function
// has returned.
VocabularyInMemoryBinSearch createVocabularyWithIndices(
    const std::string& filename, const std::vector<std::string>& words,
    const std::vector<uint64_t>& indices) {
  AD_CORRECTNESS_CHECK(words.size() == indices.size());
  {
    VocabularyInMemoryBinSearch::WordWriter writer{filename};
    for (size_t i = 0; i < words.size(); ++i) {
      EXPECT_EQ(writer(words.at(i), indices.at(i)), indices.at(i));
    }
    writer.finish();
  }
  VocabularyInMemoryBinSearch vocabulary;
  vocabulary.open(filename);
  return vocabulary;
}

// Delete the two files (words and indices) that a `WordWriter` for the given
// `filename` creates. Do not warn about files that were never created.
void deleteVocabularyFiles(const std::string& filename) {
  ad_utility::deleteFile(filename, false);
  ad_utility::deleteFile(filename + ".ids", false);
}

// Check that the two vocabularies contain exactly the same words with exactly
// the same vocabulary indices.
template <typename Vocab1, typename Vocab2>
void expectVocabulariesAreEqual(const Vocab1& vocab1, const Vocab2& vocab2) {
  ASSERT_EQ(vocab1.size(), vocab2.size());
  EXPECT_THAT(vocab2.indices(), ::testing::ElementsAreArray(vocab1.indices()));
  EXPECT_EQ(vocabulary_test::scanAllToIndexAndWordVector(vocab1.scanAll()),
            vocabulary_test::scanAllToIndexAndWordVector(vocab2.scanAll()));
}

// The `{index, word}` pairs that a vocabulary built from `wordsWithHoles` and
// `indicesWithHoles` is expected to contain.
std::vector<std::pair<uint64_t, std::string>> expectedIndicesAndWords() {
  std::vector<std::pair<uint64_t, std::string>> result;
  for (size_t i = 0; i < wordsWithHoles.size(); ++i) {
    result.emplace_back(indicesWithHoles.at(i), wordsWithHoles.at(i));
  }
  return result;
}

}  // namespace

// _____________________________________________________________________________
TEST(VocabularyInMemoryBinSearch, positionOfIndexAndAccessOperator) {
  std::string filename = gtestCurrentTestName();
  absl::Cleanup cleanup = [&filename] { deleteVocabularyFiles(filename); };
  auto vocab =
      createVocabularyWithIndices(filename, wordsWithHoles, indicesWithHoles);

  ASSERT_EQ(vocab.size(), wordsWithHoles.size());
  EXPECT_THAT(vocab.indices(), ::testing::ElementsAreArray(indicesWithHoles));
  for (size_t position = 0; position < wordsWithHoles.size(); ++position) {
    uint64_t index = indicesWithHoles.at(position);
    EXPECT_EQ(vocab.positionOfIndex(index), std::optional{position});
    EXPECT_EQ(vocab.indexAtPosition(position), index);
    EXPECT_EQ(vocab[index], std::optional{wordsWithHoles.at(position)});
  }

  // The indices that are not contained (the "holes") have neither a position
  // nor a word.
  for (uint64_t index : missingIndices) {
    EXPECT_EQ(vocab.positionOfIndex(index), std::nullopt);
    EXPECT_EQ(vocab[index], std::nullopt);
  }

  // A position that is out of range is a bug and hence throws.
  EXPECT_THROW(vocab.indexAtPosition(vocab.size()), ad_utility::Exception);
}

// _____________________________________________________________________________
TEST(VocabularyInMemoryBinSearch, endIndexAndGetPositionOfWord) {
  std::string filename = gtestCurrentTestName();
  absl::Cleanup cleanup = [&filename] { deleteVocabularyFiles(filename); };
  auto vocab =
      createVocabularyWithIndices(filename, wordsWithHoles, indicesWithHoles);

  vocabulary_test::testEndIndexAndGetPositionOfWord(
      vocab, wordsWithHoles, indicesWithHoles,
      {{"aaa", 0}, {"alx", 3}, {"cat", 4}});

  // In an empty vocabulary, every word yields the empty range at index 0.
  auto emptyVocab = createVocabularyWithIndices(filename, {}, {});
  EXPECT_EQ(emptyVocab.endIndex(), 0);
  EXPECT_EQ(emptyVocab.getPositionOfWord("alpha", ql::ranges::less{}),
            (std::pair<uint64_t, uint64_t>{0, 0}));
}

// _____________________________________________________________________________
TEST(VocabularyInMemoryBinSearch, scanAll) {
  std::string filename = gtestCurrentTestName();
  absl::Cleanup cleanup = [&filename] { deleteVocabularyFiles(filename); };
  auto vocab =
      createVocabularyWithIndices(filename, wordsWithHoles, indicesWithHoles);

  EXPECT_EQ(vocabulary_test::scanAllToIndexAndWordVector(vocab.scanAll()),
            expectedIndicesAndWords());

  // An empty vocabulary yields nothing.
  auto emptyVocab = createVocabularyWithIndices(filename, {}, {});
  EXPECT_TRUE(vocabulary_test::scanAllToIndexAndWordVector(emptyVocab.scanAll())
                  .empty());
}

// _____________________________________________________________________________
TEST(VocabularyInMemoryBinSearch, lookupBatch) {
  std::string filename = gtestCurrentTestName();
  absl::Cleanup cleanup = [&filename] { deleteVocabularyFiles(filename); };
  auto vocab =
      createVocabularyWithIndices(filename, wordsWithHoles, indicesWithHoles);

  // Reordered and duplicated indices, all of which are contained.
  std::vector<size_t> indices{4, 0, 9, 3, 0};
  vocabulary_test::assertLookupResultMatchesVocabularyAtIndices(
      vocab, vocab.lookupBatch(indices), indices);

  // The same via the streamed interface.
  std::vector<std::vector<size_t>> batches{{4, 0}, {9}, {3, 0, 4}};
  const auto expectedBatches = batches;
  auto streamed =
      vocab.lookupBatchesStreamed(VocabLookupInput{std::move(batches)});
  vocabulary_test::assertStreamedLookupMatchesVocabularyAtIndices(
      vocab, streamed, expectedBatches);

  // An index that is not contained (one of the "holes") yields a placeholder.
  std::vector<size_t> indicesWithMissingOnes{0, 5, 9};
  auto result = vocab.lookupBatch(indicesWithMissingOnes);
  ASSERT_EQ(result->size(), 3);
  EXPECT_EQ((*result)[0], "alpha");
  EXPECT_EQ((*result)[1],
            ad_utility::vocabulary::placeholderForMissingVocabIndex(5));
  EXPECT_EQ((*result)[2], "gamma");
}

// _____________________________________________________________________________
TEST(VocabularyInMemoryBinSearch, genericSerialization) {
  std::string filename = gtestCurrentTestName();
  absl::Cleanup cleanup = [&filename] { deleteVocabularyFiles(filename); };
  auto vocab =
      createVocabularyWithIndices(filename, wordsWithHoles, indicesWithHoles);

  ad_utility::serialization::ByteBufferWriteSerializer writeSerializer;
  writeSerializer << vocab;
  ASSERT_FALSE(writeSerializer.data().empty());

  VocabularyInMemoryBinSearch readVocab;
  ad_utility::serialization::ByteBufferReadSerializer readSerializer{
      std::move(writeSerializer).data()};
  readSerializer >> readVocab;
  expectVocabulariesAreEqual(vocab, readVocab);

  // The deserialized vocabulary owns its indices, so it can be closed and read
  // again.
  readVocab.close();
  EXPECT_EQ(readVocab.size(), 0);
}

// _____________________________________________________________________________
TEST(VocabularyInMemoryBinSearch, zeroCopyDeserialization) {
  std::string filename = gtestCurrentTestName();
  absl::Cleanup cleanup = [&filename] { deleteVocabularyFiles(filename); };
  auto vocab =
      createVocabularyWithIndices(filename, wordsWithHoles, indicesWithHoles);

  // The zero-copy read requires an aligned serializer, and reads back exactly
  // the layout that the generic serialization writes.
  ad_utility::serialization::AlignedByteBufferWriteSerializer writeSerializer;
  writeSerializer << vocab;
  ad_utility::serialization::AlignedByteBufferReadSerializer readSerializer{
      std::move(writeSerializer).data()};
  auto view =
      VocabularyInMemoryBinSearch::fromZeroCopyDeserializer(readSerializer);
  expectVocabulariesAreEqual(vocab, view);
  EXPECT_EQ(view[3], std::optional{std::string_view{"beta"}});
  EXPECT_EQ(view[5], std::nullopt);
}

// _____________________________________________________________________________
TEST(VocabularyInMemoryBinSearch, makeDiskWriterPtrThrows) {
  // A vocabulary with holes cannot be built via the `WordWriterBase` interface,
  // which cannot express the explicit indices.
  AD_EXPECT_THROW_WITH_MESSAGE(
      VocabularyInMemoryBinSearch::makeDiskWriterPtr(gtestCurrentTestName()),
      ::testing::HasSubstr("cannot be built word by word"));
}
