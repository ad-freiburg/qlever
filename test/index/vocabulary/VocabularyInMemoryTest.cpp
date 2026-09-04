//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include <absl/strings/str_cat.h>
#include <gtest/gtest.h>

#include "./VocabularyTestHelpers.h"
#include "backports/algorithm.h"
#include "index/vocabulary/VocabularyInMemory.h"
#include "util/Serializer/ByteBufferSerializer.h"
using Vocab = VocabularyInMemory;

namespace {

using namespace vocabulary_test;

// Create a `Vocab` with the given `words` by writing it to the file with the
// name of the current test and reading it back. The caller has to delete that
// file again, for which `getFileCleanup` below is used.
auto createVocabulary(const std::vector<std::string>& words) {
  auto filename = gtestCurrentTestName();
  {
    Vocab v;
    auto writerPtr = v.makeDiskWriterPtr(filename);
    auto& writer = *writerPtr;
    for (const auto& [i, word] : ::ranges::views::enumerate(words)) {
      auto idx = writer(word, false);
      EXPECT_EQ(idx, static_cast<uint64_t>(i));
    }
    writer.readableName() = "blubb";
    EXPECT_EQ(writer.readableName(), "blubb");
  }
  Vocab v;
  v.open(filename);
  return v;
}

// An `absl::Cleanup` that deletes the file that a `Vocab` with the given base
// `filename` consists of.
auto getFileCleanup(const std::string& filename) {
  return makeVocabFileCleanup<Vocab>(filename);
}

// Same as above, for the file that `createVocabulary` above writes.
auto getFileCleanup() { return getFileCleanup(gtestCurrentTestName()); }

TEST(VocabularyInMemory, UpperLowerBound) {
  auto cleanup = getFileCleanup();
  testUpperAndLowerBoundWithStdLess(createVocabulary);
}

TEST(VocabularyInMemory, UpperLowerBoundAlternativeComparator) {
  auto cleanup = getFileCleanup();
  testUpperAndLowerBoundWithNumericComparator(createVocabulary);
}

TEST(VocabularyInMemory, AccessOperator) {
  auto cleanup = getFileCleanup();
  testAccessOperatorForUnorderedVocabulary(createVocabulary);
}

TEST(VocabularyInMemory, ReadAndWriteFromFile) {
  const std::vector<std::string> words{"alpha", "delta", "beta", "42",
                                       "31",    "0",     "al"};
  auto cleanup = getFileCleanup();
  const auto vocab = createVocabulary(words);
  const std::string vocabularyFilename =
      absl::StrCat(gtestCurrentTestName(), ".copy");
  auto copyCleanup = getFileCleanup(vocabularyFilename);
  vocab.writeToFile(vocabularyFilename);

  Vocab readVocab;
  readVocab.open(vocabularyFilename);
  assertThatRangesAreEqual(vocab, readVocab);
}

TEST(VocabularyInMemory, WriteAndReadWithSerializer) {
  const std::vector<std::string> words{"alpha", "delta", "beta", "42",
                                       "31",    "0",     "al"};
  auto cleanup = getFileCleanup();
  const auto vocab = createVocabulary(words);

  // Write using serializer.
  ad_utility::serialization::ByteBufferWriteSerializer writeSerializer;
  writeSerializer | vocab;
  const auto& blob = writeSerializer.data();
  ASSERT_FALSE(blob.empty());

  // Read using serializer into a different vocabulary.
  Vocab readVocab;
  ad_utility::serialization::ByteBufferReadSerializer readSerializer{blob};
  readSerializer | readVocab;
  assertThatRangesAreEqual(vocab, readVocab);
}

TEST(VocabularyInMemory, EmptyVocabulary) {
  auto cleanup = getFileCleanup();
  testEmptyVocabulary(createVocabulary);
}

// _____________________________________________________________________________
TEST(VocabularyInMemory, ScanAll) {
  // `scanAll` uses the generic `operator[]` fallback here and must yield all
  // words in order.
  const std::vector<std::string> words{"alpha", "delta", "beta", "42", "0"};
  auto cleanup = getFileCleanup();
  const auto vocab = createVocabulary(words);
  EXPECT_THAT(scanAllToVector(vocab.scanAll()),
              ::testing::ElementsAreArray(words));
}

// _____________________________________________________________________________
TEST(VocabularyInMemory, ScanAllEmptyVocabulary) {
  auto cleanup = getFileCleanup();
  const auto vocab = createVocabulary({});
  EXPECT_TRUE(scanAllToVector(vocab.scanAll()).empty());
}

// _____________________________________________________________________________
TEST(VocabularyInMemory, ZeroCopyDeserialization) {
  const std::vector<std::string> words{"alpha", "delta", "beta", "42",
                                       "31",    "0",     "al"};
  auto cleanup = getFileCleanup();
  const auto vocab = createVocabulary(words);

  ad_utility::serialization::AlignedByteBufferWriteSerializer writeSerializer;
  writeSerializer << vocab;

  ad_utility::serialization::AlignedByteBufferReadSerializer readSerializer{
      std::move(writeSerializer).data()};
  auto view = Vocab::fromZeroCopyDeserializer(readSerializer);

  assertThatRangesAreEqual(vocab, view);
}

// _____________________________________________________________________________
TEST(VocabularyInMemory, WordWriterDestructorBehavior) {
  const std::string filename = gtestCurrentTestName();
  auto cleanup = getFileCleanup(filename);
  Vocab v;
  {
    auto writerPtr = v.makeDiskWriterPtr(filename);
    auto& writer = *writerPtr;
    writer("alpha", false);
  }
  v.open(filename);
  { auto writerPtr = v.makeDiskWriterPtr(filename); };
  {
    VocabularyInMemory vocab;
    {
      auto wwPtr = vocab.makeDiskWriterPtr(filename);
      auto& ww = *wwPtr;
      ww("alpha", false);
    }
    vocab.open(filename);
    EXPECT_EQ(vocab[0], "alpha");
  }
  {
    VocabularyInMemory vocab;
    auto wwPtr = vocab.makeDiskWriterPtr(filename);
    auto& ww = *wwPtr;
    ww("beta", false);
    ww.finish();
    ww.finish();
    vocab.open(filename);
    EXPECT_EQ(vocab[0], "beta");
  }
}

}  // namespace
