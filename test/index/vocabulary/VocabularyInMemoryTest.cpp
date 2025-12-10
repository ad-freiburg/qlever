//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include <gtest/gtest.h>

#include "./VocabularyTestHelpers.h"
#include "backports/algorithm.h"
#include "index/vocabulary/VocabularyInMemory.h"
#include "util/Serializer/ByteBufferSerializer.h"
using Vocab = VocabularyInMemory;

namespace {

using namespace vocabulary_test;

auto createVocabulary(const std::vector<std::string>& words) {
  auto filename = "vocabInMemoryCreation.tmp";
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

TEST(VocabularyInMemory, UpperLowerBound) {
  testUpperAndLowerBoundWithStdLess(createVocabulary);
}

TEST(VocabularyInMemory, UpperLowerBoundAlternativeComparator) {
  testUpperAndLowerBoundWithNumericComparator(createVocabulary);
}

TEST(VocabularyInMemory, AccessOperator) {
  testAccessOperatorForUnorderedVocabulary(createVocabulary);
}

TEST(VocabularyInMemory, ReadAndWriteFromFile) {
  const std::vector<std::string> words{"alpha", "delta", "beta", "42",
                                       "31",    "0",     "al"};
  const auto vocab = createVocabulary(words);
  const std::string vocabularyFilename = "testvocab.dat";
  vocab.writeToFile(vocabularyFilename);

  Vocab readVocab;
  readVocab.open(vocabularyFilename);
  assertThatRangesAreEqual(vocab, readVocab);
  ad_utility::deleteFile(vocabularyFilename);
}

TEST(VocabularyInMemory, WriteAndReadWithSerializer) {
  const std::vector<std::string> words{"alpha", "delta", "beta", "42",
                                       "31",    "0",     "al"};
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
  testEmptyVocabulary(createVocabulary);
}

// _____________________________________________________________________________
TEST(VocabularyInMemory, WordWriterDestructorBehavior) {
  const std::string filename = "VocabInMemoryWordWriterDestructorBehavior.tmp";
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
  ad_utility::deleteFile(filename);
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
  ad_utility::deleteFile(filename);

  // This class doesn't automatically call `finish` in the destructor, so the
  // base class terminates in this case.
  struct WordWriter : WordWriterBase {
    WordWriter() = default;
    uint64_t operator()(std::string_view, bool) override { return 0; }
    void finishImpl() override {}
  };

  auto f = []() { WordWriter w{}; };
  EXPECT_DEATH(f(), "");
}

}  // namespace
