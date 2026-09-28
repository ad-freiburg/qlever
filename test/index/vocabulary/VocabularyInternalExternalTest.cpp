// Copyright 2024, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach <johannes.kalmbach@gmail.com>

#include <gtest/gtest.h>

#include <array>
#include <string>
#include <vector>

#include "./VocabularyTestHelpers.h"
#include "backports/algorithm.h"
#include "index/vocabulary/VocabularyInternalExternal.h"
#include "util/Exception.h"
#include "util/Forward.h"

namespace {
using namespace vocabulary_test;

// A common suffix for all files to reduce the probability of colliding file
// names, when other tests are run in parallel.
std::string suffix = ".vocabularyInternalExternalTest.dat";

// Store a VocabularyInternalExternal and read it back from file. For each
// instance of `VocabularyCreator` that exists at the same time, a different
// filename has to be chosen.
class VocabularyCreator {
 private:
  std::string vocabFilename_;

 public:
  explicit VocabularyCreator(const std::string& filename)
      : vocabFilename_{filename + suffix} {
    ad_utility::deleteFile(vocabFilename_, false);
  }
  ~VocabularyCreator() { ad_utility::deleteFile(vocabFilename_); }

  // Create and return a `VocabularyInternalExternal` from the given words.
  auto createVocabularyImpl(const std::vector<std::string>& words) {
    VocabularyInternalExternal vocabulary;
    {
      auto writerPtr =
          VocabularyInternalExternal::makeDiskWriterPtr(vocabFilename_);
      auto& writer = *writerPtr;
      for (const auto& [i, word] : ::ranges::views::enumerate(words)) {
        EXPECT_EQ(writer(word, i % 2 == 0), static_cast<uint64_t>(i));
      }
      writer.readableName() = "blabbiblu";
      EXPECT_EQ(writer.readableName(), "blabbiblu");
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

  // Like `createVocabularyImpl` above, but the resulting vocabulary will be
  // destroyed and re-initialized from disk before it is returned.
  auto createVocabularyFromDiskImpl(const std::vector<std::string>& words) {
    { createVocabularyImpl(words); }
    VocabularyInternalExternal vocabulary;
    vocabulary.open(vocabFilename_);
    return vocabulary;
  }

  // Create and return a `VocabularyInternalExternal` from words. The ids will
  // be [0, .. words.size()).
  auto createVocabulary(const std::vector<std::string>& words) {
    return createVocabularyImpl(words);
  }

  // Create and return a `VocabularyInternalExternal` from words. The ids will
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

TEST(VocabularyInternalExternal, LowerUpperBoundStdLess) {
  testUpperAndLowerBoundWithStdLess(
      createVocabulary("lowerUpperBoundStdLess1"));
  testUpperAndLowerBoundWithStdLess(
      createVocabularyFromDisk("lowerUpperBoundStdLess2"));
}

TEST(VocabularyInternalExternal, LowerUpperBoundNumeric) {
  testUpperAndLowerBoundWithNumericComparator(
      createVocabulary("lowerUpperBoundNumeric1"));
  testUpperAndLowerBoundWithNumericComparator(
      createVocabularyFromDisk("lowerUpperBoundNumeric2"));
}

TEST(VocabularyInternalExternal, AccessOperator) {
  testAccessOperatorForUnorderedVocabulary(createVocabulary("AccessOperator1"));
  testAccessOperatorForUnorderedVocabulary(
      createVocabularyFromDisk("AccessOperator2"));
}

// _____________________________________________________________________________
TEST(VocabularyInternalExternal, LookupBatchMatchesAccessOperator) {
  const std::vector<std::string> words{"alpha", "beta", "gamma", "delta",
                                       "epsilon"};
  // The batch result must preserve request order across all-internal,
  // all-external, and mixed-source requests, including duplicates.
  auto vocab = createVocabulary("LookupBatch")(words);
  const std::array<size_t, 7> indices{4, 1, 0, 3, 1, 2, 4};
  auto result = vocab.lookupBatch(indices);
  assertLookupResultMatchesVocabularyAtIndices(vocab, result, indices);
  AD_EXPECT_THROW_WITH_MESSAGE(vocab.lookupBatch(ql::span<const size_t>{}),
                               ::testing::HasSubstr("!indices.empty()"));

  // The test writer marks even IDs as external; odd IDs and ID 0 (the first
  // milestone) are also stored in the internal vocabulary.
  const std::array<size_t, 3> ramOnly{0, 1, 3};
  assertLookupResultMatchesVocabularyAtIndices(
      vocab, vocab.lookupBatch(ramOnly), ramOnly);
  const std::array<size_t, 3> diskOnly{2, 4, 2};
  assertLookupResultMatchesVocabularyAtIndices(
      vocab, vocab.lookupBatch(diskOnly), diskOnly);

  // Keep a mixed result alive while another lookup is performed, exercising
  // ownership of the backing storage returned by both vocabulary sources.
  auto retainedMixedResult = vocab.lookupBatch(indices);
  auto subsequentResult = vocab.lookupBatch(ramOnly);
  assertLookupResultMatchesVocabularyAtIndices(vocab, retainedMixedResult,
                                               indices);
  assertLookupResultMatchesVocabularyAtIndices(vocab, subsequentResult,
                                               ramOnly);
}

// _____________________________________________________________________________
// Words of the internal vocabulary are returned as views into it (two lookups
// of the same word see the same bytes); words of the external vocabulary are
// read into a buffer that each result owns.
TEST(VocabularyInternalExternal, LookupBatchDoesNotCopyInternalWords) {
  const std::vector<std::string> words{"alpha", "beta", "gamma", "delta"};
  auto vocab = createVocabulary("LookupBatchDoesNotCopyInternalWords")(words);
  // ID 1 is in the internal vocabulary, ID 2 only in the external one (see
  // `createVocabularyImpl`).
  const std::array<size_t, 2> indices{1, 2};
  auto first = vocab.lookupBatch(indices);
  auto second = vocab.lookupBatch(indices);
  assertLookupResultMatchesVocabularyAtIndices(vocab, first, indices);
  EXPECT_EQ(first[0].data(), second[0].data());
  EXPECT_NE(first[1].data(), second[1].data());
}

// _____________________________________________________________________________
// The external words of a result are owned by the result, so a batch of only
// external words stays valid after the vocabulary is closed.
TEST(VocabularyInternalExternal, LookupBatchExternalWordsOutliveClose) {
  const std::vector<std::string> words{"alpha", "beta", "gamma", "delta"};
  auto vocab = createVocabulary("LookupBatchExternalWordsOutliveClose")(words);
  const std::array<size_t, 2> diskOnly{2, 2};
  auto result = vocab.lookupBatch(diskOnly);
  vocab.close();

  EXPECT_THAT(result, ::testing::ElementsAre("gamma", "gamma"));
}

// _____________________________________________________________________________
TEST(VocabularyInternalExternal, EmptyVocabulary) {
  testEmptyVocabulary(createVocabulary("EmptyVocabulary"));
}

// _____________________________________________________________________________
TEST(VocabularyInternalExternal, ScanAll) {
  // `scanAll` delegates to the external vocabulary and must yield all words in
  // order.
  const std::vector<std::string> words{"alpha", "beta", "gamma", "delta"};
  auto vocab = createVocabulary("ScanAll")(words);
  EXPECT_THAT(scanAllToVector(vocab.scanAll()),
              ::testing::ElementsAreArray(words));
}

// _____________________________________________________________________________
TEST(VocabularyInternalExternal, ScanAllEmptyVocabulary) {
  auto vocab = createVocabulary("ScanAllEmpty")(std::vector<std::string>{});
  EXPECT_TRUE(scanAllToVector(vocab.scanAll()).empty());
}
