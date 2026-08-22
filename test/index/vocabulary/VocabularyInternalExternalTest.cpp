// Copyright 2024, 2026, University of Freiburg,
//                 Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach <johannes.kalmbach@gmail.com>
//         Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR

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
// `lookupBatch` must match `operator[]` in request order, including cache
// hits (even `i` in the writer: stored in RAM) and misses (odd `i`: disk
// only), plus reordered and duplicated indices.
TEST(VocabularyInternalExternal, LookupBatchMatchesAccessOperator) {
  const std::vector<std::string> words{"alpha", "beta", "gamma", "delta",
                                       "epsilon"};
  auto vocab = createVocabulary("LookupBatch")(words);
  const std::array<size_t, 7> indices{4, 1, 0, 3, 1, 2, 4};
  auto result = vocab.lookupBatch(indices);
  assertLookupResultMatchesVocabularyAtIndices(vocab, result, indices);
  AD_EXPECT_THROW_WITH_MESSAGE(vocab.lookupBatch(ql::span<const size_t>{}),
                               ::testing::HasSubstr("!indices.empty()"));

  // Even writer indices are RAM-cached; odd indices are disk-only.
  const std::array<size_t, 3> ramOnly{0, 2, 4};
  assertLookupResultMatchesVocabularyAtIndices(
      vocab, vocab.lookupBatch(ramOnly), ramOnly);
  const std::array<size_t, 3> diskOnly{1, 3, 1};
  assertLookupResultMatchesVocabularyAtIndices(
      vocab, vocab.lookupBatch(diskOnly), diskOnly);
}

// _____________________________________________________________________________
TEST(VocabularyInternalExternal, EmptyVocabulary) {
  testEmptyVocabulary(createVocabulary("EmptyVocabulary"));
}

// _____________________________________________________________________________
// `scanAll` delegates to the external vocabulary and must yield all words in
// order.
TEST(VocabularyInternalExternal, ScanAll) {
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
