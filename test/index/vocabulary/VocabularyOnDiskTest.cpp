// Copyright 2022, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach <johannes.kalmbach@gmail.com>

#include <gtest/gtest.h>

#include "./VocabularyTestHelpers.h"
#include "backports/algorithm.h"
#include "index/vocabulary/VocabularyOnDisk.h"
#include "util/Forward.h"

namespace {
using namespace vocabulary_test;

// Store a VocabularyOnDisk and read it back from file. For each instance of
// `VocabularyCreator` that exists at the same time, a different filename has to
// be chosen.
class VocabularyCreator {
 private:
  std::string vocabFilename_;

 public:
  explicit VocabularyCreator(std::string filename)
      : vocabFilename_{std::move(filename)} {
    ad_utility::deleteFile(vocabFilename_, false);
  }
  ~VocabularyCreator() { ad_utility::deleteFile(vocabFilename_); }

  // Create and return a `VocabularyOnDisk` from words.
  void createVocabularyImpl(const std::vector<std::string>& words) {
    auto writer = VocabularyOnDisk::WordWriter(vocabFilename_);
    for (const auto& [i, word] : ::ranges::views::enumerate(words)) {
      EXPECT_EQ(writer(word, false), static_cast<uint64_t>(i));
    }
    writer.readableName() = "blubb";
    EXPECT_EQ(writer.readableName(), "blubb");
    static std::atomic<unsigned> doFinish = 0;
    // In some tests, call `finish` explicitly, in others let the destructor
    // handle this.
    if (doFinish.fetch_add(1) % 2 == 0) {
      writer.finish();
    }
  }

  // Create and return a `VocabularyOnDisk` from words. The ids will be [0, ..
  // words.size()).
  auto createVocabulary(const std::vector<std::string>& words) {
    createVocabularyImpl(words);
    VocabularyOnDisk vocabulary;
    vocabulary.open(vocabFilename_);
    return vocabulary;
  }
};

auto createVocabulary(std::string filename) {
  return [c = VocabularyCreator{std::move(filename)}](auto&&... args) mutable {
    return c.createVocabulary(AD_FWD(args)...);
  };
}
}  // namespace

TEST(VocabularyOnDisk, LowerUpperBoundStdLess) {
  testUpperAndLowerBoundWithStdLess(createVocabulary("lowerUpperBoundStdLess"));
}

TEST(VocabularyOnDisk, LowerUpperBoundNumeric) {
  testUpperAndLowerBoundWithNumericComparator(
      createVocabulary("lowerUpperBoundNumeric"));
}

TEST(VocabularyOnDisk, AccessOperator) {
  testAccessOperatorForUnorderedVocabulary(createVocabulary("AccessOperator"));
}

TEST(VocabularyOnDisk, AccessOperatorWithNonContiguousIds) {
  std::vector<std::string> words{"game",  "4",      "nobody", "33",
                                 "alpha", "\n\1\t", "222",    "1111"};
  std::vector<uint64_t> ids{2, 4, 8, 16, 17, 19, 42, 42 * 42 + 7};
  testAccessOperatorForUnorderedVocabulary(
      createVocabulary("AccessOperatorWithNonContiguousIds"));
}

TEST(VocabularyOnDisk, EmptyVocabulary) {
  testEmptyVocabulary(createVocabulary("EmptyVocabulary"));
}
