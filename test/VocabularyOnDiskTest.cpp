// Copyright 2022, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach <johannes.kalmbach@gmail.com>

#include <gtest/gtest.h>

#include "../src/index/VocabularyOnDisk.h"
#include "../src/util/Forward.h"
#include "./VocabularyTestHelpers.h"

using namespace vocabulary_test;

// A common suffix for all files to reduce the probability of colliding file
// names, when other tests are run in parallel.
std::string suffix = ".vocabularyOnDiskTest.dat";

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

  // Create and return a `VocabularyOnDisk` from words and ids. `words` and
  // `ids` must have the same size.
  auto createVocabularyImpl(
      const std::vector<std::string>& words,
      std::optional<std::vector<uint64_t>> ids = std::nullopt) {
    VocabularyOnDisk vocabulary;
    if (!ids.has_value()) {
      vocabulary.buildFromVector(words, vocabFilename_);
    } else {
      AD_CHECK(words.size() == ids.value().size());
      std::vector<std::pair<std::string, uint64_t>> wordsAndIds;
      for (size_t i = 0; i < words.size(); ++i) {
        wordsAndIds.emplace_back(words[i], ids.value()[i]);
      }
      vocabulary.buildFromStringsAndIds(wordsAndIds, vocabFilename_);
    }
    return vocabulary;
  }

  // Create and return a `VocabularyOnDisk` from words and ids. `words` and
  // `ids` must have the same size. Note: The resulting vocabulary will be
  // destroyed and re-initialized from disk before it is returned.
  auto createVocabularyFromDiskImpl(
      const std::vector<std::string>& words,
      std::optional<std::vector<uint64_t>> ids = std::nullopt) {
    { createVocabularyImpl(words, std::move(ids)); }
    VocabularyOnDisk vocabulary;
    vocabulary.open(vocabFilename_);
    return vocabulary;
  }

  // Create and return a `VocabularyOnDisk` from words. The ids will be [0, ..
  // words.size()).
  auto createVocabulary(const std::vector<std::string>& words) {
    return createVocabularyImpl(words);
  }

  // Create and return a `VocabularyOnDisk` from words. The ids will be [0, ..
  // words.size()). Note: The resulting vocabulary will be destroyed and
  // re-initialized from disk before it is returned.
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

auto createVocabularyFromDiskImpl(std::string filename) {
  return [c = VocabularyCreator{std::move(filename)}](auto&&... args) mutable {
    return c.createVocabularyFromDiskImpl(AD_FWD(args)...);
  };
}

TEST(VocabularyOnDisk, LowerUpperBoundStdLess) {
  testUpperAndLowerBoundWithStdLess(
      createVocabulary("lowerUpperBoundStdLess1"));
  testUpperAndLowerBoundWithStdLess(
      createVocabularyFromDisk("lowerUpperBoundStdLess2"));
}

TEST(VocabularyOnDisk, LowerUpperBoundStdLessNonContiguousIds) {
  std::vector<std::string> words{"alpha", "betta", "chimes", "someVery123Word"};
  std::vector<uint64_t> ids{2, 4, 8, 42};
  VocabularyCreator creator1{"lowerUppperBoundStdLessNonContiguousIds1"};
  testUpperAndLowerBoundWithStdLessFromWordsAndIds(
      creator1.createVocabularyImpl(words, ids), words, ids);

  VocabularyCreator creator2{"lowerUppperBoundStdLessNonContiguousIds2"};
  testUpperAndLowerBoundWithStdLessFromWordsAndIds(
      creator2.createVocabularyFromDiskImpl(words, ids), words, ids);
}

TEST(VocabularyOnDisk, LowerUpperBoundNumeric) {
  testUpperAndLowerBoundWithNumericComparator(
      createVocabulary("lowerUpperBoundNumeric1"));
  testUpperAndLowerBoundWithNumericComparator(
      createVocabularyFromDisk("lowerUpperBoundNumeric2"));
}

TEST(VocabularyOnDisk, LowerUpperBoundNumericNonContiguousIds) {
  std::vector<std::string> words{"4", "33", "222", "1111"};
  std::vector<uint64_t> ids{2, 4, 8, 42};

  VocabularyCreator creator1{"lowerUpperBoundNumericNonContiguousIds1"};
  testUpperAndLowerBoundWithNumericComparatorFromWordsAndIds(
      creator1.createVocabularyImpl(words, ids), words, ids);
  VocabularyCreator creator2{"lowerUpperBoundNumericNonContiguousIds2"};
  testUpperAndLowerBoundWithNumericComparatorFromWordsAndIds(
      creator2.createVocabularyFromDiskImpl(words, ids), words, ids);
}

TEST(VocabularyOnDisk, AccessOperator) {
  testAccessOperatorForUnorderedVocabulary(createVocabulary("AccessOperator1"));
  testAccessOperatorForUnorderedVocabulary(
      createVocabularyFromDisk("AccessOperator2"));
}

TEST(VocabularyOnDisk, AccessOperatorWithNonContiguousIds) {
  std::vector<std::string> words{"game",  "4",      "nobody", "33",
                                 "alpha", "\n\1\t", "222",    "1111"};
  std::vector<uint64_t> ids{2, 4, 8, 16, 17, 19, 42, 42 * 42 + 7};
  testAccessOperatorForUnorderedVocabulary(
      createVocabulary("AccessOperatorWithNonContiguousIds1"));
  testAccessOperatorForUnorderedVocabulary(
      createVocabularyFromDisk("AccessOperatorWithNonContiguousIds2"));
}

TEST(VocabularyOnDisk, ErrorOnNonAscendingIds) {
  std::vector<std::string> words{"game", "4", "nobody"};
  std::vector<uint64_t> ids{2, 4, 3};
  VocabularyCreator creator1{"ErrorOnNonAscendingIds1"};
  ASSERT_THROW(creator1.createVocabularyImpl(words, ids),
               ad_semsearch::Exception);
  VocabularyCreator creator2{"ErrorOnNonAscendingIds2"};
  ASSERT_THROW(creator2.createVocabularyFromDiskImpl(words, ids),
               ad_semsearch::Exception);
}

TEST(VocabularyOnDisk, EmptyVocabulary) {
  testEmptyVocabulary(createVocabulary("EmptyVocabulary"));
}
