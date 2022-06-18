// Copyright 2022, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach <johannes.kalmbach@gmail.com>

#include <gtest/gtest.h>

#include "../src/index/VocabularyOnDisk.h"
#include "./VocabularyTestHelpers.h"

using namespace vocabulary_test;

const std::string vocabFilename = "vocabulary.tmp.test.dat";

// Create and return a `VocabularyOnDisk` from words and ids. `words` and `ids`
// must have the same size.
auto createVocabularyImpl(
    const std::vector<std::string>& words,
    std::optional<std::vector<uint64_t>> ids = std::nullopt) {
  VocabularyOnDisk vocabulary;
  if (!ids.has_value()) {
    vocabulary.buildFromVector(words, vocabFilename);
  } else {
    AD_CHECK(words.size() == ids.value().size());
    std::vector<std::pair<std::string, uint64_t>> wordsAndIds;
    for (size_t i = 0; i < words.size(); ++i) {
      wordsAndIds.emplace_back(words[i], ids.value()[i]);
    }
    vocabulary.buildFromStringsAndIds(wordsAndIds, vocabFilename);
  }
  return vocabulary;
}

// Create and return a `VocabularyOnDisk` from words and ids. `words` and `ids`
// must have the same size. Note: The resulting vocabulary will be destroyed and
// re-initialized from disk before it is returned.
auto createVocabularyFromDiskImpl(
    const std::vector<std::string>& words,
    std::optional<std::vector<uint64_t>> ids = std::nullopt) {
  { createVocabularyImpl(words, std::move(ids)); }
  VocabularyOnDisk vocabulary;
  vocabulary.open(vocabFilename);
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

TEST(VocabularyOnDisk, LowerUpperBoundStdLess) {
  ad_utility::deleteFile(vocabFilename);
  testUpperAndLowerBoundWithStdLess(createVocabulary);
  ad_utility::deleteFile(vocabFilename);
  testUpperAndLowerBoundWithStdLess(createVocabularyFromDisk);
  ad_utility::deleteFile(vocabFilename);
}

TEST(VocabularyOnDisk, LowerUpperBoundStdLessNonContiguousIds) {
  std::vector<std::string> words{"alpha", "betta", "chimes", "someVery123Word"};
  std::vector<uint64_t> ids{2, 4, 8, 42};
  ad_utility::deleteFile(vocabFilename);
  testUpperAndLowerBoundWithStdLessFromWordsAndIds(
      createVocabularyImpl(words, ids), words, ids);
  ad_utility::deleteFile(vocabFilename);
  testUpperAndLowerBoundWithStdLessFromWordsAndIds(
      createVocabularyFromDiskImpl(words, ids), words, ids);
  ad_utility::deleteFile(vocabFilename);
}

TEST(VocabularyOnDisk, LowerUpperBoundNumeric) {
  ad_utility::deleteFile(vocabFilename);
  testUpperAndLowerBoundWithNumericComparator(createVocabulary);
  ad_utility::deleteFile(vocabFilename);
  testUpperAndLowerBoundWithNumericComparator(createVocabularyFromDisk);
  ad_utility::deleteFile(vocabFilename);
}

TEST(VocabularyOnDisk, LowerUpperBoundNumericNonContiguousIds) {
  std::vector<std::string> words{"4", "33", "222", "1111"};
  std::vector<uint64_t> ids{2, 4, 8, 42};
  ad_utility::deleteFile(vocabFilename);
  testUpperAndLowerBoundWithNumericComparatorFromWordsAndIds(
      createVocabularyImpl(words, ids), words, ids);
  ad_utility::deleteFile(vocabFilename);
  testUpperAndLowerBoundWithNumericComparatorFromWordsAndIds(
      createVocabularyFromDiskImpl(words, ids), words, ids);
  ad_utility::deleteFile(vocabFilename);
}

TEST(VocabularyOnDisk, AccessOperator) {
  ad_utility::deleteFile(vocabFilename);
  testAccessOperatorForUnorderedVocabulary(createVocabulary);
  ad_utility::deleteFile(vocabFilename);
  testAccessOperatorForUnorderedVocabulary(createVocabularyFromDisk);
}

TEST(VocabularyOnDisk, AccessOperatorWithNonContiguousIds) {
  std::vector<std::string> words{"game",  "4",      "nobody", "33",
                                 "alpha", "\n\1\t", "222",    "1111"};
  std::vector<uint64_t> ids{2, 4, 8, 16, 17, 19, 42, 42 * 42 + 7};
  ad_utility::deleteFile(vocabFilename);
  testAccessOperatorForUnorderedVocabulary(createVocabulary);
  ad_utility::deleteFile(vocabFilename);
  testAccessOperatorForUnorderedVocabulary(createVocabularyFromDisk);
}

TEST(VocabularyOnDisk, ErrorOnNonAscendingIds) {
  std::vector<std::string> words{"game", "4", "nobody"};
  std::vector<uint64_t> ids{2, 4, 3};
  ASSERT_THROW(createVocabularyImpl(words, ids), ad_semsearch::Exception);
  ASSERT_THROW(createVocabularyFromDiskImpl(words, ids),
               ad_semsearch::Exception);
}

TEST(VocabularyOnDisk, EmptyVocabulary) {
  testEmptyVocabulary(createVocabulary);
}
