// Copyright 2022, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach <johannes.kalmbach@gmail.com>

#include <gtest/gtest.h>

#include "../src/index/VocabularyOnDisk.h"
#include "./VocabularyTestHelpers.h"

using namespace vocabulary_test;

const std::string vocabFilename = "vocabulary.tmp.test.dat";
auto createVocabulary(const std::vector<std::string>& words) {
  VocabularyOnDisk vocabulary;
  vocabulary.buildFromVector(words, vocabFilename);
  return vocabulary;
}

auto createVocabularyFromDisk(const std::vector<std::string>& words) {
  {
    VocabularyOnDisk vocabulary;
    vocabulary.buildFromVector(words, vocabFilename);
  }
  VocabularyOnDisk vocabulary;
  vocabulary.readFromFile(vocabFilename);
  return vocabulary;
}

TEST(ExternalVocabulary, LowerUpperBoundStdLess) {
  ad_utility::deleteFile(vocabFilename);
  testUpperAndLowerBoundWithStdLess(createVocabulary);
  ad_utility::deleteFile(vocabFilename);
  testUpperAndLowerBoundWithStdLess(createVocabularyFromDisk);
  ad_utility::deleteFile(vocabFilename);
};

TEST(ExternalVocabulary, LowerUpperBoundNumeric) {
  ad_utility::deleteFile(vocabFilename);
  testUpperAndLowerBoundWithNumericComparator(createVocabulary);
  ad_utility::deleteFile(vocabFilename);
  testUpperAndLowerBoundWithNumericComparator(createVocabularyFromDisk);
  ad_utility::deleteFile(vocabFilename);
};

TEST(ExternalVocabulary, AccessOperator) {
  ad_utility::deleteFile(vocabFilename);
  testAccessOperatorForUnorderedVocabulary(createVocabulary);
  ad_utility::deleteFile(vocabFilename);
  testAccessOperatorForUnorderedVocabulary(createVocabularyFromDisk);
}

// TODO<joka921> Also test an externalVocabulary with IDs that are NOT
// contiguous.
