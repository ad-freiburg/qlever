//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Hannah Bast <bast@cs.uni-freiburg.de>

#include <gtest/gtest.h>

#include <sstream>
#include <string>

#include "engine/ResultTable.h"
#include "global/Id.h"

TEST(LocalVocab, construction) {
  // Generate a test collection of words. For the tests below to work, these
  // must all be different.
  std::vector<std::string> testVocab(10'000);
  for (size_t i = 0; i < testVocab.size(); ++i) {
    testVocab[i] = std::to_string(i * 7635475567ULL);
  }
  // Create empty local vocabulary.
  LocalVocab localVocab;
  ASSERT_TRUE(localVocab.empty());
  // Add the words from our test vocabulary and check that they get the expected
  // local IDs.
  for (size_t i = 0; i < testVocab.size(); ++i) {
    ASSERT_EQ(localVocab.getIndexAndAddIfNotContained(testVocab[i]),
              LocalVocabIndex::make(i));
  }
  ASSERT_EQ(localVocab.size(), testVocab.size());
  // Check that we get the same IDs if we do this again, but that no new words
  // will be added.
  for (size_t i = 0; i < testVocab.size(); ++i) {
    ASSERT_EQ(localVocab.getIndexAndAddIfNotContained(testVocab[i]),
              LocalVocabIndex::make(i));
  }
  ASSERT_EQ(localVocab.size(), testVocab.size());
  // Check that the lookup by ID gives the correct words.
  for (size_t i = 0; i < testVocab.size(); ++i) {
    ASSERT_EQ(localVocab.getWord(LocalVocabIndex::make(i)), testVocab[i]);
  }
  ASSERT_EQ(localVocab.size(), testVocab.size());
}
