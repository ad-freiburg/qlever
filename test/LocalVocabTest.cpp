//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Hannah Bast <bast@cs.uni-freiburg.de>

#include <gtest/gtest.h>

#include "engine/ResultTable.h"
#include "global/Id.h"

TEST(LocalVocab, construction) {
  auto makeIndex = [](size_t i) { return LocalVocabIndex::make(i); };
  LocalVocab localVocab;
  ASSERT_TRUE(localVocab.empty());
  ASSERT_EQ(localVocab.getIndexAndAddIfNotContained("bla"), makeIndex(0));
  ASSERT_EQ(localVocab.getIndexAndAddIfNotContained("blu"), makeIndex(1));
  ASSERT_EQ(localVocab.getIndexAndAddIfNotContained("bli"), makeIndex(2));
  ASSERT_EQ(localVocab.getIndexAndAddIfNotContained("bla"), makeIndex(0));
  ASSERT_EQ(localVocab.size(), 3);
  ASSERT_EQ(localVocab.getWord(LocalVocabIndex::make(0)), "bla");
  ASSERT_EQ(localVocab.getWord(LocalVocabIndex::make(1)), "blu");
  ASSERT_EQ(localVocab.getWord(LocalVocabIndex::make(2)), "bli");
}
