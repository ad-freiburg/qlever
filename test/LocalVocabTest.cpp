//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Hannah Bast <bast@cs.uni-freiburg.de>

#include <gtest/gtest.h>

#include "engine/ResultTable.h"
#include "global/Id.h"

TEST(LocalVocab, construction) {
  auto checkId = [](Id id, size_t i) {
    ASSERT_EQ(id, Id::makeFromLocalVocabIndex(LocalVocabIndex::make(i)));
  };
  LocalVocab localVocab;
  ASSERT_TRUE(localVocab.empty());
  checkId(localVocab.getIdAndAddIfNotContained("bla"), 0);
  checkId(localVocab.getIdAndAddIfNotContained("blu"), 1);
  checkId(localVocab.getIdAndAddIfNotContained("bli"), 2);
  checkId(localVocab.getIdAndAddIfNotContained("bla"), 0);
  ASSERT_EQ(localVocab.size(), 3);
  ASSERT_EQ(localVocab.getWord(LocalVocabIndex::make(0)), "bla");
  ASSERT_EQ(localVocab.getWord(LocalVocabIndex::make(1)), "blu");
  ASSERT_EQ(localVocab.getWord(LocalVocabIndex::make(2)), "bli");
}
