//  Copyright 2025, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include <gmock/gmock.h>

#include "index/vocabulary/PolymorphicVocabulary.h"

using ad_utility::VocabularyType;

namespace {
void testForVocabType(VocabularyType::Enum vocabType) {
  VocabularyType type{vocabType};
  std::string filename =
      absl::StrCat("polymorphicVocabularyTest.", type.toString(), ".vocab");

  auto writer = PolymorphicVocabulary::makeDiskWriter(filename, type);
  writer("alpha", false);
  writer("beta", true);
  writer("gamma", false);
  writer.finish();

  PolymorphicVocabulary vocab;
  vocab.open(filename, type);
  EXPECT_EQ(vocab.size(), 3);

  EXPECT_EQ(vocab[0], "alpha");
  EXPECT_EQ(vocab[1], "beta");
  EXPECT_EQ(vocab[2], "gamma");

  auto wI = vocab.lower_bound("alx", ql::ranges::less{});
  EXPECT_EQ(wI.index(), 1);
  EXPECT_EQ(wI.word(), "beta");

  wI = vocab.upper_bound("gamma", ql::ranges::less{});
  EXPECT_TRUE(wI.isEnd());
}
}  // namespace

TEST(PolymorphicVocabulary, basicTests) {
  ql::ranges::for_each(VocabularyType::all(), &testForVocabType);
}
