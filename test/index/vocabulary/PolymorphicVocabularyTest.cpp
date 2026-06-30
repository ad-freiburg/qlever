//  Copyright 2025, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include <gmock/gmock.h>

#include "index/vocabulary/PolymorphicVocabulary.h"

using ad_utility::VocabularyType;

namespace {

// Test a `PolymorphicVocabulary` with a given `vocabType`.
void testForVocabType(VocabularyType::Enum vocabType) {
  VocabularyType type{vocabType};
  std::string filename =
      absl::StrCat("polymorphicVocabularyTest.", type.toString(), ".vocab");

  auto writerPtr = PolymorphicVocabulary::makeDiskWriterPtr(filename, type);
  auto& writer = *writerPtr;
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

  EXPECT_EQ(std::visit([](auto& u) { return static_cast<uint64_t>(u.size()); },
                       vocab.getUnderlyingVocabulary()),
            3);

  const auto& vocabConst = vocab;
  EXPECT_EQ(
      std::visit([](const auto& u) { return static_cast<uint64_t>(u.size()); },
                 vocabConst.getUnderlyingVocabulary()),
      3);

  EXPECT_EQ(vocab.isGeoInfoAvailable(),
            vocabType == VocabularyType::Enum::OnDiskCompressedGeoSplit);
}
}  // namespace

// Test the general functionality of the `PolymorphicVocabulary` for all the
// possible `VocabularyType`s.
TEST(PolymorphicVocabulary, basicTests) {
  ql::ranges::for_each(VocabularyType::all(), &testForVocabType);
}

// Test that `setNumWordsPerCodebook` is forwarded to the active underlying
// vocabulary if it is a compressed one, and is a no-op otherwise.
TEST(PolymorphicVocabulary, setNumWordsPerCodebook) {
  PolymorphicVocabulary vocab;

  // Compressed type: the value is forwarded to the underlying compressed
  // vocabulary.
  vocab.resetToType(VocabularyType{VocabularyType::Enum::OnDiskCompressed});
  vocab.setNumWordsPerCodebook(13);
  std::visit(
      [](auto& v) {
        using T = std::decay_t<decltype(v)>;
        if constexpr (requires(T& vv) { vv.getNumWordsPerCodebook(); }) {
          EXPECT_EQ(v.getNumWordsPerCodebook(), 13u);
        } else {
          FAIL();
        }
      },
      vocab.getUnderlyingVocabulary());

  // Uncompressed type: `setNumWordsPerCodebook` is a no-op (no codebooks).
  vocab.resetToType(VocabularyType{VocabularyType::Enum::InMemoryUncompressed});
  EXPECT_NO_THROW(vocab.setNumWordsPerCodebook(13));
}

// Test a corner case in a `switch` statement.
TEST(PolymorphicVocabulary, invalidVocabularyType) {
  PolymorphicVocabulary vocab;
  auto invalidType = VocabularyType{static_cast<VocabularyType::Enum>(23401)};
  EXPECT_ANY_THROW(vocab.resetToType(invalidType));
}
