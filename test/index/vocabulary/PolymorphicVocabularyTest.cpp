//  Copyright 2025, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include <gmock/gmock.h>

#include "../../util/FileTestHelpers.h"
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

// Write a small vocabulary of the given `vocabType` to `filename` and open it
// into `vocab`. The exact words don't matter (they only have to be sorted, as
// the underlying vocabularies require sorted input at write time).
void setupVocab(PolymorphicVocabulary& vocab, VocabularyType::Enum vocabType,
                const std::string& filename) {
  VocabularyType type{vocabType};
  auto writerPtr = PolymorphicVocabulary::makeDiskWriterPtr(filename, type);
  auto& writer = *writerPtr;
  for (const auto& word : {"alpha", "beta", "delta", "gamma"}) {
    writer(word, false);
  }
  writer.finish();
  vocab.open(filename, type);
}
}  // namespace

// Test the general functionality of the `PolymorphicVocabulary` for all the
// possible `VocabularyType`s.
TEST(PolymorphicVocabulary, basicTests) {
  ql::ranges::for_each(VocabularyType::all(), &testForVocabType);
}

// `lookupBatch` must return, for each requested index, exactly what `vocab[]`
// returns for that index, preserving the order of the requested indices
// (including reordered and duplicated ones). Checked for every
// `VocabularyType`.
TEST(PolymorphicVocabulary, lookupBatchMatchesIndividualLookups) {
  for (auto vocabType : VocabularyType::all()) {
    auto [filename, cleanup] = ad_utility::testing::filenameForTesting();
    PolymorphicVocabulary vocab;
    setupVocab(vocab, vocabType, filename.string());

    std::array<size_t, 6> indices{2, 0, 3, 1, 1, 0};
    auto result = vocab.lookupBatch(indices);
    ASSERT_EQ(result->size(), indices.size());
    for (size_t i = 0; i < indices.size(); ++i) {
      EXPECT_EQ((*result)[i], vocab[indices[i]]);
    }
  }
}

// `lookupBatchesStreamed` must yield, for each batch and in input order,
// exactly what the individual `vocab[]` lookups return. Checked for every
// `VocabularyType`.
TEST(PolymorphicVocabulary, lookupBatchesStreamedMatchesIndividualLookups) {
  for (auto vocabType : VocabularyType::all()) {
    auto [filename, cleanup] = ad_utility::testing::filenameForTesting();
    PolymorphicVocabulary vocab;
    setupVocab(vocab, vocabType, filename.string());

    std::vector<std::vector<size_t>> batches{{2, 0}, {1}, {0, 3, 1}};
    // `VocabLookupInput` takes ownership of the batches, so keep a copy to
    // compare against.
    const auto expectedBatches = batches;
    auto streamed =
        vocab.lookupBatchesStreamed(VocabLookupInput{std::move(batches)});

    size_t b = 0;
    for (auto& result : streamed) {
      ASSERT_LT(b, expectedBatches.size());
      const auto& indices = expectedBatches[b];
      ASSERT_EQ(result->size(), indices.size());
      for (size_t i = 0; i < indices.size(); ++i) {
        EXPECT_EQ((*result)[i], vocab[indices[i]]);
      }
      ++b;
    }
    EXPECT_EQ(b, expectedBatches.size());
  }
}

// Test a corner case in a `switch` statement.
TEST(PolymorphicVocabulary, invalidVocabularyType) {
  PolymorphicVocabulary vocab;
  auto invalidType = VocabularyType{static_cast<VocabularyType::Enum>(23401)};
  EXPECT_ANY_THROW(vocab.resetToType(invalidType));
}
