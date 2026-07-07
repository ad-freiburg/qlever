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

// Write a small vocabulary of the given `vocabType` to disk and open it into
// `vocab`. The batch-lookup tests below only compare against individual
// `vocab[]` lookups, so the exact words don't matter (they just have to be
// sorted, as the underlying vocabularies require sorted input at write time).
void setupVocabForBatchTest(PolymorphicVocabulary& vocab,
                            VocabularyType::Enum vocabType) {
  VocabularyType type{vocabType};
  std::string filename = absl::StrCat("polymorphicVocabularyBatchTest.",
                                      type.toString(), ".vocab");
  auto writerPtr = PolymorphicVocabulary::makeDiskWriterPtr(filename, type);
  auto& writer = *writerPtr;
  std::vector<std::string> words{"alpha", "beta", "delta", "gamma"};
  for (const auto& word : words) {
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

// `lookupBatch` must yield, for each requested index, exactly the same string
// as an individual `vocab[]` lookup — including for reordered and duplicated
// indices. Checked for every `VocabularyType`.
TEST(PolymorphicVocabulary, lookupBatchMatchesIndividualLookups) {
  for (auto vocabType : VocabularyType::all()) {
    PolymorphicVocabulary vocab;
    setupVocabForBatchTest(vocab, vocabType);
    ASSERT_GE(vocab.size(), 4u);

    std::array<size_t, 6> indices{2, 0, 3, 1, 1, 0};
    auto result = vocab.lookupBatch(indices);
    ASSERT_EQ(result->size(), indices.size());
    for (size_t i = 0; i < indices.size(); ++i) {
      EXPECT_EQ((*result)[i], vocab[indices[i]]);
    }

    // An empty batch is invalid.
    EXPECT_ANY_THROW(vocab.lookupBatch(ql::span<const size_t>{}));
  }
}

// `lookupBatchesStreamed` must yield, for each batch, exactly the same strings
// as individual `vocab[]` lookups for that batch's indices. Checked for every
// `VocabularyType`.
TEST(PolymorphicVocabulary, lookupBatchesStreamedMatchesIndividualLookups) {
  for (auto vocabType : VocabularyType::all()) {
    PolymorphicVocabulary vocab;
    setupVocabForBatchTest(vocab, vocabType);
    ASSERT_GE(vocab.size(), 4u);

    std::vector<std::vector<size_t>> batches{{2, 0}, {1}, {0, 3, 1}};
    // `VocabLookupInput` takes ownership of the batches, so keep a copy to
    // compare against.
    const auto expectedBatches = batches;
    auto streamed =
        vocab.lookupBatchesStreamed(VocabLookupInput{std::move(batches)});

    size_t b = 0;
    for (auto& result : streamed) {
      ASSERT_LT(b, expectedBatches.size());
      const auto& batchIndices = expectedBatches[b];
      ASSERT_EQ(result->size(), batchIndices.size());
      for (size_t i = 0; i < batchIndices.size(); ++i) {
        EXPECT_EQ((*result)[i], vocab[batchIndices[i]]);
      }
      ++b;
    }
    EXPECT_EQ(b, expectedBatches.size());

    // An empty batch within the stream is invalid and must throw when pulled.
    {
      std::vector<std::vector<size_t>> withEmpty{{2, 0}, {}, {1}};
      auto streamedWithEmpty =
          vocab.lookupBatchesStreamed(VocabLookupInput{std::move(withEmpty)});
      EXPECT_ANY_THROW({
        for ([[maybe_unused]] auto& result : streamedWithEmpty) {
        }
      });
    }

    // An empty input stream produces no results.
    {
      std::vector<std::vector<size_t>> noBatches;
      auto streamedEmpty =
          vocab.lookupBatchesStreamed(VocabLookupInput{std::move(noBatches)});
      size_t count = 0;
      for ([[maybe_unused]] auto& result : streamedEmpty) {
        ++count;
      }
      EXPECT_EQ(count, 0);
    }
  }
}

// Test a corner case in a `switch` statement.
TEST(PolymorphicVocabulary, invalidVocabularyType) {
  PolymorphicVocabulary vocab;
  auto invalidType = VocabularyType{static_cast<VocabularyType::Enum>(23401)};
  EXPECT_ANY_THROW(vocab.resetToType(invalidType));
}
