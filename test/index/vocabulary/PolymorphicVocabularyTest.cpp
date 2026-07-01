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

  // Test lookupBatch.
  std::array<size_t, 3> indices{2, 0, 1};
  auto result = vocab.lookupBatch(indices);
  ASSERT_EQ(result->size(), 3);
  EXPECT_EQ((*result)[0], "gamma");
  EXPECT_EQ((*result)[1], "alpha");
  EXPECT_EQ((*result)[2], "beta");

  // `lookupBatch` edge cases: an empty batch (invalid -> throws), and a batch
  // with duplicate indices (each position must be resolved independently).
  {
    EXPECT_ANY_THROW(vocab.lookupBatch(ql::span<const size_t>{}));

    std::array<size_t, 4> dupIndices{1, 1, 0, 1};
    auto dupResult = vocab.lookupBatch(dupIndices);
    ASSERT_EQ(dupResult->size(), 4);
    EXPECT_EQ((*dupResult)[0], "beta");
    EXPECT_EQ((*dupResult)[1], "beta");
    EXPECT_EQ((*dupResult)[2], "alpha");
    EXPECT_EQ((*dupResult)[3], "beta");
  }

  // Test lookupBatchesStreamed: a stream of batches, where each yielded result
  // must match the eager `lookupBatch` on the same batch. The input is built
  // from a plain container (no coroutine) so this also compiles in the C++17
  // reduced-feature build.
  {
    std::vector<std::vector<size_t>> batches{{2, 0}, {1}};
    auto streamed = vocab.lookupBatchesStreamed(VocabLookupInput{batches});

    std::vector<VocabBatchLookupResult> results;
    for (auto& r : streamed) {
      results.push_back(std::move(r));
    }
    ASSERT_EQ(results.size(), 2);

    auto expectMatchesEager = [&vocab](const VocabBatchLookupResult& actual,
                                       ql::span<const size_t> batchIndices) {
      auto expected = vocab.lookupBatch(batchIndices);
      ASSERT_EQ(actual->size(), expected->size());
      for (size_t i = 0; i < expected->size(); ++i) {
        EXPECT_EQ((*actual)[i], (*expected)[i]);
      }
    };
    expectMatchesEager(results[0], batches[0]);
    expectMatchesEager(results[1], batches[1]);

    // Exact contents.
    ASSERT_EQ(results[0]->size(), 2);
    EXPECT_EQ((*results[0])[0], "gamma");
    EXPECT_EQ((*results[0])[1], "alpha");
    ASSERT_EQ(results[1]->size(), 1);
    EXPECT_EQ((*results[1])[0], "beta");
  }

  // An empty batch within the stream is invalid and must throw when pulled.
  {
    std::vector<std::vector<size_t>> batches{{2, 0}, {}, {1}};
    auto streamed = vocab.lookupBatchesStreamed(VocabLookupInput{batches});
    EXPECT_ANY_THROW({
      for ([[maybe_unused]] auto& r : streamed) {
      }
    });
  }

  // An empty input stream must produce no results.
  {
    std::vector<std::vector<size_t>> noBatches;
    auto streamed = vocab.lookupBatchesStreamed(VocabLookupInput{noBatches});
    size_t count = 0;
    for ([[maybe_unused]] auto& r : streamed) {
      ++count;
    }
    EXPECT_EQ(count, 0);
  }
}
}  // namespace

// Test the general functionality of the `PolymorphicVocabulary` for all the
// possible `VocabularyType`s.
TEST(PolymorphicVocabulary, basicTests) {
  ql::ranges::for_each(VocabularyType::all(), &testForVocabType);
}

// Test a corner case in a `switch` statement.
TEST(PolymorphicVocabulary, invalidVocabularyType) {
  PolymorphicVocabulary vocab;
  auto invalidType = VocabularyType{static_cast<VocabularyType::Enum>(23401)};
  EXPECT_ANY_THROW(vocab.resetToType(invalidType));
}
