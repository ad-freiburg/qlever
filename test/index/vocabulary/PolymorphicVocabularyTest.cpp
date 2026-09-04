// Copyright 2025 - 2026, The QLever Authors, in particular:
//
// 2025 - 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
// 2026        Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
// Copyright 2025, Bayerische Motoren Werke Aktiengesellschaft (BMW AG)

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <absl/cleanup/cleanup.h>
#include <gmock/gmock.h>

#include "../../util/FileTestHelpers.h"
#include "VocabularyTestHelpers.h"
#include "index/vocabulary/PolymorphicVocabulary.h"

using ad_utility::VocabularyType;

namespace {

// Is the given `vocabType` one of the vocabulary types with "holes" (see
// `VocabularyType.h`)? Those cannot be built word by word, and hence require a
// special handling in the tests below.
bool isWithHoles(VocabularyType::Enum vocabType) {
  return vocabType == VocabularyType::Enum::InMemoryUncompressedWithHoles ||
         vocabType == VocabularyType::Enum::InMemoryCompressedWithHoles;
}

// Delete all the files that a vocabulary of the given `type` with the given
// base `filename` consists of.
void deleteVocabFiles(VocabularyType type, const std::string& filename) {
  vocabulary_test::deleteVocabularyFiles(
      filename, PolymorphicVocabulary::fileSuffixes(type));
}

// Write the `vocabulary_test::defaultTestWords` with the given (non-contiguous)
// `indices` to `filename`, using a vocabulary of the given `vocabType`, which
// must be one of the types with holes. Their `WordWriter`s take an explicit
// index for each word and hence do not implement the `WordWriterBase`
// interface, so they cannot be obtained via
// `PolymorphicVocabulary::makeDiskWriterPtr` (which throws for those types).
void writeVocabWithHoles(VocabularyType::Enum vocabType,
                         const std::string& filename,
                         const std::vector<uint64_t>& indices) {
  auto writeWords = [&indices](auto& writer) {
    ASSERT_EQ(indices.size(), vocabulary_test::defaultTestWords.size());
    for (size_t i = 0; i < indices.size(); ++i) {
      EXPECT_EQ(writer(vocabulary_test::defaultTestWords.at(i), indices.at(i)),
                indices.at(i));
    }
    writer.finish();
  };
  if (vocabType == VocabularyType::Enum::InMemoryUncompressedWithHoles) {
    VocabularyInMemoryBinSearch::WordWriter writer{filename};
    writeWords(writer);
  } else {
    ASSERT_EQ(vocabType, VocabularyType::Enum::InMemoryCompressedWithHoles);
    CompressedVocabulary<VocabularyInMemoryBinSearch>::WordWriter writer{
        filename};
    writeWords(writer);
  }
}

// Test a `PolymorphicVocabulary` with one of the `vocabType`s with holes. Those
// cannot be built word by word (only by filtering an existing vocabulary), so
// they need a separate test.
void testForVocabTypeWithHoles(VocabularyType::Enum vocabType) {
  VocabularyType type{vocabType};
  std::string filename =
      absl::StrCat("polymorphicVocabularyTest.", type.toString(), ".vocab");
  absl::Cleanup cleanup = [&type, &filename] {
    deleteVocabFiles(type, filename);
  };

  // The `WordWriterBase` interface cannot express the explicit indices that a
  // vocabulary with holes requires.
  AD_EXPECT_THROW_WITH_MESSAGE(
      PolymorphicVocabulary::makeDiskWriterPtr(filename, type),
      ::testing::HasSubstr("cannot be built word by word"));

  std::vector<uint64_t> indices{0, 2, 4, 6};
  writeVocabWithHoles(vocabType, filename, indices);
  PolymorphicVocabulary vocab;
  vocab.open(filename, type);
  EXPECT_EQ(vocab.size(), 4);

  vocabulary_test::assertVocabularyMatchesAtIndices(
      vocab, indices, {"alpha", "beta", "delta", "gamma"});

  // The indices that are not contained (the "holes") yield a placeholder.
  for (uint64_t index : {1, 3, 5, 100}) {
    EXPECT_EQ(vocab[index],
              ad_utility::vocabulary::placeholderForMissingVocabIndex(index));
  }

  // The binary search reports the (non-contiguous) vocabulary indices.
  auto wI = vocab.lower_bound("alx", ql::ranges::less{});
  EXPECT_EQ(wI.index(), 2);
  EXPECT_EQ(wI.word(), "beta");
  wI = vocab.upper_bound("gamma", ql::ranges::less{});
  EXPECT_TRUE(wI.isEnd());
  EXPECT_EQ(vocab.getPositionOfWord("beta", ql::ranges::less{}),
            (std::pair<uint64_t, uint64_t>{2, 3}));
  // A word that sorts after all contained words has to be reported as "one
  // past the largest contained index" (here 7), and not as `size()` (here 4),
  // which because of the holes is the index of an actual, smaller word.
  EXPECT_EQ(vocab.getPositionOfWord("zzz", ql::ranges::less{}),
            (std::pair<uint64_t, uint64_t>{7, 7}));

  // A vocabulary with holes never provides geometry information.
  EXPECT_FALSE(vocab.isGeoInfoAvailable());
  EXPECT_FALSE(vocab.getGeoInfo(0).has_value());

  EXPECT_THAT(vocabulary_test::scanAllToIndexAndWordVector(vocab.scanAll()),
              ::testing::ElementsAre(std::pair{uint64_t{0}, "alpha"},
                                     std::pair{uint64_t{2}, "beta"},
                                     std::pair{uint64_t{4}, "delta"},
                                     std::pair{uint64_t{6}, "gamma"}));

  vocab.close();
  EXPECT_EQ(vocab.size(), 0);
}

// Test a `PolymorphicVocabulary` with a given `vocabType`.
void testForVocabType(VocabularyType::Enum vocabType) {
  if (isWithHoles(vocabType)) {
    testForVocabTypeWithHoles(vocabType);
    return;
  }
  VocabularyType type{vocabType};
  std::string filename =
      absl::StrCat("polymorphicVocabularyTest.", type.toString(), ".vocab");
  absl::Cleanup cleanup = [&type, &filename] {
    deleteVocabFiles(type, filename);
  };

  auto writerPtr = PolymorphicVocabulary::makeDiskWriterPtr(filename, type);
  auto& writer = *writerPtr;
  writer("alpha", false);
  writer("beta", true);
  writer("gamma", false);
  writer.finish();

  PolymorphicVocabulary vocab;
  vocab.open(filename, type);
  EXPECT_EQ(vocab.size(), 3);

  vocabulary_test::assertVocabularyMatchesContiguousIndices(
      vocab, {"alpha", "beta", "gamma"});

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

  // `scanAll` must enumerate all words in order together with their index, for
  // every vocabulary type (implementations without a specialized `scanAll` use
  // a generic fallback). Here all words are in the main vocabulary, so the
  // indices are simply `0, 1, 2`.
  std::vector<std::pair<uint64_t, std::string>> scanned;
  for (const IndexAndWord& indexAndWord : vocab.scanAll()) {
    scanned.emplace_back(indexAndWord.index_, std::string{indexAndWord.word_});
  }
  EXPECT_THAT(scanned, ::testing::ElementsAre(std::pair{uint64_t{0}, "alpha"},
                                              std::pair{uint64_t{1}, "beta"},
                                              std::pair{uint64_t{2}, "gamma"}));
}

// Write a small vocabulary of the given `vocabType` to `filename` and open it
// into `vocab`. The exact words don't matter (they only have to be sorted, as
// the underlying vocabularies require sorted input at write time).
void setupVocab(PolymorphicVocabulary& vocab, VocabularyType::Enum vocabType,
                const std::string& filename) {
  VocabularyType type{vocabType};
  if (isWithHoles(vocabType)) {
    // For the vocabularies with holes, the indices `1` and `3` are the holes,
    // for which the lookups below have to report a placeholder.
    writeVocabWithHoles(vocabType, filename, {0, 2, 4, 6});
  } else {
    auto writerPtr = PolymorphicVocabulary::makeDiskWriterPtr(filename, type);
    vocabulary_test::writeWordsAndFinish(*writerPtr);
  }
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
    auto [temporaryFile, cleanup] = ad_utility::testing::filenameForTesting();
    // NOTE: A structured binding must not be captured by a lambda in C++17,
    // hence the copy into an ordinary local variable.
    std::string filename = temporaryFile.string();
    absl::Cleanup deleteFiles = [vocabType, &filename] {
      deleteVocabFiles(VocabularyType{vocabType}, filename);
    };
    PolymorphicVocabulary vocab;
    setupVocab(vocab, vocabType, filename);

    std::array<size_t, 6> indices{2, 0, 3, 1, 1, 0};
    auto result = vocab.lookupBatch(indices);
    vocabulary_test::assertLookupResultMatchesVocabularyAtIndices(vocab, result,
                                                                  indices);
  }
}

// `lookupBatchesStreamed` must yield, for each batch and in input order,
// exactly what the individual `vocab[]` lookups return. Checked for every
// `VocabularyType`.
TEST(PolymorphicVocabulary, lookupBatchesStreamedMatchesIndividualLookups) {
  for (auto vocabType : VocabularyType::all()) {
    auto [temporaryFile, cleanup] = ad_utility::testing::filenameForTesting();
    // NOTE: A structured binding must not be captured by a lambda in C++17,
    // hence the copy into an ordinary local variable.
    std::string filename = temporaryFile.string();
    absl::Cleanup deleteFiles = [vocabType, &filename] {
      deleteVocabFiles(VocabularyType{vocabType}, filename);
    };
    PolymorphicVocabulary vocab;
    setupVocab(vocab, vocabType, filename);

    std::vector<std::vector<size_t>> batches{{2, 0}, {1}, {0, 3, 1}};
    // `VocabLookupInput` takes ownership of the batches, so keep a copy to
    // compare against.
    const auto expectedBatches = batches;
    auto streamed =
        vocab.lookupBatchesStreamed(VocabLookupInput{std::move(batches)});

    vocabulary_test::assertStreamedLookupMatchesVocabularyAtIndices(
        vocab, streamed, expectedBatches);
  }
}

// Test a corner case in a `switch` statement.
TEST(PolymorphicVocabulary, invalidVocabularyType) {
  PolymorphicVocabulary vocab;
  auto invalidType = VocabularyType{static_cast<VocabularyType::Enum>(23401)};
  EXPECT_ANY_THROW(vocab.resetToType(invalidType));
}
