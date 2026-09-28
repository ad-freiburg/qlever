// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>, UFR
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <absl/functional/function_ref.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <array>
#include <cstring>
#include <optional>
#include <utility>

#include "../../util/GTestHelpers.h"
#include "index/vocabulary/VocabularyInMemoryBinSearch.h"
#include "index/vocabulary/VocabularyTypes.h"
#include "util/File.h"
#include "util/MemorySize/MemorySize.h"

using namespace ad_utility::memory_literals;

namespace {
// _____________________________________________________________________________
// A class that executes a passed function in its constructor.
class Caller {
 public:
  explicit Caller(absl::FunctionRef<void()> f) { std::invoke(f); }
};

// _____________________________________________________________________________
// A class inheriting from `WordWriterBase` that throws when initializing a
// member.
class WordWriterThrowing : public WordWriterBase {
 private:
  Caller caller_;

 public:
  // ___________________________________________________________________________
  WordWriterThrowing()
      : caller_{[]() { throw std::runtime_error("Constructor failed"); }} {}
  uint64_t operator()(std::string_view, bool) override { return 0; }
  void finishImpl() override {}
};

// _____________________________________________________________________________
// A class inheriting from `WordWriterBase` that doesn't call finish.
class WordWriterNoFinish : public WordWriterBase {
 public:
  WordWriterNoFinish() {}
  uint64_t operator()(std::string_view, bool) override { return 0; }
  void finishImpl() override {}
};
}  // namespace

// _____________________________________________________________________________
TEST(VocabularyTypes, verifyWordWriterBaseDestructorBehavesAsExpected) {
  // Test that the original exception from `WordWriterThrowing` is propagated.
  AD_EXPECT_THROW_WITH_MESSAGE_AND_TYPE(WordWriterThrowing{},
                                        ::testing::StrEq("Constructor failed"),
                                        std::runtime_error);

  // Test that the no finish exception is thrown when destroying a
  // `WordWriterNoFinish`.
  AD_EXPECT_THROW_WITH_MESSAGE_AND_TYPE(
      WordWriterNoFinish{}, ::testing::HasSubstr("WordWriterBase::finish was"),
      std::runtime_error);

  // Test that no exception is thrown when `finish` is called.
  EXPECT_NO_THROW({
    WordWriterNoFinish writer;
    writer.finish();
  });
}

// _____________________________________________________________________________

TEST(VocabBatchLookupData, ContiguousBuilderExposesViewsAndKeepsDataAlive) {
  const std::array<size_t, 2> sizes{3, 3};
  ContiguousVocabBatchBuilder builder(sizes);
  auto targets = builder.targets();
  ASSERT_EQ(targets.size(), 2u);
  std::memcpy(targets[0], "foo", 3);
  std::memcpy(targets[1], "bar", 3);

  VocabBatchLookupResult result = std::move(builder).finalize();
  EXPECT_THAT(result, ::testing::ElementsAre("foo", "bar"));
  EXPECT_EQ(result[0].data(), targets[0]);
  EXPECT_EQ(result[1].data(), targets[1]);
}

// _____________________________________________________________________________

TEST(VocabBatchLookupData, ContiguousBuilderEmpty) {
  AD_EXPECT_THROW_WITH_MESSAGE(ContiguousVocabBatchBuilder({}),
                               ::testing::HasSubstr("!wordSizes.empty()"));
}

// _____________________________________________________________________________

TEST(VocabBatchLookupData, ContiguousBuilderZeroSizedWordsAndMixed) {
  const std::array<size_t, 4> sizes{0, 3, 0, 4};
  ContiguousVocabBatchBuilder builder(sizes);
  auto targets = builder.targets();
  ASSERT_EQ(targets.size(), 4u);
  for (char* target : targets) {
    EXPECT_NE(target, nullptr);
  }
  std::memcpy(targets[1], "cat", 3);
  std::memcpy(targets[3], "bird", 4);

  VocabBatchLookupResult result = std::move(builder).finalize();
  EXPECT_THAT(result, ::testing::ElementsAre("", "cat", "", "bird"));
  for (size_t i = 0; i < result.size(); ++i) {
    EXPECT_NE(result[i].data(), nullptr);
  }
}

// _____________________________________________________________________________

TEST(VocabBatchLookupData, MakeStringVectorResultKeepsViewsValid) {
  auto result = makeStringVectorVocabBatchLookupResult({"alpha", "beta"});

  EXPECT_THAT(result, ::testing::ElementsAre("alpha", "beta"));
}

// _____________________________________________________________________________
// Moves transfer ownership and leave the source empty, never a null owner
// paired with a stale view into the moved-to storage.
TEST(VocabBatchLookupData, MovedFromResultIsEmpty) {
  auto result = makeStringVectorVocabBatchLookupResult({"foo", "bar"});
  ASSERT_EQ(result.size(), 2u);

  auto moved = std::move(result);
  EXPECT_TRUE(result.empty());
  EXPECT_EQ(result.size(), 0u);
  EXPECT_THAT(moved, ::testing::ElementsAre("foo", "bar"));

  auto target = makeStringVectorVocabBatchLookupResult({"x"});
  target = std::move(moved);
  EXPECT_TRUE(moved.empty());
  EXPECT_THAT(target, ::testing::ElementsAre("foo", "bar"));
}

// _____________________________________________________________________________
// Copies share ownership of the frozen storage: both the copy and the
// original observe the same words, and both stay valid.
TEST(VocabBatchLookupData, CopiedResultSharesStorage) {
  auto original = makeStringVectorVocabBatchLookupResult({"foo", "bar"});
  auto copy = original;
  EXPECT_THAT(copy, ::testing::ElementsAre("foo", "bar"));
  EXPECT_THAT(original, ::testing::ElementsAre("foo", "bar"));
  EXPECT_EQ(copy[0].data(), original[0].data());

  auto assigned = makeStringVectorVocabBatchLookupResult({"x"});
  assigned = original;
  EXPECT_THAT(assigned, ::testing::ElementsAre("foo", "bar"));
  EXPECT_EQ(assigned[1].data(), original[1].data());
}

// _____________________________________________________________________________
// An assembler for zero words can never finalize (finalization requires a
// non-empty view list), so construction fails fast like every other factory.
TEST(VocabBatchLookupData, MultiSourceAssemblerRejectsEmptyTotal) {
  AD_EXPECT_THROW_WITH_MESSAGE(MultiSourceVocabBatchAssembler(0),
                               ::testing::HasSubstr("totalExpectedWords > 0"));
}

// _____________________________________________________________________________
TEST(VocabBatchLookupData, ScatterBatchResultRetainsOwner) {
  auto first = makeStringVectorVocabBatchLookupResult({"apple", "banana"});
  auto second = makeStringVectorVocabBatchLookupResult({"cherry"});

  MultiSourceVocabBatchAssembler assembler(3);
  const std::array<size_t, 2> firstPos{0, 2};
  const std::array<size_t, 1> secondPos{1};
  assembler.scatterSubBatchResultAtPositions(std::move(first), firstPos);
  assembler.scatterSubBatchResultAtPositions(std::move(second), secondPos);

  auto result = std::move(assembler).finalizeVocabBatchLookupResult();
  EXPECT_THAT(result, ::testing::ElementsAre("apple", "cherry", "banana"));
}

// _____________________________________________________________________________
TEST(VocabBatchLookupData, MultiSourceAssemblerDoesNotCopyBytes) {
  auto first = makeStringVectorVocabBatchLookupResult({"alpha", "beta"});
  auto second = makeStringVectorVocabBatchLookupResult({"gamma"});

  const char* alphaData = first[0].data();
  const char* gammaData = second[0].data();

  MultiSourceVocabBatchAssembler assembler(3);
  const std::array<size_t, 2> firstPos{0, 2};
  const std::array<size_t, 1> secondPos{1};
  assembler.scatterSubBatchResultAtPositions(std::move(first), firstPos);
  assembler.scatterSubBatchResultAtPositions(std::move(second), secondPos);

  auto result = std::move(assembler).finalizeVocabBatchLookupResult();
  EXPECT_THAT(result, ::testing::ElementsAre("alpha", "gamma", "beta"));
  EXPECT_EQ(result[0].data(), alphaData);
  EXPECT_EQ(result[1].data(), gammaData);
}

// _____________________________________________________________________________
// Fixture for the "batch result outlives its vocabulary" tests: provides a
// one-word `VocabularyInMemoryBinSearch` built via a `WordWriter`, with
// per-test filenames so the suites are independent.
class VocabBatchLookupDataVocabTest : public ::testing::Test {
 protected:
  // Build a vocabulary containing exactly `word` at index 0 and open it.
  VocabularyInMemoryBinSearch buildVocab(std::string_view word) {
    const std::string filename = gtestCurrentTestName();
    ad_utility::deleteFile(filename, false);
    ad_utility::deleteFile(filename + ".ids", false);
    VocabularyInMemoryBinSearch vocabulary;
    {
      VocabularyInMemoryBinSearch::WordWriter writer{filename};
      writer(word, 0);
      writer.finish();
    }
    vocabulary.open(filename);
    return vocabulary;
  }
};

// _____________________________________________________________________________
// A batch result obtained from `VocabularyInMemoryBinSearch` stays valid when
// the vocabulary is `close()`d afterwards: the result owns the bytes, and
// `close()` only installs a fresh empty buffer instead of mutating the old one.
TEST_F(VocabBatchLookupDataVocabTest, MultiSourceAssemblerOutlivesClose) {
  auto vocabulary = buildVocab("ram-word");
  const std::array<size_t, 1> positions{0};
  auto batch = vocabulary.lookupBatch(positions);
  const char* wordData = batch[0].data();
  MultiSourceVocabBatchAssembler assembler(1);
  assembler.scatterSubBatchResultAtPositions(std::move(batch), positions);
  auto result = std::move(assembler).finalizeVocabBatchLookupResult();

  vocabulary.close();
  EXPECT_EQ(vocabulary.size(), 0u);
  EXPECT_THAT(result, ::testing::ElementsAre("ram-word"));
  EXPECT_EQ(result[0].data(), wordData);
}

// _____________________________________________________________________________
// Same guarantee when the vocabulary object is destroyed entirely while the
// batch result still lives: shared ownership of the word storage keeps the
// bytes alive past the destructor.
TEST_F(VocabBatchLookupDataVocabTest,
       MultiSourceAssemblerOutlivesVocabularyDestruction) {
  auto vocabulary = std::make_optional(buildVocab("other-word"));
  const std::array<size_t, 1> positions{0};
  auto batch = vocabulary->lookupBatch(positions);
  const char* wordData = batch[0].data();
  MultiSourceVocabBatchAssembler assembler(1);
  assembler.scatterSubBatchResultAtPositions(std::move(batch), positions);
  auto result = std::move(assembler).finalizeVocabBatchLookupResult();

  vocabulary.reset();
  EXPECT_THAT(result, ::testing::ElementsAre("other-word"));
  EXPECT_EQ(result[0].data(), wordData);
}

// _____________________________________________________________________________
// Verify that `ArenaVocabBatchBuilder` supports incremental word appends:
// each word is copied into the arena-backed storage in order, and the
// finalized batch result exposes all appended words with their contents
// intact.
TEST(PmrVocabBatchLookupData, IncrementalAppendsProduceWordsInOrder) {
  ArenaVocabBatchBuilder builder(2);
  builder.appendWord("foo");
  builder.appendWord("barbaz");

  VocabBatchLookupResult result = std::move(builder).finalize();
  EXPECT_THAT(result, ::testing::ElementsAre("foo", "barbaz"));
}

// _____________________________________________________________________________
TEST(VocabBatchLookupData, ScatterSubBatchSizeMismatchThrows) {
  auto batch = makeStringVectorVocabBatchLookupResult({"only-one"});
  MultiSourceVocabBatchAssembler assembler(2);
  const std::array<size_t, 2> positions{0, 1};
  // Test a mismatch between two result positions and one batch word.
  AD_EXPECT_THROW_WITH_MESSAGE(
      assembler.scatterSubBatchResultAtPositions(std::move(batch), positions),
      ::testing::HasSubstr("subBatchResult.size() == resultPositions.size()"));
}

// _____________________________________________________________________________
TEST(VocabBatchLookupData, ArenaVocabBatchBuilderKeepsViewsAlive) {
  VocabBatchLookupResult result;
  {
    ArenaVocabBatchBuilder builder(2);
    builder.appendWord("one");
    builder.appendWord("two");
    result = std::move(builder).finalize();
  }
  EXPECT_THAT(result, ::testing::ElementsAre("one", "two"));
}

// _____________________________________________________________________________
TEST(PmrVocabBatchLookupData, LimitedAllocatorThrowsWhenArenaExceedsBudget) {
  auto alloc = ad_utility::makeAllocatorWithLimit<Id>(8_B);
  ArenaVocabBatchBuilder builder(1, alloc);
  EXPECT_THROW(
      builder.appendWord("this string is definitely more than eight bytes"),
      ad_utility::detail::AllocationExceedsLimitException);
}

// _____________________________________________________________________________
TEST(VocabBatchLookupData, MakePmrVocabBatchLookupResultCopiesWords) {
  auto result = makePmrVocabBatchLookupResult({"first", "second"});
  EXPECT_THAT(result, ::testing::ElementsAre("first", "second"));
}

// _____________________________________________________________________________
TEST(VocabBatchLookupData, ScatterSubBatchDoubleWriteThrows) {
  auto batch1 = makeStringVectorVocabBatchLookupResult({"first"});
  auto batch2 = makeStringVectorVocabBatchLookupResult({"second"});
  MultiSourceVocabBatchAssembler assembler(2);
  const std::array<size_t, 1> pos0{0};
  assembler.scatterSubBatchResultAtPositions(std::move(batch1), pos0);
  AD_EXPECT_THROW_WITH_MESSAGE(
      assembler.scatterSubBatchResultAtPositions(std::move(batch2), pos0),
      ::testing::HasSubstr("!slotFilledTracking_[resultPosition]"));
}

// _____________________________________________________________________________
// Verify that a legitimately empty word does not trip any correctness check:
// the filled/unfilled invariant is structural, not based on the view contents.
TEST(VocabBatchLookupData, MultiSourceVocabBatchAssemblerToleratesEmptyWord) {
  auto batch = makeStringVectorVocabBatchLookupResult({"", "x"});
  MultiSourceVocabBatchAssembler assembler(2);
  const std::array<size_t, 2> positions{1, 0};
  assembler.scatterSubBatchResultAtPositions(std::move(batch), positions);

  auto result = std::move(assembler).finalizeVocabBatchLookupResult();
  EXPECT_THAT(result, ::testing::ElementsAre("x", ""));
}

// _____________________________________________________________________________
TEST(VocabBatchLookupData, MultiSourceVocabBatchAssemblerSuccessfulAssembly) {
  MultiSourceVocabBatchAssembler assembler(3);
  auto middleBatch = makeStringVectorVocabBatchLookupResult({"middle"});
  const std::array<size_t, 1> middlePosition{1};
  assembler.scatterSubBatchResultAtPositions(std::move(middleBatch),
                                             middlePosition);
  auto subBatch = makeStringVectorVocabBatchLookupResult({"first", "last"});
  const std::array<size_t, 2> subPositions{0, 2};
  assembler.scatterSubBatchResultAtPositions(std::move(subBatch), subPositions);

  auto result = std::move(assembler).finalizeVocabBatchLookupResult();
  ASSERT_FALSE(result.empty());
  EXPECT_THAT(result, ::testing::ElementsAre("first", "middle", "last"));
}

// _____________________________________________________________________________
TEST(VocabBatchLookupData,
     MultiSourceVocabBatchAssemblerDoubleAssignmentThrows) {
  MultiSourceVocabBatchAssembler assembler(2);
  auto firstBatch = makeStringVectorVocabBatchLookupResult({"first"});
  const std::array<size_t, 1> position{0};
  assembler.scatterSubBatchResultAtPositions(std::move(firstBatch), position);

  auto overwriteBatch = makeStringVectorVocabBatchLookupResult({"overwrite"});
  AD_EXPECT_THROW_WITH_MESSAGE(
      assembler.scatterSubBatchResultAtPositions(std::move(overwriteBatch),
                                                 position),
      ::testing::HasSubstr("!slotFilledTracking_[resultPosition]"));
}

// _____________________________________________________________________________
TEST(VocabBatchLookupData,
     MultiSourceVocabBatchAssemblerIncompleteCoverageThrows) {
  MultiSourceVocabBatchAssembler assembler(2);
  auto subBatch = makeStringVectorVocabBatchLookupResult({"first"});
  const std::array<size_t, 1> subPositions{0};
  assembler.scatterSubBatchResultAtPositions(std::move(subBatch), subPositions);
  // Leave slot 1 unassigned.

  AD_EXPECT_THROW_WITH_MESSAGE(
      (void)std::move(assembler).finalizeVocabBatchLookupResult(),
      ::testing::HasSubstr("ql::ranges::all_of("));
}

// _____________________________________________________________________________
TEST(VocabBatchLookupData,
     MultiSourceVocabBatchAssemblerOutOfBoundsPositionThrows) {
  MultiSourceVocabBatchAssembler assembler(2);
  auto outOfBoundsBatch =
      makeStringVectorVocabBatchLookupResult({"out-of-bounds"});
  const std::array<size_t, 1> invalidPos{2};
  AD_EXPECT_THROW_WITH_MESSAGE(
      assembler.scatterSubBatchResultAtPositions(std::move(outOfBoundsBatch),
                                                 invalidPos),
      ::testing::HasSubstr("resultPosition < assembledWordViews_.size()"));

  auto subBatch = makeStringVectorVocabBatchLookupResult({"out-of-bounds"});
  const std::array<size_t, 1> otherInvalidPos{5};
  AD_EXPECT_THROW_WITH_MESSAGE(
      assembler.scatterSubBatchResultAtPositions(std::move(subBatch),
                                                 otherInvalidPos),
      ::testing::HasSubstr("resultPosition < assembledWordViews_.size()"));
}

// _____________________________________________________________________________
TEST(VocabBatchLookupData, MarkerBatchLookupsAndMergeInInputOrder) {
  MarkerBatchLookups<2> lookups;
  lookups[0] = makeStringVectorVocabBatchLookupResult({"apple", "cherry"});
  lookups[1] = makeStringVectorVocabBatchLookupResult({"banana"});

  IndicesAndPositionsByMarker<2> partitions;
  partitions[0].addPair(0, 0);  // apple -> pos 0
  partitions[1].addPair(0, 1);  // banana -> pos 1
  partitions[0].addPair(1, 2);  // cherry -> pos 2

  auto result = mergeMarkerBatchesInInputOrder(std::move(lookups), partitions);
  EXPECT_THAT(result, ::testing::ElementsAre("apple", "banana", "cherry"));
}

namespace {
// A minimal vocabulary with "holes": its `operator[]` returns `std::nullopt`
// for odd indices. It does not opt in to the placeholder mechanism (see
// `replaceOptionalByPlaceholderOnExport` in `VocabularyTypes.h`).
struct VocabWithHolesThrowing {
  std::optional<std::string_view> operator[](uint64_t index) const {
    if (index % 2 == 1) {
      return std::nullopt;
    }
    return "word";
  }
};

// The same vocabulary, but opting in to the placeholder mechanism.
struct VocabWithHolesPlaceholder : VocabWithHolesThrowing {
  static constexpr bool replaceOptionalByPlaceholderOnExport = true;
};

// A vocabulary without holes, for which the placeholder mechanism is
// irrelevant, because its `operator[]` doesn't return a `std::optional`.
struct VocabWithoutHoles {
  std::string_view operator[]([[maybe_unused]] uint64_t index) const {
    return "word";
  }
};
}  // namespace

// _____________________________________________________________________________
TEST(VocabularyTypes, replaceOptionalByPlaceholderOnExportIsOptIn) {
  using namespace ad_utility::vocabulary;
  // Only a vocabulary that explicitly declares the member opts in.
  static_assert(!replaceOptionalByPlaceholderOnExport<VocabWithHolesThrowing>);
  static_assert(
      replaceOptionalByPlaceholderOnExport<VocabWithHolesPlaceholder>);
  static_assert(!replaceOptionalByPlaceholderOnExport<VocabWithoutHoles>);
}

// _____________________________________________________________________________
TEST(VocabularyTypes, wordAsStringOrPlaceholder) {
  using namespace ad_utility::vocabulary;
  // Words that are contained are returned as they are, no matter whether the
  // `operator[]` returns a `std::optional`.
  EXPECT_EQ(wordAsStringOrPlaceholder(VocabWithHolesThrowing{}, 4), "word");
  EXPECT_EQ(wordAsStringOrPlaceholder(VocabWithHolesPlaceholder{}, 4), "word");
  EXPECT_EQ(wordAsStringOrPlaceholder(VocabWithoutHoles{}, 5), "word");

  // A missing word is reported as a placeholder only by the vocabulary that has
  // opted in, the other one throws.
  EXPECT_EQ(wordAsStringOrPlaceholder(VocabWithHolesPlaceholder{}, 5),
            placeholderForMissingVocabIndex(5));
  AD_EXPECT_THROW_WITH_MESSAGE(
      wordAsStringOrPlaceholder(VocabWithHolesThrowing{}, 5),
      ::testing::HasSubstr("replaceOptionalByPlaceholderOnExport"));
}

// _____________________________________________________________________________
TEST(VocabularyTypes, sequentialLookupBatchWithMissingWords) {
  using namespace ad_utility::vocabulary;
  std::vector<size_t> indices{4, 5};

  // The opted-in vocabulary reports the placeholder for the missing word.
  auto result = sequentialLookupBatch(VocabWithHolesPlaceholder{}, indices);
  ASSERT_EQ(result.size(), 2u);
  EXPECT_EQ(result[0], "word");
  EXPECT_EQ(result[1], placeholderForMissingVocabIndex(5));

  // The vocabulary that has not opted in throws.
  AD_EXPECT_THROW_WITH_MESSAGE(
      sequentialLookupBatch(VocabWithHolesThrowing{}, indices),
      ::testing::HasSubstr("replaceOptionalByPlaceholderOnExport"));
}
