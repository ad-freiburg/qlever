// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>, UFR
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <absl/cleanup/cleanup.h>
#include <absl/functional/function_ref.h>
#include <absl/strings/str_cat.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <array>
#include <cstring>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "../../util/DanglingViewTestHelpers.h"
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
  auto result = StringVectorVocabBatchLookupData::fromWords({"alpha", "beta"});

  EXPECT_THAT(result, ::testing::ElementsAre("alpha", "beta"));
}

// _____________________________________________________________________________
// Moves transfer ownership and leave the source empty, never a null owner
// paired with a stale view into the moved-to storage.
TEST(VocabBatchLookupData, MovedFromResultIsEmpty) {
  auto result = StringVectorVocabBatchLookupData::fromWords({"foo", "bar"});
  ASSERT_EQ(result.size(), 2u);

  auto moved = std::move(result);
  EXPECT_TRUE(result.empty());
  EXPECT_EQ(result.size(), 0u);
  EXPECT_THAT(moved, ::testing::ElementsAre("foo", "bar"));

  auto target = StringVectorVocabBatchLookupData::fromWords({"x"});
  target = std::move(moved);
  EXPECT_TRUE(moved.empty());
  EXPECT_THAT(target, ::testing::ElementsAre("foo", "bar"));
}

// _____________________________________________________________________________
// Copies share ownership of the frozen storage: both the copy and the
// original observe the same words, and both stay valid.
TEST(VocabBatchLookupData, CopiedResultSharesStorage) {
  auto original = StringVectorVocabBatchLookupData::fromWords({"foo", "bar"});
  auto copy = original;
  EXPECT_THAT(copy, ::testing::ElementsAre("foo", "bar"));
  EXPECT_THAT(original, ::testing::ElementsAre("foo", "bar"));
  EXPECT_EQ(copy[0].data(), original[0].data());

  auto assigned = StringVectorVocabBatchLookupData::fromWords({"x"});
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
  auto first = StringVectorVocabBatchLookupData::fromWords({"apple", "banana"});
  auto second = StringVectorVocabBatchLookupData::fromWords({"cherry"});

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
  auto first = StringVectorVocabBatchLookupData::fromWords({"alpha", "beta"});
  auto second = StringVectorVocabBatchLookupData::fromWords({"gamma"});

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
  // `suffix` distinguishes several vocabularies within one test.
  VocabularyInMemoryBinSearch buildVocab(std::string_view word,
                                         std::string_view suffix = "") {
    const std::string filename = absl::StrCat(gtestCurrentTestName(), suffix);
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
  auto batch = StringVectorVocabBatchLookupData::fromWords({"only-one"});
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
  auto batch1 = StringVectorVocabBatchLookupData::fromWords({"first"});
  auto batch2 = StringVectorVocabBatchLookupData::fromWords({"second"});
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
  auto batch = StringVectorVocabBatchLookupData::fromWords({"", "x"});
  MultiSourceVocabBatchAssembler assembler(2);
  const std::array<size_t, 2> positions{1, 0};
  assembler.scatterSubBatchResultAtPositions(std::move(batch), positions);

  auto result = std::move(assembler).finalizeVocabBatchLookupResult();
  EXPECT_THAT(result, ::testing::ElementsAre("x", ""));
}

// _____________________________________________________________________________
TEST(VocabBatchLookupData, MultiSourceVocabBatchAssemblerSuccessfulAssembly) {
  MultiSourceVocabBatchAssembler assembler(3);
  auto middleBatch = StringVectorVocabBatchLookupData::fromWords({"middle"});
  const std::array<size_t, 1> middlePosition{1};
  assembler.scatterSubBatchResultAtPositions(std::move(middleBatch),
                                             middlePosition);
  auto subBatch =
      StringVectorVocabBatchLookupData::fromWords({"first", "last"});
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
  auto firstBatch = StringVectorVocabBatchLookupData::fromWords({"first"});
  const std::array<size_t, 1> position{0};
  assembler.scatterSubBatchResultAtPositions(std::move(firstBatch), position);

  auto overwriteBatch =
      StringVectorVocabBatchLookupData::fromWords({"overwrite"});
  AD_EXPECT_THROW_WITH_MESSAGE(
      assembler.scatterSubBatchResultAtPositions(std::move(overwriteBatch),
                                                 position),
      ::testing::HasSubstr("!slotFilledTracking_[resultPosition]"));
}

// _____________________________________________________________________________
TEST(VocabBatchLookupData,
     MultiSourceVocabBatchAssemblerIncompleteCoverageThrows) {
  MultiSourceVocabBatchAssembler assembler(2);
  auto subBatch = StringVectorVocabBatchLookupData::fromWords({"first"});
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
      StringVectorVocabBatchLookupData::fromWords({"out-of-bounds"});
  const std::array<size_t, 1> invalidPos{2};
  AD_EXPECT_THROW_WITH_MESSAGE(
      assembler.scatterSubBatchResultAtPositions(std::move(outOfBoundsBatch),
                                                 invalidPos),
      ::testing::HasSubstr("resultPosition < assembledWordViews_.size()"));

  auto subBatch =
      StringVectorVocabBatchLookupData::fromWords({"out-of-bounds"});
  const std::array<size_t, 1> otherInvalidPos{5};
  AD_EXPECT_THROW_WITH_MESSAGE(
      assembler.scatterSubBatchResultAtPositions(std::move(subBatch),
                                                 otherInvalidPos),
      ::testing::HasSubstr("resultPosition < assembledWordViews_.size()"));
}

// _____________________________________________________________________________
TEST(VocabBatchLookupData, MarkerBatchLookupsAndMergeInInputOrder) {
  MarkerBatchLookups<2> lookups;
  lookups[0] = StringVectorVocabBatchLookupData::fromWords({"apple", "cherry"});
  lookups[1] = StringVectorVocabBatchLookupData::fromWords({"banana"});

  IndicesAndPositionsByMarker<2> partitions;
  partitions[0].addPair(0, 0);  // apple -> pos 0
  partitions[1].addPair(0, 1);  // banana -> pos 1
  partitions[0].addPair(1, 2);  // cherry -> pos 2

  auto result = mergeMarkerBatchesInInputOrder(std::move(lookups), partitions);
  EXPECT_THAT(result, ::testing::ElementsAre("apple", "banana", "cherry"));
}

// _____________________________________________________________________________
// Words that live in memory owned by a vocabulary can be placed without any
// owner and without copying: the result points straight into that memory.
TEST(VocabBatchLookupData, AssemblerUnownedViewsPointIntoTheirStorage) {
  // Stands in for the in-memory words of a vocabulary that outlives the
  // result. The words are longer than any SSO buffer, so their bytes live on
  // the heap and are not moved by anything below.
  const std::vector<std::string> ramWords{"ram-word-number-zero-is-long",
                                          "ram-word-number-one-is-long"};
  MultiSourceVocabBatchAssembler assembler(3);
  assembler.assignUnownedViewAtPosition(2, ramWords[0]);
  assembler.assignUnownedViewAtPosition(0, ramWords[1]);
  assembler.assignUnownedViewAtPosition(1, ramWords[0]);
  // No owner at all is needed when every word is un-owned.
  auto result = std::move(assembler).finalizeVocabBatchLookupResult();
  EXPECT_THAT(result,
              ::testing::ElementsAre(ramWords[1], ramWords[0], ramWords[0]));
  EXPECT_EQ(result[0].data(), ramWords[1].data());
  EXPECT_EQ(result[1].data(), ramWords[0].data());
  EXPECT_EQ(result[2].data(), ramWords[0].data());
}

// _____________________________________________________________________________
// Mixed batch: un-owned views into long-lived storage plus a received
// sub-batch whose storage the result keeps alive after every other handle to
// it is gone (the "aliasing" case: the received batch is retained and only its
// views are re-arranged).
TEST(VocabBatchLookupData, AssemblerMixedOwnedAndUnownedViews) {
  const std::vector<std::string> ramWords{"internal-word-zero-is-long",
                                          "internal-word-one-is-long"};
  VocabBatchLookupResult result;
  const char* externalDataA = nullptr;
  const char* externalDataB = nullptr;
  {
    MultiSourceVocabBatchAssembler assembler(4);
    assembler.assignUnownedViewAtPosition(0, ramWords[0]);
    assembler.assignUnownedViewAtPosition(3, ramWords[1]);
    // Short words on purpose: with SSO their bytes live inside the string
    // objects of the sub-batch storage, so they dangle as soon as that storage
    // dies unless the assembled result keeps it alive.
    auto external = StringVectorVocabBatchLookupData::fromWords({"ex-a", "b"});
    externalDataA = external[0].data();
    externalDataB = external[1].data();
    const std::array<size_t, 2> positions{2, 1};
    assembler.scatterSubBatchResultAtPositions(std::move(external), positions);
    EXPECT_TRUE(external.empty());
    result = std::move(assembler).finalizeVocabBatchLookupResult();
  }
  // Overwrite the stack region the builder-side locals used, so a view that
  // dangles into a destroyed local reads garbage instead of stale bytes.
  EXPECT_EQ(clobberStack(), '#');
  EXPECT_EQ(clobberStack<16384>('%'), '%');
  EXPECT_THAT(result,
              ::testing::ElementsAre(ramWords[0], "b", "ex-a", ramWords[1]));
  // Nothing was copied: every view points into its original storage.
  EXPECT_EQ(result[0].data(), ramWords[0].data());
  EXPECT_EQ(result[1].data(), externalDataB);
  EXPECT_EQ(result[2].data(), externalDataA);
  EXPECT_EQ(result[3].data(), ramWords[1].data());

  // Copies share the same storage and stay valid after the original is gone.
  auto copy = result;
  result = VocabBatchLookupResult{};
  EXPECT_EQ(clobberStack(), '#');
  EXPECT_THAT(copy,
              ::testing::ElementsAre(ramWords[0], "b", "ex-a", ramWords[1]));
  EXPECT_EQ(copy[2].data(), externalDataA);
}

// _____________________________________________________________________________
// Un-owned and owned placements share the "each position exactly once" and
// "every position filled" checks.
TEST(VocabBatchLookupData, AssemblerUnownedViewContractChecks) {
  const std::string ramWord = "ram";
  {
    MultiSourceVocabBatchAssembler assembler(2);
    assembler.assignUnownedViewAtPosition(0, ramWord);
    AD_EXPECT_THROW_WITH_MESSAGE(
        assembler.assignUnownedViewAtPosition(0, ramWord),
        ::testing::HasSubstr("!slotFilledTracking_[resultPosition]"));
    auto batch = StringVectorVocabBatchLookupData::fromWords({"x"});
    const std::array<size_t, 1> positions{0};
    AD_EXPECT_THROW_WITH_MESSAGE(
        assembler.scatterSubBatchResultAtPositions(std::move(batch), positions),
        ::testing::HasSubstr("!slotFilledTracking_[resultPosition]"));
    AD_EXPECT_THROW_WITH_MESSAGE(
        assembler.assignUnownedViewAtPosition(2, ramWord),
        ::testing::HasSubstr("resultPosition < assembledWordViews_.size()"));
    // Position 1 is still empty.
    AD_EXPECT_THROW_WITH_MESSAGE(
        (void)std::move(assembler).finalizeVocabBatchLookupResult(),
        ::testing::HasSubstr("ql::ranges::all_of("));
  }
  // A legitimately empty un-owned word is a filled position.
  MultiSourceVocabBatchAssembler assembler(1);
  assembler.assignUnownedViewAtPosition(0, std::string_view{});
  EXPECT_THAT(std::move(assembler).finalizeVocabBatchLookupResult(),
              ::testing::ElementsAre(""));
}

// _____________________________________________________________________________
// The pattern of `VocabularyInternalExternal::lookupBatch` with real
// vocabularies: words that the in-memory vocabulary contains are placed as
// un-owned views into its memory, the rest comes as one batch from another
// vocabulary. That other vocabulary may even be destroyed: the result keeps
// the received batch alive.
TEST_F(VocabBatchLookupDataVocabTest, AssemblerMixesInternalAndExternalVocab) {
  auto internal = buildVocab("internal-word-in-ram", "internal");
  auto external = std::make_optional(buildVocab("external-word", "external"));
  auto internalWord = internal[0];
  ASSERT_TRUE(internalWord.has_value());

  MultiSourceVocabBatchAssembler assembler(3);
  assembler.assignUnownedViewAtPosition(1, internalWord.value());
  const std::array<size_t, 2> externalIndices{0, 0};
  const std::array<size_t, 2> externalPositions{2, 0};
  assembler.scatterSubBatchResultAtPositions(
      external->lookupBatch(externalIndices), externalPositions);
  auto result = std::move(assembler).finalizeVocabBatchLookupResult();

  external.reset();
  EXPECT_EQ(clobberStack(), '#');
  EXPECT_THAT(result,
              ::testing::ElementsAre("external-word", "internal-word-in-ram",
                                     "external-word"));
  // The internal word was not copied.
  EXPECT_EQ(result[1].data(), internalWord.value().data());
}

// _____________________________________________________________________________
// The arena charges its bytes to the query's `AllocatorWithLimit` and gives
// them back exactly when the last copy of the result dies.
TEST(PmrVocabBatchLookupData, ArenaChargesAndReleasesAllocatorBudget) {
  auto alloc = ad_utility::makeAllocatorWithLimit<Id>(1_MB);
  const size_t before = alloc.amountMemoryLeft().getBytes();
  VocabBatchLookupResult result;
  {
    ArenaVocabBatchBuilder builder(2, alloc);
    builder.appendWord(std::string(100, 'a'));
    builder.appendWord(std::string(200, 'b'));
    result = std::move(builder).finalize();
  }
  // At least the 300 word bytes are charged while the result lives.
  EXPECT_LE(alloc.amountMemoryLeft().getBytes() + 300, before);
  auto copy = result;
  result = VocabBatchLookupResult{};
  EXPECT_LT(alloc.amountMemoryLeft().getBytes(), before);
  EXPECT_EQ(copy[1], std::string(200, 'b'));
  copy = VocabBatchLookupResult{};
  EXPECT_EQ(alloc.amountMemoryLeft().getBytes(), before);
}

namespace {
// A memory resource that counts the allocations it forwards upstream.
class CountingMemoryResource : public ql::pmr::memory_resource {
  ql::pmr::memory_resource* upstream_ = ql::pmr::new_delete_resource();

 public:
  size_t numAllocations_ = 0;

 private:
  void* do_allocate(size_t bytes, size_t alignment) override {
    ++numAllocations_;
    return upstream_->allocate(bytes, alignment);
  }
  void do_deallocate(void* p, size_t bytes, size_t alignment) override {
    upstream_->deallocate(p, bytes, alignment);
  }
  bool do_is_equal(
      const ql::pmr::memory_resource& other) const noexcept override {
    return this == &other;
  }
};
}  // namespace

// _____________________________________________________________________________
// The arena does not allocate per word: many words share a few geometrically
// growing chunks.
TEST(PmrVocabBatchLookupData, ArenaDoesNotAllocatePerWord) {
  CountingMemoryResource counter;
  auto* previous = ql::pmr::set_default_resource(&counter);
  absl::Cleanup restore = [previous] {
    ql::pmr::set_default_resource(previous);
  };
  constexpr size_t numWords = 10'000;
  const std::string word(40, 'w');
  ArenaVocabBatchBuilder builder(numWords);
  for (size_t i = 0; i < numWords; ++i) {
    builder.appendWord(word);
  }
  auto result = std::move(builder).finalize();
  ASSERT_EQ(result.size(), numWords);
  EXPECT_EQ(result[numWords - 1], word);
  // 400 kB in geometrically growing chunks; one allocation per word would be
  // 10'000.
  EXPECT_LT(counter.numAllocations_, 64u);
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
