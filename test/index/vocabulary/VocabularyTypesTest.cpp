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
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "../../util/GTestHelpers.h"
#include "./VocabularyTestHelpers.h"
#include "backports/memory_resource.h"
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

TEST(VocabBatchLookupResult, ContiguousBuilderResultOutlivesBuilder) {
  const std::array<size_t, 2> sizes{3, 3};
  VocabBatchLookupResult result;
  std::vector<char*> targets;
  {
    ContiguousVocabBatchBuilder builder(sizes);
    targets = builder.targets();
    ASSERT_EQ(targets.size(), 2u);
    std::memcpy(targets[0], "foo", 3);
    std::memcpy(targets[1], "bar", 3);
    result = std::move(builder).finalize();
  }

  // The views point at exactly the destinations handed out by `targets()`,
  // because `finalize()` moves (and does not copy) the buffer into the result.
  EXPECT_THAT(result, ::testing::ElementsAre("foo", "bar"));
  EXPECT_EQ(result[0].data(), targets[0]);
  EXPECT_EQ(result[1].data(), targets[1]);
}

// _____________________________________________________________________________

TEST(VocabBatchLookupResult, ContiguousBuilderRejectsEmptySizes) {
  AD_EXPECT_THROW_WITH_MESSAGE(ContiguousVocabBatchBuilder({}),
                               ::testing::HasSubstr("!wordSizes.empty()"));
}

// _____________________________________________________________________________

TEST(VocabBatchLookupResult, ContiguousBuilderZeroAndNonZeroSizedWords) {
  const std::array<size_t, 4> sizes{0, 3, 0, 4};
  ContiguousVocabBatchBuilder builder(sizes);
  auto targets = builder.targets();
  ASSERT_EQ(targets.size(), 4u);
  // Even zero-sized words get a valid (non-null) destination address.
  for (char* target : targets) {
    EXPECT_NE(target, nullptr);
  }
  std::memcpy(targets[1], "cat", 3);
  std::memcpy(targets[3], "bird", 4);

  VocabBatchLookupResult result = std::move(builder).finalize();
  EXPECT_THAT(result, ::testing::ElementsAre("", "cat", "", "bird"));
  for (std::string_view word : result) {
    EXPECT_NE(word.data(), nullptr);
  }
}

// _____________________________________________________________________________

TEST(VocabBatchLookupResult, StringVectorFromWordsContainsWordsInOrder) {
  const auto result =
      StringVectorVocabBatchLookupData::fromWords({"alpha", "beta"});

  EXPECT_THAT(result, ::testing::ElementsAre("alpha", "beta"));
}

// _____________________________________________________________________________
// Moves transfer ownership and leave the source empty, never a null owner
// paired with a stale view into the moved-to storage.
TEST(VocabBatchLookupResult, MovedFromResultIsEmpty) {
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
TEST(VocabBatchLookupResult, CopiedResultSharesStorage) {
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
TEST(VocabBatchLookupResult, MultiSourceAssemblerRejectsEmptyTotal) {
  AD_EXPECT_THROW_WITH_MESSAGE(MultiSourceVocabBatchAssembler(0),
                               ::testing::HasSubstr("totalExpectedWords > 0"));
}

// _____________________________________________________________________________
TEST(VocabBatchLookupResult, MultiSourceAssemblerDoesNotCopyBytes) {
  auto first = StringVectorVocabBatchLookupData::fromWords({"alpha", "beta"});
  auto second = StringVectorVocabBatchLookupData::fromWords({"gamma"});

  const char* alphaData = first[0].data();
  const char* betaData = first[1].data();
  const char* gammaData = second[0].data();

  MultiSourceVocabBatchAssembler assembler(3);
  const std::array<size_t, 2> firstPositions{0, 2};
  const std::array<size_t, 1> secondPositions{1};
  assembler.scatterSubBatchResultAtPositions(std::move(first), firstPositions);
  assembler.scatterSubBatchResultAtPositions(std::move(second),
                                             secondPositions);

  // The sub-batches were moved into the assembler, so the assembled result is
  // their only remaining owner.
  auto result = std::move(assembler).finalizeVocabBatchLookupResult();
  EXPECT_THAT(result, ::testing::ElementsAre("alpha", "gamma", "beta"));
  EXPECT_EQ(result[0].data(), alphaData);
  EXPECT_EQ(result[1].data(), gammaData);
  EXPECT_EQ(result[2].data(), betaData);
}

// _____________________________________________________________________________
// Fixture for the "batch result outlives its vocabulary" tests: provides a
// one-word `VocabularyInMemoryBinSearch` built via a `WordWriter`, with
// per-test filenames so the suites are independent.
class VocabBatchLookupResultVocabTest : public ::testing::Test {
 private:
  // The files written by `buildVocab`, removed again in `TearDown`.
  std::vector<std::string> filenames_;

 protected:
  void TearDown() override {
    for (const std::string& filename : filenames_) {
      vocabulary_test::deleteVocabularyFiles<VocabularyInMemoryBinSearch>(
          filename);
    }
  }

  // Build a vocabulary containing exactly `word` at index 0, open it, and
  // return it. `suffix` distinguishes several vocabularies within one test.
  VocabularyInMemoryBinSearch buildVocab(std::string_view word,
                                         std::string_view suffix = "") {
    const std::string filename = absl::StrCat(gtestCurrentTestName(), suffix);
    filenames_.push_back(filename);
    vocabulary_test::deleteVocabularyFiles<VocabularyInMemoryBinSearch>(
        filename);
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
// the vocabulary is `close()`d afterwards: `lookupBatch` copies the words into
// the result, so the result owns its bytes independently of the vocabulary.
TEST_F(VocabBatchLookupResultVocabTest, MultiSourceAssemblerOutlivesClose) {
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
// batch result still lives: the result owns its own copy of the bytes.
TEST_F(VocabBatchLookupResultVocabTest,
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
TEST(PmrVocabBatchLookupData, IncrementalAppendsProduceWordsInOrder) {
  ArenaVocabBatchBuilder builder(2);
  builder.appendWord("foo");
  builder.appendWord("barbaz");

  VocabBatchLookupResult result = std::move(builder).finalize();
  EXPECT_THAT(result, ::testing::ElementsAre("foo", "barbaz"));
}

// _____________________________________________________________________________
TEST(VocabBatchLookupResult, ScatterSubBatchSizeMismatchThrows) {
  auto batch = StringVectorVocabBatchLookupData::fromWords({"only-one"});
  MultiSourceVocabBatchAssembler assembler(2);
  const std::array<size_t, 2> positions{0, 1};
  // Test a mismatch between two result positions and one batch word.
  AD_EXPECT_THROW_WITH_MESSAGE(
      assembler.scatterSubBatchResultAtPositions(std::move(batch), positions),
      ::testing::HasSubstr("subBatchResult.size() == resultPositions.size()"));
}

// _____________________________________________________________________________
// The finalized result owns the arena, so it stays valid after the builder is
// destroyed.
TEST(VocabBatchLookupResult, ArenaResultOutlivesBuilder) {
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
  AD_EXPECT_THROW_WITH_MESSAGE_AND_TYPE(
      builder.appendWord("this string is definitely more than eight bytes"),
      ::testing::HasSubstr("Tried to allocate"),
      ad_utility::detail::AllocationExceedsLimitException);
}

// _____________________________________________________________________________
TEST(VocabBatchLookupResult, MakePmrVocabBatchLookupResultCopiesWords) {
  const std::string first = "first-word-that-is-longer-than-any-sso-buffer";
  const std::string second = "second-word-that-is-longer-than-any-sso-buffer";
  const std::array<std::string_view, 2> words{first, second};
  const auto result = makePmrVocabBatchLookupResult(words);
  EXPECT_THAT(result, ::testing::ElementsAre(first, second));
  // The words were copied into the arena, not aliased.
  EXPECT_NE(result[0].data(), first.data());
  EXPECT_NE(result[1].data(), second.data());
}

// _____________________________________________________________________________
TEST(VocabBatchLookupResult, ScatterSubBatchDoubleWriteThrows) {
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
TEST(VocabBatchLookupResult, MultiSourceVocabBatchAssemblerToleratesEmptyWord) {
  auto batch = StringVectorVocabBatchLookupData::fromWords({"", "x"});
  MultiSourceVocabBatchAssembler assembler(2);
  const std::array<size_t, 2> positions{1, 0};
  assembler.scatterSubBatchResultAtPositions(std::move(batch), positions);

  auto result = std::move(assembler).finalizeVocabBatchLookupResult();
  EXPECT_THAT(result, ::testing::ElementsAre("x", ""));
}

// _____________________________________________________________________________
TEST(VocabBatchLookupResult, MultiSourceVocabBatchAssemblerSuccessfulAssembly) {
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
TEST(VocabBatchLookupResult,
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
TEST(VocabBatchLookupResult,
     MultiSourceVocabBatchAssemblerOutOfBoundsPositionThrows) {
  MultiSourceVocabBatchAssembler assembler(2);
  auto outOfBoundsBatch =
      StringVectorVocabBatchLookupData::fromWords({"out-of-bounds"});
  const std::array<size_t, 1> invalidPos{2};
  AD_EXPECT_THROW_WITH_MESSAGE(
      assembler.scatterSubBatchResultAtPositions(std::move(outOfBoundsBatch),
                                                 invalidPos),
      ::testing::HasSubstr("resultPosition < assembledWordViews_.size()"));
}

// _____________________________________________________________________________
TEST(VocabBatchLookupResult, MarkerBatchLookupsAndMergeInInputOrder) {
  MarkerBatchLookups<2> batchesByMarker;
  batchesByMarker[0] =
      StringVectorVocabBatchLookupData::fromWords({"apple", "cherry"});
  batchesByMarker[1] = StringVectorVocabBatchLookupData::fromWords({"banana"});

  IndicesAndPositionsByMarker<2> partitions;
  partitions[0].addPair(0, 0);  // apple -> pos 0
  partitions[1].addPair(0, 1);  // banana -> pos 1
  partitions[0].addPair(1, 2);  // cherry -> pos 2

  auto result =
      mergeMarkerBatchesInInputOrder(std::move(batchesByMarker), partitions);
  EXPECT_THAT(result, ::testing::ElementsAre("apple", "banana", "cherry"));
}

// _____________________________________________________________________________
// Words that live in memory owned by a vocabulary can be placed without any
// owner and without copying: the result points straight into that memory.
TEST(VocabBatchLookupResult, AssemblerUnownedViewsPointIntoTheirStorage) {
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
// Combine un-owned views into long-lived storage with a received sub-batch.
// The assembled result keeps the sub-batch's storage alive after every other
// handle to it is gone, and only the views are re-arranged.
TEST(VocabBatchLookupResult, AssemblerMixesOwnedAndUnownedViews) {
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
  EXPECT_THAT(result,
              ::testing::ElementsAre(ramWords[0], "b", "ex-a", ramWords[1]));
  EXPECT_EQ(result[0].data(), ramWords[0].data());
  EXPECT_EQ(result[1].data(), externalDataB);
  EXPECT_EQ(result[2].data(), externalDataA);
  EXPECT_EQ(result[3].data(), ramWords[1].data());

  // Copies share the same storage and stay valid after the original is gone.
  auto copy = result;
  result = VocabBatchLookupResult{};
  EXPECT_THAT(copy,
              ::testing::ElementsAre(ramWords[0], "b", "ex-a", ramWords[1]));
  EXPECT_EQ(copy[2].data(), externalDataA);
}

// _____________________________________________________________________________
// Un-owned and owned placements share the "each position exactly once" and
// "every position filled" checks.
TEST(VocabBatchLookupResult, AssemblerUnownedViewContractChecks) {
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
// The pattern for a vocabulary that combines an in-memory part with an
// external part: words that the in-memory vocabulary contains are placed as
// un-owned views into its memory, the rest comes as one batch from another
// vocabulary. That other vocabulary may even be destroyed: the result keeps
// the received batch alive.
TEST_F(VocabBatchLookupResultVocabTest,
       AssemblerMixesInternalAndExternalVocab) {
  auto internal = buildVocab("internal-word-in-ram", "internal");
  auto external = std::make_optional(buildVocab("external-word", "external"));
  const auto internalWord = internal[0];
  ASSERT_TRUE(internalWord.has_value());

  MultiSourceVocabBatchAssembler assembler(3);
  assembler.assignUnownedViewAtPosition(1, internalWord.value());
  const std::array<size_t, 2> externalIndices{0, 0};
  const std::array<size_t, 2> externalPositions{2, 0};
  assembler.scatterSubBatchResultAtPositions(
      external->lookupBatch(externalIndices), externalPositions);
  auto result = std::move(assembler).finalizeVocabBatchLookupResult();

  external.reset();
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
  const size_t memoryLeftBefore = alloc.amountMemoryLeft().getBytes();
  constexpr size_t firstWordSize = 100;
  constexpr size_t secondWordSize = 200;
  VocabBatchLookupResult result;
  {
    ArenaVocabBatchBuilder builder(2, alloc);
    builder.appendWord(std::string(firstWordSize, 'a'));
    builder.appendWord(std::string(secondWordSize, 'b'));
    result = std::move(builder).finalize();
  }
  // At least the word bytes are charged while the result lives.
  EXPECT_LE(
      alloc.amountMemoryLeft().getBytes() + firstWordSize + secondWordSize,
      memoryLeftBefore);
  auto copy = result;
  result = VocabBatchLookupResult{};
  EXPECT_LT(alloc.amountMemoryLeft().getBytes(), memoryLeftBefore);
  EXPECT_EQ(copy[1], std::string(secondWordSize, 'b'));
  copy = VocabBatchLookupResult{};
  EXPECT_EQ(alloc.amountMemoryLeft().getBytes(), memoryLeftBefore);
}

namespace {
// A memory resource that counts the allocations it forwards upstream.
class CountingMemoryResource : public ql::pmr::memory_resource {
  ql::pmr::memory_resource& upstream_ = *ql::pmr::new_delete_resource();
  size_t numAllocations_ = 0;

 public:
  size_t numAllocations() const { return numAllocations_; }

 private:
  void* do_allocate(size_t bytes, size_t alignment) override {
    ++numAllocations_;
    return upstream_.allocate(bytes, alignment);
  }
  void do_deallocate(void* p, size_t bytes, size_t alignment) override {
    upstream_.deallocate(p, bytes, alignment);
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
  // The builder takes its upstream from the default resource, so at least one
  // chunk was requested. The 400 kB are served from geometrically growing
  // chunks; one allocation per word would be 10'000.
  EXPECT_GT(counter.numAllocations(), 0u);
  EXPECT_LT(counter.numAllocations(), 64u);
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
  const auto result =
      sequentialLookupBatch(VocabWithHolesPlaceholder{}, indices);
  EXPECT_THAT(result, ::testing::ElementsAre(
                          "word", placeholderForMissingVocabIndex(5)));

  // The vocabulary that has not opted in throws.
  AD_EXPECT_THROW_WITH_MESSAGE(
      sequentialLookupBatch(VocabWithHolesThrowing{}, indices),
      ::testing::HasSubstr("replaceOptionalByPlaceholderOnExport"));
}

// _____________________________________________________________________________
// A vocabulary whose `operator[]` returns a plain view is copied word by word.
TEST(VocabularyTypes, sequentialLookupBatchWithoutHoles) {
  using namespace ad_utility::vocabulary;
  std::vector<size_t> indices{3, 7};
  auto result = sequentialLookupBatch(VocabWithoutHoles{}, indices);
  EXPECT_THAT(result, ::testing::ElementsAre("word", "word"));
  // Each word is an owned copy, not a view into the vocabulary's literal.
  EXPECT_NE(result[0].data(), result[1].data());
}

// _____________________________________________________________________________
// The result's accessors, including the null-owner and self-move edge cases.
TEST(VocabBatchLookupResult, ResultAccessorsAndEdgeCases) {
  VocabBatchLookupResult fromNullOwner{VocabBatchOwner{}};
  EXPECT_TRUE(fromNullOwner.empty());
  EXPECT_EQ(fromNullOwner.size(), 0u);

  auto result = StringVectorVocabBatchLookupData::fromWords({"a", "bc"});
  ASSERT_NE(result.data(), nullptr);
  EXPECT_EQ(result.data()[1], "bc");
  AD_EXPECT_THROW_WITH_MESSAGE((void)result[2],
                               ::testing::HasSubstr("index < span_.size()"));

  // Self-move assignment keeps the result intact.
  auto& alias = result;
  result = std::move(alias);
  EXPECT_THAT(result, ::testing::ElementsAre("a", "bc"));
}

// _____________________________________________________________________________
// The factories and builders reject an empty batch, and the contiguous builder
// rejects word sizes whose sum overflows.
TEST(VocabBatchLookupResult, BuildersRejectEmptyOrOverflowingBatches) {
  AD_EXPECT_THROW_WITH_MESSAGE(StringVectorVocabBatchLookupData::fromWords({}),
                               ::testing::HasSubstr("!words.empty()"));
  AD_EXPECT_THROW_WITH_MESSAGE(
      (void)makePmrVocabBatchLookupResult(ql::span<const std::string_view>{}),
      ::testing::HasSubstr("!words.empty()"));
  AD_EXPECT_THROW_WITH_MESSAGE(ArenaVocabBatchBuilder{0},
                               ::testing::HasSubstr("expectedNumWords > 0"));
  auto alloc = ad_utility::makeAllocatorWithLimit<Id>(1_MB);
  AD_EXPECT_THROW_WITH_MESSAGE((ArenaVocabBatchBuilder{0, alloc}),
                               ::testing::HasSubstr("expectedNumWords > 0"));

  const std::array<size_t, 2> overflowingSizes{SIZE_MAX, 1};
  AD_EXPECT_THROW_WITH_MESSAGE(ContiguousVocabBatchBuilder{overflowingSizes},
                               ::testing::HasSubstr("- totalBytes"));

  // A contiguous builder whose state was moved away has no words left to
  // finalize, while the builder that received the state still can.
  const std::array<size_t, 1> sizes{3};
  ContiguousVocabBatchBuilder movedFromBuilder{sizes};
  ContiguousVocabBatchBuilder movedToBuilder{std::move(movedFromBuilder)};
  // NOLINTNEXTLINE(bugprone-use-after-move)
  AD_EXPECT_THROW_WITH_MESSAGE((void)std::move(movedFromBuilder).finalize(),
                               ::testing::HasSubstr("!views_.empty()"));
  EXPECT_EQ(std::move(movedToBuilder).finalize().size(), 1u);

  // An arena builder to which no word was appended cannot be finalized.
  ArenaVocabBatchBuilder emptyBuilder{1};
  AD_EXPECT_THROW_WITH_MESSAGE((void)std::move(emptyBuilder).finalize(),
                               ::testing::HasSubstr("!views_.empty()"));
}

// _____________________________________________________________________________
// For a batch of only empty words, the contiguous builder still allocates one
// byte so that the empty views point at a valid address. The arena builder
// accepts empty words next to non-empty ones.
TEST(VocabBatchLookupResult, BatchesOfEmptyWords) {
  const std::array<size_t, 2> sizes{0, 0};
  ContiguousVocabBatchBuilder contiguousBuilder{sizes};
  const auto contiguousResult = std::move(contiguousBuilder).finalize();
  EXPECT_THAT(contiguousResult, ::testing::ElementsAre("", ""));
  EXPECT_NE(contiguousResult[0].data(), nullptr);

  ArenaVocabBatchBuilder arenaBuilder{2};
  arenaBuilder.appendWord("");
  arenaBuilder.appendWord("x");
  EXPECT_THAT(std::move(arenaBuilder).finalize(),
              ::testing::ElementsAre("", "x"));
}

// _____________________________________________________________________________
TEST(VocabBatchLookupResult, AllocatorAsMemoryResourceIsEqualOnlyToItself) {
  auto alloc = ad_utility::makeAllocatorWithLimit<std::byte>(1_MB);
  AllocatorAsMemoryResource first{alloc};
  AllocatorAsMemoryResource second{alloc};
  EXPECT_TRUE(first.is_equal(first));
  EXPECT_FALSE(first.is_equal(second));
}

// _____________________________________________________________________________
// `MarkerIndicesAndPositions` keeps its two arrays paired, and merging skips
// markers without indices.
TEST(VocabBatchLookupResult, MarkerIndicesAndPositionsAndEmptyMarker) {
  MarkerIndicesAndPositions pairs;
  pairs.reserve(2);
  EXPECT_TRUE(pairs.empty());
  pairs.addPair(7, 1);
  pairs.addPair(9, 0);
  EXPECT_THAT(pairs.getUnderlyingIndices(), ::testing::ElementsAre(7, 9));
  EXPECT_THAT(pairs.getResultPositions(), ::testing::ElementsAre(1, 0));
  EXPECT_EQ(pairs.size(), 2u);

  MarkerBatchLookups<3> batchesByMarker;
  batchesByMarker[2] = StringVectorVocabBatchLookupData::fromWords({"x", "y"});
  IndicesAndPositionsByMarker<3> partitions;
  partitions[2].addPair(0, 1);
  partitions[2].addPair(1, 0);
  // Markers 0 and 1 have no indices, so their (empty) slots are never read.
  auto result =
      mergeMarkerBatchesInInputOrder(std::move(batchesByMarker), partitions);
  EXPECT_THAT(result, ::testing::ElementsAre("y", "x"));
}

// _____________________________________________________________________________
// `partitionMarkerIndicesAndPositions` groups the indices by marker and pairs
// each of them with its position in the input.
TEST(VocabBatchLookupResult, PartitionMarkerIndicesAndPositions) {
  // The marker is the lowest bit of a marked index, the rest is the index.
  const std::array<size_t, 4> markedIndices{4, 3, 6, 1};
  auto partitions = partitionMarkerIndicesAndPositions<2>(
      markedIndices, [](size_t markedIndex) {
        return std::pair<size_t, size_t>{markedIndex % 2, markedIndex / 2};
      });
  EXPECT_THAT(partitions[0].getUnderlyingIndices(),
              ::testing::ElementsAre(2, 3));
  EXPECT_THAT(partitions[0].getResultPositions(), ::testing::ElementsAre(0, 2));
  EXPECT_THAT(partitions[1].getUnderlyingIndices(),
              ::testing::ElementsAre(1, 0));
  EXPECT_THAT(partitions[1].getResultPositions(), ::testing::ElementsAre(1, 3));

  // A marker that is not smaller than the number of vocabularies is rejected.
  AD_EXPECT_THROW_WITH_MESSAGE((void)partitionMarkerIndicesAndPositions<2>(
                                   markedIndices,
                                   [](size_t markedIndex) {
                                     return std::pair<size_t, size_t>{
                                         markedIndex, 0};
                                   }),
                               ::testing::HasSubstr("marker < NumVocabs"));
}

// _____________________________________________________________________________
// A result can be released from `MarkerBatchLookups` only once, and only for a
// marker that is in range.
TEST(VocabBatchLookupResult, MarkerBatchLookupsReleaseContract) {
  MarkerBatchLookups<2> batchesByMarker;
  batchesByMarker[1] = StringVectorVocabBatchLookupData::fromWords({"x"});
  const auto& constBatchesByMarker = batchesByMarker;
  EXPECT_FALSE(constBatchesByMarker[0].has_value());
  EXPECT_TRUE(constBatchesByMarker[1].has_value());

  EXPECT_THAT(batchesByMarker.release(1), ::testing::ElementsAre("x"));
  AD_EXPECT_THROW_WITH_MESSAGE((void)batchesByMarker.release(1),
                               ::testing::HasSubstr("has_value()"));
  AD_EXPECT_THROW_WITH_MESSAGE((void)batchesByMarker.release(2),
                               ::testing::HasSubstr("marker < NumVocabs"));
  AD_EXPECT_THROW_WITH_MESSAGE((void)constBatchesByMarker[2],
                               ::testing::HasSubstr("marker < NumVocabs"));
}

// _____________________________________________________________________________
TEST(VocabularyTypes, WordAndIndexAccessorsAndEnd) {
  WordAndIndex end = WordAndIndex::end();
  EXPECT_TRUE(end.isEnd());
  EXPECT_EQ(end.indexOrDefault(42), 42u);
  AD_EXPECT_THROW_WITH_MESSAGE((void)end.word(),
                               ::testing::HasSubstr("has_value()"));
  AD_EXPECT_THROW_WITH_MESSAGE((void)end.index(),
                               ::testing::HasSubstr("has_value()"));

  WordAndIndex wordAndIndex{std::string{"word"}, 3};
  EXPECT_FALSE(wordAndIndex.isEnd());
  EXPECT_EQ(wordAndIndex.word(), "word");
  EXPECT_EQ(wordAndIndex.index(), 3u);
  EXPECT_EQ(wordAndIndex.indexOrDefault(42), 3u);
}

// _____________________________________________________________________________
// A decoder may write fewer bytes than its bound, including none at all; the
// arena then holds a (possibly empty) view of exactly the written bytes.
TEST(PmrVocabBatchLookupData, DecompressedWordShorterThanBound) {
  ArenaVocabBatchBuilder builder{2};
  builder.appendDecompressedWord(8, [](ql::span<char>) -> size_t { return 0; });
  builder.appendDecompressedWord(8, [](ql::span<char> destination) -> size_t {
    destination[0] = 'a';
    destination[1] = 'b';
    return 2;
  });
  EXPECT_THAT(std::move(builder).finalize(), ::testing::ElementsAre("", "ab"));
}

// _____________________________________________________________________________
// A bound of zero yields an empty word without invoking the decoder, and a
// decoder that claims to have written more than its bound is rejected.
TEST(PmrVocabBatchLookupData, DecompressedWordBoundZeroAndOverrun) {
  ArenaVocabBatchBuilder builder{1};
  bool decoderWasCalled = false;
  builder.appendDecompressedWord(0, [&decoderWasCalled](ql::span<char>) {
    decoderWasCalled = true;
    return size_t{0};
  });
  EXPECT_FALSE(decoderWasCalled);
  EXPECT_THAT(std::move(builder).finalize(), ::testing::ElementsAre(""));

  ArenaVocabBatchBuilder overrunningBuilder{1};
  AD_EXPECT_THROW_WITH_MESSAGE(
      overrunningBuilder.appendDecompressedWord(
          2, [](ql::span<char>) -> size_t { return 3; }),
      ::testing::HasSubstr("bytesWritten <= bound"));
}

// _____________________________________________________________________________
// Every view of an arena result points into memory that the arena allocated
// from its resource, also for words short enough for the small string
// optimization. A view into a temporary that was destroyed inside the builder
// would fail `contains` instead of reading stale bytes.
TEST(VocabBatchLookupResult, ArenaViewsPointIntoTheResource) {
  vocabulary_test::RecordingMemoryResource resource;
  const std::vector<std::string> words{"a", "bc", "", "def",
                                       std::string(40, 'x')};
  ArenaVocabBatchBuilder builder(words.size(), resource);
  for (const std::string& word : words) {
    builder.appendWord(word);
  }
  const auto result = std::move(builder).finalize();
  ASSERT_EQ(result.size(), words.size());
  EXPECT_GT(resource.numBlocks(), 0u);
  for (size_t i = 0; i < words.size(); ++i) {
    EXPECT_EQ(result[i], words[i]);
    if (!words[i].empty()) {
      EXPECT_TRUE(resource.contains(result[i])) << "word " << i;
    }
  }
}
