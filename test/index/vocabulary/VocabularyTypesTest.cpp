// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>, UFR
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

#include <absl/functional/function_ref.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "../../util/GTestHelpers.h"
#include "index/vocabulary/VocabularyTypes.h"

namespace {
// A class that executes a passed function in its constructor.
class Caller {
 public:
  explicit Caller(absl::FunctionRef<void()> f) { std::invoke(f); }
};

// A class inheriting from `WordWriterBase` that throws when initializing a
// member.
class WordWriterThrowing : public WordWriterBase {
 private:
  Caller caller_;

 public:
  WordWriterThrowing()
      : caller_{[]() { throw std::runtime_error("Constructor failed"); }} {}
  uint64_t operator()(std::string_view, bool) override { return 0; }
  ql::span<const std::string_view> fileSuffixes() const override { return {}; }
  void finishImpl() override {}
};

// A class inheriting from `WordWriterBase` that doesn't call finish.
class WordWriterNoFinish : public WordWriterBase {
 public:
  WordWriterNoFinish() {}
  uint64_t operator()(std::string_view, bool) override { return 0; }
  ql::span<const std::string_view> fileSuffixes() const override { return {}; }
  void finishImpl() override {}
};
}  // namespace

// _____________________________________________________________________________
// A default-constructed `FileSuffixes` is empty, and `add` appends single
// suffixes in the order in which they are added.
TEST(FileSuffixes, defaultConstructedAndAdd) {
  FileSuffixes suffixes;
  EXPECT_THAT(suffixes.asSpan(), ::testing::ElementsAre());
  suffixes.add("");
  suffixes.add(".ids");
  EXPECT_THAT(suffixes.asSpan(), ::testing::ElementsAre("", ".ids"));
}

// _____________________________________________________________________________
// A `FileSuffixes` can also be constructed from a fixed list of suffixes.
TEST(FileSuffixes, constructFromInitializerList) {
  FileSuffixes suffixes{"", ".offsets"};
  EXPECT_THAT(suffixes.asSpan(), ::testing::ElementsAre("", ".offsets"));
}

// _____________________________________________________________________________
// `addPrefixed` prepends the given prefix to each of the added suffixes. This
// is how a `WordWriter` that delegates to other `WordWriter`s composes its own
// suffixes.
TEST(FileSuffixes, addPrefixed) {
  FileSuffixes underlying{"", ".offsets"};
  FileSuffixes suffixes;
  suffixes.addPrefixed(".internal", underlying.asSpan());
  suffixes.addPrefixed(".external", underlying.asSpan());
  suffixes.add(".codebooks");
  EXPECT_THAT(
      suffixes.asSpan(),
      ::testing::ElementsAre(".internal", ".internal.offsets", ".external",
                             ".external.offsets", ".codebooks"));
}

// _____________________________________________________________________________
// `addPrefixed` also works if the added suffixes are the suffixes of the very
// object that is added to (which the implementation has to materialize before
// appending, because appending invalidates them).
TEST(FileSuffixes, addPrefixedFromSelf) {
  FileSuffixes suffixes{"", ".ids"};
  suffixes.addPrefixed(".words", suffixes.asSpan());
  EXPECT_THAT(suffixes.asSpan(),
              ::testing::ElementsAre("", ".ids", ".words", ".words.ids"));
}

// _____________________________________________________________________________
// The copy and move operations of `FileSuffixes` are written manually (because
// the views have to be recomputed), so test that they work as expected.
TEST(FileSuffixes, copyAndMove) {
  FileSuffixes suffixes{"", ".ids"};

  // Copy construction, including the case where the source is modified
  // afterwards (which must not affect the copy).
  FileSuffixes copy{suffixes};
  suffixes.add(".codebooks");
  EXPECT_THAT(copy.asSpan(), ::testing::ElementsAre("", ".ids"));
  EXPECT_THAT(suffixes.asSpan(),
              ::testing::ElementsAre("", ".ids", ".codebooks"));

  // Copy assignment.
  FileSuffixes assigned;
  assigned = copy;
  EXPECT_THAT(assigned.asSpan(), ::testing::ElementsAre("", ".ids"));

  // Move construction leaves the moved-from object in a valid (empty) state.
  FileSuffixes moved{std::move(copy)};
  EXPECT_THAT(moved.asSpan(), ::testing::ElementsAre("", ".ids"));
  EXPECT_THAT(copy.asSpan(),  // NOLINT(bugprone-use-after-move)
              ::testing::ElementsAre());

  // Move assignment.
  FileSuffixes moveAssigned;
  moveAssigned = std::move(moved);
  EXPECT_THAT(moveAssigned.asSpan(), ::testing::ElementsAre("", ".ids"));
}

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

// `asResult` exposes the span over the filled views, and the returned aliasing
// shared_ptr keeps the backing buffer/views alive after the original owning
// shared_ptr is dropped (the whole point of the aliasing shared_ptr).
TEST(VocabBatchLookupData, AsResultExposesViewsAndKeepsDataAlive) {
  auto data = std::make_shared<VocabBatchLookupData>();
  data->buffer() = {'f', 'o', 'o', 'b', 'a', 'r'};
  data->views().emplace_back(data->buffer().data(), 3);      // "foo"
  data->views().emplace_back(data->buffer().data() + 3, 3);  // "bar"

  VocabBatchLookupResult result = VocabBatchLookupData::asResult(data);

  ASSERT_EQ(result->size(), 2u);
  EXPECT_EQ((*result)[0], "foo");
  EXPECT_EQ((*result)[1], "bar");

  // Drop our reference; the aliasing shared_ptr must keep the data alive.
  data.reset();
  EXPECT_EQ((*result)[0], "foo");
  EXPECT_EQ((*result)[1], "bar");
}

// An empty lookup result is valid: no views, empty span.
TEST(VocabBatchLookupData, AsResultEmpty) {
  auto data = std::make_shared<VocabBatchLookupData>();
  VocabBatchLookupResult result = VocabBatchLookupData::asResult(data);
  EXPECT_TRUE(result->empty());
}

// Tests for `PmrVocabBatchLookupData`: the `monotonic_buffer_resource` backing
// used when words are produced incrementally with sizes not known up front
// (e.g. decompressing one word at a time in `CompressedVocabulary`). Each word
// gets a pointer-stable allocation, so appending a later (differently sized)
// word never invalidates an earlier `string_view`, unlike the single growing
// buffer of `VocabBatchLookupData`, which would reallocate and leave the
// already-recorded views dangling.
TEST(PmrVocabBatchLookupData, PmrAsResultPointerStableAcrossAppends) {
  auto data = std::make_shared<PmrVocabBatchLookupData>();
  data->buffer() = std::make_unique<ql::pmr::monotonic_buffer_resource>();
  auto* resource = data->buffer().get();

  // Allocate each word separately from the monotonic resource and record a view
  // into it. Because the allocations are pointer-stable, the first view stays
  // valid after the second word is appended.
  auto appendWord = [&](std::string_view word) {
    char* p = static_cast<char*>(resource->allocate(word.size()));
    std::memcpy(p, word.data(), word.size());
    data->views().emplace_back(p, word.size());
  };
  appendWord("foo");
  std::string_view firstView = data->views().front();
  appendWord("barbaz");
  // Appending the second word did not invalidate the first view.
  EXPECT_EQ(firstView, "foo");

  VocabBatchLookupResult result = PmrVocabBatchLookupData::asResult(data);
  ASSERT_EQ(result->size(), 2u);
  EXPECT_EQ((*result)[0], "foo");
  EXPECT_EQ((*result)[1], "barbaz");

  // The aliasing shared_ptr keeps the resource (and thus its allocations)
  // alive.
  data.reset();
  EXPECT_EQ((*result)[0], "foo");
  EXPECT_EQ((*result)[1], "barbaz");
}

// An empty pmr lookup result is valid: no views, empty span (matches the
// `VocabBatchLookupData` `AsResultEmpty` case).
TEST(PmrVocabBatchLookupData, PmrAsResultEmpty) {
  auto data = std::make_shared<PmrVocabBatchLookupData>();
  data->buffer() = std::make_unique<ql::pmr::monotonic_buffer_resource>();
  VocabBatchLookupResult result = PmrVocabBatchLookupData::asResult(data);
  EXPECT_TRUE(result->empty());
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
  ASSERT_EQ(result->size(), 2u);
  EXPECT_EQ((*result)[0], "word");
  EXPECT_EQ((*result)[1], placeholderForMissingVocabIndex(5));

  // The vocabulary that has not opted in throws.
  AD_EXPECT_THROW_WITH_MESSAGE(
      sequentialLookupBatch(VocabWithHolesThrowing{}, indices),
      ::testing::HasSubstr("replaceOptionalByPlaceholderOnExport"));
}
