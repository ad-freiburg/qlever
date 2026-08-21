// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>, UFR
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

#include <absl/functional/function_ref.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <array>
#include <cstring>

#include "../../util/GTestHelpers.h"
#include "index/vocabulary/VocabularyInMemoryBinSearch.h"
#include "index/vocabulary/VocabularyTypes.h"
#include "util/File.h"

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
  void finishImpl() override {}
};

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

// `asResult` exposes the span over the filled views, and the returned aliasing
// shared_ptr keeps the backing buffer/views alive after the original owning
// shared_ptr is dropped (the whole point of the aliasing shared_ptr).
TEST(VocabBatchLookupData, AsResultExposesViewsAndKeepsDataAlive) {
  auto data = std::make_shared<VocabBatchLookupData>();
  data->buffer() = {'f', 'o', 'o', 'b', 'a', 'r'};
  data->views().emplace_back(data->buffer().data(), 3);      // "foo"
  data->views().emplace_back(data->buffer().data() + 3, 3);  // "bar"

  VocabBatchLookupResult result = VocabBatchLookupData::asResult(data);

  EXPECT_THAT(*result, ::testing::ElementsAre("foo", "bar"));

  // Drop our reference; the aliasing shared_ptr must keep the data alive.
  data.reset();
  EXPECT_THAT(*result, ::testing::ElementsAre("foo", "bar"));
}

// An empty lookup result is valid: no views, empty span.
TEST(VocabBatchLookupData, AsResultEmpty) {
  auto data = std::make_shared<VocabBatchLookupData>();
  VocabBatchLookupResult result = VocabBatchLookupData::asResult(data);
  EXPECT_TRUE(result->empty());
}

TEST(VocabBatchLookupData, MakeStringVectorResultKeepsViewsValid) {
  auto result = makeStringVectorVocabBatchLookupResult({"alpha", "beta"});

  EXPECT_THAT(*result, ::testing::ElementsAre("alpha", "beta"));
}

TEST(VocabBatchLookupData, ScatterBatchResultRetainsOwner) {
  auto first = makeStringVectorVocabBatchLookupResult({"alpha", "beta"});
  auto second = makeStringVectorVocabBatchLookupResult({"gamma"});
  const char* alphaData = (*first)[0].data();
  const char* gammaData = (*second)[0].data();

  std::vector<std::string_view> viewsInInputOrder(3);
  std::vector<VocabBatchOwner> owners;
  const std::array<size_t, 2> firstPositions{2, 0};
  const std::array<size_t, 1> secondPositions{1};
  scatterVocabBatchLookupResult(std::move(first), firstPositions,
                                viewsInInputOrder, owners);
  scatterVocabBatchLookupResult(std::move(second), secondPositions,
                                viewsInInputOrder, owners);

  auto result =
      keepAliveVocabBatch(std::move(owners), std::move(viewsInInputOrder));
  EXPECT_THAT(*result, ::testing::ElementsAre("beta", "gamma", "alpha"));
  EXPECT_EQ((*result)[2].data(), alphaData);
  EXPECT_EQ((*result)[1].data(), gammaData);
}

TEST(VocabBatchLookupData, KeepAliveVocabBatchDoesNotCopyBytes) {
  auto firstOwner = std::make_shared<StringVectorVocabBatchLookupData>();
  firstOwner->buffer() = {"alpha", "beta"};
  firstOwner->views() = {firstOwner->buffer()[0], firstOwner->buffer()[1]};
  auto first = StringVectorVocabBatchLookupData::asResult(firstOwner);

  auto secondOwner = std::make_shared<StringVectorVocabBatchLookupData>();
  secondOwner->buffer() = {"gamma"};
  secondOwner->views() = {secondOwner->buffer()[0]};
  auto second = StringVectorVocabBatchLookupData::asResult(secondOwner);

  const char* alphaData = (*first)[0].data();
  const char* gammaData = (*second)[0].data();
  std::vector<std::string_view> mixed{(*first)[0], (*second)[0], (*first)[1]};
  std::vector<VocabBatchOwner> owners{std::move(first), std::move(second)};
  firstOwner.reset();
  secondOwner.reset();

  auto result = keepAliveVocabBatch(std::move(owners), std::move(mixed));
  EXPECT_THAT(*result, ::testing::ElementsAre("alpha", "gamma", "beta"));
  EXPECT_EQ((*result)[0].data(), alphaData);
  EXPECT_EQ((*result)[1].data(), gammaData);
}

TEST(VocabBatchLookupData, KeepAliveRequiresAnOwner) {
  std::vector<std::string_view> views{"orphan"};
  AD_EXPECT_THROW_WITH_MESSAGE(keepAliveVocabBatch({}, std::move(views)),
                               ::testing::HasSubstr("owners"));
}

// A view obtained from `VocabularyInMemoryBinSearch` stays valid after
// `close()` and after the vocabulary object is replaced/destroyed, because the
// batch result retains `wordStorage()` shared ownership of the bytes.
TEST(VocabBatchLookupData, KeepAliveOutlivesSharedWordStorage) {
  const std::string filename =
      "KeepAliveOutlivesSharedWordStorage.vocabularyTypesTest.dat";
  ad_utility::deleteFile(filename, false);
  ad_utility::deleteFile(filename + ".ids", false);

  auto buildVocab = [&](std::string_view word) {
    VocabularyInMemoryBinSearch vocabulary;
    {
      VocabularyInMemoryBinSearch::WordWriter writer{filename};
      writer(word, 0);
      writer.finish();
    }
    vocabulary.open(filename);
    return vocabulary;
  };

  // close() path: vocabulary installs a fresh empty words_ buffer.
  {
    auto vocabulary = buildVocab("ram-word");
    auto maybeWord = vocabulary[0];
    ASSERT_TRUE(maybeWord.has_value());
    const char* wordData = maybeWord->data();
    std::vector<std::string_view> views{maybeWord.value()};
    std::vector<VocabBatchOwner> owners{vocabulary.wordStorage()};
    auto result = keepAliveVocabBatch(std::move(owners), std::move(views));

    vocabulary.close();
    EXPECT_THAT(*result, ::testing::ElementsAre("ram-word"));
    EXPECT_EQ((*result)[0].data(), wordData);
  }

  // destruction path: drop the vocabulary object while the result still lives.
  {
    auto vocabulary = std::make_optional(buildVocab("other-word"));
    auto maybeWord = (*vocabulary)[0];
    ASSERT_TRUE(maybeWord.has_value());
    const char* wordData = maybeWord->data();
    std::vector<std::string_view> views{maybeWord.value()};
    std::vector<VocabBatchOwner> owners{vocabulary->wordStorage()};
    auto result = keepAliveVocabBatch(std::move(owners), std::move(views));

    vocabulary.reset();
    EXPECT_THAT(*result, ::testing::ElementsAre("other-word"));
    EXPECT_EQ((*result)[0].data(), wordData);
  }

  ad_utility::deleteFile(filename);
  ad_utility::deleteFile(filename + ".ids", false);
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
  EXPECT_THAT(*result, ::testing::ElementsAre("foo", "barbaz"));

  // The aliasing shared_ptr keeps the resource (and thus its allocations)
  // alive.
  data.reset();
  EXPECT_THAT(*result, ::testing::ElementsAre("foo", "barbaz"));
}

// An empty pmr lookup result is valid: no views, empty span (matches the
// `VocabBatchLookupData` `AsResultEmpty` case).
TEST(PmrVocabBatchLookupData, PmrAsResultEmpty) {
  auto data = std::make_shared<PmrVocabBatchLookupData>();
  data->buffer() = std::make_unique<ql::pmr::monotonic_buffer_resource>();
  VocabBatchLookupResult result = PmrVocabBatchLookupData::asResult(data);
  EXPECT_TRUE(result->empty());
}

// _____________________________________________________________________________
TEST(VocabBatchLookupData, ScatterBatchResultSizeMismatchThrows) {
  auto batch = makeStringVectorVocabBatchLookupResult({"only-one"});
  std::vector<std::string_view> views(2);
  std::vector<VocabBatchOwner> owners;
  const std::array<size_t, 2> positions{0, 1};
  // Two positions but one word in the batch.
  AD_EXPECT_THROW_WITH_MESSAGE(
      scatterVocabBatchLookupResult(std::move(batch), positions, views, owners),
      ::testing::HasSubstr("result->size() == resultPositions.size()"));
}

// _____________________________________________________________________________
TEST(VocabBatchLookupData, MakePmrResultKeepsViewsAlive) {
  auto buffer = std::make_unique<ql::pmr::monotonic_buffer_resource>();
  auto* resource = buffer.get();
  auto allocCopy = [&](std::string_view word) {
    char* p = static_cast<char*>(resource->allocate(word.size()));
    std::memcpy(p, word.data(), word.size());
    return std::string_view{p, word.size()};
  };
  std::vector<std::string_view> views{allocCopy("one"), allocCopy("two")};
  const char* firstData = views[0].data();
  auto result =
      makePmrVocabBatchLookupResult(std::move(buffer), std::move(views));
  EXPECT_THAT(*result, ::testing::ElementsAre("one", "two"));
  EXPECT_EQ((*result)[0].data(), firstData);
}
