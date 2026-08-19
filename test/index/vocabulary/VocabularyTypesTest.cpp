// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>, UFR
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

#include <absl/functional/function_ref.h>
#include <gtest/gtest.h>

#include <array>

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
  std::vector<VocabBatchLookupResult> owners{std::move(first),
                                             std::move(second)};
  firstOwner.reset();
  secondOwner.reset();

  auto result = keepAliveVocabBatch(std::move(owners), std::move(mixed),
                                    NoIndexRamWords{});
  ASSERT_EQ(result->size(), 3u);
  EXPECT_EQ((*result)[0], "alpha");
  EXPECT_EQ((*result)[1], "gamma");
  EXPECT_EQ((*result)[2], "beta");
  EXPECT_EQ((*result)[0].data(), alphaData);
  EXPECT_EQ((*result)[1].data(), gammaData);
}

TEST(VocabBatchLookupData, KeepAliveRequiresOwnerOrIndexRamWords) {
  std::vector<std::string_view> views{"orphan"};
  EXPECT_ANY_THROW(
      keepAliveVocabBatch({}, std::move(views), NoIndexRamWords{}));
}

TEST(VocabBatchLookupData, KeepAliveNamesIndexRamWords) {
  const std::string stored = "ram-word";
  const std::array<std::string_view, 1> indexRamWords{stored};
  std::vector<std::string_view> views{stored};
  auto result = keepAliveVocabBatch({}, std::move(views), indexRamWords);
  ASSERT_EQ(result->size(), 1u);
  EXPECT_EQ((*result)[0], "ram-word");
  EXPECT_EQ((*result)[0].data(), stored.data());
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
