// Copyright 2011, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold <buchholb>

#include <absl/cleanup/cleanup.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <cstdio>
#include <vector>

#include "index/Vocabulary.h"
#include "index/vocabulary/VocabularyTestHelpers.h"
#include "index/vocabulary/VocabularyType.h"
#include "util/json.h"

using json = nlohmann::json;
using std::string;
using ::testing::ElementsAre;

namespace {
using ad_utility::VocabularyType;

// TODO: write descriptive comment here.
class RdfsVocabularyCreator {
  std::string filename_;
  void deleteFiles() const {
    for (std::string_view suffix : {".words", ".words.offsets", ".codebooks"}) {
      ad_utility::deleteFile(absl::StrCat(filename_, suffix), false);
    }
  }

 public:
  explicit RdfsVocabularyCreator(std::string filename)
      : filename_{std::move(filename)} {
    deleteFiles();  // clear any leftovers from a previous run.
  }

  RdfsVocabularyCreator(const RdfsVocabularyCreator&) = delete;
  RdfsVocabularyCreator& operator=(const RdfsVocabularyCreator&) = delete;
  ~RdfsVocabularyCreator() { deleteFiles(); }

  RdfsVocabulary createVocabulary(const ad_utility::HashSet<std::string>& words,
                                  VocabularyType type) {
    RdfsVocabulary v;
    v.resetToType(type);
    v.createFromSet(words, filename_);
    return v;
  }
};

// Owns an `RdfsVocabulary` together with the creator that manages its files,
// so the files live exactly as long as the vocabulary and are removed on
// destruction.
class RdfsVocabularyHandle {
 public:
  RdfsVocabularyHandle(std::string filename,
                       const ad_utility::HashSet<std::string>& words,
                       VocabularyType type =
                           VocabularyType{
                               VocabularyType::Enum::OnDiskCompressed})
      : creator_{std::move(filename)},
        vocabulary_{creator_.createVocabulary(words, type)} {}

  // Non-copyable/movable: exactly one owner of the backing files.
  RdfsVocabularyHandle(const RdfsVocabularyHandle&) = delete;
  RdfsVocabularyHandle& operator=(const RdfsVocabularyHandle&) = delete;
  RdfsVocabularyHandle(RdfsVocabularyHandle&&) = delete;
  RdfsVocabularyHandle& operator=(const RdfsVocabularyHandle&&) = delete;

  RdfsVocabulary& operator*() { return vocabulary_; }
  RdfsVocabulary* operator->() { return &vocabulary_; }

 private:
  // `vocabulary_` listed after `creator_`, so `vocabulary_` is destroyed first,
  // releasing the files before `creator_` removes them.
  RdfsVocabularyCreator creator_;
  RdfsVocabulary vocabulary_;
};

RdfsVocabularyHandle createExampleVocabulary(
    const ad_utility::HashSet<std::string>& words = {"a", "ab", "ba", "car"}) {
  return RdfsVocabularyHandle{absl::StrCat(gtestCurrentTestName(), ".dat"),
                              words};
}
}  // namespace

// _____________________________________________________________________________
TEST(VocabularyTest, getIdForWordTest) {
  std::vector<TextVocabulary> vec(2);

  ad_utility::HashSet<std::string> s{"a", "ab", "ba", "car"};
  for (auto& v : vec) {
    auto filename = "vocTest1.dat";
    v.createFromSet(s, filename);
    WordVocabIndex idx;
    ASSERT_TRUE(v.getId("ba", &idx));
    ASSERT_EQ(2u, idx.get());
    ASSERT_TRUE(v.getId("a", &idx));
    ASSERT_EQ(0u, idx.get());
    ASSERT_FALSE(v.getId("foo", &idx));
    ad_utility::deleteFile(filename);
  }

  // with case-insensitive ordering
  TextVocabulary voc;
  voc.setLocale("en", "US", false);
  ad_utility::HashSet<string> s2{"a", "A", "Ba", "car"};
  auto filename = "vocTest2.dat";
  voc.createFromSet(s2, filename);
  WordVocabIndex idx;
  ASSERT_TRUE(voc.getId("Ba", &idx));
  ASSERT_EQ(2u, idx.get());
  ASSERT_TRUE(voc.getId("a", &idx));
  ASSERT_EQ(0u, idx.get());
  // getId only gets exact matches;
  ASSERT_FALSE(voc.getId("ba", &idx));
  ad_utility::deleteFile(filename);

  // A TextVocabulary does not return anything for getGeoInfo
  ASSERT_EQ(voc.getGeoInfo(WordVocabIndex::make(0)), std::nullopt);
  ASSERT_EQ(voc.getGeoInfo(WordVocabIndex::make(1)), std::nullopt);
  ASSERT_EQ(voc.getGeoInfo(WordVocabIndex::make(2)), std::nullopt);
}

// _____________________________________________________________________________
TEST(VocabularyTest, getIdRangeForFullTextPrefixTest) {
  TextVocabulary v;
  ad_utility::HashSet<string> s{"wordA0", "wordA1", "wordB2", "wordB3",
                                "wordB4"};
  auto filename = "vocTest3.dat";
  v.createFromSet(s, filename);

  uint64_t word0 = 0;
  // Match exactly one
  auto retVal = v.getIdRangeForFullTextPrefix("wordA1*");
  ASSERT_TRUE(retVal.has_value());
  ASSERT_EQ(word0 + 1, retVal.value().first().get());
  ASSERT_EQ(word0 + 1, retVal.value().last().get());

  // Match all
  retVal = v.getIdRangeForFullTextPrefix("word*");
  ASSERT_TRUE(retVal.has_value());
  ASSERT_EQ(word0, retVal.value().first().get());
  ASSERT_EQ(word0 + 4, retVal.value().last().get());

  // Match first two
  retVal = v.getIdRangeForFullTextPrefix("wordA*");
  ASSERT_TRUE(retVal.has_value());
  ASSERT_EQ(word0, retVal.value().first().get());
  ASSERT_EQ(word0 + 1, retVal.value().last().get());

  // Match last three
  retVal = v.getIdRangeForFullTextPrefix("wordB*");
  ASSERT_TRUE(retVal.has_value());
  ASSERT_EQ(word0 + 2, retVal.value().first().get());
  ASSERT_EQ(word0 + 4, retVal.value().last().get());

  retVal = v.getIdRangeForFullTextPrefix("foo*");
  ASSERT_FALSE(retVal.has_value());
  ad_utility::deleteFile(filename);
}

// _____________________________________________________________________________
TEST(VocabularyTest, createFromSetTest) {
  ad_utility::HashSet<string> s;
  s.insert("a");
  s.insert("ab");
  s.insert("ba");
  s.insert("car");
  TextVocabulary v;
  auto filename = "vocTest4.dat";
  v.createFromSet(s, filename);
  WordVocabIndex idx;
  ASSERT_TRUE(v.getId("ba", &idx));
  ASSERT_EQ(2u, idx.get());
  ASSERT_TRUE(v.getId("a", &idx));
  ASSERT_EQ(0u, idx.get());
  ASSERT_FALSE(v.getId("foo", &idx));
  ad_utility::deleteFile(filename);
}

// _____________________________________________________________________________
TEST(VocabularyTest, IncompleteLiterals) {
  TripleComponentComparator comp("en", "US", false);

  ASSERT_TRUE(comp("\"fieldofwork", "\"GOLD\"@en"));
}

// _____________________________________________________________________________
TEST(Vocabulary, PrefixFilter) {
  RdfsVocabulary vocabulary;
  vocabulary.setLocale("en", "US", true);
  ad_utility::HashSet<string> words;

  words.insert("\"exa\"");
  words.insert("\"exp\"");
  words.insert("\"ext\"");
  words.insert(R"("["Ex-vivo" renal artery revascularization]"@en)");
  auto filename = "vocTest5.dat";
  vocabulary.createFromSet(words, filename);

  // Found in internal but not in external vocabulary.
  auto ranges = vocabulary.prefixRanges("\"exp");
  RdfsVocabulary::PrefixRanges expectedRanges{
      {std::pair{VocabIndex::make(1u), VocabIndex::make(2u)}}};
  ASSERT_EQ(ranges, expectedRanges);
  ad_utility::deleteFile(filename);
}

// _____________________________________________________________________________
TEST(Vocabulary, IsGeoInfoAvailable) {
  using ad_utility::VocabularyType;
  using enum VocabularyType::Enum;

  RdfsVocabulary v1;
  v1.resetToType(VocabularyType{OnDiskCompressed});
  ASSERT_FALSE(v1.isGeoInfoAvailable());

  RdfsVocabulary v2;
  v2.resetToType(VocabularyType{InMemoryUncompressed});
  ASSERT_FALSE(v2.isGeoInfoAvailable());

  RdfsVocabulary v3;
  v3.resetToType(VocabularyType{OnDiskCompressedGeoSplit});
  ASSERT_TRUE(v3.isGeoInfoAvailable());

  TextVocabulary v4;
  ASSERT_FALSE(v4.isGeoInfoAvailable());
}

// _____________________________________________________________________________
TEST(VocabularyTest, LookupBatch) {
  auto v = createExampleVocabulary();
  std::vector<size_t> indices{2, 0, 3, 1};
  auto result = v->lookupBatch(indices);
  EXPECT_THAT((*result), ::testing::ElementsAre("ba", "a", "car", "ab"));
  vocabulary_test::assertLookupResultMatchesVocabularyAtIndices(*v, result,
                                                                indices);
  // An empty batch is an invalid request and must throw.
  EXPECT_ANY_THROW(v->lookupBatch(ql::span<const size_t>{}));

  // Duplicate indices: each position resolved independently.
  std::vector<size_t> dup{1, 1, 0};
  auto dupResult = v->lookupBatch(dup);
  EXPECT_THAT((*dupResult), ::testing::ElementsAre("ab", "ab", "a"));
}

// Each streamed result must equal the eager `lookupBatch` for that batch's
// indices, and the batches must be yielded in input order.
TEST(VocabularyTest, LookupBatchesStreamed) {
  auto v = createExampleVocabulary();
  std::vector<std::vector<size_t>> batches{{2, 0}, {3}};
  // `VocabLookupInput` takes ownership, so keep a copy to compare against.
  const auto expectedBatches = batches;
  auto streamed =
      v->lookupBatchesStreamed(VocabLookupInput{std::move(batches)});
  vocabulary_test::assertStreamedLookupMatchesVocabularyAtIndices(
      *v, streamed, expectedBatches);
}

// An empty batch within the stream is invalid and must throw when pulled.
TEST(VocabularyTest, LookupBatchesStreamedEmptyBatchThrows) {
  auto v = createExampleVocabulary();
  std::vector<std::vector<size_t>> batches{{2, 0}, {}, {3}};
  auto streamed =
      v->lookupBatchesStreamed(VocabLookupInput{std::move(batches)});
  EXPECT_ANY_THROW({
    for ([[maybe_unused]] auto& r : streamed) {
    }
  });
}

// An empty input stream (no batches) yields no results.
TEST(VocabularyTest, LookupBatchesStreamedEmptyStreamYieldsNothing) {
  auto v = createExampleVocabulary();
  std::vector<std::vector<size_t>> noBatches;
  auto streamed =
      v->lookupBatchesStreamed(VocabLookupInput{std::move(noBatches)});
  EXPECT_EQ(ql::ranges::distance(streamed), 0);
}
