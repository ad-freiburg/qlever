// Copyright 2011, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold <buchholb>

#include <gtest/gtest.h>

#include <cstdio>
#include <vector>

#include "index/Vocabulary.h"
#include "index/vocabulary/VocabularyType.h"
#include "util/json.h"

using json = nlohmann::json;
using std::string;

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

  // with case insensitive ordering
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
  using ad_utility::VocabularyType;
  RdfsVocabulary v;
  v.resetToType(VocabularyType{VocabularyType::Enum::OnDiskCompressed});
  ad_utility::HashSet<string> s{"a", "ab", "ba", "car"};
  auto filename = "vocTestLookupBatch.dat";
  v.createFromSet(s, filename);

  // Sorted order: a=0, ab=1, ba=2, car=3. Look up in shuffled order.
  std::vector<size_t> indices{2, 0, 3, 1};
  auto result = v.lookupBatch(indices);
  ASSERT_EQ(result->size(), 4);
  EXPECT_EQ((*result)[0], "ba");
  EXPECT_EQ((*result)[1], "a");
  EXPECT_EQ((*result)[2], "car");
  EXPECT_EQ((*result)[3], "ab");

  // Each batch result must match the single-index `operator[]`.
  for (size_t i = 0; i < indices.size(); ++i) {
    EXPECT_EQ((*result)[i], v[VocabIndex::make(indices[i])]);
  }

  // Empty batch -> empty result.
  EXPECT_EQ(v.lookupBatch(ql::span<const size_t>{})->size(), 0);

  // Duplicate indices: each position resolved independently.
  std::vector<size_t> dup{1, 1, 0};
  auto dupResult = v.lookupBatch(dup);
  ASSERT_EQ(dupResult->size(), 3);
  EXPECT_EQ((*dupResult)[0], "ab");
  EXPECT_EQ((*dupResult)[1], "ab");
  EXPECT_EQ((*dupResult)[2], "a");

  ad_utility::deleteFile(filename);
}

// _____________________________________________________________________________
TEST(VocabularyTest, LookupBatchesStreamed) {
  using ad_utility::VocabularyType;
  RdfsVocabulary v;
  v.resetToType(VocabularyType{VocabularyType::Enum::OnDiskCompressed});
  ad_utility::HashSet<string> s{"a", "ab", "ba", "car"};
  auto filename = "vocTestLookupBatchesStreamed.dat";
  v.createFromSet(s, filename);

  // Three batches: mixed, empty (mid-stream), single.
  std::vector<std::vector<size_t>> batches{{2, 0}, {}, {3}};
  auto streamed = v.lookupBatchesStreamed(VocabLookupInput{batches});

  std::vector<VocabBatchLookupResult> results;
  for (auto& r : streamed) {
    results.push_back(std::move(r));
  }
  ASSERT_EQ(results.size(), 3);

  // Each streamed result must match the eager `lookupBatch`.
  auto expectedMatchesEager = [&v](const VocabBatchLookupResult& actual,
                                   ql::span<const size_t> batchIndices) {
    auto expected = v.lookupBatch(batchIndices);
    ASSERT_EQ(actual->size(), expected->size());
    for (size_t i = 0; i < expected->size(); ++i) {
      EXPECT_EQ((*actual)[i], (*expected)[i]);
    }
  };
  expectedMatchesEager(results[0], batches[0]);
  expectedMatchesEager(results[1], batches[1]);
  expectedMatchesEager(results[2], batches[2]);

  // Exact contents
  ASSERT_EQ(results[0]->size(), 2);
  EXPECT_EQ((*results[0])[0], "ba");
  EXPECT_EQ((*results[0])[1], "a");
  EXPECT_EQ((*results[0])[1], "a");
  ASSERT_EQ(results[1]->size(), 0);
  ASSERT_EQ(results[2]->size(), 1);
  EXPECT_EQ((*results[1])[0], "car");

  // Empty input stream -> no results.
  std::vector<std::vector<size_t>> noBatches;
  auto empty = v.lookupBatchesStreamed(VocabLookupInput{noBatches});
  size_t count = 0;
  for ([[maybe_unused]] auto& r : empty) {
    ++count;
  }
  EXPECT_EQ(count, 0);

  ad_utility::deleteFile(filename);
}
