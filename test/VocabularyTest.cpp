// Copyright 2011 - 2025, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Authors: Björn Buchhold <buchholb>
//          Christoph Ullinger <ullingec@cs.uni-freiburg.de>

#include <gtest/gtest.h>

#include <cstdio>
#include <vector>

#include "global/IndexTypes.h"
#include "index/Vocabulary.h"
#include "index/vocabulary/CompressedVocabulary.h"
// Including the SplitVocabulary.cpp file is necessary for the compiler to fully
// see the SplitVocabulary template. The template is required to instatiate
// example split vocabularies, which should not pollute the main implementation
// of SplitVocabulary.
#include "index/vocabulary/SplitVocabulary.cpp"
#include "index/vocabulary/SplitVocabulary.h"
#include "index/vocabulary/VocabularyInMemory.h"
#include "index/vocabulary/VocabularyInternalExternal.h"
#include "util/json.h"

// Anonymous namespace for the normal tests
namespace {

using json = nlohmann::json;
using std::string;

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
}

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

TEST(VocabularyTest, createFromSetTest) {
  ad_utility::HashSet<string> s;
  s.insert("a");
  s.insert("ab");
  s.insert(
      "\"POLYGON((1 2, 3 "
      "4))\"^^<http://www.opengis.net/ont/geosparql#wktLiteral>");
  s.insert("ba");
  s.insert("car");
  s.insert(
      "\"LINESTRING(1 2, 3 "
      "4)\"^^<http://www.opengis.net/ont/geosparql#wktLiteral>");

  TextVocabulary v;
  auto filename = "vocTest4.dat";
  v.createFromSet(s, filename);

  WordVocabIndex idx;
  ASSERT_TRUE(v.getId("ba", &idx));
  ASSERT_EQ(2u, idx.get());

  ASSERT_TRUE(v.getId("a", &idx));
  ASSERT_EQ(0u, idx.get());

  ASSERT_FALSE(v.getId("foo", &idx));

  ASSERT_TRUE(
      v.getId("\"LINESTRING(1 2, 3 "
              "4)\"^^<http://www.opengis.net/ont/geosparql#wktLiteral>",
              &idx));
  ASSERT_EQ(static_cast<uint64_t>(1) << 59, idx.get());
  ASSERT_TRUE(
      v.getId("\"POLYGON((1 2, 3 "
              "4))\"^^<http://www.opengis.net/ont/geosparql#wktLiteral>",
              &idx));
  ASSERT_EQ((static_cast<uint64_t>(1) << 59) | 1, idx.get());

  ad_utility::deleteFile(filename);
}

TEST(VocabularyTest, IncompleteLiterals) {
  TripleComponentComparator comp("en", "US", false);

  ASSERT_TRUE(comp("\"fieldofwork", "\"GOLD\"@en"));
}

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

TEST(VocabularyTest, ItemAt) {
  ad_utility::HashSet<string> s;
  s.insert("a");
  s.insert("ab");
  s.insert(
      "\"POLYGON((1 2, 3 "
      "4))\"^^<http://www.opengis.net/ont/geosparql#wktLiteral>");
  s.insert("ba");
  s.insert("car");
  s.insert(
      "\"LINESTRING(1 2, 3 "
      "4)\"^^<http://www.opengis.net/ont/geosparql#wktLiteral>");

  RdfsVocabulary v;
  auto filename = "vocTest6.dat";
  v.createFromSet(s, filename);

  ASSERT_EQ(v[VocabIndex::make(0)], "a");
  ASSERT_EQ(v[VocabIndex::make(1)], "ab");
  ASSERT_EQ(v[VocabIndex::make(2)], "ba");
  ASSERT_EQ(v[VocabIndex::make(3)], "car");

  // Out of range indices
  EXPECT_ANY_THROW(v[VocabIndex::make(42)]);
  EXPECT_ANY_THROW(v[VocabIndex::make((1ull << 59) | 42)]);

  auto idx = VocabIndex::make(1ull << 59);
  ASSERT_EQ(v[idx],
            "\"LINESTRING(1 2, 3 "
            "4)\"^^<http://www.opengis.net/ont/geosparql#wktLiteral>");
  idx = VocabIndex::make(static_cast<uint64_t>(1) << 59 | 1);
  ASSERT_EQ(v[idx],
            "\"POLYGON((1 2, 3 "
            "4))\"^^<http://www.opengis.net/ont/geosparql#wktLiteral>");

  ad_utility::deleteFile(filename);
}

TEST(Vocabulary, SplitGeoVocab) {
  using SGV =
      SplitGeoVocabulary<CompressedVocabulary<VocabularyInternalExternal>>;

  // Test check: Is a geo literal?
  ASSERT_EQ(SGV::getMarkerForWord(
                "\"POLYGON((1 2, 3 "
                "4))\"^^<http://www.opengis.net/ont/geosparql#wktLiteral>"),
            1);
  ASSERT_EQ(SGV::getMarkerForWord(
                "\"LINESTRING(1 2, 3 "
                "4)\"^^<http://www.opengis.net/ont/geosparql#wktLiteral>"),
            1);
  ASSERT_EQ(SGV::getMarkerForWord(""), 0);
  ASSERT_EQ(SGV::getMarkerForWord("\"abc\""), 0);
  ASSERT_EQ(SGV::getMarkerForWord("\"\"^^<http://example.com>"), 0);

  // Add marker bit
  ASSERT_EQ(SGV::addMarker(0, 1), (1ull << 59));
  ASSERT_EQ(SGV::addMarker(25, 1), (1ull << 59) | 25);

  // Vocab index is out of range
  EXPECT_ANY_THROW(SGV::addMarker((1ull << 60) | 42, 5));
  EXPECT_ANY_THROW(SGV::addMarker(1ull << 59, 5));

  // Check marker bit
  ASSERT_TRUE(SGV::isSpecialVocabIndex((1ull << 59) | 42));
  ASSERT_TRUE(SGV::isSpecialVocabIndex(1ull << 59));
  ASSERT_FALSE(SGV::isSpecialVocabIndex(0));
  ASSERT_FALSE(SGV::isSpecialVocabIndex(42));
  ASSERT_FALSE(SGV::isSpecialVocabIndex((1ull << 59) - 1));
  ASSERT_FALSE(SGV::isSpecialVocabIndex(1ull << 58));
}

TEST(Vocabulary, SplitWordWriter) {
  // The word writer in the Vocabulary class runs the SplitGeoVocabulary word
  // writer. Its task is to split words to two different vocabularies for geo
  // and non-geo words. This split is tested here.
  RdfsVocabulary vocabulary;
  auto wordCallback = vocabulary.makeWordWriterPtr("vocTest7.dat");

  // Call word writer
  ASSERT_EQ((*wordCallback)("a", true), 0);
  ASSERT_EQ((*wordCallback)("ab", true), 1);
  ASSERT_EQ(
      (*wordCallback)("\"LINESTRING(1 2, 3 "
                      "4)\"^^<http://www.opengis.net/ont/geosparql#wktLiteral>",
                      true),
      (1ull << 59));
  ASSERT_EQ((*wordCallback)("ba", true), 2);
  ASSERT_EQ((*wordCallback)("car", true), 3);
  ASSERT_EQ((*wordCallback)(
                "\"POLYGON((1 2, 3 "
                "4))\"^^<http://www.opengis.net/ont/geosparql#wktLiteral>",
                true),
            (1ull << 59) | 1);

  wordCallback->finish();

  vocabulary.readFromFile("vocTest7.dat");

  // Check that the resulting vocabulary is correct
  VocabIndex idx;
  ASSERT_TRUE(vocabulary.getId("a", &idx));
  ASSERT_EQ(idx.get(), 0);
  ASSERT_EQ(vocabulary[VocabIndex::make(0)], "a");
  ASSERT_TRUE(vocabulary.getId("ab", &idx));
  ASSERT_EQ(idx.get(), 1);
  ASSERT_EQ(vocabulary[VocabIndex::make(1)], "ab");
  ASSERT_TRUE(vocabulary.getId("ba", &idx));
  ASSERT_EQ(idx.get(), 2);
  ASSERT_EQ(vocabulary[VocabIndex::make(2)], "ba");
  ASSERT_TRUE(vocabulary.getId("car", &idx));
  ASSERT_EQ(idx.get(), 3);
  ASSERT_EQ(vocabulary[VocabIndex::make(3)], "car");

  ASSERT_TRUE(vocabulary.getId(
      "\"LINESTRING(1 2, 3 "
      "4)\"^^<http://www.opengis.net/ont/geosparql#wktLiteral>",
      &idx));
  ASSERT_EQ(idx.get(), 1ull << 59);
  ASSERT_EQ(vocabulary[VocabIndex::make(1ull << 59)],
            "\"LINESTRING(1 2, 3 "
            "4)\"^^<http://www.opengis.net/ont/geosparql#wktLiteral>");

  ASSERT_TRUE(vocabulary.getId(
      "\"POLYGON((1 2, 3 "
      "4))\"^^<http://www.opengis.net/ont/geosparql#wktLiteral>",
      &idx));
  ASSERT_EQ(idx.get(), (1ull << 59) | 1);
  ASSERT_EQ(vocabulary[VocabIndex::make((1ull << 59) | 1)],
            "\"POLYGON((1 2, 3 "
            "4))\"^^<http://www.opengis.net/ont/geosparql#wktLiteral>");

  ASSERT_FALSE(vocabulary.getId("xyz", &idx));
  ASSERT_ANY_THROW(vocabulary[VocabIndex::make(42)]);
}

}  // namespace

namespace splitVocabTest {

uint8_t testSplitTwoFunction(std::string_view s) {
  return s.starts_with("\"a");
}

std::array<std::string, 2> testSplitFnTwoFunction(std::string_view s) {
  return {std::string(s), absl::StrCat(s, ".a")};
}

using TwoSplitVocabulary =
    SplitVocabulary<splitVocabTest::testSplitTwoFunction,
                    splitVocabTest::testSplitFnTwoFunction, VocabularyInMemory,
                    VocabularyInMemory>;

TEST(Vocabulary, SplitVocabularyCustomWithTwoVocabs) {
  // Tests the SplitVocabulary class with a custom split function that separates
  // all words in two underlying vocabularies
  TwoSplitVocabulary sv;

  ASSERT_EQ(sv.numberOfVocabs, 2);
  ASSERT_EQ(sv.markerBitMaskSize, 1);
  ASSERT_EQ(sv.markerBitMask, 1ull << 59);
  ASSERT_EQ(sv.markerShift, 59);
  ASSERT_EQ(sv.vocabIndexBitMask, (1ull << 59) - 1);

  ASSERT_EQ(sv.addMarker(42, 0), 42);
  ASSERT_EQ(sv.addMarker(42, 1), (1ull << 59) | 42);
  ASSERT_ANY_THROW(sv.addMarker(1ull << 60, 1));
  ASSERT_ANY_THROW(sv.addMarker(5, 2));

  ASSERT_EQ(sv.getMarker((1ull << 59) | 42), 1);
  ASSERT_EQ(sv.getMarker(42), 0);

  ASSERT_EQ(sv.getVocabIndex((1ull << 59) | 42), 42);
  ASSERT_EQ(sv.getVocabIndex(1ull << 59), 0);
  ASSERT_EQ(sv.getVocabIndex(0), 0);
  ASSERT_EQ(sv.getVocabIndex((1ull << 59) - 1), (1ull << 59) - 1);
  ASSERT_EQ(sv.getVocabIndex(42), 42);

  ASSERT_TRUE(sv.isSpecialVocabIndex((1ull << 59) | 42));
  ASSERT_TRUE(sv.isSpecialVocabIndex(1ull << 59));
  ASSERT_FALSE(sv.isSpecialVocabIndex(42));
  ASSERT_FALSE(sv.isSpecialVocabIndex(0));

  ASSERT_EQ(sv.getMarkerForWord("\"xyz\""), 0);
  ASSERT_EQ(sv.getMarkerForWord("<abc>"), 0);
  ASSERT_EQ(sv.getMarkerForWord("\"abc\""), 1);
  ;
}

// TODO test with three

}  // namespace splitVocabTest

// Explicit instantiations for SplitVocabulary tests
template class SplitVocabulary<splitVocabTest::testSplitTwoFunction,
                               splitVocabTest::testSplitFnTwoFunction,
                               VocabularyInMemory, VocabularyInMemory>;
