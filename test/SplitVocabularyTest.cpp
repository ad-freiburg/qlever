// Copyright 2025, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Christoph Ullinger <ullingec@cs.uni-freiburg.de>

#include <gtest/gtest.h>

#include <variant>

#include "index/Vocabulary.h"
#include "index/vocabulary/SplitVocabularyImpl.h"
#include "index/vocabulary/VocabularyType.h"

namespace splitVocabTestHelpers {

using SGV =
    SplitGeoVocabulary<CompressedVocabulary<VocabularyInternalExternal>>;
using VocabOnSGV = Vocabulary<SGV, TripleComponentComparator, VocabIndex>;

[[maybe_unused]] auto testSplitTwoFunction = [](std::string_view s) -> uint8_t {
  return s.starts_with("\"a");
};

[[maybe_unused]] auto testSplitFnTwoFunction =
    [](std::string_view s) -> std::array<std::string, 2> {
  return {std::string(s), absl::StrCat(s, ".a")};
};

using TwoSplitVocabulary =
    SplitVocabulary<decltype(testSplitTwoFunction),
                    decltype(testSplitFnTwoFunction), VocabularyInMemory,
                    VocabularyInMemory>;

[[maybe_unused]] auto testSplitThreeFunction =
    [](std::string_view s) -> uint8_t {
  if (s.starts_with("\"")) {
    if (s.ends_with("\"^^<http://example.com>")) {
      return 1;
    } else if (s.ends_with("\"^^<blabliblu>")) {
      return 2;
    }
  }
  return 0;
};

[[maybe_unused]] auto testSplitFnThreeFunction =
    [](std::string_view s) -> std::array<std::string, 3> {
  return {absl::StrCat(s, ".a"), absl::StrCat(s, ".b"), absl::StrCat(s, ".c")};
};

using ThreeSplitVocabulary =
    SplitVocabulary<decltype(testSplitThreeFunction),
                    decltype(testSplitFnThreeFunction), VocabularyInMemory,
                    VocabularyInMemory, VocabularyInMemory>;

}  // namespace splitVocabTestHelpers

namespace {
using namespace splitVocabTestHelpers;
using namespace ad_utility;
const VocabularyType geoSplitVocabType{
    VocabularyType::Enum::OnDiskCompressedGeoSplit};

// _____________________________________________________________________________
TEST(Vocabulary, SplitGeoVocab) {
  // Test check: Is a geo literal?
  ASSERT_EQ(SGV::getMarkerForWord(
                "\"POLYGON((1 2, 3 4))\""
                "^^<http://www.opengis.net/ont/geosparql#wktLiteral>"),
            1);
  ASSERT_EQ(SGV::getMarkerForWord(
                "\"LINESTRING(1 2, 3 4)\""
                "^^<http://www.opengis.net/ont/geosparql#wktLiteral>"),
            1);
  ASSERT_EQ(SGV::getMarkerForWord(""), 0);
  ASSERT_EQ(SGV::getMarkerForWord("\"abc\""), 0);
  ASSERT_EQ(SGV::getMarkerForWord("\"\"^^<http://example.com>"), 0);

  // Add marker bit
  ASSERT_EQ(SGV::addMarker(0, 1), (1ull << 59));
  ASSERT_EQ(SGV::addMarker(25, 1), (1ull << 59) | 25);

  // Get vocab index
  ASSERT_EQ(SGV::getVocabIndex(0), 0);
  ASSERT_EQ(SGV::getVocabIndex(1), 1);
  ASSERT_EQ(SGV::getVocabIndex(1ull << 59), 0);
  ASSERT_EQ(SGV::getVocabIndex((1ull << 59) | 25), 25);

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

// _____________________________________________________________________________
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

  auto ww = sv.makeDiskWriterPtr("twoSplitVocab.dat");
  ASSERT_EQ((*ww)("\"\"", true), sv.addMarker(0, 0));
  ASSERT_EQ((*ww)("\"abc\"", true), sv.addMarker(0, 1));
  ASSERT_EQ((*ww)("\"axyz\"", true), sv.addMarker(1, 1));
  ASSERT_EQ((*ww)("\"xyz\"", true), sv.addMarker(1, 0));
  ww->readableName() = "Split Vocab with Two Underlying Vocabs";
  ww->finish();

  sv.readFromFile("twoSplitVocab.dat");
  ASSERT_EQ(sv.size(), 4);
  ASSERT_EQ(sv[1], "\"xyz\"");
  ASSERT_EQ(sv[(1ull << 59) | 1], "\"axyz\"");

  // Test access to and content of underlying vocabs
  std::visit(
      [](auto& vocab) {
        ASSERT_EQ(vocab.size(), 2);
        ASSERT_EQ(vocab[0], "\"\"");
        ASSERT_EQ(vocab[1], "\"xyz\"");
      },
      sv.getUnderlyingMainVocabulary());
  std::visit(
      [](auto& vocab) {
        ASSERT_EQ(vocab.size(), 2);
        ASSERT_EQ(vocab[0], "\"\"");
        ASSERT_EQ(vocab[1], "\"xyz\"");
      },
      sv.getUnderlyingVocabulary(0));
  std::visit(
      [](auto& vocab) {
        ASSERT_EQ(vocab.size(), 2);
        ASSERT_EQ(vocab[0], "\"abc\"");
        ASSERT_EQ(vocab[1], "\"axyz\"");
      },
      sv.getUnderlyingVocabulary(1));
  EXPECT_ANY_THROW(sv.getUnderlyingVocabulary(2));

  // Also test the const variant
  const auto& um = sv.getUnderlyingMainVocabulary();
  std::visit(
      [](auto& vocab) {
        ASSERT_EQ(vocab.size(), 2);
        ASSERT_EQ(vocab[0], "\"\"");
        ASSERT_EQ(vocab[1], "\"xyz\"");
      },
      um);
  const auto& u0 = sv.getUnderlyingVocabulary(0);
  std::visit(
      [](auto& vocab) {
        ASSERT_EQ(vocab.size(), 2);
        ASSERT_EQ(vocab[0], "\"\"");
        ASSERT_EQ(vocab[1], "\"xyz\"");
      },
      u0);
  const auto& u1 = sv.getUnderlyingVocabulary(1);
  std::visit(
      [](auto& vocab) {
        ASSERT_EQ(vocab.size(), 2);
        ASSERT_EQ(vocab[0], "\"abc\"");
        ASSERT_EQ(vocab[1], "\"axyz\"");
      },
      u1);

  const auto& svConstRef = sv;
  EXPECT_ANY_THROW(svConstRef.getUnderlyingVocabulary(2));

  sv.close();
}

// _____________________________________________________________________________
TEST(Vocabulary, SplitVocabularyCustomWithThreeVocabs) {
  // Tests the SplitVocabulary class with a custom split function that separates
  // all words in three underlying vocabularies (of different types)
  ThreeSplitVocabulary sv;

  ASSERT_EQ(sv.numberOfVocabs, 3);
  ASSERT_EQ(sv.markerBitMaskSize, 2);
  ASSERT_EQ(sv.markerBitMask, 3ull << 58);
  ASSERT_EQ(sv.markerShift, 58);
  ASSERT_EQ(sv.vocabIndexBitMask, (1ull << 58) - 1);

  ASSERT_EQ(sv.addMarker(42, 0), 42);
  ASSERT_EQ(sv.addMarker(42, 1), (1ull << 58) | 42);
  ASSERT_EQ(sv.addMarker(42, 2), (2ull << 58) | 42);
  ASSERT_ANY_THROW(sv.addMarker(1ull << 60, 1));
  ASSERT_ANY_THROW(sv.addMarker(5, 3));

  ASSERT_EQ(sv.getMarker((1ull << 58) | 42), 1);
  ASSERT_EQ(sv.getMarker((2ull << 58) | 42), 2);
  ASSERT_EQ(sv.getMarker(42), 0);

  ASSERT_EQ(sv.getVocabIndex((1ull << 58) | 42), 42);
  ASSERT_EQ(sv.getVocabIndex((2ull << 58) | 42), 42);
  ASSERT_EQ(sv.getVocabIndex(1ull << 58), 0);
  ASSERT_EQ(sv.getVocabIndex(2ull << 58), 0);
  ASSERT_EQ(sv.getVocabIndex(0), 0);
  ASSERT_EQ(sv.getVocabIndex((1ull << 58) - 1), (1ull << 58) - 1);
  ASSERT_EQ(sv.getVocabIndex(42), 42);

  ASSERT_TRUE(sv.isSpecialVocabIndex((1ull << 58) | 42));
  ASSERT_TRUE(sv.isSpecialVocabIndex((2ull << 58) | 42));
  ASSERT_TRUE(sv.isSpecialVocabIndex(1ull << 58));
  ASSERT_FALSE(sv.isSpecialVocabIndex(42));
  ASSERT_FALSE(sv.isSpecialVocabIndex(0));

  ASSERT_EQ(sv.getMarkerForWord("\"xyz\"^^<http://example.com>"), 1);
  ASSERT_EQ(sv.getMarkerForWord("\"xyz\"^^<blabliblu>"), 2);
  ASSERT_EQ(sv.getMarkerForWord("<abc>"), 0);
  ASSERT_EQ(sv.getMarkerForWord("\"abc\""), 0);

  auto ww = sv.makeDiskWriterPtr("threeSplitVocab.dat");
  ASSERT_EQ((*ww)("\"\"", true), sv.addMarker(0, 0));
  ASSERT_EQ((*ww)("\"abc\"", true), sv.addMarker(1, 0));
  ASSERT_EQ((*ww)("\"axyz\"", true), sv.addMarker(2, 0));
  ASSERT_EQ((*ww)("\"xyz\"^^<blabliblu>", true), sv.addMarker(0, 2));
  ASSERT_EQ((*ww)("\"xyz\"^^<http://example.com>", true), sv.addMarker(0, 1));
  ASSERT_EQ((*ww)("\"zzz\"^^<blabliblu>", true), sv.addMarker(1, 2));
  ww->readableName() = "Split Vocab with Three Underlying Vocabs";
  ww->finish();

  sv.readFromFile("threeSplitVocab.dat");
  ASSERT_EQ(sv.size(), 6);
  ASSERT_EQ(sv[2], "\"axyz\"");
  ASSERT_EQ(sv[2ull << 58], "\"xyz\"^^<blabliblu>");
  ASSERT_EQ(sv[(2ull << 58) | 1], "\"zzz\"^^<blabliblu>");
  ASSERT_EQ(sv[1ull << 58], "\"xyz\"^^<http://example.com>");
}

// _____________________________________________________________________________
TEST(Vocabulary, SplitVocabularyItemAt) {
  HashSet<string> s;
  s.insert("a");
  s.insert("ab");
  s.insert(
      "\"POLYGON((1 2, 3 4))\""
      "^^<http://www.opengis.net/ont/geosparql#wktLiteral>");
  s.insert("ba");
  s.insert("car");
  s.insert(
      "\"LINESTRING(1 2, 3 4)\""
      "^^<http://www.opengis.net/ont/geosparql#wktLiteral>");

  RdfsVocabulary v;
  v.resetToType(geoSplitVocabType);
  auto filename = "vocTest6.dat";
  v.createFromSet(s, filename);
  absl::Cleanup del = [&]() { deleteFile(filename); };

  ASSERT_EQ(v[VocabIndex::make(0)], "a");
  ASSERT_EQ(v[VocabIndex::make(1)], "ab");
  ASSERT_EQ(v[VocabIndex::make(2)], "ba");
  ASSERT_EQ(v[VocabIndex::make(3)], "car");

  // Out of range indices
  EXPECT_ANY_THROW(v[VocabIndex::make(42)]);
  EXPECT_ANY_THROW(v[VocabIndex::make((1ull << 59) | 42)]);

  auto idx = VocabIndex::make(1ull << 59);
  ASSERT_EQ(v[idx],
            "\"LINESTRING(1 2, 3 4)\""
            "^^<http://www.opengis.net/ont/geosparql#wktLiteral>");
  idx = VocabIndex::make(static_cast<uint64_t>(1) << 59 | 1);
  ASSERT_EQ(v[idx],
            "\"POLYGON((1 2, 3 4))\""
            "^^<http://www.opengis.net/ont/geosparql#wktLiteral>");
}

// _____________________________________________________________________________
TEST(Vocabulary, SplitVocabularyWordWriter) {
  // The word writer in the Vocabulary class runs the SplitGeoVocabulary word
  // writer. Its task is to split words to two different vocabularies for geo
  // and non-geo words. This split is tested here.
  RdfsVocabulary vocabulary;
  vocabulary.resetToType(geoSplitVocabType);
  auto wordCallback = vocabulary.makeWordWriterPtr("vocTest7.dat");

  // Call word writer
  ASSERT_EQ((*wordCallback)("a", true), 0);
  ASSERT_EQ((*wordCallback)("ab", true), 1);
  ASSERT_EQ(
      (*wordCallback)("\"LINESTRING(1 2, 3 4)\""
                      "^^<http://www.opengis.net/ont/geosparql#wktLiteral>",
                      true),
      (1ull << 59));
  ASSERT_EQ((*wordCallback)("ba", true), 2);
  ASSERT_EQ((*wordCallback)("car", true), 3);
  ASSERT_EQ(
      (*wordCallback)("\"POLYGON((1 2, 3 4))\""
                      "^^<http://www.opengis.net/ont/geosparql#wktLiteral>",
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

  ASSERT_TRUE(
      vocabulary.getId("\"LINESTRING(1 2, 3 4)\""
                       "^^<http://www.opengis.net/ont/geosparql#wktLiteral>",
                       &idx));
  ASSERT_EQ(idx.get(), 1ull << 59);
  ASSERT_EQ(vocabulary[VocabIndex::make(1ull << 59)],
            "\"LINESTRING(1 2, 3 4)\""
            "^^<http://www.opengis.net/ont/geosparql#wktLiteral>");

  ASSERT_TRUE(
      vocabulary.getId("\"POLYGON((1 2, 3 4))\""
                       "^^<http://www.opengis.net/ont/geosparql#wktLiteral>",
                       &idx));
  ASSERT_EQ(idx.get(), (1ull << 59) | 1);
  ASSERT_EQ(vocabulary[VocabIndex::make((1ull << 59) | 1)],
            "\"POLYGON((1 2, 3 4))\""
            "^^<http://www.opengis.net/ont/geosparql#wktLiteral>");

  ASSERT_FALSE(vocabulary.getId("xyz", &idx));
  ASSERT_ANY_THROW(vocabulary[VocabIndex::make(42)]);
}

}  // namespace
