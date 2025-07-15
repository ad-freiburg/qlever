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
  ASSERT_EQ(SGV::addMarker(0, 1), (1ULL << 59));
  ASSERT_EQ(SGV::addMarker(25, 1), (1ULL << 59) | 25);

  // Get vocab index
  ASSERT_EQ(SGV::getVocabIndex(0), 0);
  ASSERT_EQ(SGV::getVocabIndex(1), 1);
  ASSERT_EQ(SGV::getVocabIndex(1ULL << 59), 0);
  ASSERT_EQ(SGV::getVocabIndex((1ULL << 59) | 25), 25);

  // Vocab index is out of range
  EXPECT_ANY_THROW(SGV::addMarker((1ULL << 60) | 42, 5));
  EXPECT_ANY_THROW(SGV::addMarker(1ULL << 59, 5));

  // Check marker bit
  ASSERT_TRUE(SGV::isSpecialVocabIndex((1ULL << 59) | 42));
  ASSERT_TRUE(SGV::isSpecialVocabIndex(1ULL << 59));
  ASSERT_FALSE(SGV::isSpecialVocabIndex(0));
  ASSERT_FALSE(SGV::isSpecialVocabIndex(42));
  ASSERT_FALSE(SGV::isSpecialVocabIndex((1ULL << 59) - 1));
  ASSERT_FALSE(SGV::isSpecialVocabIndex(1ULL << 58));
}

// _____________________________________________________________________________
TEST(Vocabulary, SplitVocabularyCustomWithTwoVocabs) {
  // Tests the SplitVocabulary class with a custom split function that separates
  // all words in two underlying vocabularies
  TwoSplitVocabulary sv;

  ASSERT_EQ(sv.numberOfVocabs, 2);
  ASSERT_EQ(sv.markerBitMaskSize, 1);
  ASSERT_EQ(sv.markerBitMask, 1ULL << 59);
  ASSERT_EQ(sv.markerShift, 59);
  ASSERT_EQ(sv.vocabIndexBitMask, (1ULL << 59) - 1);

  ASSERT_EQ(sv.addMarker(42, 0), 42);
  ASSERT_EQ(sv.addMarker(42, 1), (1ULL << 59) | 42);
  ASSERT_ANY_THROW(sv.addMarker(1ULL << 60, 1));
  ASSERT_ANY_THROW(sv.addMarker(5, 2));

  ASSERT_EQ(sv.getMarker((1ULL << 59) | 42), 1);
  ASSERT_EQ(sv.getMarker(42), 0);

  ASSERT_EQ(sv.getVocabIndex((1ULL << 59) | 42), 42);
  ASSERT_EQ(sv.getVocabIndex(1ULL << 59), 0);
  ASSERT_EQ(sv.getVocabIndex(0), 0);
  ASSERT_EQ(sv.getVocabIndex((1ULL << 59) - 1), (1ULL << 59) - 1);
  ASSERT_EQ(sv.getVocabIndex(42), 42);

  ASSERT_TRUE(sv.isSpecialVocabIndex((1ULL << 59) | 42));
  ASSERT_TRUE(sv.isSpecialVocabIndex(1ULL << 59));
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
  ASSERT_EQ(sv[(1ULL << 59) | 1], "\"axyz\"");

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

  // There is not GeoInfo because none of the underlying vocabularies is a
  // `GeoVocabulary`
  ASSERT_FALSE(sv.getGeoInfo(0).has_value());
  ASSERT_FALSE(sv.getGeoInfo(1).has_value());
  ASSERT_FALSE(sv.getGeoInfo(1ULL << 59).has_value());
  ASSERT_FALSE(sv.getGeoInfo((1ULL << 59) | 1).has_value());

  sv.close();
}

// _____________________________________________________________________________
TEST(Vocabulary, SplitVocabularyCustomWithThreeVocabs) {
  // Tests the SplitVocabulary class with a custom split function that separates
  // all words in three underlying vocabularies (of different types)
  ThreeSplitVocabulary sv;

  ASSERT_EQ(sv.numberOfVocabs, 3);
  ASSERT_EQ(sv.markerBitMaskSize, 2);
  ASSERT_EQ(sv.markerBitMask, 3ULL << 58);
  ASSERT_EQ(sv.markerShift, 58);
  ASSERT_EQ(sv.vocabIndexBitMask, (1ULL << 58) - 1);

  ASSERT_EQ(sv.addMarker(42, 0), 42);
  ASSERT_EQ(sv.addMarker(42, 1), (1ULL << 58) | 42);
  ASSERT_EQ(sv.addMarker(42, 2), (2ULL << 58) | 42);
  ASSERT_ANY_THROW(sv.addMarker(1ULL << 60, 1));
  ASSERT_ANY_THROW(sv.addMarker(5, 3));

  ASSERT_EQ(sv.getMarker((1ULL << 58) | 42), 1);
  ASSERT_EQ(sv.getMarker((2ULL << 58) | 42), 2);
  ASSERT_EQ(sv.getMarker(42), 0);

  ASSERT_EQ(sv.getVocabIndex((1ULL << 58) | 42), 42);
  ASSERT_EQ(sv.getVocabIndex((2ULL << 58) | 42), 42);
  ASSERT_EQ(sv.getVocabIndex(1ULL << 58), 0);
  ASSERT_EQ(sv.getVocabIndex(2ULL << 58), 0);
  ASSERT_EQ(sv.getVocabIndex(0), 0);
  ASSERT_EQ(sv.getVocabIndex((1ULL << 58) - 1), (1ULL << 58) - 1);
  ASSERT_EQ(sv.getVocabIndex(42), 42);

  ASSERT_TRUE(sv.isSpecialVocabIndex((1ULL << 58) | 42));
  ASSERT_TRUE(sv.isSpecialVocabIndex((2ULL << 58) | 42));
  ASSERT_TRUE(sv.isSpecialVocabIndex(1ULL << 58));
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
  ASSERT_EQ(sv[2ULL << 58], "\"xyz\"^^<blabliblu>");
  ASSERT_EQ(sv[(2ULL << 58) | 1], "\"zzz\"^^<blabliblu>");
  ASSERT_EQ(sv[1ULL << 58], "\"xyz\"^^<http://example.com>");
}

// _____________________________________________________________________________
TEST(Vocabulary, SplitVocabularyItemAt) {
  HashSet<std::string> s;
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
  EXPECT_ANY_THROW(v[VocabIndex::make((1ULL << 59) | 42)]);

  auto idx = VocabIndex::make(1ULL << 59);
  ASSERT_EQ(v[idx],
            "\"LINESTRING(1 2, 3 4)\""
            "^^<http://www.opengis.net/ont/geosparql#wktLiteral>");
  idx = VocabIndex::make(static_cast<uint64_t>(1) << 59 | 1);
  ASSERT_EQ(v[idx],
            "\"POLYGON((1 2, 3 4))\""
            "^^<http://www.opengis.net/ont/geosparql#wktLiteral>");
}

// _____________________________________________________________________________
TEST(Vocabulary, SplitVocabularyWordWriterAndGetPosition) {
  // The word writer in the Vocabulary class runs the SplitGeoVocabulary word
  // writer. Its task is to split words to two different vocabularies for geo
  // and non-geo words. This split is tested here.
  RdfsVocabulary vocabulary;
  vocabulary.resetToType(geoSplitVocabType);
  auto wordCallback = vocabulary.makeWordWriterPtr("vocTest7.dat");

  // Call word writer
  ASSERT_EQ((*wordCallback)("\"a\"", true), 0);
  ASSERT_EQ((*wordCallback)("\"ab\"", true), 1);
  ASSERT_EQ(
      (*wordCallback)("\"LINESTRING(1 2, 3 4)\""
                      "^^<http://www.opengis.net/ont/geosparql#wktLiteral>",
                      true),
      (1ULL << 59));
  ASSERT_EQ((*wordCallback)("\"ba\"", true), 2);
  ASSERT_EQ((*wordCallback)("\"car\"@en", true), 3);
  ASSERT_EQ(
      (*wordCallback)("\"POLYGON((1 2, 3 4))\""
                      "^^<http://www.opengis.net/ont/geosparql#wktLiteral>",
                      true),
      (1ULL << 59) | 1);

  wordCallback->finish();

  vocabulary.readFromFile("vocTest7.dat");

  // Check that the resulting vocabulary is correct
  VocabIndex idx;
  ASSERT_TRUE(vocabulary.getId("\"a\"", &idx));
  ASSERT_EQ(idx.get(), 0);
  ASSERT_EQ(vocabulary[VocabIndex::make(0)], "\"a\"");
  ASSERT_TRUE(vocabulary.getId("\"ab\"", &idx));
  ASSERT_EQ(idx.get(), 1);
  ASSERT_EQ(vocabulary[VocabIndex::make(1)], "\"ab\"");
  ASSERT_TRUE(vocabulary.getId("\"ba\"", &idx));
  ASSERT_EQ(idx.get(), 2);
  ASSERT_EQ(vocabulary[VocabIndex::make(2)], "\"ba\"");
  ASSERT_TRUE(vocabulary.getId("\"car\"@en", &idx));
  ASSERT_EQ(idx.get(), 3);
  ASSERT_EQ(vocabulary[VocabIndex::make(3)], "\"car\"@en");

  ASSERT_TRUE(
      vocabulary.getId("\"LINESTRING(1 2, 3 4)\""
                       "^^<http://www.opengis.net/ont/geosparql#wktLiteral>",
                       &idx));
  ASSERT_EQ(idx.get(), 1ULL << 59);
  ASSERT_EQ(vocabulary[VocabIndex::make(1ULL << 59)],
            "\"LINESTRING(1 2, 3 4)\""
            "^^<http://www.opengis.net/ont/geosparql#wktLiteral>");

  ASSERT_TRUE(
      vocabulary.getId("\"POLYGON((1 2, 3 4))\""
                       "^^<http://www.opengis.net/ont/geosparql#wktLiteral>",
                       &idx));
  ASSERT_EQ(idx.get(), (1ULL << 59) | 1);
  ASSERT_EQ(vocabulary[VocabIndex::make((1ULL << 59) | 1)],
            "\"POLYGON((1 2, 3 4))\""
            "^^<http://www.opengis.net/ont/geosparql#wktLiteral>");

  ASSERT_FALSE(vocabulary.getId("\"xyz\"", &idx));
  ASSERT_ANY_THROW(vocabulary[VocabIndex::make(42)]);

  // Check that `getPositionOfWord` returns correct boundaries:
  //  - Non-existing normal word, at the end
  auto [l1, u1] = vocabulary.getPositionOfWord("\"xyz\"");
  ASSERT_EQ(l1, VocabIndex::make(4));
  ASSERT_EQ(u1, VocabIndex::make(4));

  //  - Non-existing normal word, not at the end
  auto [l2, u2] = vocabulary.getPositionOfWord("\"0\"");
  ASSERT_EQ(l2, VocabIndex::make(0));
  ASSERT_EQ(u2, VocabIndex::make(0));

  //  - Existing normal word
  auto [l3, u3] = vocabulary.getPositionOfWord("\"car\"@en");
  ASSERT_EQ(l3, VocabIndex::make(3));
  ASSERT_EQ(u3, VocabIndex::make(4));

  //  - Non-existing split word, not at the end
  auto [l4, u4] = vocabulary.getPositionOfWord(
      "\"POLYGON((0 0, 3 4))\""
      "^^<http://www.opengis.net/ont/geosparql#wktLiteral>");
  ASSERT_EQ(l4, VocabIndex::make((1ULL << 59) | 1));
  ASSERT_EQ(u4, VocabIndex::make((1ULL << 59) | 1));

  //  - Non-existing split word, at the end
  auto [l5, u5] = vocabulary.getPositionOfWord(
      "\"POLYGON((9 9, 9 9))\""
      "^^<http://www.opengis.net/ont/geosparql#wktLiteral>");
  ASSERT_EQ(l5, VocabIndex::make((1ULL << 59) | 2));
  ASSERT_EQ(u5, VocabIndex::make((1ULL << 59) | 2));

  //  - Existing split word
  auto [l6, u6] = vocabulary.getPositionOfWord(
      "\"POLYGON((1 2, 3 4))\""
      "^^<http://www.opengis.net/ont/geosparql#wktLiteral>");
  ASSERT_EQ(l6, VocabIndex::make((1ULL << 59) | 1));
  ASSERT_EQ(u6, VocabIndex::make((1ULL << 59) | 2));

  //  - Non-existing prefix of an existing split word
  auto [l7, u7] = vocabulary.getPositionOfWord("\"POLYGON((1 2, 3 4))");
  ASSERT_EQ(l7, VocabIndex::make(4));
  ASSERT_EQ(u7, VocabIndex::make(4));
}

// _____________________________________________________________________________
TEST(Vocabulary, SplitVocabularyWordWriterDestructor) {
  // Create a `SplitVocabulary::WordWriter` and destruct it without a call to
  // `finish()`.
  TwoSplitVocabulary sv1;
  auto wordWriter1 =
      sv1.makeDiskWriterPtr("SplitVocabularyWordWriterDestructor1.dat");
  (*wordWriter1)("\"abc\"", true);
  ASSERT_FALSE(wordWriter1->finishWasCalled());
  wordWriter1.reset();

  // Create a `SplitVocabulary::WordWriter` and destruct it after an explicit
  // call to `finish()`.
  TwoSplitVocabulary sv2;
  auto wordWriter2 =
      sv2.makeDiskWriterPtr("SplitVocabularyWordWriterDestructor2.dat");
  (*wordWriter2)("\"abc\"", true);
  wordWriter2->finish();
  ASSERT_TRUE(wordWriter2->finishWasCalled());
  wordWriter2.reset();
}

}  // namespace
