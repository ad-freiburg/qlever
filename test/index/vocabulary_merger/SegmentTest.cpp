// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Hannah Bast <bast@cs.uni-freiburg.de>, UFR
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <memory>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "../../util/GTestHelpers.h"
#include "VocabularyMergerTestHelpers.h"
#include "global/Constants.h"
#include "index/vocabulary_merger/Segment.h"

using namespace ad_utility::vocabulary_merger::detail;
using namespace vocabularyMergerTestHelpers;
using ::testing::AllOf;
using ::testing::ElementsAre;
using ::testing::Field;
using ::testing::Pair;

namespace {
// Shorthands for the segment-local ID of a word and of a blank node.
auto S = &SegmentLocalId::makeFromWordIndex;
auto B = &SegmentLocalId::makeBlankNode;
constexpr uint8_t blankNodeSubVocabularyIndex =
    SegmentLocalId::blankNodeSubVocabularyIndex;

// The ID converter with the given `numWordsInPreviousSegments_` per
// sub-vocabulary index (all others are zero).
IdConverterForSegment makeConverter(
    const std::vector<std::pair<uint8_t, uint64_t>>& counts) {
  IdConverterForSegment converter;
  for (const auto& [sub, count] : counts) {
    converter.numWordsInPreviousSegments_[sub] = count;
  }
  return converter;
}

// A block writer (see `BlockWriterBase` in
// `index/vocabulary/VocabularyTypes.h`) that precomputes a payload of one byte
// per word, the length of the word, and otherwise does nothing.
class PayloadBlockWriter : public BlockWriterBase {
 public:
  size_t blockSize() const override {
    return DEFAULT_WORDS_PER_VOCABULARY_BLOCK;
  }
  size_t precomputedPayloadSize() const override { return 1; }
  void precomputePayload(std::string_view word,
                         ql::span<char> payload) const override {
    payload[0] = static_cast<char>(word.size());
  }
  AppendBlock prepare([[maybe_unused]] WordBlock block) override {
    return []() {};
  }

 private:
  void finishImpl() override {}
};

// Matcher for a `LocalIdRange` that has seen the words from `first` to
// `last`.
auto localIdRange(SegmentLocalId first, SegmentLocalId last) {
  using Range = ad_utility::vocabulary_merger::SegmentMetaData::LocalIdRange;
  return AllOf(Field(&Range::wasSeen_, true), Field(&Range::first_, first),
               Field(&Range::last_, last));
}

// The entry of the ID map of the partial vocabulary `partial` for its word
// with the local index `localIndex`, which is the merged word with the
// segment-local ID `id`.
SegmentIdMapEntry entry(uint32_t partial, uint32_t localIndex,
                        SegmentLocalId id) {
  return SegmentIdMapEntry{partial, localIndex, id};
}
}  // namespace

// Test the packing of a sub-vocabulary and an index into a segment-local ID.
TEST(Segment, segmentLocalId) {
  // The ID of a word: the sub-vocabulary index in the upper bits, the index in
  // the lower bits.
  auto id = S(3, 12345);
  EXPECT_EQ(id.subVocabIndex(), 3u);
  EXPECT_EQ(id.index(), 12345u);
  EXPECT_FALSE(id.isBlankNode());
  EXPECT_EQ(id.bits(), (uint64_t{3} << SegmentLocalId::numIndexBits) | 12345u);

  // The ID of a blank node: the pseudo sub-vocabulary with all upper bits set.
  auto blank = B(7);
  EXPECT_EQ(blank.subVocabIndex(), blankNodeSubVocabularyIndex);
  EXPECT_EQ(blank.index(), 7u);
  EXPECT_TRUE(blank.isBlankNode());
  EXPECT_EQ(blank.bits(), (uint64_t{blankNodeSubVocabularyIndex}
                           << SegmentLocalId::numIndexBits) |
                              7u);
  EXPECT_EQ(blank, B(7));
  EXPECT_NE(blank, id);

  // The index has to fit into its bits, and a word is never in the pseudo
  // sub-vocabulary of the blank nodes.
  EXPECT_ANY_THROW(S(0, SegmentLocalId::maxIndex + 1));
  EXPECT_ANY_THROW(B(SegmentLocalId::maxIndex + 1));
  EXPECT_ANY_THROW(S(blankNodeSubVocabularyIndex, 0));

  // The printers for failed tests, of an ID and of an ID map entry.
  std::stringstream str;
  str << id;
  EXPECT_EQ(str.str(), "SegmentLocalId(3, 12345)");
  str.str("");
  str << entry(2, 7, S(1, 3));
  EXPECT_EQ(str.str(), "{2, 7, SegmentLocalId(1, 3)}");
}

// Test that the ID converter turns segment-local IDs into global indices and
// IDs.
TEST(Segment, idConverterForSegment) {
  auto converter =
      makeConverter({{0, 100}, {1, 20}, {blankNodeSubVocabularyIndex, 5}});
  EXPECT_EQ(converter.globalIndexOf(S(0, 3)), 103u);
  EXPECT_EQ(converter.globalIndexOf(S(1, 3)), 23u);
  EXPECT_EQ(converter.globalIdOf(S(0, 3)), V(103));
  EXPECT_EQ(converter.globalIdOf(S(1, 3)), V(23));
  EXPECT_EQ(converter.globalIdOf(B(2)), BN(7));
}

// Test `buildSegment` on three blocks of merged words with a word that spans
// the first block boundary (with different external flags) and a different
// word at the second, a blank node, a WKT literal (which goes to the second
// sub-vocabulary of the `TestSplitWriter`), two language-tagged predicates,
// a special internal IRI and an internal IRI that is not special: the
// distinct words per sub-vocabulary, the external flags, the blank node count,
// the metadata with segment-local IDs, the ID map entries grouped by partial
// vocabulary, and the ID converter of the next segment.
TEST(Segment, buildSegment) {
  std::string wkt =
      "\"POINT(1 2)\"^^<http://www.opengis.net/ont/geosparql#wktLiteral>";
  std::string special{HAS_PATTERN_PREDICATE};
  std::string internal =
      std::string{QLEVER_INTERNAL_PREFIX_IRI_WITHOUT_CLOSING_BRACKET} + "zzz>";
  // The words in the order in which the merge yields them (the `std::less`
  // order): the WKT literal, `"a"`, `"b"`, the two `<http://qlever...>` IRIs,
  // `@en@<p>`, `@en@<q>`, `_:bn`. The word `"b"` occurs in the partial
  // vocabularies 0 (block 0, not external) and 1 (block 1, external), the
  // literal `"a"` in 0 and 2 (folded within the block).
  // NOTE: The second `"a"` is folded into the first one by `makeMergedBlocks`,
  // as the merge does it.
  auto blocks = makeMergedBlocks(
      {{{wkt, true, 2, 1},
        {"\"a\"", true, 0, 5},
        {"\"a\"", false, 2, 0},
        {"\"b\"", false, 0, 6}},
       {{"\"b\"", true, 1, 1}, {special, false, 0, 7}, {internal, false, 2, 2}},
       {{"@en@<p>", false, 1, 2},
        {"@en@<q>", false, 0, 8},
        {"_:bn", false, 1, 3}}});

  TestSplitWriter writer;
  Segment segment = buildSegment(std::move(blocks), writer,
                                 ad_utility::RegexSet{}, 3, lessThan);

  // The words of the two sub-vocabularies. `"b"` is external because one of
  // its occurrences was.
  ASSERT_EQ(segment.wordBlocks_.size(), 2u);
  const auto& main = segment.wordBlocks_[0];
  EXPECT_THAT(main.words(), ElementsAre("\"a\"", "\"b\"", special, internal,
                                        "@en@<p>", "@en@<q>"));
  EXPECT_THAT(main.isExternal_,
              ElementsAre(true, true, false, false, false, false));
  EXPECT_THAT(segment.wordBlocks_[1].words(), ElementsAre(wkt));
  EXPECT_EQ(segment.numBlankNodes_, 1u);
  EXPECT_EQ(segment.numDistinctWords(), 8u);

  // The metadata with segment-local IDs: the geo literal has the marker of
  // the `TestSplitWriter`, the blank node is not part of the metadata, and
  // only the special internal IRI is a special ID.
  const auto& meta = segment.metaData_;
  EXPECT_EQ(meta.numWords_, 7u);
  EXPECT_THAT(meta.langTaggedPredicates_, localIdRange(S(0, 4), S(0, 5)));
  EXPECT_THAT(meta.internalEntities_, localIdRange(S(0, 2), S(0, 3)));
  EXPECT_THAT(meta.specialIds_, ElementsAre(Pair(special, S(0, 2))));

  // The ID map entries, grouped by partial vocabulary (and in word order
  // within a partial vocabulary).
  auto geoId = S(1, TestSplitWriter::geoMarker);
  auto blankId = B(0);
  const auto& idMap = segment.idMapEntries_;
  ASSERT_EQ(idMap.size(), 3u);
  EXPECT_THAT(idMap[0],
              ElementsAre(entry(0, 5, S(0, 0)), entry(0, 6, S(0, 1)),
                          entry(0, 7, S(0, 2)), entry(0, 8, S(0, 5))));
  EXPECT_THAT(idMap[1], ElementsAre(entry(1, 1, S(0, 1)), entry(1, 2, S(0, 4)),
                                    entry(1, 3, blankId)));
  EXPECT_THAT(idMap[2], ElementsAre(entry(2, 1, geoId), entry(2, 0, S(0, 0)),
                                    entry(2, 2, S(0, 3))));

  // The ID converter of the next segment.
  auto next = segment.idConverterForNextSegment(makeConverter({{0, 10}}));
  EXPECT_EQ(next.numWordsInPreviousSegments_,
            makeConverter({{0, 16}, {1, 1}, {blankNodeSubVocabularyIndex, 1}})
                .numWordsInPreviousSegments_);
}

// Test that the IRIs that match one of the `blankNodeIriRegexes` become blank
// nodes, and that a violated order is detected (if the expensive checks are
// enabled).
TEST(Segment, blankNodeRegexesAndOrder) {
  auto blocks = makeMergedBlocks(
      {{{"<http://ex/bn_1>", false, 0, 0}, {"<http://ex/x>", false, 0, 1}}});
  ad_utility::RegexSet regexes{{"<http://ex/bn_.*>"}, "for the test"};
  size_t numWords = 0;
  auto writer = makeParallelWriter(makeCountingWordCallback(numWords));
  Segment segment =
      buildSegment(std::move(blocks), writer, regexes, 1, lessThan);
  EXPECT_EQ(segment.numBlankNodes_, 1u);
  EXPECT_THAT(segment.wordBlocks_[0].words(), ElementsAre("<http://ex/x>"));

  if constexpr (ad_utility::areExpensiveChecksEnabled) {
    auto unordered =
        makeMergedBlocks({{{"\"b\"", false, 0, 0}, {"\"a\"", false, 0, 1}}});
    AD_EXPECT_THROW_WITH_MESSAGE(
        buildSegment(std::move(unordered), writer, regexes, 1, lessThan),
        ::testing::HasSubstr("vocabulary order violated"));
  }
}

// Test that `buildSegment` computes the payload of every word into its word
// block if the block writer of the sub-vocabulary has one (see
// `BlockWriterBase::precomputePayload`).
TEST(Segment, buildSegmentPrecomputesPayload) {
  auto blocks =
      makeMergedBlocks({{{"\"a\"", false, 0, 0}, {"<abc>", false, 0, 1}}});
  SingleVocabularyParallelWriter writer{std::make_unique<PayloadBlockWriter>()};
  Segment segment = buildSegment(std::move(blocks), writer,
                                 ad_utility::RegexSet{}, 1, lessThan);
  const auto& block = segment.wordBlocks_.at(0);
  EXPECT_THAT(block.words(), ElementsAre("\"a\"", "<abc>"));
  EXPECT_EQ(block.payloadSize_, 1u);
  EXPECT_EQ(block.payload(0), std::string_view("\x03", 1));
  EXPECT_EQ(block.payload(1), std::string_view("\x05", 1));
}
