// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Hannah Bast <bast@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <string>
#include <vector>

#include "../../util/GTestHelpers.h"
#include "VocabularyMergerTestHelpers.h"
#include "global/Constants.h"
#include "index/vocabulary_merger/Segment.h"

using namespace ad_utility::vocabulary_merger::detail;
using namespace vocabularyMergerTestHelpers;
using ::testing::ElementsAre;
using ::testing::Pair;

namespace {
// Shorthand for a segment-local ID.
auto S = &makeSegmentLocalId;

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
  auto id = makeSegmentLocalId(3, 12345);
  EXPECT_EQ(subVocabularyOf(id), 3u);
  EXPECT_EQ(localIndexOf(id), 12345u);
  auto blank = makeSegmentLocalId(blankNodeSubVocabulary, 7);
  EXPECT_EQ(subVocabularyOf(blank), blankNodeSubVocabulary);
  EXPECT_EQ(localIndexOf(blank), 7u);
  EXPECT_ANY_THROW(makeSegmentLocalId(0, localIndexMask + 1));
}

// Test that the bases turn segment-local IDs into global indices and IDs.
TEST(Segment, segmentBases) {
  SegmentBases bases;
  bases.bases_[0] = 100;
  bases.bases_[1] = 20;
  bases.bases_[blankNodeSubVocabulary] = 5;
  EXPECT_EQ(bases.globalIndexOf(S(0, 3)), 103u);
  EXPECT_EQ(bases.globalIndexOf(S(1, 3)), 23u);
  EXPECT_EQ(bases.globalIdOf(S(0, 3)), V(103));
  EXPECT_EQ(bases.globalIdOf(S(1, 3)), V(23));
  EXPECT_EQ(bases.globalIdOf(S(blankNodeSubVocabulary, 2)), BN(7));
}

// Test `buildSegment` on two blocks of merged words with a word that spans
// the block boundary (with different external flags), a blank node, a WKT
// literal (which goes to the second sub-vocabulary of the `TestSplitWriter`),
// a language-tagged predicate and a special internal IRI: the distinct words
// per sub-vocabulary, the external flags, the blank node count, the metadata
// with segment-local IDs, the ID map entries grouped by partial vocabulary,
// and the bases of the next segment.
TEST(Segment, buildSegment) {
  std::string wkt =
      "\"POINT(1 2)\"^^<http://www.opengis.net/ont/geosparql#wktLiteral>";
  std::string special{HAS_PATTERN_PREDICATE};
  // The words in the order in which the merge yields them (the `std::less`
  // order): `"a"`, `"b"`, the WKT literal, `@en@<p>`, `<http://qlever...>`,
  // `_:bn`. The word `"b"` occurs in the partial vocabularies 0 (block 0,
  // not external) and 1 (block 1, external), the literal `"a"` in 0 and 2
  // (folded within the block).
  std::vector<std::vector<QueueWord>> blocks(2);
  auto a = makeQueueWord("\"a\"", true, 0, 5);
  a.moreOccurrences_.emplace_back(2, 0);
  blocks[0].push_back(std::move(a));
  blocks[0].push_back(makeQueueWord("\"b\"", false, 0, 6));
  blocks[1].push_back(makeQueueWord("\"b\"", true, 1, 1));
  blocks[1].push_back(makeQueueWord(wkt, true, 2, 1));
  blocks[1].push_back(makeQueueWord("@en@<p>", false, 1, 2));
  blocks[1].push_back(makeQueueWord(special, false, 0, 7));
  blocks[1].push_back(makeQueueWord("_:bn", false, 1, 3));

  TestSplitWriter writer;
  Segment segment = buildSegment(std::move(blocks), writer,
                                 ad_utility::RegexSet{}, 3, lessThan);

  // The words of the two sub-vocabularies. `"b"` is external because one of
  // its occurrences was.
  ASSERT_EQ(segment.words_.size(), 2u);
  const auto& main = segment.words_[0];
  ASSERT_EQ(main.numWords(), 4u);
  EXPECT_EQ(main.word(0), "\"a\"");
  EXPECT_EQ(main.word(1), "\"b\"");
  EXPECT_EQ(main.word(2), "@en@<p>");
  EXPECT_EQ(main.word(3), special);
  EXPECT_THAT(main.isExternal_, ElementsAre(true, true, false, false));
  const auto& geo = segment.words_[1];
  ASSERT_EQ(geo.numWords(), 1u);
  EXPECT_EQ(geo.word(0), wkt);
  EXPECT_EQ(segment.numBlankNodes_, 1u);
  EXPECT_EQ(segment.numMergedWords_, 7u);
  EXPECT_EQ(segment.numDistinctWords(), 6u);

  // The metadata with segment-local IDs: the geo literal has the marker of
  // the `TestSplitWriter`, the blank node is not part of the metadata.
  const auto& meta = segment.metaData_;
  EXPECT_EQ(meta.numWords_, 5u);
  EXPECT_TRUE(meta.langTaggedPredicates_.wasSeen_);
  EXPECT_EQ(meta.langTaggedPredicates_.first_, S(0, 2));
  EXPECT_EQ(meta.langTaggedPredicates_.last_, S(0, 2));
  EXPECT_TRUE(meta.internalEntities_.wasSeen_);
  EXPECT_EQ(meta.internalEntities_.first_, S(0, 3));
  EXPECT_THAT(meta.specialIds_, ElementsAre(Pair(special, S(0, 3))));

  // The ID map entries, grouped by partial vocabulary (and in word order
  // within a partial vocabulary).
  auto geoId = S(1, TestSplitWriter::geoMarker);
  auto blankId = S(blankNodeSubVocabulary, 0);
  EXPECT_THAT(segment.idMapEntries_,
              ElementsAre(entry(0, 5, S(0, 0)), entry(0, 6, S(0, 1)),
                          entry(0, 7, S(0, 3)), entry(1, 1, S(0, 1)),
                          entry(1, 2, S(0, 2)), entry(1, 3, blankId),
                          entry(2, 0, S(0, 0)), entry(2, 1, geoId)));
  EXPECT_THAT(segment.idMapRunStarts_, ElementsAre(0, 3, 6, 8));

  // The bases of the next segment.
  SegmentBases bases;
  bases.bases_[0] = 10;
  auto next = segment.basesOfNextSegment(bases);
  EXPECT_EQ(next.bases_[0], 14u);
  EXPECT_EQ(next.bases_[1], 1u);
  EXPECT_EQ(next.bases_[blankNodeSubVocabulary], 1u);
}

// Test that the IRIs that match one of the `blankNodeIriRegexes` become blank
// nodes, and that a violated order is detected (if the expensive checks are
// enabled).
TEST(Segment, blankNodeRegexesAndOrder) {
  std::vector<std::vector<QueueWord>> blocks(1);
  blocks[0].push_back(makeQueueWord("<http://ex/bn_1>", false, 0, 0));
  blocks[0].push_back(makeQueueWord("<http://ex/x>", false, 0, 1));
  ad_utility::RegexSet regexes{{"<http://ex/bn_.*>"}, "for the test"};
  auto writer = makeParallelWriter(
      makeCountingWordCallback(*std::make_unique<size_t>(0).release()));
  Segment segment =
      buildSegment(std::move(blocks), writer, regexes, 1, lessThan);
  EXPECT_EQ(segment.numBlankNodes_, 1u);
  EXPECT_EQ(segment.words_[0].numWords(), 1u);
  EXPECT_EQ(segment.words_[0].word(0), "<http://ex/x>");

  if constexpr (ad_utility::areExpensiveChecksEnabled) {
    std::vector<std::vector<QueueWord>> unordered(1);
    unordered[0].push_back(makeQueueWord("\"b\"", false, 0, 0));
    unordered[0].push_back(makeQueueWord("\"a\"", false, 0, 1));
    AD_EXPECT_THROW_WITH_MESSAGE(
        buildSegment(std::move(unordered), writer, regexes, 1, lessThan),
        ::testing::HasSubstr("vocabulary order violated"));
  }
}
