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

#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "VocabularyMergerTestHelpers.h"
#include "global/Constants.h"
#include "index/vocabulary_merger/Segment.h"
#include "index/vocabulary_merger/SegmentCommitter.h"

using namespace ad_utility::vocabulary_merger;
using namespace ad_utility::vocabulary_merger::detail;
using namespace vocabularyMergerTestHelpers;
using ::testing::ElementsAre;
using ::testing::Pair;

namespace {
// A block writer (for a `ParallelWordWriterBase` with a single sub-vocabulary)
// with small blocks, so that a few words already span several blocks.
class SmallBlockWriter : public BlockWriterFromCallback<
                             std::function<uint64_t(std::string_view, bool)>> {
 public:
  std::vector<std::pair<std::string, bool>> words_;
  SmallBlockWriter()
      : BlockWriterFromCallback<
            std::function<uint64_t(std::string_view, bool)>>{
            makeCollectingWordCallback(words_)} {}
  size_t blockSize() const override { return 3; }
};

// The words of the `block` together with their `isExternal` flags.
std::vector<std::pair<std::string, bool>> wordsOf(const BlockPieces& block) {
  WordBlock words = gatherWords(block);
  std::vector<std::pair<std::string, bool>> result;
  for (size_t i = 0; i < words.numWords(); ++i) {
    result.emplace_back(words.word(i), words.isExternal_[i]);
  }
  return result;
}
}  // namespace

// Test that the committer cuts the words of consecutive segments into blocks
// of the block size of the block writer (across the segment boundaries, the
// last block being partial), returns the ID converter of each segment, and
// accumulates the metadata (the number of words, the blank nodes, the prefix
// ranges and the special IDs shifted by the bases).
// _____________________________________________________________________________
TEST(SegmentCommitter, commitSegments) {
  SingleVocabularyParallelWriter writer{std::make_unique<SmallBlockWriter>()};

  // Segment 0: two words and a blank node from partial vocabulary 0.
  // Segment 1: two words from partial vocabulary 1, one of them a special
  // internal IRI and one a language-tagged predicate (in the `std::less`
  // order: `"..."` < `<...>` < `@...` < `_:...`).
  std::string special{HAS_PATTERN_PREDICATE};
  auto segment0 = makeSegment({"\"a\"", "\"b\"", "_:bn"}, 0, 2, writer);
  auto segment1 = makeSegment({special, "@en@<p>"}, 1, 2, writer);

  std::vector<std::pair<uint8_t, BlockPieces>> blocks;
  SegmentCommitter committer{writer, [&blocks](uint8_t sub, BlockPieces block) {
                               blocks.emplace_back(sub, std::move(block));
                             }};
  EXPECT_EQ(committer.numSubVocabularies(), 1u);
  auto converter0 = committer.commit(segment0);
  // The first block is not full yet.
  ASSERT_EQ(blocks.size(), 0u);
  auto converter1 = committer.commit(segment1);
  // The first block is full after the first word of the second segment.
  ASSERT_EQ(blocks.size(), 1u);
  VocabularyMetaData meta = committer.finish();
  // The last, partial block is handed to the sink by `finish()`.
  ASSERT_EQ(blocks.size(), 2u);

  // The blocks, with their positions and words.
  EXPECT_EQ(blocks[0].first, 0u);
  EXPECT_EQ(blocks[0].second.firstPosition_, 0u);
  EXPECT_EQ(blocks[0].second.numWords_, 3u);
  EXPECT_THAT(wordsOf(blocks[0].second),
              ElementsAre(Pair("\"a\"", true), Pair("\"b\"", true),
                          Pair(special, true)));
  EXPECT_EQ(blocks[1].first, 0u);
  EXPECT_EQ(blocks[1].second.firstPosition_, 3u);
  EXPECT_EQ(blocks[1].second.numWords_, 1u);
  EXPECT_THAT(wordsOf(blocks[1].second), ElementsAre(Pair("@en@<p>", true)));
  // The first block spans both segments.
  EXPECT_EQ(blocks[0].second.pieces_.size(), 2u);
  EXPECT_EQ(blocks[1].second.pieces_.size(), 1u);

  // The ID converters turn the segment-local IDs into the global ones.
  const auto& entries0 = segment0->idMapEntries_[0];
  ASSERT_EQ(entries0.size(), 3u);
  EXPECT_EQ(converter0.globalIdOf(entries0[0].id_), V(0));
  EXPECT_EQ(converter0.globalIdOf(entries0[1].id_), V(1));
  EXPECT_EQ(converter0.globalIdOf(entries0[2].id_), BN(0));
  const auto& entries1 = segment1->idMapEntries_[1];
  ASSERT_EQ(entries1.size(), 2u);
  EXPECT_EQ(converter1.globalIdOf(entries1[0].id_), V(2));
  EXPECT_EQ(converter1.globalIdOf(entries1[1].id_), V(3));

  // The metadata.
  EXPECT_EQ(meta.numWordsTotal(), 4u);
  EXPECT_EQ(meta.getNextBlankNodeIndex(), 1u);
  EXPECT_EQ(meta.internalEntities().begin(), V(2));
  EXPECT_EQ(meta.internalEntities().end(), V(3));
  EXPECT_EQ(meta.langTaggedPredicates().begin(), V(3));
  EXPECT_EQ(meta.langTaggedPredicates().end(), V(4));
  EXPECT_EQ(meta.specialIdMapping().at(special), V(2));
}
