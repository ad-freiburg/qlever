// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Hannah Bast <bast@cs.uni-freiburg.de>, UFR
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <absl/cleanup/cleanup.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "../../util/GTestHelpers.h"
#include "VocabularyMergerTestHelpers.h"
#include "global/Constants.h"
#include "index/vocabulary_merger/Segment.h"
#include "index/vocabulary_merger/SegmentCommitter.h"
#include "util/ProgressBar.h"

using namespace ad_utility::vocabulary_merger;
using namespace ad_utility::vocabulary_merger::detail;
using namespace vocabularyMergerTestHelpers;
using ::testing::ElementsAre;
using ::testing::HasSubstr;
using ::testing::Pair;

namespace {
// A block writer (for a `ParallelWordWriterBase` with a single sub-vocabulary)
// with small blocks (of `blockSize` words), so that a few words already span
// several blocks.
class SmallBlockWriter : public BlockWriterFromCallback<
                             std::function<uint64_t(std::string_view, bool)>> {
 public:
  std::vector<std::pair<std::string, bool>> words_;

 private:
  size_t blockSize_;

 public:
  explicit SmallBlockWriter(size_t blockSize = 3)
      : BlockWriterFromCallback<std::function<uint64_t(
            std::string_view, bool)>>{makeCollectingWordCallback(words_)},
        blockSize_{blockSize} {}
  size_t blockSize() const override { return blockSize_; }
};

// A `BlockSink` that collects the blocks, together with the index of their
// sub-vocabulary, in `blocks`.
auto collectingSink(std::vector<std::pair<uint8_t, BlockPieces>>& blocks) {
  return [&blocks](uint8_t sub, BlockPieces block) {
    blocks.emplace_back(sub, std::move(block));
  };
}

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

// Test that the committer rejects a block writer with a block size of zero and
// a segment whose number of sub-vocabularies differs from the one of the
// writer.
// _____________________________________________________________________________
TEST(SegmentCommitter, contractChecks) {
  SingleVocabularyParallelWriter zeroBlockSizeWriter{
      std::make_unique<SmallBlockWriter>(0)};
  AD_EXPECT_THROW_WITH_MESSAGE(
      (SegmentCommitter{zeroBlockSizeWriter, [](uint8_t, BlockPieces) {}}),
      HasSubstr("blockSize"));

  SingleVocabularyParallelWriter writer{std::make_unique<SmallBlockWriter>()};
  TestSplitWriter splitWriter;
  auto segment = makeSegment({"\"a\""}, 0, 1, splitWriter);
  ASSERT_EQ(segment->wordBlocks_.size(), 2u);
  SegmentCommitter committer{writer, [](uint8_t, BlockPieces) {}};
  AD_EXPECT_THROW_WITH_MESSAGE(committer.commit(segment),
                               HasSubstr("numSubVocabularies()"));
}

// Test that the committer never hands an empty block to its sink: neither
// when no segment was committed at all, nor when the words fill the last block
// exactly. Also test that `gatherWords` of a block without pieces is empty.
// _____________________________________________________________________________
TEST(SegmentCommitter, noEmptyBlocks) {
  SingleVocabularyParallelWriter writer{std::make_unique<SmallBlockWriter>()};
  {
    std::vector<std::pair<uint8_t, BlockPieces>> blocks;
    SegmentCommitter committer{writer, collectingSink(blocks)};
    VocabularyMetaData meta = committer.finish();
    EXPECT_TRUE(blocks.empty());
    EXPECT_EQ(meta.numWordsTotal(), 0u);
  }
  {
    std::vector<std::pair<uint8_t, BlockPieces>> blocks;
    SegmentCommitter committer{writer, collectingSink(blocks)};
    committer.commit(makeSegment({"\"a\"", "\"b\"", "\"c\""}, 0, 1, writer));
    // The block is full, so it is handed to the sink right away.
    ASSERT_EQ(blocks.size(), 1u);
    VocabularyMetaData meta = committer.finish();
    // There is no partial block left for `finish()`.
    ASSERT_EQ(blocks.size(), 1u);
    EXPECT_EQ(blocks[0].second.numWords_, 3u);
    EXPECT_THAT(wordsOf(blocks[0].second),
                ElementsAre(Pair("\"a\"", true), Pair("\"b\"", true),
                            Pair("\"c\"", true)));
    EXPECT_EQ(meta.numWordsTotal(), 3u);
  }

  BlockPieces emptyBlock{{}, 0, 42};
  WordBlock words = gatherWords(emptyBlock);
  EXPECT_EQ(words.numWords(), 0u);
  EXPECT_EQ(words.firstPosition_, 42u);
}

// Test that the progress bar of the committer counts the committed words.
// _____________________________________________________________________________
TEST(SegmentCommitter, progressBar) {
  // Use a small batch size, so that the progress bar is updated after a few
  // words already.
  size_t previousBatchSize = std::exchange(DEFAULT_PROGRESS_BAR_BATCH_SIZE, 2);
  absl::Cleanup restoreBatchSize{[previousBatchSize] {
    DEFAULT_PROGRESS_BAR_BATCH_SIZE = previousBatchSize;
  }};

  SingleVocabularyParallelWriter writer{std::make_unique<SmallBlockWriter>()};
  std::vector<std::pair<uint8_t, BlockPieces>> blocks;
  SegmentCommitter committer{writer, collectingSink(blocks)};
  committer.commit(makeSegment({"\"a\"", "\"b\""}, 0, 2, writer));
  committer.commit(makeSegment({"\"c\"", "\"d\""}, 1, 2, writer));
  committer.finish();
  EXPECT_THAT(committer.progressBar().getFinalProgressString(),
              HasSubstr("Words merged: 4"));
}
