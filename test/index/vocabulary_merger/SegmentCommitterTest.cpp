// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Hannah Bast <bast@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <absl/strings/str_format.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

#include "../../util/GTestHelpers.h"
#include "VocabularyMergerTestHelpers.h"
#include "global/Constants.h"
#include "index/vocabulary_merger/IdMap.h"
#include "index/vocabulary_merger/Segment.h"
#include "index/vocabulary_merger/SegmentCommitter.h"

using namespace ad_utility::vocabulary_merger;
using namespace ad_utility::vocabulary_merger::detail;
using namespace vocabularyMergerTestHelpers;
using ::testing::ElementsAre;
using ::testing::Pair;

namespace {
// The basename of the partial vocabularies of the tests below.
const std::string partialVocabBasename = "segmentCommitterTest";

// Build the segment of a single block with the given `words`, all from the
// partial vocabulary `partial` with the local indices `0, 1, ...`, and all
// external.
std::shared_ptr<const Segment> makeSegment(
    const std::vector<std::string>& words, uint32_t partial,
    size_t numPartialVocabularies, ParallelWordWriterBase& writer) {
  std::vector<MergeBlock> blocks(1);
  for (size_t i = 0; i < words.size(); ++i) {
    pushWord(blocks[0], words[i], true, partial, i);
  }
  return std::make_shared<const Segment>(
      buildSegment(std::move(blocks), writer, ad_utility::RegexSet{},
                   numPartialVocabularies, lessThan));
}

// A `ParallelWordWriterBase` with a single sub-vocabulary whose block writer
// has small blocks (so that a few words span several blocks) and collects
// the words with their positions.
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
}  // namespace

// Test that the committer hands the words of consecutive segments to the
// block writer in blocks of its block size (across the segment boundaries),
// writes the ID maps with the global IDs, and accumulates the metadata (the
// number of words, the blank nodes, the prefix ranges and the special IDs
// shifted by the bases).
TEST(SegmentCommitter, commitSegments) {
  auto [filenames, cleanup] =
      makePartialVocabularyFilenamesInFreshDirectory(partialVocabBasename, 2);
  auto blockWriter = std::make_unique<SmallBlockWriter>();
  auto* blockWriterPtr = blockWriter.get();
  SingleVocabularyParallelWriter writer{std::move(blockWriter)};

  // Segment 0: three words and a blank node from partial vocabulary 0.
  // Segment 1: two words from partial vocabulary 1, one of them a special
  // internal IRI and one a language-tagged predicate (in the `std::less`
  // order: `"..."` < `<...>` < `@...` < `_:...`).
  std::string special{HAS_PATTERN_PREDICATE};
  auto segment0 =
      makeSegment({"\"a\"", "\"b\"", "\"c\"", "_:bn"}, 0, 2, writer);
  auto segment1 = makeSegment({special, "@en@<p>"}, 1, 2, writer);

  VocabularyMetaData meta;
  {
    SegmentCommitter committer{
        writer, partialVocabularyIdMapFilenames(partialVocabBasename, 2)};
    committer.commit(segment0);
    committer.commit(segment1);
    EXPECT_FALSE(committer.hasFailed());
    meta = committer.finish();
    writer.finish();
  }

  // The words, in order, with consecutive positions (which the
  // `BlockWriterFromCallback` checks).
  EXPECT_THAT(
      blockWriterPtr->words_,
      ElementsAre(Pair("\"a\"", true), Pair("\"b\"", true), Pair("\"c\"", true),
                  Pair(special, true), Pair("@en@<p>", true)));

  // The ID maps with the global IDs.
  EXPECT_THAT(getIdMapFromFile(filenames.idMapFiles_[0]),
              ElementsAre(IdMapEntry{L(0), V(0)}, IdMapEntry{L(1), V(1)},
                          IdMapEntry{L(2), V(2)}, IdMapEntry{L(3), BN(0)}));
  EXPECT_THAT(getIdMapFromFile(filenames.idMapFiles_[1]),
              ElementsAre(IdMapEntry{L(0), V(3)}, IdMapEntry{L(1), V(4)}));

  // The metadata.
  EXPECT_EQ(meta.numWordsTotal(), 5u);
  EXPECT_EQ(meta.getNextBlankNodeIndex(), 1u);
  EXPECT_EQ(meta.internalEntities().begin(), V(3));
  EXPECT_EQ(meta.internalEntities().end(), V(4));
  EXPECT_EQ(meta.langTaggedPredicates().begin(), V(4));
  EXPECT_EQ(meta.langTaggedPredicates().end(), V(5));
  EXPECT_EQ(meta.specialIdMapping().at(special), V(3));
}

// Test that an exception from the block writer (thrown on the appending
// thread) is rethrown by `finish()`, and that the committer stops.
TEST(SegmentCommitter, exceptionIsPropagated) {
  auto [filenames, cleanup] =
      makePartialVocabularyFilenamesInFreshDirectory(partialVocabBasename, 1);
  auto writer = makeParallelWriter([](std::string_view, bool) -> uint64_t {
    throw std::runtime_error{"The vocabulary could not be written"};
  });
  auto segment = makeSegment({"\"a\""}, 0, 1, writer);
  SegmentCommitter committer{
      writer, partialVocabularyIdMapFilenames(partialVocabBasename, 1)};
  committer.commit(segment);
  AD_EXPECT_THROW_WITH_MESSAGE_AND_TYPE(
      committer.finish(), ::testing::HasSubstr("could not be written"),
      std::runtime_error);
  EXPECT_TRUE(committer.hasFailed());
}
