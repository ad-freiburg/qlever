// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <vector>

#include "VocabularyMergerTestHelpers.h"
#include "index/vocabulary_merger/IdMap.h"
#include "index/vocabulary_merger/IdMapWriters.h"
#include "index/vocabulary_merger/Segment.h"

using namespace ad_utility::vocabulary_merger;
using namespace ad_utility::vocabulary_merger::detail;
using namespace vocabularyMergerTestHelpers;
using ::testing::ElementsAre;

namespace {
// The basename of the partial vocabularies of the tests below.
const std::string partialVocabBasename = "idMapWritersTest";

// Build the segment of the given `blocks` (see `makeMergedBlocks`) for a
// vocabulary with `numPartialVocabularies` partial vocabularies.
std::shared_ptr<const Segment> makeSegmentOfBlocks(
    std::vector<MergedBlock> blocks, size_t numPartialVocabularies,
    ParallelWordWriterBase& writer) {
  return std::make_shared<const Segment>(
      buildSegment(std::move(blocks), writer, ad_utility::RegexSet{},
                   numPartialVocabularies, lessThan));
}
}  // namespace

// Test that the entries of the segments are written to the map of their
// partial vocabulary with the global IDs, in groups of consecutive maps.
TEST(IdMapWriters, writeSegments) {
  auto [filenames, cleanup] =
      makePartialVocabularyFilenamesInFreshDirectory(partialVocabBasename, 5);
  size_t numWords = 0;
  auto writer = makeParallelWriter(makeCountingWordCallback(numWords));
  // Segment 0: `"a"` in the partial vocabularies 0 and 2, `"b"` in 4.
  // Segment 1: `"c"` in the partial vocabularies 1 and 3.
  auto segment0 =
      makeSegmentOfBlocks(makeMergedBlocks({{{"\"a\"", true, 0, 0},
                                             {"\"a\"", true, 2, 0},
                                             {"\"b\"", true, 4, 0}}}),
                          5, writer);
  auto segment1 = makeSegmentOfBlocks(
      makeMergedBlocks({{{"\"c\"", true, 1, 0}, {"\"c\"", true, 3, 0}}}), 5,
      writer);
  IdConverterForSegment converter0;
  IdConverterForSegment converter1 =
      segment0->idConverterForNextSegment(converter0);

  {
    IdMapWriters writers{
        partialVocabularyIdMapFilenames(partialVocabBasename, 5), 2};
    EXPECT_EQ(writers.numPartialVocabularies(), 5u);
    // Two groups of three and two maps.
    EXPECT_EQ(writers.numGroups(), 2u);
    writers.write(segment0, converter0);
    writers.write(segment1, converter1);
    EXPECT_FALSE(writers.hasFailed());
    writers.finish();
  }

  EXPECT_THAT(getIdMapFromFile(filenames.idMapFiles_[0]),
              ElementsAre(IdMapEntry{L(0), V(0)}));
  EXPECT_THAT(getIdMapFromFile(filenames.idMapFiles_[1]),
              ElementsAre(IdMapEntry{L(0), V(2)}));
  EXPECT_THAT(getIdMapFromFile(filenames.idMapFiles_[2]),
              ElementsAre(IdMapEntry{L(0), V(0)}));
  EXPECT_THAT(getIdMapFromFile(filenames.idMapFiles_[3]),
              ElementsAre(IdMapEntry{L(0), V(2)}));
  EXPECT_THAT(getIdMapFromFile(filenames.idMapFiles_[4]),
              ElementsAre(IdMapEntry{L(0), V(1)}));
}

// Test that the number of groups is bounded by the number of maps.
TEST(IdMapWriters, numGroupsIsBoundedByNumMaps) {
  auto [filenames, cleanup] =
      makePartialVocabularyFilenamesInFreshDirectory(partialVocabBasename, 3);
  IdMapWriters writers{partialVocabularyIdMapFilenames(partialVocabBasename, 3),
                       100};
  EXPECT_EQ(writers.numGroups(), 3u);
  IdMapWriters oneGroup{
      partialVocabularyIdMapFilenames(partialVocabBasename, 3), 0};
  EXPECT_EQ(oneGroup.numGroups(), 1u);
}
