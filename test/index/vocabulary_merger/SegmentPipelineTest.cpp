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

#include <atomic>
#include <memory>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include "../../util/GTestHelpers.h"
#include "VocabularyMergerTestHelpers.h"
#include "index/vocabulary_merger/IdMap.h"
#include "index/vocabulary_merger/SegmentPipeline.h"
#include "util/Exception.h"

using namespace ad_utility::vocabulary_merger;
using namespace ad_utility::vocabulary_merger::detail;
using namespace vocabularyMergerTestHelpers;
using ::testing::ElementsAre;
using ::testing::Pair;

namespace {
const ad_utility::RegexSet noRegexes{};

// A `BlockWriterBase` with blocks of a single word, so that every committed
// word is immediately handed to the block writer, and whose `AppendBlock`
// counts its calls in `numAppendCalls` and then throws.
class ThrowingAppendBlockWriter : public BlockWriterBase {
 private:
  std::atomic<size_t>& numAppendCalls_;

 public:
  explicit ThrowingAppendBlockWriter(std::atomic<size_t>& numAppendCalls)
      : numAppendCalls_{numAppendCalls} {}

  size_t blockSize() const override { return 1; }

  AppendBlock prepare([[maybe_unused]] WordBlock block) override {
    return [this]() {
      ++numAppendCalls_;
      throw std::runtime_error{"The vocabulary could not be written"};
    };
  }

 private:
  void finishImpl() override {}
};
}  // namespace

// Test that the merged blocks are cut into segments of at least the given
// number of words, but never between two blocks that share a word, and that
// the segments are committed in order: the words arrive at the writer in
// order, the ID maps map the occurrences of a word to its single global ID,
// and the metadata counts all the words.
TEST(SegmentPipeline, cutAndCommitSegments) {
  const std::string basename = gtestCurrentTestName();
  auto [filenames, cleanup] =
      makePartialVocabularyFilenamesInFreshDirectory(basename, 2);
  std::vector<std::pair<std::string, bool>> words;
  auto writer = makeParallelWriter(makeCollectingWordCallback(words));
  // The word `"b"` ends the first block and starts the second one, so the two
  // blocks have to end up in the same segment.
  auto blocks =
      makeMergedBlocks({{{"\"a\"", true, 0, 0}, {"\"b\"", true, 0, 1}},
                        {{"\"b\"", false, 1, 0}, {"\"c\"", true, 0, 2}},
                        {{"\"d\"", true, 0, 3}},
                        {},
                        {{"\"e\"", true, 0, 4}, {"\"f\"", true, 0, 5}}});
  VocabularyMetaData meta;
  {
    SegmentPipeline<decltype(lessThan)> pipeline{
        writer,
        partialVocabularyIdMapFilenames(basename, 2),
        2,
        noRegexes,
        lessThan,
        /*maxSegmentsInFlight=*/2,
        /*numWordsPerSegment=*/2};
    for (MergedBlock& block : blocks) {
      EXPECT_FALSE(pipeline.hasFailed());
      pipeline.push(std::move(block));
    }
    meta = pipeline.finish();
  }
  EXPECT_THAT(words, ElementsAre(Pair("\"a\"", true), Pair("\"b\"", true),
                                 Pair("\"c\"", true), Pair("\"d\"", true),
                                 Pair("\"e\"", true), Pair("\"f\"", true)));
  EXPECT_THAT(getIdMapFromFile(filenames.idMapFiles_[0]),
              ElementsAre(IdMapEntry{L(0), V(0)}, IdMapEntry{L(1), V(1)},
                          IdMapEntry{L(2), V(2)}, IdMapEntry{L(3), V(3)},
                          IdMapEntry{L(4), V(4)}, IdMapEntry{L(5), V(5)}));
  EXPECT_THAT(getIdMapFromFile(filenames.idMapFiles_[1]),
              ElementsAre(IdMapEntry{L(0), V(1)}));
  EXPECT_EQ(meta.numWordsTotal(), 6u);
  EXPECT_FALSE(writer.blockWriter(0).finishWasCalled());
}

// Test that an exception from the writer of the vocabulary is rethrown by
// `finish()`.
TEST(SegmentPipeline, exceptionOfWriterIsPropagated) {
  const std::string basename = gtestCurrentTestName();
  auto [filenames, cleanup] =
      makePartialVocabularyFilenamesInFreshDirectory(basename, 1);
  auto writer = makeParallelWriter([](std::string_view, bool) -> uint64_t {
    throw std::runtime_error{"The vocabulary could not be written"};
  });
  SegmentPipeline<decltype(lessThan)> pipeline{
      writer,
      partialVocabularyIdMapFilenames(basename, 1),
      1,
      noRegexes,
      lessThan,
      /*maxSegmentsInFlight=*/1,
      /*numWordsPerSegment=*/1};
  auto blocks =
      makeMergedBlocks({{{"\"a\"", true, 0, 0}}, {{"\"b\"", true, 0, 1}}});
  for (MergedBlock& block : blocks) {
    pipeline.push(std::move(block));
  }
  AD_EXPECT_THROW_WITH_MESSAGE_AND_TYPE(
      pipeline.finish(), ::testing::HasSubstr("could not be written"),
      std::runtime_error);
}

// Test that once a stage has failed, `push` ignores further blocks: it does not
// throw, the blocks do not reach the writer, and `finish()` rethrows the
// original exception.
TEST(SegmentPipeline, pushAfterFailureIsIgnored) {
  const std::string basename = gtestCurrentTestName();
  auto [filenames, cleanup] =
      makePartialVocabularyFilenamesInFreshDirectory(basename, 1);
  std::atomic<size_t> numAppendCalls = 0;
  SingleVocabularyParallelWriter writer{
      std::make_unique<ThrowingAppendBlockWriter>(numAppendCalls)};
  SegmentPipeline<decltype(lessThan)> pipeline{
      writer,
      partialVocabularyIdMapFilenames(basename, 1),
      1,
      noRegexes,
      lessThan,
      /*maxSegmentsInFlight=*/1,
      /*numWordsPerSegment=*/1};
  auto blocks = makeMergedBlocks({{{"\"a\"", true, 0, 0}},
                                  {{"\"b\"", true, 0, 1}},
                                  {{"\"c\"", true, 0, 2}},
                                  {{"\"d\"", true, 0, 3}}});
  // Pushing the second block commits the segment of the first one, whose
  // writing then fails.
  pipeline.push(std::move(blocks[0]));
  pipeline.push(std::move(blocks[1]));
  ASSERT_TRUE(waitUntil([&pipeline]() { return pipeline.hasFailed(); }));
  EXPECT_NO_THROW(pipeline.push(std::move(blocks[2])));
  EXPECT_NO_THROW(pipeline.push(std::move(blocks[3])));
  AD_EXPECT_THROW_WITH_MESSAGE_AND_TYPE(
      pipeline.finish(), ::testing::HasSubstr("could not be written"),
      std::runtime_error);
  EXPECT_EQ(numAppendCalls, 1u);
}

// Test that a pipeline without room for a single segment in flight is
// rejected.
TEST(SegmentPipeline, zeroSegmentsInFlightIsRejected) {
  const std::string basename = gtestCurrentTestName();
  auto [filenames, cleanup] =
      makePartialVocabularyFilenamesInFreshDirectory(basename, 1);
  size_t numWords = 0;
  auto writer = makeParallelWriter(makeCountingWordCallback(numWords));
  AD_EXPECT_THROW_WITH_MESSAGE(
      (SegmentPipeline<decltype(lessThan)>{
          writer, partialVocabularyIdMapFilenames(basename, 1), 1, noRegexes,
          lessThan, /*maxSegmentsInFlight=*/0}),
      ::testing::HasSubstr("maxSegmentsInFlight_ > 0"));
}

// Test that a pipeline with segments of zero words is rejected (the cutter
// would call `back()` on its empty vector of blocks).
TEST(SegmentPipeline, zeroWordsPerSegmentIsRejected) {
  const std::string basename = gtestCurrentTestName();
  auto [filenames, cleanup] =
      makePartialVocabularyFilenamesInFreshDirectory(basename, 1);
  size_t numWords = 0;
  auto writer = makeParallelWriter(makeCountingWordCallback(numWords));
  AD_EXPECT_THROW_WITH_MESSAGE(
      (SegmentPipeline<decltype(lessThan)>{
          writer, partialVocabularyIdMapFilenames(basename, 1), 1, noRegexes,
          lessThan, /*maxSegmentsInFlight=*/1, /*numWordsPerSegment=*/0}),
      ::testing::HasSubstr("numWordsPerSegment_ > 0"));
}

// Test that an exception from the building of a segment (here: a violated
// order, which the expensive checks detect) is rethrown on the thread that
// pushes the blocks.
TEST(SegmentPipeline, exceptionOfSegmentIsPropagated) {
  if constexpr (!ad_utility::areExpensiveChecksEnabled) {
    GTEST_SKIP() << "The order check is only done with expensive checks";
  }
  const std::string basename = gtestCurrentTestName();
  auto [filenames, cleanup] =
      makePartialVocabularyFilenamesInFreshDirectory(basename, 1);
  size_t numWords = 0;
  auto writer = makeParallelWriter(makeCountingWordCallback(numWords));
  SegmentPipeline<decltype(lessThan)> pipeline{
      writer,
      partialVocabularyIdMapFilenames(basename, 1),
      1,
      noRegexes,
      lessThan,
      /*maxSegmentsInFlight=*/1,
      /*numWordsPerSegment=*/1};
  auto blocks =
      makeMergedBlocks({{{"\"b\"", true, 0, 0}, {"\"a\"", true, 0, 1}}});
  pipeline.push(std::move(blocks[0]));
  AD_EXPECT_THROW_WITH_MESSAGE(
      pipeline.finish(), ::testing::HasSubstr("vocabulary order violated"));
}

// Test that an exception from the building of a segment is rethrown by the
// `push` that waits for that segment, and that the pipeline can then be
// destroyed while the next segment may still be being built (its destructor
// waits for that task, which uses the pipeline).
TEST(SegmentPipeline, exceptionOfSegmentInPush) {
  if constexpr (!ad_utility::areExpensiveChecksEnabled) {
    GTEST_SKIP() << "The order check is only done with expensive checks";
  }
  const std::string basename = gtestCurrentTestName();
  auto [filenames, cleanup] =
      makePartialVocabularyFilenamesInFreshDirectory(basename, 1);
  size_t numWords = 0;
  auto writer = makeParallelWriter(makeCountingWordCallback(numWords));
  SegmentPipeline<decltype(lessThan)> pipeline{
      writer,
      partialVocabularyIdMapFilenames(basename, 1),
      1,
      noRegexes,
      lessThan,
      /*maxSegmentsInFlight=*/2,
      /*numWordsPerSegment=*/1};
  // The first block violates the order. The `push` of the second block posts
  // the first segment, and the `push` of the third block posts the second
  // segment and then waits for the first one (two segments are in flight), so
  // the second segment may still be being built when the exception is thrown.
  // NOTE: If the first segment has already failed when the second block is
  // pushed, then that `push` already rethrows the exception.
  auto blocks =
      makeMergedBlocks({{{"\"b\"", true, 0, 0}, {"\"a\"", true, 0, 1}},
                        {{"\"c\"", true, 0, 2}},
                        {{"\"d\"", true, 0, 3}}});
  pipeline.push(std::move(blocks[0]));
  auto pushRemainingBlocks = [&pipeline, &blocks]() {
    pipeline.push(std::move(blocks[1]));
    pipeline.push(std::move(blocks[2]));
  };
  AD_EXPECT_THROW_WITH_MESSAGE(
      pushRemainingBlocks(), ::testing::HasSubstr("vocabulary order violated"));
}

// Test that the `SegmentCutter` cuts the blocks into segments of at least the
// given number of words, but never between two blocks that share a word, that
// it ignores empty blocks, and that `finish()` returns the task for the
// remaining blocks.
TEST(SegmentCutter, cutSegments) {
  size_t numWords = 0;
  auto writer = makeParallelWriter(makeCountingWordCallback(numWords));
  auto blocks =
      makeMergedBlocks({{{"\"a\"", true, 0, 0}, {"\"b\"", true, 0, 1}},
                        {{"\"b\"", false, 1, 0}, {"\"c\"", true, 0, 2}},
                        {{"\"d\"", true, 0, 3}},
                        {},
                        {{"\"e\"", true, 0, 4}, {"\"f\"", true, 0, 5}}});
  SegmentCutter<decltype(lessThan)> cutter{writer, noRegexes, 2, lessThan,
                                           /*numWordsPerSegment=*/2};
  EXPECT_FALSE(cutter.push(std::move(blocks[0])).has_value());
  // Enough words for a segment, but `"b"` must not span two segments.
  EXPECT_FALSE(cutter.push(std::move(blocks[1])).has_value());
  auto first = cutter.push(std::move(blocks[2]));
  ASSERT_TRUE(first.has_value());
  EXPECT_FALSE(cutter.push(std::move(blocks[3])).has_value());
  EXPECT_FALSE(cutter.push(std::move(blocks[4])).has_value());
  auto last = cutter.finish();
  ASSERT_TRUE(last.has_value());

  // The words `"a"`, `"b"`, `"c"`, and the words `"d"`, `"e"`, `"f"`.
  EXPECT_EQ((*first)()->numDistinctWords(), 3u);
  EXPECT_EQ((*last)()->numDistinctWords(), 3u);
}

// Test that `finish()` returns no task if there are no remaining blocks.
TEST(SegmentCutter, finishWithoutBlocks) {
  size_t numWords = 0;
  auto writer = makeParallelWriter(makeCountingWordCallback(numWords));
  SegmentCutter<decltype(lessThan)> cutter{writer, noRegexes, 1, lessThan, 1};
  EXPECT_FALSE(cutter.push(std::move(makeMergedBlocks({{}})[0])).has_value());
  EXPECT_FALSE(cutter.finish().has_value());
  EXPECT_EQ(cutter.segmentBusyTime(), std::chrono::milliseconds::zero());
}
