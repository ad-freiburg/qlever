// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <absl/cleanup/cleanup.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <atomic>
#include <future>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "../../util/GTestHelpers.h"
#include "VocabularyMergerTestHelpers.h"
#include "index/vocabulary_merger/IdMap.h"
#include "index/vocabulary_merger/IdMapWriters.h"
#include "index/vocabulary_merger/Segment.h"
#include "util/AsioHelpers.h"
#include "util/GlobalExecutor.h"

using namespace ad_utility::vocabulary_merger;
using namespace ad_utility::vocabulary_merger::detail;
using namespace vocabularyMergerTestHelpers;
using ::testing::ElementsAre;

namespace {
// Build the segment of the given `blocks` (see `makeMergedBlocks`) for a
// vocabulary with `numPartialVocabularies` partial vocabularies.
std::shared_ptr<const Segment> makeSegmentOfBlocks(
    std::vector<MergedBlock> blocks, size_t numPartialVocabularies,
    ParallelWordWriterBase& writer) {
  return std::make_shared<const Segment>(
      buildSegment(std::move(blocks), writer, ad_utility::RegexSet{},
                   numPartialVocabularies, lessThan));
}

// A copy of the ID map entries of the `segment`, as `IdMapWriters::write`
// takes them.
std::shared_ptr<const SegmentIdMapEntries> entriesOf(
    const std::shared_ptr<const Segment>& segment) {
  return std::make_shared<const SegmentIdMapEntries>(
      segment->idMapEntries_.clone());
}
}  // namespace

// Test that the entries of the segments are written to the map of their
// partial vocabulary with the global IDs, in groups of consecutive maps.
TEST(IdMapWriters, writeSegments) {
  const std::string basename = gtestCurrentTestName();
  auto [filenames, cleanup] =
      makePartialVocabularyFilenamesInFreshDirectory(basename, 5);
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
    IdMapWriters writers{partialVocabularyIdMapFilenames(basename, 5), 2};
    EXPECT_EQ(writers.numPartialVocabularies(), 5u);
    // Two groups of three and two maps.
    EXPECT_EQ(writers.numGroups(), 2u);
    writers.write(entriesOf(segment0), converter0);
    writers.write(entriesOf(segment1), converter1);
    EXPECT_FALSE(writers.hasFailed());
    writers.finish();
    EXPECT_EQ(writers.numPendingTasks(), 0u);
  }

  // Each partial vocabulary has a single word with the local index `0`, whose
  // global index is `0` for `"a"`, `1` for `"b"`, and `2` for `"c"`.
  const std::vector<Id> expectedGlobalIds{V(0), V(2), V(0), V(2), V(1)};
  for (size_t i = 0; i < expectedGlobalIds.size(); ++i) {
    EXPECT_THAT(getIdMapFromFile(filenames.idMapFiles_[i]),
                ElementsAre(IdMapEntry{L(0), expectedGlobalIds[i]}));
  }
}

// Test that the number of groups is bounded by the number of maps.
TEST(IdMapWriters, numGroupsIsBoundedByNumMaps) {
  const std::string basename = gtestCurrentTestName();
  auto [filenames, cleanup] =
      makePartialVocabularyFilenamesInFreshDirectory(basename, 3);
  IdMapWriters writers{partialVocabularyIdMapFilenames(basename, 3), 100};
  EXPECT_EQ(writers.numGroups(), 3u);
  IdMapWriters oneGroup{partialVocabularyIdMapFilenames(basename, 3), 0};
  EXPECT_EQ(oneGroup.numGroups(), 1u);
}

// Test that a segment must have the entries of exactly as many partial
// vocabularies as there are maps.
TEST(IdMapWriters, writeChecksNumPartialVocabularies) {
  const std::string basename = gtestCurrentTestName();
  auto [filenames, cleanup] =
      makePartialVocabularyFilenamesInFreshDirectory(basename, 3);
  size_t numWords = 0;
  auto writer = makeParallelWriter(makeCountingWordCallback(numWords));
  auto segment = makeSegmentOfBlocks(
      makeMergedBlocks({{{"\"a\"", true, 4, 0}}}), 5, writer);
  IdMapWriters writers{partialVocabularyIdMapFilenames(basename, 3), 1};
  EXPECT_ANY_THROW(writers.write(entriesOf(segment), IdConverterForSegment{}));
  writers.finish();
}

// Test that once a task has failed, the tasks of the remaining segments as well
// as further calls to `write` are skipped, and that `finish()` rethrows the
// exception of the failed task.
TEST(IdMapWriters, failedTaskSkipsRemainingSegments) {
  const std::string basename = gtestCurrentTestName();
  auto [filenames, cleanup] =
      makePartialVocabularyFilenamesInFreshDirectory(basename, 1);
  size_t numWords = 0;
  auto writer = makeParallelWriter(makeCountingWordCallback(numWords));
  auto segment0 = makeSegmentOfBlocks(
      makeMergedBlocks({{{"\"a\"", true, 0, 0}}}), 1, writer);
  auto segment1 = makeSegmentOfBlocks(
      makeMergedBlocks({{{"\"b\"", true, 0, 1}}}), 1, writer);
  // A converter that makes every global index overflow, such that the task
  // that writes a segment with it throws.
  IdConverterForSegment overflowingConverter;
  overflowingConverter.numWordsInPreviousSegments_.fill(
      SegmentLocalId::maxIndex + 1);

  IdMapWriters writers{partialVocabularyIdMapFilenames(basename, 1), 1};

  // Block all the threads of the global executor, such that the tasks of both
  // segments are queued before the first of them runs. Otherwise the second
  // `write` might already see the failure and not queue its task at all.
  const size_t numThreads = ad_utility::globalExecutorNumThreads();
  std::promise<void> release;
  std::shared_future<void> released = release.get_future().share();
  std::atomic<size_t> numBlocked = 0;
  std::promise<void> allBlocked;
  std::vector<std::future<void>> blockers;
  for (size_t i = 0; i < numThreads; ++i) {
    blockers.push_back(ad_utility::runFunctionOnExecutor(
        ad_utility::globalExecutor(),
        [&numBlocked, &allBlocked, released, numThreads]() {
          if (++numBlocked == numThreads) {
            allBlocked.set_value();
          }
          released.wait();
        },
        ad_utility::net::use_future));
  }
  bool isReleased = false;
  auto releaseBlockers = [&]() {
    if (!isReleased) {
      isReleased = true;
      release.set_value();
    }
    for (auto& blocker : blockers) {
      blocker.get();
    }
    blockers.clear();
  };
  absl::Cleanup releaseOnError = releaseBlockers;
  allBlocked.get_future().wait();

  // The task of `segment0` fails, so the one of `segment1` must be skipped.
  writers.write(entriesOf(segment0), overflowingConverter);
  writers.write(entriesOf(segment1), IdConverterForSegment{});
  EXPECT_FALSE(writers.hasFailed());
  EXPECT_EQ(writers.numPendingTasks(), 2u);
  releaseBlockers();

  // Wait until the failure has been observed. A further `write` then doesn't
  // write anything either.
  while (!writers.hasFailed()) {
    std::this_thread::yield();
  }
  writers.write(entriesOf(segment1), IdConverterForSegment{});
  AD_EXPECT_THROW_WITH_MESSAGE(
      writers.finish(), ::testing::HasSubstr("SegmentLocalId::maxIndex"));
  EXPECT_THAT(getIdMapFromFile(filenames.idMapFiles_[0]), ::testing::IsEmpty());
}
