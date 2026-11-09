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

#include <string>

#include "../../util/GTestHelpers.h"
#include "./ChunkWriterTestHelpers.h"
#include "index/chunkwisePermutationWriter/Statistics.h"
#include "util/ProgressBar.h"

// NOTE: The `Statistics` only exist in the C++20 mode of the build.
#ifndef QLEVER_REDUCED_FEATURE_SET_FOR_CPP17

namespace {
using namespace chunkWriterTestHelpers;
using ::testing::HasSubstr;
using ::testing::Not;

// Test that `reportProgress` logs the progress only once a full batch of the
// progress bar has been processed by the chunks, and that `logSummary` logs the
// final progress.
TEST(ChunkwisePermutationWriterStatistics, progressAndSummary) {
  TwinWriters writers{gtestCurrentTestName()};
  auto& context = writers.context_;
  Statistics statistics{context};
  auto [cleanup, logStream] = setGlobalLoggingStreamToStringStream();

  // Less than a batch of the progress bar, nothing is reported.
  context.numRowsProcessed_ = 1;
  statistics.reportProgress();
  EXPECT_EQ(logStream.str(), "");

  // Two full batches at once (which the chunks can process between two
  // reports), the progress is reported.
  context.numRowsProcessed_ = 2 * DEFAULT_PROGRESS_BAR_BATCH_SIZE;
  statistics.reportProgress();
  EXPECT_THAT(logStream.str(), HasSubstr("Triples sorted: "));

  logStream.str("");
  statistics.logSummary();
  EXPECT_THAT(logStream.str(), HasSubstr("Triples sorted: "));
  // The duplicates are not removed, so their numbers are not logged.
  EXPECT_THAT(logStream.str(), Not(HasSubstr("uniqueView")));
  writers.finish();
}

// Test that `logSummary` logs the number of duplicates if they are removed.
TEST(ChunkwisePermutationWriterStatistics, summaryWithDuplicateRemoval) {
  TwinWriters writers{gtestCurrentTestName(), qlever::KeyOrder{0, 1, 2, 3},
                      true};
  auto& context = writers.context_;
  context.numInputRows_ = 42;
  context.numUniqueRows_ = 37;
  context.numRowsProcessed_ = 37;
  Statistics statistics{context};
  auto [cleanup, logStream] = setGlobalLoggingStreamToStringStream();
  statistics.logSummary();
  EXPECT_THAT(logStream.str(),
              HasSubstr("Number of inputs to `uniqueView`: 42"));
  EXPECT_THAT(logStream.str(), HasSubstr("Number of unique elements: 37"));
  EXPECT_THAT(logStream.str(), HasSubstr("Triples sorted: 37"));
  writers.finish();
}

}  // namespace

#endif  // QLEVER_REDUCED_FEATURE_SET_FOR_CPP17
