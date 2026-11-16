// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_INDEX_CHUNKWISEPERMUTATIONWRITER_STATISTICS_H
#define QLEVER_SRC_INDEX_CHUNKWISEPERMUTATIONWRITER_STATISTICS_H

#include "index/chunkwisePermutationWriter/ChunkWriter.h"
#include "util/NoCopyNoMove.h"
#include "util/ProgressBar.h"
#include "util/Timer.h"

namespace chunkwisePermutationWriter {

#ifndef QLEVER_REDUCED_FEATURE_SET_FOR_CPP17

// The progress report and the timing statistics of the chunkwise permutation
// writer. The rows are counted by the chunks (see
// `ChunkWriterContext::numRowsProcessed_`), and the driving thread reports
// them via `reportProgress` while it waits for the chunks.
class Statistics : public ad_utility::NoCopyNoMove {
 public:
  // The total time that the chunks spent in the writer (measured by the
  // chunks, concurrently), and the time that the consolidation on the driving
  // thread spent waiting for the chunks and working.
  ad_utility::timer::ThreadSafeTimer chunkTimer_;
  ad_utility::Timer consolidationWaitTimer_{ad_utility::Timer::Stopped};
  ad_utility::Timer consolidationWorkTimer_{ad_utility::Timer::Stopped};

 private:
  const ChunkWriterContext& context_;
  // The progress bar needs a plain counter, so the atomic counter of the
  // `context_` is copied into this one for every report.
  size_t numRowsForProgressBar_ = 0;
  ad_utility::ProgressBar progressBar_{numRowsForProgressBar_,
                                       "Triples sorted: "};

 public:
  // Create the statistics for the chunks that share the `context`.
  explicit Statistics(const ChunkWriterContext& context) : context_{context} {}

  // Log the progress of the chunks (if the progress bar says so). Must only
  // be called by the driving thread.
  void reportProgress();

  // Log the final progress, the number of duplicates (if they were removed),
  // and the timers. Must only be called by the driving thread, after all the
  // chunks are done.
  void logSummary();
};

#endif  // QLEVER_REDUCED_FEATURE_SET_FOR_CPP17

}  // namespace chunkwisePermutationWriter

#endif  // QLEVER_SRC_INDEX_CHUNKWISEPERMUTATIONWRITER_STATISTICS_H
