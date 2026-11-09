// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "index/chunkwisePermutationWriter/Statistics.h"

#ifndef QLEVER_REDUCED_FEATURE_SET_FOR_CPP17

#include "util/Log.h"
#include "util/ParseableDuration.h"

namespace chunkwisePermutationWriter {

// _____________________________________________________________________________
void Statistics::reportProgress() {
  numRowsForProgressBar_ = context_.numRowsProcessed_.load();
  // NOTE: The counter may have advanced by several batches since the last
  // report, hence the loop.
  while (progressBar_.update()) {
    AD_LOG_INFO << progressBar_.getProgressString() << std::flush;
  }
}

// _____________________________________________________________________________
void Statistics::logSummary() {
  if (context_.removeDuplicates_) {
    AD_LOG_INFO << "Number of inputs to `uniqueView`: "
                << context_.numInputRows_.load() << '\n';
    AD_LOG_INFO << "Number of unique elements: "
                << context_.numUniqueRows_.load() << std::endl;
  }
  numRowsForProgressBar_ = context_.numRowsProcessed_.load();
  AD_LOG_INFO << progressBar_.getFinalProgressString() << std::flush;
  // Format the time measured by a `timer` for the log.
  auto toDuration = [](const auto& timer) {
    return ad_utility::ParseableDuration<std::chrono::milliseconds>{
        timer.msecs()};
  };
  AD_LOG_TIMING << "Total time of the chunks of the permutation writer "
                << toDuration(chunkTimer_) << std::endl;
  AD_LOG_TIMING << "Time the consolidation spent waiting for the chunks "
                << toDuration(consolidationWaitTimer_) << std::endl;
  AD_LOG_TIMING << "Time the consolidation spent working "
                << toDuration(consolidationWorkTimer_) << std::endl;
}

}  // namespace chunkwisePermutationWriter

#endif  // QLEVER_REDUCED_FEATURE_SET_FOR_CPP17
