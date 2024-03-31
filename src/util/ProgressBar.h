// Copyright 2024, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Hannah Bast (bast@cs.uni-freiburg.de)

#pragma once

#include <absl/strings/str_cat.h>
#include <absl/strings/str_format.h>

#include <string>

#include "util/StringUtils.h"
#include "util/Timer.h"

// Default batch size for progress bar.
static constexpr size_t DEFAULT_PROGRESS_BAR_BATCH_SIZE = 10'000'000;

// Default function for computing speed descriptions.
static std::string DEFAULT_SPEED_DESCRIPTION_FUNCTION(
    size_t numSteps, ad_utility::Timer::Duration duration) {
  double durationSecs = ad_utility::Timer::toSeconds(duration);
  double speed = (numSteps / 1'000'000.0) / durationSecs;
  return absl::StrCat(absl::StrFormat("%.1f", speed), " M/s");
}

namespace ad_utility {

// A class that keeps track of the progress of a long-running computation which
// processed in (typically many and small) steps (for example, the lines of a
// large input file or the triples of a permutation). The total number of steps
// does not have to be known in advance.
//
// For a given number of steps, called the batch size, the class maintains
// various level statistics of the processing speed per batch (speed for the
// last batch, average speed, minimum speed, maximum speed, etc.). Note that
// the batch size is purely a parameter of this class, the actual computation
// need not proceed in batches in any way.
//
// Exampe usage:
//
// numTriplesProcessed = 0;
// ad_utility::ProgressBar progressBar(numTriplesProcessed,
//                                     "Triples processed: ", 10'000'000);
// while (...) {
//   // Code that does the processing.
//   ++numTriplesProcessed;
//   if (progressBar.update()) {
//     LOG(INFO) << progressBar.getProgressString() << std::flush;
//   }
// }
// LOG(INFO) << progressBar.getFinalProgressString() << std::flush;
//
class ProgressBar {
 public:
  // Use new line for each update (with `\n`) or one line overall (with `\r`).
  enum class DisplayUpdateOptions { UseNewLine, ReuseLine };
  static constexpr auto UseNewLine = DisplayUpdateOptions::UseNewLine;
  static constexpr auto ReuseLine = DisplayUpdateOptions::ReuseLine;

  // Function that returns a string with a speed description (e.g., "3.4 M/s")
  // from a number of steps and a `Timer::Duration`.
  using SpeedDescriptionFunction =
      std::function<std::string(size_t, Timer::Duration)>;

  // Create and initialize a progress bar.
  //
  // NOTE: The variable for counting the number of steps must come from the
  // outside (and be incremented there). That is because the calling code
  // typically has such a variable anyway (also for other purposes) and it
  // would we unnatural to have it originally in this class.
  ProgressBar(size_t& numStepsProcessed, std::string displayStringPrefix,
              size_t statisticsBatchSize = DEFAULT_PROGRESS_BAR_BATCH_SIZE,
              SpeedDescriptionFunction getSpeedDescription =
                  DEFAULT_SPEED_DESCRIPTION_FUNCTION,
              DisplayUpdateOptions displayUpdateOptions = ReuseLine)
      : numStepsProcessed_(numStepsProcessed),
        displayStringPrefix_(std::move(displayStringPrefix)),
        statisticsBatchSize_(statisticsBatchSize),
        getSpeedDescription_(std::move(getSpeedDescription)),
        displayUpdateOptions_(displayUpdateOptions) {}

  // Call this whenever a unit has been processed. Returns `true` if an update
  // should be displayed, `false` otherwise.
  //
  // IMPORTANT: This call is (and should) return `false` most of the time, in
  // which case it is (and should be) very cheap (namely, and increment and a
  // simple check).
  bool update() {
    if (numStepsProcessed_ < updateWhenThisManyStepsProcessed_) {
      return false;
    }
    Timer::Duration newDuration = timer_.value();
    lastBatchDuration_ = newDuration - totalDuration_;
    minBatchDuration_ = std::min(minBatchDuration_, lastBatchDuration_);
    maxBatchDuration_ = std::max(maxBatchDuration_, lastBatchDuration_);
    totalDuration_ = newDuration;
    updateWhenThisManyStepsProcessed_ += statisticsBatchSize_;
    return true;
  }

  // Progress string with statistics.
  std::string getProgressString() {
    bool notYetFinished = timer_.isRunning();
    auto& speed = getSpeedDescription_;
    auto withThousandSeparators = [](size_t number) {
      return ad_utility::insertThousandSeparator(std::to_string(number), ',');
    };
    // During the computation, always show the last multiple of the batch size.
    // In the end, show the exact (and final) number of processed steps.
    size_t numStepsProcessedShow_ =
        notYetFinished
            ? updateWhenThisManyStepsProcessed_ - statisticsBatchSize_
            : numStepsProcessed_;
    return absl::StrCat(
        displayStringPrefix_, withThousandSeparators(numStepsProcessedShow_),
        " [average speed ", speed(numStepsProcessed_, totalDuration_),
        ", last batch ", speed(statisticsBatchSize_, lastBatchDuration_),
        ", fastest ", speed(statisticsBatchSize_, minBatchDuration_),
        ", slowest ", speed(statisticsBatchSize_, maxBatchDuration_), "] ",
        displayUpdateOptions_ == ReuseLine && notYetFinished ? "\r" : "\n");
  }

  // Final progress string (should only be called once aftr the computation has
  // finished).
  std::string getFinalProgressString() {
    AD_CONTRACT_CHECK(timer_.isRunning(),
                      "`ProgressBar::getFinalProgressString()` should only be "
                      "called once after the computation has finished");
    timer_.stop();
    totalDuration_ = timer_.value();
    return getProgressString();
  }

 private:
  // The total number of units that have been processed so far.
  size_t& numStepsProcessed_;
  // The first part of the display string (e.g., "Triples processed: ").
  std::string displayStringPrefix_;
  // Update statistics every this many steps.
  size_t statisticsBatchSize_;
  // Function that returns a string with a speed description (e.g., "3.4 M/s")
  // given a number of steps and a `Timer::Duration`.
  SpeedDescriptionFunction getSpeedDescription_;
  // See `DisplayUpdateOptions` above.
  DisplayUpdateOptions displayUpdateOptions_;

  // Timer that is started as soon as this progress bar is created.
  Timer timer_{Timer::Started};
  // Update the statistics when at least this many steps have been processed.
  size_t updateWhenThisManyStepsProcessed_ = statisticsBatchSize_;

  // Duration of all batches so far.
  Timer::Duration totalDuration_ = Timer::Duration::zero();
  // Duration of last batch.
  Timer::Duration lastBatchDuration_;
  // Duration of fastest batch.
  Timer::Duration minBatchDuration_ = Timer::Duration::max();
  // Duration of slowest batch.
  Timer::Duration maxBatchDuration_ = Timer::Duration::min();
};

}  // namespace ad_utility
