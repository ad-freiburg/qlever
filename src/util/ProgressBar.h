// Copyright 2024, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Hannah Bast (bast@cs.uni-freiburg.de)

#ifndef QLEVER_SRC_UTIL_PROGRESSBAR_H
#define QLEVER_SRC_UTIL_PROGRESSBAR_H

#include <absl/strings/str_cat.h>
#include <absl/strings/str_format.h>

#include <algorithm>
#include <mutex>
#include <ostream>
#include <string>

#include "util/Exception.h"
#include "util/Log.h"
#include "util/StringUtils.h"
#include "util/Timer.h"

// Default batch size for progress bar (not const so that we can change it in
// our tests).
inline size_t DEFAULT_PROGRESS_BAR_BATCH_SIZE = 10'000'000;

// Default function for computing speed descriptions.
inline std::string DEFAULT_SPEED_DESCRIPTION_FUNCTION(double stepsPerSecond) {
  return absl::StrCat(absl::StrFormat("%.1f", stepsPerSecond / 1e6), " M/s");
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
// Typical usage (note the `std::flush` at the end of the `AD_LOG_INFO` calls in
// order to ensure a flush for lines ending in `\r` instead of `\n`):
//
// numTriplesProcessed = 0;
// ad_utility::ProgressBar progressBar(numTriplesProcessed,
//                                     "Triples processed: ", 10'000'000);
// while (...) {
//   // Code that does the processing.
//   ++numTriplesProcessed;
//   if (progressBar.update()) {
//     AD_LOG_INFO << progressBar.getProgressString() << std::flush;
//   }
// }
// AD_LOG_INFO << progressBar.getFinalProgressString() << std::flush;
//
class ProgressBar {
 public:
  // Use new line for each update (with `\n`) or one line overall (with `\r`).
  enum class DisplayUpdateOptions { UseNewLine, ReuseLine };
  static constexpr auto UseNewLine = DisplayUpdateOptions::UseNewLine;
  static constexpr auto ReuseLine = DisplayUpdateOptions::ReuseLine;

  // Function that returns a string with a speed description (e.g., "3.4 M/s")
  // from a speed given as steps per second.
  using SpeedDescriptionFunction = std::function<std::string(double)>;

  // Create and initialize a progress bar.
  //
  // NOTE: The variable for counting the number of steps must come from the
  // outside (and be incremented there). That is because the calling code
  // typically has such a variable anyway (also for other purposes) and it
  // would we unnatural to have it originally in this class.
  CPP_template(typename SizeT)(requires ad_utility::SimilarTo<SizeT, size_t>)
      ProgressBar(SizeT& numStepsProcessed, std::string displayStringPrefix,
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
  std::string getProgressString() const {
    bool notYetFinished = timer_.isRunning();
    // Two helper functions.
    auto withThousandSeparators = [](size_t number) {
      return ad_utility::insertThousandSeparator(std::to_string(number), ',');
    };
    auto speed = [this](size_t numSteps, Timer::Duration duration) {
      return this->getSpeedDescription_(static_cast<double>(numSteps) /
                                        Timer::toSeconds(duration));
    };
    // In the typical use case, where the total number of steps is at least the
    // batch size, show the full statistics. Otherwise, only show the average
    // speed.
    if (numStepsProcessed_ >= statisticsBatchSize_) {
      // During the computation, always show the last multiple of the batch
      // size. In the end, show the exact number of processed steps.
      size_t numStepsProcessedShow =
          notYetFinished
              ? updateWhenThisManyStepsProcessed_ - statisticsBatchSize_
              : numStepsProcessed_;
      return absl::StrCat(
          displayStringPrefix_, withThousandSeparators(numStepsProcessedShow),
          " [average speed ", speed(numStepsProcessed_, totalDuration_),
          ", last batch ", speed(statisticsBatchSize_, lastBatchDuration_),
          ", fastest ", speed(statisticsBatchSize_, minBatchDuration_),
          ", slowest ", speed(statisticsBatchSize_, maxBatchDuration_), "] ",
          displayUpdateOptions_ == ReuseLine && notYetFinished ? "\r" : "\n");
    } else {
      return absl::StrCat(displayStringPrefix_,
                          withThousandSeparators(numStepsProcessed_),
                          " [average speed ",
                          speed(numStepsProcessed_, totalDuration_), "] \n");
    }
  }

  // Final progress string (should only be called once after the computation has
  // finished).
  std::string getFinalProgressString() {
    AD_CONTRACT_CHECK(!finished_,
                      "`ProgressBar::getFinalProgressString()` should only be "
                      "called once after the computation has finished");
    timer_.stop();
    totalDuration_ = timer_.value();
    finished_ = true;
    return getProgressString();
  }

  // Get timer. This is useful for more advanced use cases, where parts of the
  // processing should not be timed (for example, when it takes a long time
  // before the first step is processed and we do not want to include that).
  Timer& getTimer() { return timer_; }

 private:
  // The total number of units that have been processed so far.
  const size_t& numStepsProcessed_;
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
  // Finished yet or not.
  bool finished_ = false;
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

// A class for the same general goal as `ProgressBar` above (reporting progress
// of a long-running computation), but with two differences: (1) the total
// number of steps is known in advance, and (2) the computation is done by
// several threads that concurrently report their progress. Unlike for
// `ProgressBar`, the counter therefore lives inside the class, and the class
// writes its output (timestamped lines) to the given stream itself.
//
// Typical usage:
//
// ad_utility::ConcurrentProgressBar progressBar(
//     std::cout, "Triples processed: ", numTriplesTotal);
// std::vector<std::thread> threads;
// for (size_t i = 0; i < numThreads; ++i) {
//   threads.emplace_back([&]() {
//     while (...) {
//       // Code that processes one chunk of triples.
//       progressBar.add(chunk.size());
//     }
//   });
// }
// for (auto& thread : threads) {
//   thread.join();
// }
// progressBar.finish();
class ConcurrentProgressBar {
 public:
  // Create and initialize a concurrent progress bar that writes its progress
  // lines to `out`.
  //
  // NOTE: A `batchSize` of `0` (the default) means that the batch size is
  // chosen automatically, namely such that about 50 lines are written in
  // total, but at least one line every `DEFAULT_PROGRESS_BAR_BATCH_SIZE`
  // steps.
  ConcurrentProgressBar(std::ostream& out, std::string prefix,
                        size_t totalSteps, size_t batchSize = 0)
      : out_{out},
        prefix_{std::move(prefix)},
        totalSteps_{totalSteps},
        batchSize_{batchSize != 0 ? batchSize
                                  : std::max(DEFAULT_PROGRESS_BAR_BATCH_SIZE,
                                             totalSteps / 50)},
        nextPrintAt_{batchSize_} {}

  // Call this whenever one or more units have been processed. Threadsafe.
  //
  // IMPORTANT: Each call takes a lock, so callers in hot loops should
  // accumulate steps locally and report them in larger batches.
  void add(size_t numSteps) {
    std::lock_guard lock{mutex_};
    processed_ += numSteps;
    if (processed_ >= nextPrintAt_) {
      nextPrintAt_ = (processed_ / batchSize_ + 1) * batchSize_;
      print(false);
    }
  }

  // Write a final progress line with the total number of processed steps
  // (typically showing 100%), ended by a newline instead of a carriage
  // return. Should only be called once, after all threads have finished.
  void finish() {
    std::lock_guard lock{mutex_};
    print(true);
  }

 private:
  // Write one progress line; requires that `mutex_` is held. Like
  // `ProgressBar` with `ReuseLine`, intermediate updates end with `\r`, so
  // that a viewer of the stream shows them on one line that updates in
  // place; only the final line ends with `\n`. When a line is shorter than
  // its predecessor (e.g. because the average speed dropped by a digit), it
  // is padded with spaces to the widest line so far, so that the `\r`
  // overwrites all of it and no leftover characters remain.
  void print(bool final) {
    double seconds = Timer::toSeconds(timer_.value());
    // A phase with a total of zero steps is trivially complete.
    double percentage =
        totalSteps_ == 0
            ? 100.0
            : std::min(100.0, 100.0 * static_cast<double>(processed_) /
                                  static_cast<double>(totalSteps_));
    std::string line = absl::StrCat(
        prefix_, insertThousandSeparator(std::to_string(processed_), ','),
        " of ", insertThousandSeparator(std::to_string(totalSteps_), ','),
        absl::StrFormat(" (%.1f%%)", percentage), " [average speed ",
        DEFAULT_SPEED_DESCRIPTION_FUNCTION(static_cast<double>(processed_) /
                                           std::max(seconds, 0.001)),
        "]");
    maxLineWidth_ = std::max(maxLineWidth_, line.size());
    line.resize(maxLineWidth_, ' ');
    out_ << Log::getTimeStamp() << " - INFO: " << line << (final ? "\n" : "\r")
         << std::flush;
  }

  // The stream to which the progress lines are written (e.g., the log file
  // of a runtime index rebuild).
  std::ostream& out_;
  // The first part of each progress line (e.g., "Triples processed: ").
  std::string prefix_;
  // The total number of steps, known in advance.
  size_t totalSteps_;
  // Write a progress line every this many steps.
  size_t batchSize_;
  // Write the next progress line when this many steps have been processed.
  size_t nextPrintAt_;
  // The total number of units that have been processed so far.
  size_t processed_ = 0;
  // Timer that is started as soon as this progress bar is created.
  Timer timer_{Timer::Started};
  // Mutex that protects the counters above as well as the writes to `out_`.
  std::mutex mutex_;
  // The width of the widest line printed so far, used to pad shorter lines,
  // see `print`.
  size_t maxLineWidth_ = 0;
};

}  // namespace ad_utility

#endif  // QLEVER_SRC_UTIL_PROGRESSBAR_H
