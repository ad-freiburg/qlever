// Copyright 2024, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Hannah Bast (bast@cs.uni-freiburg.de)

#ifndef QLEVER_SRC_UTIL_PROGRESSBAR_H
#define QLEVER_SRC_UTIL_PROGRESSBAR_H

#include <absl/strings/str_cat.h>
#include <absl/strings/str_format.h>

#include <algorithm>
#include <functional>
#include <mutex>
#include <optional>
#include <string>

#include "util/Exception.h"
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
// proceeds in (typically many and small) steps (for example, the lines of a
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
  // would be unnatural to have it originally in this class.
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
  // which case it is (and should be) very cheap (namely, an increment and a
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

// A class for the same general goal as `ProgressBar` above (reporting the
// progress of a long-running computation), but with two differences:
//
// 1. The total number of steps is known in advance, so the progress can be
// shown as a percentage of the total, together with the average speed; no need
// to show the last, fastest, and slowest batch speeds like for `ProgressBar`.
//
// 2. The computation is done by several threads that concurrently report their
// progress. The progress counter therefore lives inside this class, and the
// progress string is only handed out via a lock-holding `Update` object, so
// that the output of concurrent updates can neither interleave nor overtake
// each other.
//
// Typical usage, analogous to the usage of `ProgressBar` above; in particular,
// the `std::flush` at the end of the `AD_LOG_INFO` calls is important.
//
// ad_utility::ConcurrentProgressBar progressBar("Triples processed: ",
//                                               numTriplesTotal);
// std::vector<std::thread> threads;
// for (size_t i = 0; i < numThreads; ++i) {
//   threads.emplace_back([&]() {
//     while (...) {
//       // Code that processes one chunk of triples.
//       progressBar.add(chunk.size());
//       if (auto update = progressBar.update()) {
//         AD_LOG_INFO << update->getProgressString() << std::flush;
//       }
//     }
//   });
// }
// for (auto& thread : threads) {
//   thread.join();
// }
// AD_LOG_INFO << progressBar.getFinalProgressString() << std::flush;
//
class ConcurrentProgressBar {
 public:
  using SpeedDescriptionFunction = ProgressBar::SpeedDescriptionFunction;
  using DisplayUpdateOptions = ProgressBar::DisplayUpdateOptions;

  // The result of a successful `update()`: provides the progress string, and
  // holds a lock until it is destroyed, so that the display of this update
  // cannot interleave with (or be overtaken by) the display of updates from
  // other threads. Hence write the string while the `Update` object is still
  // alive, as in the typical usage above.
  class Update {
   public:
    const std::string& getProgressString() const { return progressString_; }

   private:
    friend class ConcurrentProgressBar;
    Update(std::unique_lock<std::mutex> displayLock, std::string progressString)
        : displayLock_{std::move(displayLock)},
          progressString_{std::move(progressString)} {}
    std::unique_lock<std::mutex> displayLock_;
    std::string progressString_;
  };

  // Create and initialize a concurrent progress bar.
  ConcurrentProgressBar(
      std::string displayStringPrefix, size_t totalSteps,
      size_t statisticsBatchSize = DEFAULT_PROGRESS_BAR_BATCH_SIZE,
      SpeedDescriptionFunction getSpeedDescription =
          DEFAULT_SPEED_DESCRIPTION_FUNCTION,
      DisplayUpdateOptions displayUpdateOptions = ProgressBar::ReuseLine)
      : displayStringPrefix_(std::move(displayStringPrefix)),
        totalSteps_(totalSteps),
        statisticsBatchSize_(statisticsBatchSize),
        getSpeedDescription_(std::move(getSpeedDescription)),
        displayUpdateOptions_(displayUpdateOptions) {}

  // Call this whenever one or more units have been processed. Threadsafe.
  //
  // IMPORTANT: Each call takes a lock, so callers in hot loops should
  // accumulate steps locally and report them in larger batches.
  void add(size_t numSteps) {
    std::lock_guard lock{countMutex_};
    numStepsProcessed_ += numSteps;
  }

  // Call this after `add`. Returns an `Update` if an update should be
  // displayed and this thread is the one that should display it, and
  // `std::nullopt` otherwise. Threadsafe.
  std::optional<Update> update() {
    {
      std::lock_guard lock{countMutex_};
      if (numStepsProcessed_ < updateWhenThisManyStepsProcessed_) {
        return std::nullopt;
      }
      // This thread claims the display; other threads get `std::nullopt`
      // until the next multiple of the batch size is reached.
      updateWhenThisManyStepsProcessed_ =
          (numStepsProcessed_ / statisticsBatchSize_ + 1) *
          statisticsBatchSize_;
    }
    // NOTE: The progress string is composed after acquiring the display lock
    // (and from the current count, not from the count at the time of the
    // claim above), so that a display that happens to be delayed can never
    // show a smaller count than its predecessor.
    std::unique_lock displayLock{displayMutex_};
    return Update{std::move(displayLock), getProgressStringImpl(false)};
  }

  // Final progress string (should only be called once, after all threads
  // have finished). Always ends with a newline.
  std::string getFinalProgressString() {
    AD_CONTRACT_CHECK(!finished_,
                      "`ConcurrentProgressBar::getFinalProgressString()` "
                      "should only be called once after the computation has "
                      "finished");
    timer_.stop();
    finished_ = true;
    std::unique_lock displayLock{displayMutex_};
    return getProgressStringImpl(true);
  }

 private:
  // Compose one progress string; requires that `displayMutex_` is held. Like
  // for `ProgressBar` with `ReuseLine`, intermediate updates end with `\r`,
  // so that a viewer of the output shows them on one line that updates in
  // place; only the final string ends with `\n`. When a string is shorter
  // than its predecessor (e.g., because the average speed dropped by a
  // digit), it is padded with spaces to the widest string so far, so that
  // the `\r` overwrites all of it and no leftover characters remain.
  std::string getProgressStringImpl(bool final) {
    size_t numStepsProcessed;
    {
      std::lock_guard lock{countMutex_};
      numStepsProcessed = numStepsProcessed_;
    }
    // A total of zero steps is trivially complete.
    double percentage =
        totalSteps_ == 0
            ? 100.0
            : std::min(100.0, 100.0 * static_cast<double>(numStepsProcessed) /
                                  static_cast<double>(totalSteps_));
    auto withThousandSeparators = [](size_t number) {
      return ad_utility::insertThousandSeparator(std::to_string(number), ',');
    };
    std::string progressString = absl::StrCat(
        displayStringPrefix_, withThousandSeparators(numStepsProcessed), " of ",
        withThousandSeparators(totalSteps_),
        absl::StrFormat(" (%.1f%%)", percentage), " [average speed ",
        getSpeedDescription_(static_cast<double>(numStepsProcessed) /
                             std::max(Timer::toSeconds(timer_.value()), 0.001)),
        "]");
    bool reuseLine = displayUpdateOptions_ == ProgressBar::ReuseLine;
    if (reuseLine) {
      maxStringWidth_ = std::max(maxStringWidth_, progressString.size());
      progressString.resize(maxStringWidth_, ' ');
    }
    return absl::StrCat(progressString, reuseLine && !final ? "\r" : "\n");
  }

  // The first part of the display string (e.g., "Triples processed: ").
  std::string displayStringPrefix_;
  // The total number of steps, known in advance.
  size_t totalSteps_;
  // Produce a progress string every this many steps.
  size_t statisticsBatchSize_;
  // Function that returns a string with a speed description (e.g., "3.4
  // M/s") given a speed in steps per second.
  SpeedDescriptionFunction getSpeedDescription_;
  // See `ProgressBar::DisplayUpdateOptions`.
  DisplayUpdateOptions displayUpdateOptions_;

  // Timer that is started as soon as this progress bar is created.
  Timer timer_{Timer::Started};
  // Finished yet or not.
  bool finished_ = false;
  // The total number of units that have been processed so far. Protected by
  // `countMutex_`.
  size_t numStepsProcessed_ = 0;
  // Produce the next progress string when at least this many steps have been
  // processed. Protected by `countMutex_`.
  size_t updateWhenThisManyStepsProcessed_ = statisticsBatchSize_;
  // The width of the widest progress string so far, used to pad shorter
  // strings, see `getProgressStringImpl`. Protected by `displayMutex_`.
  size_t maxStringWidth_ = 0;
  // Mutex that protects the two counters above. It is taken on every `add`,
  // so it must never be held while an update is displayed.
  std::mutex countMutex_;
  // Mutex that is held while an update is displayed (via the `Update` class
  // above), so that concurrent displays cannot interleave.
  std::mutex displayMutex_;
};
}  // namespace ad_utility

#endif  // QLEVER_SRC_UTIL_PROGRESSBAR_H
