// Copyright 2011-2023, University of Freiburg, Chair of Algorithms and Data
// Structures.
// Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#ifndef QLEVER_SRC_UTIL_TIMER_H
#define QLEVER_SRC_UTIL_TIMER_H

#include <atomic>
#include <chrono>

#include "util/Log.h"
#include "util/TypeTraits.h"

namespace ad_utility {
using namespace std::string_literals;
using namespace std::chrono_literals;

// This internal namespace is used such that we can use an alias for the rather
// long `std::chrono` namespace without leaking this alias into `ad_utility`.
// All the defined types are exported into the `ad_utility` namespace via
// `using` declarations.
namespace timer {
namespace chr = std::chrono;

// A simple class for time measurement.
class Timer {
 public:
  // Typedefs for some types from `std::chrono`. milli- and microseconds are
  // stored as integrals and seconds are stored as doubles. The timer internally
  // works with microseconds.
  using Microseconds = chr::microseconds;
  using Milliseconds = chr::milliseconds;
  using Seconds = chr::duration<double>;
  using Duration = Microseconds;
  using Clock = chr::steady_clock;
  using TimePoint = chr::time_point<Clock>;

  // A simple enum used in the constructor to decide whether the timer is
  // immediately started or not.
  enum class InitialStatus { Started, Stopped };
  // Allow the usage of `Timer::Started` and `Timer::Stopped`.
  // TODO<joka921, GCC 12.3> This could be `using enum InitialStatus`,
  // but that leads to an internal compiler error in GCC. I suspect that it is
  // this bug: https://gcc.gnu.org/bugzilla/show_bug.cgi?id=103081
  static constexpr InitialStatus Started = InitialStatus::Started;
  static constexpr InitialStatus Stopped = InitialStatus::Stopped;

  // Convert any `std::chrono::duration` to the underlying `Duration` type
  // of the `Timer` class.
  CPP_template(typename T)(
      requires ad_utility::isInstantiation<T, chr::duration>) static Duration
      toDuration(T duration) {
    return chr::duration_cast<Duration>(duration);
  }

  // Convert a `Duration` to seconds (as a plain `double`).
  // TODO<joka921> As soon as we have `std::format` or something similar that
  // allows for simple formatting of `std::chrono` types, these functions
  // also should return `std::chrono::duration` for more typesafety.
  static double toSeconds(Duration d) {
    return chr::duration_cast<Seconds>(d).count();
  }

 private:
  // The timer value (initially zero)
  Duration value_ = Duration::zero();
  TimePoint timeOfStart_;
  bool isRunning_ = false;

 public:
  Timer(InitialStatus initialStatus) {
    if (initialStatus == Started) {
      start();
    }
  }

  // Reset the timer value to zero and stops the measurement.
  void reset() {
    value_ = Duration::zero();
    isRunning_ = false;
  }

  // Reset the timer value to zero and starts the measurement.
  inline void start() {
    value_ = Duration::zero();
    timeOfStart_ = Clock::now();
    isRunning_ = true;
  }

  // Continue the measurement without resetting
  // the timer value (no effect if running)
  inline void cont() {
    if (isRunning_ == false) {
      timeOfStart_ = Clock::now();
      isRunning_ = true;
    }
  }

  // Stop the measurement.
  inline void stop() {
    if (isRunning_) {
      value_ += timeSinceLastStart();
      isRunning_ = false;
    }
  }
  // The following functions return the current time of the timer.
  // Note that this also works while the timer is running.
  Duration value() const {
    if (isRunning()) {
      return value_ + timeSinceLastStart();
    } else {
      return value_;
    }
  }

  chr::milliseconds msecs() const {
    return chr::duration_cast<Milliseconds>(value());
  }

  // is the timer currently running
  bool isRunning() const { return isRunning_; }

 private:
  Duration timeSinceLastStart() const {
    return toDuration(Clock::now() - timeOfStart_);
  }
};

// A timer that can be used in multithreaded contexts without additional
// synchronization. It consists of several single-threaded time measurements,
// the times of which are (atomically) summed up. This means that if some of the
// single-threaded measurements run in parallel, then the total time will be
// larger than the elapsed wall clock time.
class ThreadSafeTimer {
 private:
  using Rep = Timer::Duration::rep;
  std::atomic<Rep> totalTime_ = 0;
  // The implementation of the single-threaded measurements.
  class [[nodiscard(
      "This class measures the time between its construction and destruction. "
      "Not binding it to a variable thus is probably a bug")]] TimeMeasurement {
    Timer measuringTimer_{Timer::Started};
    ThreadSafeTimer* parentTimer_;
    bool isStopped_ = false;

   public:
    explicit TimeMeasurement(ThreadSafeTimer* timer)
        : measuringTimer_(Timer::Started), parentTimer_{timer} {}
    TimeMeasurement(const TimeMeasurement&) = delete;
    TimeMeasurement& operator=(const TimeMeasurement&) = delete;

    // Explicitly stop the measurement.
    void stop() {
      if (isStopped_) {
        return;
      }
      isStopped_ = true;
      measuringTimer_.stop();
      parentTimer_->totalTime_.fetch_add(measuringTimer_.value().count(),
                                         std::memory_order_release);
    }

    // Destructor. Implicitly stops the measurement.
    ~TimeMeasurement() { stop(); }
  };

 public:
  // Start a single-threaded time measurement. An explicit call to `stop` on the
  // returned object, or its destruction, will stop the time measurement, and
  // will add the elapsed wall clock to the total time of this
  // `ThreadSafeTimer`.
  TimeMeasurement startMeasurement() { return TimeMeasurement{this}; }

  // Return the summed time over all finished measurements.
  Timer::Duration value() const {
    return Timer::Duration{totalTime_.load(std::memory_order_acquire)};
  }

  // Return the summed time over all finished measurements, in milliseconds.
  Timer::Milliseconds msecs() const {
    return std::chrono::duration_cast<Timer::Milliseconds>(value());
  }
};

namespace detail {
// A helper struct that measures the time from its creation until its
// destruction and logs the time together with a specified message
// The callback can be used to change the logging mechanism. It must be
// callable with a `size_t` (the number of milliseconds) and `message`.
[[maybe_unused]] inline auto defaultLogger = [](chr::milliseconds msecs,
                                                std::string_view message) {
  LOG(TIMING) << message << " took " << msecs.count() << "ms" << std::endl;
};
template <typename Callback = decltype(defaultLogger)>
struct [[nodiscard(
    "TimeBlockAndLog objects are RAII types that always have to be bound to a "
    "variable")]] TimeBlockAndLog {
  Timer t_{Timer::Started};
  std::string message_;
  Callback callback_;
  explicit TimeBlockAndLog(std::string message, Callback callback = {})
      : message_{std::move(message)}, callback_{std::move(callback)} {
    t_.start();
  }

  // The semantics of copying/moving this class are unclear and copying/moving
  // is not needed for the typical usage, so those operations are deleted.
  TimeBlockAndLog(const TimeBlockAndLog&) = delete;
  TimeBlockAndLog& operator=(const TimeBlockAndLog&) = delete;
  ~TimeBlockAndLog() { callback_(t_.msecs(), message_); }
};
}  // namespace detail

#if LOGLEVEL >= TIMING
using detail::TimeBlockAndLog;
#else
<template typename T = int> struct TimeBlockAndLog {
  TimeBlockAndLog(const std::string&, T t = {}) {}
};
#endif

}  // namespace timer
using timer::TimeBlockAndLog;
using timer::Timer;
}  // namespace ad_utility

#endif  // QLEVER_SRC_UTIL_TIMER_H
