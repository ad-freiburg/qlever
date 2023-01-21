// Copyright 2011-2023, University of Freiburg, Chair of Algorithms and Data
// Structures.
// Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>
#pragma once

#include <sys/time.h>
#include <sys/types.h>

#include <chrono>
#include <iomanip>
#include <memory>
#include <sstream>

#include "absl/strings/str_cat.h"
#include "util/Log.h"
#include "util/Synchronized.h"
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
  using TimePoint = chr::time_point<chr::high_resolution_clock>;

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
  template <ad_utility::isInstantiation<chr::duration> T>
  static Duration toDuration(T duration) {
    return chr::duration_cast<Duration>(duration);
  }

  // Convert a `Duration` to seconds (as a plain `double`).
  // TODO<joka921> As soon as we have `std::format` or something similar that
  // allows for simple formatting of `std::chrono` types, these functions
  // also should return `std::chrono::duration` for more typesafety.
  static double toSeconds(Duration d) {
    return chr::duration_cast<Seconds>(d).count();
  }

  // Convert a `Duration` to milliseconds (as a plain `size_t`).
  static size_t toMilliseconds(Duration d) {
    return chr::duration_cast<Milliseconds>(d).count();
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
    timeOfStart_ = chr::high_resolution_clock::now();
    isRunning_ = true;
  }

  // Continue the measurement without resetting
  // the timer value (no effect if running)
  inline void cont() {
    if (isRunning_ == false) {
      timeOfStart_ = chr::high_resolution_clock::now();
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

  size_t msecs() const { return toMilliseconds(value()); }

  // is the timer currently running
  bool isRunning() const { return isRunning_; }

 private:
  Duration timeSinceLastStart() const {
    auto now = chr::high_resolution_clock::now();
    return toDuration(now - timeOfStart_);
  }
};

/// An exception signalling a timeout
class TimeoutException : public std::exception {
 public:
  TimeoutException(std::string message) : message_{std::move(message)} {}
  const char* what() const noexcept override { return message_.c_str(); }

 private:
  std::string message_;
};

/// A timer which also can be given a timeout value and queried whether it
/// has timed out
class TimeoutTimer : public Timer {
 public:
  /// Factory function for a timer that never times out
  static TimeoutTimer unlimited() { return TimeoutTimer(UnlimitedTag{}); }

  template <ad_utility::isInstantiation<chr::duration> T>
  TimeoutTimer(T timeLimit, Timer::InitialStatus status)
      : Timer{status}, timeLimit_{toDuration(timeLimit)} {}

  /// Did this timer already timeout
  /// Can't be const because of the internals of the Timer class.
  bool hasTimedOut() {
    if (isUnlimited_) {
      return false;
    } else {
      return value() > timeLimit_;
    }
  }

  // Check if this timer has timed out. If the timer has timed out, throws a
  // TimeoutException. Else, nothing happens.
  void checkTimeoutAndThrow(std::string_view additionalMessage = {}) {
    if (hasTimedOut()) {
      double seconds =
          std::chrono::duration_cast<Timer::Seconds>(timeLimit_).count();
      std::stringstream numberStream;
      // Seconds with three digits after the decimal point.
      // TODO<C++20> : Use std::format for formatting, it is much more readable.
      numberStream << std::setprecision(3) << std::fixed << seconds;
      throw TimeoutException{absl::StrCat(
          additionalMessage, "A Timeout occured. The time limit was "s,
          std::move(numberStream).str(), " seconds"s)};
    }
  }

  // Overload that does not take an error message directly, but a callable that
  // creates the error message lazily when the timeout occurs. This can be used
  // to make calling this function cheaper in the typical "no timeout" case.
  template <typename F>
  requires std::is_invocable_r_v<std::string_view, F>
  void checkTimeoutAndThrow(F&& f) {
    if (hasTimedOut()) {
      checkTimeoutAndThrow(f());
    }
  }

  Duration remainingTime() const {
    if (isUnlimited_) {
      return Duration::max();
    }
    auto passedTime = value();
    return passedTime < timeLimit_ ? timeLimit_ - passedTime : Duration::zero();
  }

 private:
  Timer::Duration timeLimit_ = Timer::Duration::zero();
  bool isUnlimited_ = false;  // never times out
  class UnlimitedTag {};
  TimeoutTimer(UnlimitedTag) : Timer{Timer::Started}, isUnlimited_{true} {}
};

namespace detail {
// A helper struct that measures the time from its creation until its
// destruction and logs the time together with a specified message
// The callback can be used to change the logging mechanism. It must be
// callable with a `size_t` (the number of milliseconds) and `message`.
[[maybe_unused]] inline auto defaultLogger = [](size_t msecs,
                                                std::string_view message) {
  LOG(TIMING) << message << " took " << msecs << "ms" << std::endl;
};
template <typename Callback = decltype(defaultLogger)>
struct TimeBlockAndLog {
  Timer t_{Timer::Started};
  std::string message_;
  Callback callback_;
  TimeBlockAndLog(std::string message, Callback callback = {})
      : message_{std::move(message)}, callback_{std::move(callback)} {
    t_.start();
  }
  ~TimeBlockAndLog() {
    auto msecs = Timer::toMilliseconds(t_.value());
    callback_(msecs, message_);
  }
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
using timer::TimeoutException;
using timer::TimeoutTimer;
using timer::Timer;

/// A threadsafe timeout timer
using ConcurrentTimeoutTimer =
    ad_utility::Synchronized<TimeoutTimer, std::mutex>;

/// A shared ptr to a threadsafe timeout timer
using SharedConcurrentTimeoutTimer = std::shared_ptr<ConcurrentTimeoutTimer>;
}  // namespace ad_utility
