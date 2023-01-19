// Copyright 2011, University of Freiburg, Chair of Algorithms and Data
// Structures.
#pragma once

#include <sys/time.h>
#include <sys/types.h>

#include <iomanip>
#include <memory>
#include <sstream>
#include <chrono>

#include "absl/strings/str_cat.h"
#include "util/Log.h"
#include "util/Synchronized.h"

// Björn 01Jun11: Copied this class from the CompleteSearch
// code in order to use it in the semantic search, too.
// Adapted header guard, namespace, etc.
namespace ad_utility {
using namespace std::string_literals;
using namespace std::chrono_literals;
// HOLGER 22Jan06 : changed all suseconds_t to off_t
//
//! A SIMPLE CLASS FOR TIME MEASUREMENTS.
//
namespace timer {
  namespace chr = std::chrono;
class Timer {
 public:
  using microseconds = chr::microseconds;
  using milliseconds = chr::milliseconds;
  using secondsInt = chr::seconds;
  using secondsDouble = chr::duration<double>;
  using Duration = microseconds;
  using TimePoint = chr::time_point<chr::high_resolution_clock>;
  Duration toDuration(const auto& duration) {
    return chr::duration_cast<Duration>(duration);
  }
 private:
  //! The timer value (initially zero)
  Duration value_ = Duration::zero();

    TimePoint timeOfStart_;

    //! Indicates whether a measurement is running.
    bool _running;

   public:
    //! The default constructor.
    Timer() { reset(); }

    //! Resets the timer value to zero and stops the measurement.
    void reset() {
      value_ = 0us;
      _running = false;
    }

    //! Resets the timer value to zero and starts the measurement.
    inline void start() {
      value_ = Duration::zero();
      timeOfStart_ = std::chrono::high_resolution_clock::now();
      _running = true;
    }

    //! Continues the measurement without resetting
    //! the timer value (no effect if running)
    inline void cont() {
      if (_running == false) {
        timeOfStart_ = std::chrono::high_resolution_clock::now();
        _running = true;
      }
    }

    //! Stops the measurement.
    inline void stop() {
      auto timeOfStop = std::chrono::high_resolution_clock::now();
      if (_running) {
        value_ += toDuration(timeOfStop - timeOfStart_);
      }
      _running = false;
    }
  // (13.10.10 by baumgari for testing codebase/utility/TimerStatistics.h)
  // Sets the totalTime manually.
  inline void setUsecs(off_t usecs) {
    value_ = toDuration(std::chrono::microseconds{usecs});
  }
  inline void setMsecs(off_t msecs) {
    value_ = toDuration(std::chrono::milliseconds{msecs});
  }
  inline void setSecs(off_t secs) {
    value_ = toDuration(std::chrono::seconds{secs});
  }

  //! Time at last stop (initially zero).
  Duration value() const { return value_; }
  size_t usecs() const {
    return chr::duration_cast<microseconds>(value_).count();
  }
  size_t msecs() const {
    return chr::duration_cast<milliseconds>(value_).count();
  }
  double secs() const {
    return chr::duration_cast<secondsDouble>(value_).count();
  }

  // is the timer currently running
  bool isRunning() const { return _running; }
};


/// An exception signalling a timeout
class TimeoutException : public std::exception {
 public:
  TimeoutException(std::string message) : _message{std::move(message)} {}
  const char* what() const noexcept override { return _message.c_str(); }

 private:
  std::string _message;
};

/// A timer which also can be given a timeout value and queried whether it
/// has timed out
class TimeoutTimer : public Timer {
 public:
  /// Factory function for a timer that never times out
  static TimeoutTimer unlimited() { return TimeoutTimer(UnlimitedTag{}); }
  /// Factory function for a timer that times out after a number of microseconds
  static TimeoutTimer fromMicroseconds(off_t microseconds) {
    return TimeoutTimer(chr::duration_cast<Timer::Duration>(Timer::microseconds{microseconds}));
  }
  /// Factory function for a timer that times out after a number of miliseconds
  static TimeoutTimer fromMilliseconds(off_t milliseconds) {
    return TimeoutTimer(milliseconds * 1000);
  }
  /// Factory function for a timer that times out after a number of seconds
  static TimeoutTimer fromSeconds(double seconds) {
    return TimeoutTimer(static_cast<off_t>(seconds * 1000 * 1000));
  }

  /// Did this timer already timeout
  /// Can't be const because of the internals of the Timer class.
  bool hasTimedOut() {
    if (_isUnlimited) {
      return false;
    }
    // NOTE: we cannot get the current timer value without stopping the timer.
    auto isRunningSnapshot = isRunning();
    stop();
    auto hasTimedOut = usecs() > _timeLimit;
    if (isRunningSnapshot) {
      cont();
    }
    return hasTimedOut;
  }

  // Check if this timer has timed out. If the timer has timed out, throws a
  // TimeoutException. Else, nothing happens.
  void checkTimeoutAndThrow(std::string_view additionalMessage = {}) {
    if (hasTimedOut()) {
      double seconds =
           std::chrono::duration_cast<Timer::secondsDouble>(_timeLimit).count();
      std::stringstream numberStream;
      // Seconds with three digits after the decimal point.
      // TODO<C++20> : Use std::format for formatting, it is much more readable.
      numberStream << std::setprecision(3) << std::fixed << seconds;
      throw TimeoutException{absl::StrCat(
          additionalMessage, "A Timeout occured. The time limit was "s,
          std::move(numberStream).str(), "seconds"s)};
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

  off_t remainingMicroseconds() {
    if (_isUnlimited) {
      return std::numeric_limits<off_t>::max();
    }
    auto prevRunning = isRunning();
    stop();
    off_t res = usecs() > _timeLimit ? 0
                    : _timeLimit - usecs();
    if (prevRunning) {
      cont();
    }
    return res;
  }

 private:
  Timer::Duration _timeLimit = Timer::Duration::zero();
  bool _isUnlimited = false;  // never times out
  class UnlimitedTag {};
  TimeoutTimer(UnlimitedTag) : _isUnlimited{true} {}
  TimeoutTimer(Timer::Duration timeLimit)
      : _timeLimit{timeLimit} {}
};

/// A threadsafe timeout timer
using ConcurrentTimeoutTimer =
    ad_utility::Synchronized<TimeoutTimer, std::mutex>;

/// A shared ptr to a threadsafe timeout timer
using SharedConcurrentTimeoutTimer = std::shared_ptr<ConcurrentTimeoutTimer>;

// A helper struct that measures the time from its creation until its
// destruction and logs the time together with a specified message
#if LOGLEVEL >= TIMING
struct TimeBlockAndLog {
  Timer t_;
  std::string message_;
  TimeBlockAndLog(std::string message) : message_{std::move(message)} {
    t_.start();
  }
  ~TimeBlockAndLog() {
    t_.stop();
    auto msecs = static_cast<double>(t_.usecs());
    LOG(TIMING) << message_ << " took " << msecs << "ms" << std::endl;
  }
};
#else
struct TimeBlockAndLog {
  TimeBlockAndLog(const std::string&) {}
};
#endif

} // namespace timer
using timer::Timer;
using timer::TimeoutTimer;
}  // namespace ad_utility
