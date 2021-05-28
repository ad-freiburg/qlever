// Copyright 2011, University of Freiburg, Chair of Algorithms and Data
// Structures.
#pragma once

#include <sys/time.h>
#include <sys/types.h>

#include <iomanip>
#include <memory>
#include <sstream>

#include "Synchronized.h"

// Björn 01Jun11: Copied this class from the CompleteSearch
// code in order to use it in the semantic search, too.
// Adapted header guard, namespace, etc.
namespace ad_utility {
using namespace std::string_literals;
// HOLGER 22Jan06 : changed all suseconds_t to off_t
//
//! A SIMPLE CLASS FOR TIME MEASUREMENTS.
//
class Timer {
 private:
  //! The timer value (initially zero)
  off_t _usecs;

  //! The timer value at the last mark set (initially zero)
  off_t _usecs_at_mark;

  //! Used by the gettimeofday command.
  struct timeval _tstart;

  //! Used by the gettimeofday command.
  struct timeval _tend;

  //! Used by the gettimeofday command.
  struct timezone _tz;

  //! Indicates whether a measurement is running.
  bool _running;

 public:
  //! The default constructor.
  Timer() { reset(); }

  //! Resets the timer value to zero and stops the measurement.
  void reset() {
    _usecs = _usecs_at_mark = 0;
    _running = false;
  }

  //! Mark the current point in time
  //! (to be considered by next usecs_since_mark)
  void mark() {
    stop();
    _usecs_at_mark = _usecs;
    cont();
  }

  //! Resets the timer value to zero and starts the measurement.
  inline void start() {
    _usecs = _usecs_at_mark = 0;
    gettimeofday(&_tstart, &_tz);
    _running = true;
  }

  //! Continues the measurement without resetting
  //! the timer value (no effect if running)
  inline void cont() {
    if (_running == false) {
      gettimeofday(&_tstart, &_tz);
      _running = true;
    }
  }

  //! Stops the measurement (does *not* return the timer value anymore)
  inline void stop() {
    gettimeofday(&_tend, &_tz);
    if (_running) {
      _usecs += (off_t)(1000000) * (off_t)(_tend.tv_sec - _tstart.tv_sec) +
                (off_t)(_tend.tv_usec - _tstart.tv_usec);
    }
    _running = false;
  }
  // (13.10.10 by baumgari for testing codebase/utility/TimerStatistics.h)
  // Sets the totalTime manually.
  inline void setUsecs(off_t usecs) { _usecs = usecs; }
  inline void setMsecs(off_t msecs) { _usecs = msecs * (off_t)(1000); }
  inline void setSecs(off_t secs) { _usecs = secs * (off_t)(1000000); }

  //! Time at last stop (initially zero)
  off_t value() const { return _usecs; }            /* in microseconds */
  off_t usecs() const { return _usecs; }            /* in microseconds */
  off_t msecs() const { return _usecs / 1000; }     /* in milliseconds */
  float secs() const { return _usecs / 1000000.0; } /* in seconds */

  // is the timer currently running
  bool isRunning() const { return _running; }

  //! Time from last mark to last stop (initally zero)
  off_t usecs_since_mark() const { return _usecs - _usecs_at_mark; }
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
    return TimeoutTimer(microseconds);
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
    auto hasTimedOut = usecs() > _timeLimitInMicroseconds;
    if (isRunningSnapshot) {
      cont();
    }
    return hasTimedOut;
  }

  // Check if this timer has timed out. If the timer has timed out, throws a
  // TimeoutException. Else, nothing happens.
  void checkTimeoutAndThrow(std::string additionalMessage = {}) {
    if (hasTimedOut()) {
      double seconds =
          static_cast<double>(_timeLimitInMicroseconds) / (1000 * 1000);
      std::stringstream numberStream;
      // Seconds with three digits after the decimal point.
      // TODO<C++20> : Use std::format for formatting, it is much more readable.
      numberStream << std::setprecision(3) << std::fixed << seconds;
      throw TimeoutException(additionalMessage +
                             "A Timeout occured. The time limit was "s +
                             numberStream.str() + "seconds"s);
    }
  }

  off_t remainingMicroseconds() {
    if (_isUnlimited) {
      return std::numeric_limits<off_t>::max();
    }
    auto prevRunning = isRunning();
    stop();
    off_t res = usecs() > _timeLimitInMicroseconds
                    ? 0
                    : _timeLimitInMicroseconds - usecs();
    if (prevRunning) {
      cont();
    }
    return res;
  }

 private:
  off_t _timeLimitInMicroseconds = 0;
  bool _isUnlimited = false;  // never times out
  class UnlimitedTag {};
  TimeoutTimer(UnlimitedTag) : _isUnlimited{true} {}
  TimeoutTimer(off_t timeLimitInMicroseconds)
      : _timeLimitInMicroseconds{timeLimitInMicroseconds} {}
};

/// A threadsafe timeout timer
using ConcurrentTimeoutTimer =
    ad_utility::Synchronized<TimeoutTimer, std::mutex>;

/// A shared ptr to a threadsafe timeout timer
using SharedConcurrentTimeoutTimer = std::shared_ptr<ConcurrentTimeoutTimer>;

}  // namespace ad_utility
