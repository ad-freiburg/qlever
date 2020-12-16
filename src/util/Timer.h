// Copyright 2011, University of Freiburg, Chair of Algorithms and Data
// Structures.
#pragma once

#include <sys/time.h>
#include <sys/types.h>
#include "Synchronized.h"

// Björn 01Jun11: Copied this class from the CompleteSearch
// code in order to use it in the semantic search, too.
// Adapted header guard, namespace, etc.
namespace ad_utility {
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

/// A timer which also can be given a timeout value and queried whether it
/// has timed out
class TimeoutTimer : public Timer {
 public:
  /// Factory function for a timer that never times out
  static TimeoutTimer unlimited() { return TimeoutTimer(UnlimitedTag{}); }
  /// Factory function for a timer that times out after a number of microseconds
  static TimeoutTimer usecLimited(off_t usecs) { return TimeoutTimer(usecs); }
  /// Factory function for a timer that times out after a number of miliseconds
  static TimeoutTimer msecLimited(off_t msecs) {
    return TimeoutTimer(msecs * 1000);
  }
  /// Factory function for a timer that times out after a number of seconds
  static TimeoutTimer secLimited(double secs) {
    return TimeoutTimer(static_cast<off_t>(secs * 1000 * 1000));
  }

  /// Did this timer already timeout
  /// Can't be const because of the internals of the Timer class.
  bool isTimeout() {
    if (_isUnlimited) {
      return false;
    }
    auto prevRunning = isRunning();
    stop();
    auto res = usecs() > _timeout;
    if (prevRunning) {
      cont();
    }
    return res;
  }

 private:
  off_t _timeout = 0;
  bool _isUnlimited = false;  // never times out
  class UnlimitedTag {};
  TimeoutTimer(UnlimitedTag) : _isUnlimited{true} {}
  TimeoutTimer(off_t timeout) : _timeout{timeout} {}
};

// simple interface : threadsafe timer + "checkTimeout + throw exception"
class TimeoutChecker
    : public ad_utility::Synchronized<TimeoutTimer, std::mutex> {
 public:
  class TimeoutException : public std::exception {};
  using Base = ad_utility::Synchronized<TimeoutTimer, std::mutex>;
  using Base::Synchronized;

  void checkTimeout() {
    if (wlock()->isTimeout()) {
      throw TimeoutException{};
    }
  }
};
}  // namespace ad_utility
