// Copyright 2011, University of Freiburg, Chair of Algorithms and Data
// Structures.
#pragma once

#include <sys/time.h>
#include <sys/types.h>
#include <chrono>

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
  std::chrono::steady_clock _clock;
  std::chrono::time_point<std::chrono::steady_clock> _startTime = _clock.now();
  //! The timer value (initially zero)
  std::chrono::microseconds _passedTime = 0us;

  //! Indicates whether a measurement is running.
  bool _running;

 public:
  //! The default constructor.
  Timer() { reset(); }

  //! Resets the timer value to zero and stops the measurement.
  void reset() {
    _passedTime = 0us;
    _running = false;
  }

  //! Resets the timer value to zero and starts the measurement.
  inline void start() {
    reset();
    _startTime = _clock.now();
    _running = true;
  }

  //! Continues the measurement without resetting
  //! the timer value (no effect if running)
  inline void cont() {
    if (_running == false) {
      _startTime = _clock.now();
      _running = true;
    }
  }

  //! Stops the measurement (does *not* return the timer value anymore)
  inline void stop() {
    if (_running) {
      _passedTime += std::chrono::duration_cast<std::chrono::microseconds>(
          _clock.now() - _startTime);
    }
    _running = false;
  }

  std::chrono::microseconds getTime() {
    if (_running) {
      return _passedTime +
             std::chrono::duration_cast<std::chrono::microseconds>(
                 _clock.now() - _startTime);
    } else {
      return _passedTime;
    }
  };

  // ___________________________________________________________________
  long usecs() { return getTime().count(); }

  // ___________________________________________________________________
  long msecs() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(getTime())
        .count();
  }
};
}  // namespace ad_utility
