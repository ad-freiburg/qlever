// Copyright 2011, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)

#pragma once

#include <sys/timeb.h>
#include <time.h>
#include <iomanip>
#include <iostream>
#include <locale>
#include <sstream>
#include <string>

#include <chrono>
#include "./StringUtils.h"

#ifndef LOGLEVEL
#define LOGLEVEL 3
#endif

#define LOG(x)      \
  if (x > LOGLEVEL) \
    ;               \
  else              \
    ad_utility::Log::getLog<x>()  // NOLINT
#define AD_POS_IN_CODE                                                     \
  '[' << ad_utility::getLastPartOfString(__FILE__, '/') << ':' << __LINE__ \
      << "] "  // NOLINT

#define TRACE 5
#define DEBUG 4
#define INFO 3
#define WARN 2
#define ERROR 1
#define FATAL 0

namespace ad_utility {
/* A singleton (According to Scott Meyer's pattern that holds
 * a pointer to a single std::ostream. This enables us to globally
 * redirect the LOG(LEVEL) Makros to another location.
 */
struct LogstreamChoice {
  std::ostream& getStream() { return *_stream; }
  void setStream(std::ostream* stream) { _stream = stream; }

  static LogstreamChoice& get() {
    static LogstreamChoice s;
    return s;
  }  // instance
  LogstreamChoice(const LogstreamChoice&) = delete;
  LogstreamChoice& operator=(const LogstreamChoice&) = delete;

 private:
  LogstreamChoice() {}
  ~LogstreamChoice() {}

  // default to cout since it was the default before
  std::ostream* _stream = &std::cout;

};  // struct LogstreamChoice

/** @brief Redirect every LOG(LEVEL) macro that is called afterwards
 *         to the stream that the argument points to.
 *         Typically called in the main function of an executable.
 */
inline void setGlobalLogginStream(std::ostream* streamPtr) {
  LogstreamChoice::get().setStream(streamPtr);
}

using std::string;
//! Helper class to get thousandth separators in a locale
class CommaNumPunct : public std::numpunct<char> {
 protected:
  virtual char do_thousands_sep() const { return ','; }

  virtual std::string do_grouping() const { return "\03"; }
};

const static std::locale commaLocale(std::locale(), new CommaNumPunct());

//! String representation of a double with precision and thousandth separators
inline string to_string(double in, size_t precision) {
  std::ostringstream buffer;
  buffer.imbue(commaLocale);
  buffer << std::setprecision(precision) << std::fixed << in;
  return buffer.str();
}

//! String representation of a long with thousandth separators
inline string to_string(long in) {
  std::ostringstream buffer;
  buffer.imbue(commaLocale);
  buffer << in;
  return buffer.str();
}

//! Log
class Log {
 public:
  template <unsigned char LEVEL>
  static std::ostream& getLog() {
    // use the singleton logging stream as target.
    return LogstreamChoice::get().getStream()
           << ad_utility::Log::getTimeStamp() << "\t- "
           << ad_utility::Log::getLevel<LEVEL>();
  }

  static void imbue(const std::locale& locale) { std::cout.imbue(locale); }

  static string getTimeStamp() {
    auto now = std::chrono::system_clock::now();
    auto in_time_t = std::chrono::system_clock::to_time_t(now);

    std::stringstream ss;
    ss << std::put_time(std::localtime(&in_time_t), "%Y-%m-%d %X");
    return ss.str();
  }

  template <unsigned char LEVEL>
  static string getLevel() {
    if (LEVEL == TRACE) {
      return "TRACE: ";
    }
    if (LEVEL == DEBUG) {
      return "DEBUG: ";
    }
    if (LEVEL == INFO) {
      return "INFO:  ";
    }
    if (LEVEL == WARN) {
      return "WARN:  ";
    }
    if (LEVEL == ERROR) {
      return "ERROR: ";
    }
    if (LEVEL == FATAL) {
      return "FATAL: ";
    }
  }
};
}  // namespace ad_utility
