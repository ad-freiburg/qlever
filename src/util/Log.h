// Copyright 2011, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)

#pragma once

#include <chrono>
#include <ctime>
#include <iomanip>
#include <iostream>
#include <locale>
#include <sstream>
#include <string>

#include "util/ConstexprMap.h"
#include "util/TypeTraits.h"

#ifndef LOGLEVEL
#define LOGLEVEL INFO
#endif

#define LOG(x)      \
  if (x > LOGLEVEL) \
    ;               \
  else              \
    ad_utility::Log::getLog<x>()  // NOLINT

enum class LogLevel {
  FATAL = 0,
  ERROR = 1,
  WARN = 2,
  INFO = 3,
  DEBUG = 4,
  TIMING = 5,
  TRACE = 6
};

using enum LogLevel;

namespace ad_utility {
/* A singleton (According to Scott Meyer's pattern) that holds
 * a pointer to a single std::ostream. This enables us to globally
 * redirect the LOG(LEVEL) macros to another location.
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
inline void setGlobalLoggingStream(std::ostream* streamPtr) {
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
  return std::move(buffer).str();
}

//! String representation of a long with thousandth separators
inline string to_string(long in) {
  std::ostringstream buffer;
  buffer.imbue(commaLocale);
  buffer << in;
  return std::move(buffer).str();
}

//! Log
class Log {
 public:
  template <LogLevel LEVEL>
  static std::ostream& getLog() {
    // use the singleton logging stream as target.
    return LogstreamChoice::get().getStream()
           << ad_utility::Log::getTimeStamp() << "\t- "
           << ad_utility::Log::getLevel<LEVEL>();
  }

  static void imbue(const std::locale& locale) { std::cout.imbue(locale); }

  static string getTimeStamp() {
    using namespace std::chrono;
    auto now = system_clock::now();
    auto inTimeT = system_clock::to_time_t(now);
    auto ms = duration_cast<milliseconds>(now.time_since_epoch()) % 1000;
    std::stringstream ss;

    ss << std::put_time(std::localtime(&inTimeT), "%Y-%m-%d %X") << '.'
       << std::setw(3) << std::setfill('0') << ms.count();
    return std::move(ss).str();
  }

  template <LogLevel LEVEL>
  static consteval std::string_view getLevel() {
    using std::pair;
    constexpr ad_utility::ConstexprMap map{std::array{
        pair(TRACE, "TRACE: "),
        pair(TIMING, "TIMING: "),
        pair(DEBUG, "DEBUG: "),
        pair(INFO, "INFO: "),
        pair(WARN, "WARN: "),
        pair(ERROR, "ERROR: "),
        pair(FATAL, "FATAL: "),
    }};
    return map.at(LEVEL);
  }
};
}  // namespace ad_utility
