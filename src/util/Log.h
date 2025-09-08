// Copyright 2011 - 2024, University of Freiburg
// Chair of Algorithms and Data Structures
// Authors: Björn Buchhold <buchhold@cs.uni-freiburg.de> [2011 - 2014]
//          Johannes Kalmbach <bast@cs.uni-freiburg.de>
//          Hannah Bast <bast@cs.uni-freiburg.de>

#ifndef QLEVER_SRC_UTIL_LOG_H
#define QLEVER_SRC_UTIL_LOG_H

#include <absl/strings/str_format.h>
#include <absl/time/clock.h>
#include <absl/time/time.h>

#include <iostream>
#include <locale>
#include <sstream>
#include <string>

#include "backports/keywords.h"
#include "util/ConstexprMap.h"
#include "util/TypeTraits.h"

#ifndef LOGLEVEL
#define LOGLEVEL INFO
#endif

// Abseil also defines its own LOG macro, so we need to undefine it here.
//
// NOTE: In case you run into trouble with this conflict, in particular, if you
// use the `LOG(INFO)` macro and you get compilation errors that mention
// `abseil`, use the (otherwise identical) `AD_LOG_INFO` macro below.
//
// TODO<joka921>: Eventually replace the `LOG` macro by `AD_LOG` everywhere.

#ifdef LOG
#undef LOG
#endif

#define LOG(x)      \
  if (x > LOGLEVEL) \
    ;               \
  else              \
    ad_utility::Log::getLog<x>()  // NOLINT

#define AD_LOG(x)   \
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

// Macros for the different log levels. Always use these instead of the old
// `LOG(...)` macro to avoid conflicts with `abseil`.
#define AD_LOG_FATAL AD_LOG(LogLevel::FATAL)
#define AD_LOG_ERROR AD_LOG(LogLevel::ERROR)
#define AD_LOG_WARN AD_LOG(LogLevel::WARN)
#define AD_LOG_INFO AD_LOG(LogLevel::INFO)
#define AD_LOG_DEBUG AD_LOG(LogLevel::DEBUG)
#define AD_LOG_TIMING AD_LOG(LogLevel::TIMING)
#define AD_LOG_TRACE AD_LOG(LogLevel::TRACE)

using enum LogLevel;

namespace ad_utility {
// A singleton that holds a pointer to a single `std::ostream`. This enables us
// to globally redirect the `AD_LOG_...` macros to another output stream.
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
};

// After this call, every use of `AD_LOG_...` will use the specified stream.
// Used in various tests to redirect or suppress logging output.
inline void setGlobalLoggingStream(std::ostream* stream) {
  LogstreamChoice::get().setStream(stream);
}

// Helper class to get thousandth separators in a locale
class CommaNumPunct : public std::numpunct<char> {
 protected:
  virtual char do_thousands_sep() const { return ','; }

  virtual std::string do_grouping() const { return "\03"; }
};

const static std::locale commaLocale(std::locale(), new CommaNumPunct());

// The class that actually does the logging.
class Log {
 public:
  template <LogLevel LEVEL>
  static std::ostream& getLog() {
    // use the singleton logging stream as target.
    return LogstreamChoice::get().getStream()
           << getTimeStamp() << " - " << getLevel<LEVEL>();
  }

  static void imbue(const std::locale& locale) { std::cout.imbue(locale); }

  static std::string getTimeStamp() {
    return absl::FormatTime("%Y-%m-%d %H:%M:%E3S", absl::Now(),
                            absl::LocalTimeZone());
  }

  template <LogLevel LEVEL>
  static QL_CONSTEVAL std::string_view getLevel() {
    using P = ConstexprMapPair<LogLevel, std::string_view>;
    constexpr ConstexprMap map{std::array{
        P(TRACE, "TRACE: "),
        P(TIMING, "TIMING: "),
        P(DEBUG, "DEBUG: "),
        P(INFO, "INFO: "),
        P(WARN, "WARN: "),
        P(ERROR, "ERROR: "),
        P(FATAL, "FATAL: "),
    }};
    return map.at(LEVEL);
  }
};
}  // namespace ad_utility

#endif  // QLEVER_SRC_UTIL_LOG_H
