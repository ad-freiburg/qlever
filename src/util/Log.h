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

#include <atomic>
#include <iostream>
#include <locale>
#include <mutex>
#include <sstream>
#include <stdexcept>
#include <string>

#include "backports/keywords.h"
#include "util/ConstexprMap.h"
#include "util/TypeTraits.h"

#ifndef LOGLEVEL
#define LOGLEVEL INFO
#endif

// Both the compile-time level (LOGLEVEL) and the runtime level must pass for a
// message to be logged. The LogLock temporary is held for the entire <<
// chain and released at the semicolon that ends the statement.
#define AD_LOG(x)                                                              \
  if (x > LOGLEVEL ||                                                          \
      x > ad_utility::detail::runtimeLogLevel.load(std::memory_order_relaxed)) \
    ;                                                                          \
  else                                                                         \
    (ad_utility::detail::LogLock{ad_utility::detail::logMutex},                \
     ad_utility::Log::getLog<x>())  // NOLINT

enum class LogLevel {
  FATAL = 0,
  ERROR = 1,
  WARN = 2,
  INFO = 3,
  DEBUG = 4,
  TIMING = 5,
  TRACE = 6
};

// Macros for the different log levels.
#define AD_LOG_FATAL AD_LOG(LogLevel::FATAL)
#define AD_LOG_ERROR AD_LOG(LogLevel::ERROR)
#define AD_LOG_WARN AD_LOG(LogLevel::WARN)
#define AD_LOG_INFO AD_LOG(LogLevel::INFO)
#define AD_LOG_DEBUG AD_LOG(LogLevel::DEBUG)
#define AD_LOG_TIMING AD_LOG(LogLevel::TIMING)
#define AD_LOG_TRACE AD_LOG(LogLevel::TRACE)

using enum LogLevel;

namespace ad_utility {

namespace detail {
// Global mutex to ensure log messages from different threads are not
// interleaved (acquired via the comma-operator trick in the AD_LOG macro).
inline std::mutex logMutex;
// Runtime log level; messages with a higher level than this are suppressed.
// Default is INFO so debug output is off unless explicitly requested.
inline std::atomic<LogLevel> runtimeLogLevel{LogLevel::INFO};

// Non-[[nodiscard]] wrapper so the comma-operator pattern doesn't trigger
// -Wunused-value warnings (std::lock_guard itself is [[nodiscard]] in libc++).
struct LogLock {
  std::lock_guard<std::mutex> lock_;
  explicit LogLock(std::mutex& m) : lock_{m} {}
};
}  // namespace detail

// Set the runtime log level, called from the RuntimeParameters update action.
inline void setRuntimeLogLevel(LogLevel level) {
  detail::runtimeLogLevel.store(level, std::memory_order_relaxed);
}

// Convert a log level to its string representation at runtime.
inline std::string_view logLevelToString(LogLevel level) {
  switch (level) {
    case LogLevel::FATAL:
      return "FATAL";
    case LogLevel::ERROR:
      return "ERROR";
    case LogLevel::WARN:
      return "WARN";
    case LogLevel::INFO:
      return "INFO";
    case LogLevel::DEBUG:
      return "DEBUG";
    case LogLevel::TIMING:
      return "TIMING";
    case LogLevel::TRACE:
      return "TRACE";
  }
  throw std::runtime_error{"Unknown log level"};
}

// Parse a log level from its string representation (case-sensitive).
inline LogLevel logLevelFromString(std::string_view s) {
  if (s == "FATAL") return LogLevel::FATAL;
  if (s == "ERROR") return LogLevel::ERROR;
  if (s == "WARN") return LogLevel::WARN;
  if (s == "INFO") return LogLevel::INFO;
  if (s == "DEBUG") return LogLevel::DEBUG;
  if (s == "TIMING") return LogLevel::TIMING;
  if (s == "TRACE") return LogLevel::TRACE;
  throw std::runtime_error{
      "Invalid log level \"" + std::string{s} +
      "\". Valid values: FATAL, ERROR, WARN, INFO, DEBUG, TIMING, TRACE."};
}

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
           << getTimeStamp() << " - " << getLevel<LEVEL>() << ": ";
  }

  static void imbue(const std::locale& locale) { std::cout.imbue(locale); }

  static std::string getTimeStamp() {
    return absl::FormatTime("%Y-%m-%d %H:%M:%E3S", absl::Now(),
                            absl::LocalTimeZone());
  }

  template <LogLevel LEVEL>
  static QL_CONSTEVAL std::string_view getLevel() {
    using P = ConstexprMapPair<LogLevel, std::string_view>;
    constexpr ConstexprMap map{std::array<P, 7>{
        P(TRACE, "TRACE"),
        P(TIMING, "TIMING"),
        P(DEBUG, "DEBUG"),
        P(INFO, "INFO"),
        P(WARN, "WARN"),
        P(ERROR, "ERROR"),
        P(FATAL, "FATAL"),
    }};
    return map.at(LEVEL);
  }
};
}  // namespace ad_utility

#endif  // QLEVER_SRC_UTIL_LOG_H
