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
#include <string>

#include "backports/keywords.h"
#include "util/EnumWithStrings.h"
#include "util/TypeTraits.h"

#ifndef LOGLEVEL
#define LOGLEVEL INFO
#endif

namespace ad_utility {

namespace detail {
enum class LogLevelEnum {
  FATAL = 0,
  ERROR = 1,
  WARN = 2,
  INFO = 3,
  DEBUG = 4,
  TIMING = 5,
  TRACE = 6
};
}

// Log level wrapper using the `EnumWithStrings` CRTP base to provide string
// conversion, JSON serialization, and boost::program_options integration.
class LogLevel : public EnumWithStrings<LogLevel, detail::LogLevelEnum> {
 public:
  using Enum = detail::LogLevelEnum;
  static constexpr std::array<std::pair<Enum, std::string_view>, 7>
      descriptions_{{{Enum::FATAL, "FATAL"},
                     {Enum::ERROR, "ERROR"},
                     {Enum::WARN, "WARN"},
                     {Enum::INFO, "INFO"},
                     {Enum::DEBUG, "DEBUG"},
                     {Enum::TIMING, "TIMING"},
                     {Enum::TRACE, "TRACE"}}};
  static constexpr std::string_view typeName() { return "log level"; }
  using EnumWithStrings::EnumWithStrings;
};

}  // namespace ad_utility

// Global type alias and using-enum so that `LogLevel::FATAL` etc. and the
// compile-time `LOGLEVEL` macro keep working outside `namespace ad_utility`.
using LogLevel = ad_utility::LogLevel;
using enum LogLevel::Enum;

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

// Macros for the different log levels.
#define AD_LOG_FATAL AD_LOG(LogLevel::Enum::FATAL)
#define AD_LOG_ERROR AD_LOG(LogLevel::Enum::ERROR)
#define AD_LOG_WARN AD_LOG(LogLevel::Enum::WARN)
#define AD_LOG_INFO AD_LOG(LogLevel::Enum::INFO)
#define AD_LOG_DEBUG AD_LOG(LogLevel::Enum::DEBUG)
#define AD_LOG_TIMING AD_LOG(LogLevel::Enum::TIMING)
#define AD_LOG_TRACE AD_LOG(LogLevel::Enum::TRACE)

namespace ad_utility {

namespace detail {
// Global mutex to ensure log messages from different threads are not
// interleaved (acquired via the comma-operator trick in the AD_LOG macro).
inline std::mutex logMutex;
// Runtime log level; messages with a higher level than this are suppressed.
// Default is INFO so debug output is off unless explicitly requested.
inline std::atomic<LogLevel::Enum> runtimeLogLevel{LogLevel::Enum::INFO};

// Non-[[nodiscard]] wrapper so the comma-operator pattern doesn't trigger
// -Wunused-value warnings (std::lock_guard itself is [[nodiscard]] in libc++).
struct LogLock {
  std::lock_guard<std::mutex> lock_;
  explicit LogLock(std::mutex& m) : lock_{m} {}
};
}  // namespace detail

// Set the runtime log level. Throws if `level` is more verbose than the
// compile-time LOGLEVEL, because such messages are compiled out and can never
// appear regardless of the runtime setting.
inline void setRuntimeLogLevel(LogLevel level) {
  if (level.value() > LOGLEVEL) {
    throw std::runtime_error{
        "Cannot set runtime log level to \"" + std::string{level.toString()} +
        "\" because the compile-time log level is \"" +
        std::string{LogLevel{LOGLEVEL}.toString()} +
        "\". Recompile with -DLOGLEVEL=" + std::string{level.toString()} +
        " or higher to enable this log level."};
  }
  detail::runtimeLogLevel.store(level.value(), std::memory_order_relaxed);
}

// RAII guard that temporarily sets the runtime log level for the duration of
// a scope and restores the previous level on destruction.
struct ScopedRuntimeLogLevel {
  LogLevel::Enum previous_;
  explicit ScopedRuntimeLogLevel(LogLevel::Enum level)
      : previous_{detail::runtimeLogLevel.exchange(
            level, std::memory_order_relaxed)} {}
  ~ScopedRuntimeLogLevel() {
    detail::runtimeLogLevel.store(previous_, std::memory_order_relaxed);
  }
  ScopedRuntimeLogLevel(const ScopedRuntimeLogLevel&) = delete;
  ScopedRuntimeLogLevel& operator=(const ScopedRuntimeLogLevel&) = delete;
};

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
  template <LogLevel::Enum LEVEL>
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

  template <LogLevel::Enum LEVEL>
  static QL_CONSTEVAL std::string_view getLevel() {
    return LogLevel::descriptions_[static_cast<size_t>(LEVEL)].second;
  }
};
}  // namespace ad_utility

#endif  // QLEVER_SRC_UTIL_LOG_H
