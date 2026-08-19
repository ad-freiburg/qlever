// Copyright 2011 - 2024, University of Freiburg
// Chair of Algorithms and Data Structures
// Authors: Björn Buchhold <buchhold@cs.uni-freiburg.de> [2011 - 2014]
//          Johannes Kalmbach <bast@cs.uni-freiburg.de>
//          Hannah Bast <bast@cs.uni-freiburg.de>

#ifndef QLEVER_SRC_UTIL_LOG_H
#define QLEVER_SRC_UTIL_LOG_H

#include <absl/strings/str_cat.h>
#include <absl/strings/str_format.h>
#include <absl/time/clock.h>
#include <absl/time/time.h>

#include <algorithm>
#include <atomic>
#include <iostream>
#include <locale>
#include <mutex>
#include <sstream>
#include <string>

#include "backports/keywords.h"
#include "util/EnumWithStrings.h"
#include "util/Forward.h"
#include "util/TypeTraits.h"

#ifndef LOGLEVEL
#define LOGLEVEL DEBUG
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

// The branching logger: both the compile-time level (LOGLEVEL) and the runtime
// level must pass for a message to be logged. Nothing after the `<<` is
// evaluated for a suppressed message, which makes this variant efficient, but
// also introduces a branch at every single call site, which is unfriendly to
// coverage measurements. The `LogLock` temporary is held for the entire `<<`
// chain and released at the semicolon that ends the statement.
#define AD_LOG_BRANCHING(x)                                           \
  if (x > LOGLEVEL || x > ::ad_utility::detail::runtimeLogLevel.load( \
                              std::memory_order_relaxed))             \
    ;                                                                 \
  else                                                                \
    (::ad_utility::detail::LogLock{::ad_utility::detail::logMutex},   \
     ::ad_utility::Log::getLog(x))  // NOLINT

// The branchless logger: a plain function call that always returns a stream
// (see `ad_utility::getLogStreamBranchless`). For a suppressed message that
// stream discards its input, so the arguments after the `<<` are always
// evaluated (which is less efficient), but the call site contains no branch at
// all (which is friendly to coverage measurements, as the single branch lives
// in this header instead of in each of the hundreds of call sites).
#define AD_LOG_BRANCHLESS(x) ::ad_utility::getLogStreamBranchless(x)

// The logger that is actually used by the `AD_LOG_...` macros below. This is
// the only place where the choice between the two styles above is made; it is
// controlled by the `BRANCHLESS_LOGGING` CMake option.
#ifdef QLEVER_BRANCHLESS_LOGGING
#define AD_LOG(x) AD_LOG_BRANCHLESS(x)
#else
#define AD_LOG(x) AD_LOG_BRANCHING(x)
#endif

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

static constexpr LogLevel::Enum defaultLogLevel =
    std::min(LOGLEVEL, LogLevel::Enum::INFO);
// Runtime log level; messages with a higher level than this are suppressed.
// Defaults to the less verbose of INFO and the compile-time LOGLEVEL so that
// the runtime level is never set to something the binary cannot log.
inline std::atomic<LogLevel::Enum> runtimeLogLevel = defaultLogLevel;
// A stream that discards everything that is written to it. It is created from
// a null `streambuf`, so it is in a `bad` state from the start and every
// insertion into it is a cheap no-op. It is used by the branchless logger for
// messages that are suppressed by the compile-time or the runtime log level.
inline std::ostream& nullStream() {
  static std::ostream stream{nullptr};
  return stream;
}

// Return true if a message with the given `level` has to be logged, according
// to the compile-time (`LOGLEVEL`) and the runtime log level.
inline bool logLevelIsEnabled(LogLevel::Enum level) {
  return level <= LOGLEVEL &&
         level <= runtimeLogLevel.load(std::memory_order_relaxed);
}

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
    throw std::runtime_error{absl::StrCat(
        "Cannot set runtime log level to `", level.toString(),
        "` because the compile-time log level is `",
        LogLevel{LOGLEVEL}.toString(), "`. Recompile with -DLOGLEVEL=",
        level.toString(), " or higher to enable this log level.")};
  }
  detail::runtimeLogLevel.store(level.value(), std::memory_order_relaxed);
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
// In tests use `setGlobalLoggingStreamForTesting` from `GTestHelpers.h` which
// also restores the previous value.
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
  // Write the prefix (timestamp and log level) of a single log message to the
  // global logging stream and return that stream. Note: The caller has to hold
  // the `detail::logMutex` while calling this and while writing the message
  // itself, see the `AD_LOG_BRANCHING` macro and the `LogStreamProxy` class.
  static std::ostream& getLog(LogLevel::Enum level) {
    // use the singleton logging stream as target.
    return LogstreamChoice::get().getStream()
           << getTimeStamp() << " - " << LogLevel{level}.toString() << ": ";
  }

  // Imbue the stream that is currently used for logging with the given
  // `locale`, for example to print large numbers with thousands separators.
  // Note: This has to be called again after the logging stream was changed via
  // `setGlobalLoggingStream`, as the locale is a property of the stream.
  static void imbue(const std::locale& locale) {
    LogstreamChoice::get().getStream().imbue(locale);
  }

  static std::string getTimeStamp() {
    return absl::FormatTime("%Y-%m-%d %H:%M:%E3S", absl::Now(),
                            absl::LocalTimeZone());
  }
};

// The stream-like object that is returned by the branchless logger (see the
// `AD_LOG_BRANCHLESS` macro). It holds the global log mutex and a reference to
// the stream that the message is written to. As it is returned by value, the
// temporary lives until the end of the full expression, so the mutex is held
// for the complete `<<` chain, exactly as for the branching logger. Note that
// the mutex is also held for suppressed messages, so (as for a message that is
// actually logged with the branching logger) the arguments of a log message
// must not log anything themselves.
class LogStreamProxy {
 private:
  detail::LogLock lock_;
  std::ostream& stream_;

 public:
  // Acquire the global log mutex and write the prefix of the message with the
  // given `level`. Note: `lock_` is declared before `stream_`, so the mutex is
  // acquired before `Log::getLog` writes the prefix.
  explicit LogStreamProxy(LogLevel::Enum level)
      : lock_{detail::logMutex},
        stream_{detail::logLevelIsEnabled(level) ? Log::getLog(level)
                                                 : detail::nullStream()} {}

  // Write `arg` to the underlying stream. The result is the stream itself, so
  // that the remaining arguments of the `<<` chain bypass this proxy.
  template <typename T>
  std::ostream& operator<<(T&& arg) const {
    return stream_ << AD_FWD(arg);
  }

  // Overload for stream manipulators like `std::endl`, for which the template
  // argument of the overload above cannot be deduced.
  std::ostream& operator<<(std::ostream& (*manipulator)(std::ostream&)) const {
    return stream_ << manipulator;
  }
};

// The implementation of the `AD_LOG_BRANCHLESS` macro: always return a stream,
// which discards the message if it is suppressed by the compile-time or the
// runtime log level. Note: The `LogStreamProxy` is neither copyable nor
// movable, returning it by value works because of the guaranteed copy elision
// for prvalues.
inline LogStreamProxy getLogStreamBranchless(LogLevel::Enum level) {
  return LogStreamProxy{level};
}
}  // namespace ad_utility

#endif  // QLEVER_SRC_UTIL_LOG_H
