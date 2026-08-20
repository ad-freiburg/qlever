// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <absl/strings/str_split.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <ctre.hpp>
#include <iomanip>
#include <set>
#include <sstream>
#include <tuple>
#include <type_traits>
#include <vector>

#include "./util/GTestHelpers.h"
#include "backports/algorithm.h"
#include "util/Log.h"
#include "util/jthread.h"

// _____________________________________________________________________________
TEST(LogTest, TypeName) { EXPECT_EQ(LogLevel::typeName(), "log level"); }

// _____________________________________________________________________________
TEST(LogTest, StringConversions) {
  EXPECT_EQ(LogLevel::fromString("FATAL"), LogLevel{LogLevel::Enum::FATAL});
  EXPECT_EQ(LogLevel::fromString("ERROR"), LogLevel{LogLevel::Enum::ERROR});
  EXPECT_EQ(LogLevel::fromString("WARN"), LogLevel{LogLevel::Enum::WARN});
  EXPECT_EQ(LogLevel::fromString("INFO"), LogLevel{LogLevel::Enum::INFO});
  EXPECT_EQ(LogLevel::fromString("DEBUG"), LogLevel{LogLevel::Enum::DEBUG});
  EXPECT_EQ(LogLevel::fromString("TIMING"), LogLevel{LogLevel::Enum::TIMING});
  EXPECT_EQ(LogLevel::fromString("TRACE"), LogLevel{LogLevel::Enum::TRACE});

  EXPECT_EQ(LogLevel{LogLevel::Enum::FATAL}.toString(), "FATAL");
  EXPECT_EQ(LogLevel{LogLevel::Enum::INFO}.toString(), "INFO");
  EXPECT_EQ(LogLevel{LogLevel::Enum::TRACE}.toString(), "TRACE");

  EXPECT_THROW(LogLevel::fromString("INVALID"), std::runtime_error);
}

// _____________________________________________________________________________
TEST(LogTest, SetRuntimeLogLevel) {
  // Setting to INFO requires LOGLEVEL >= INFO at compile time; skip otherwise.
  SKIP_IF_LOGLEVEL_IS_LOWER(INFO);
  auto cleanup = setLoglevelForTesting(LogLevel::Enum::FATAL);
  EXPECT_EQ(ad_utility::detail::runtimeLogLevel.load(), LogLevel::Enum::FATAL);

  // Setting to INFO must succeed (SKIP_IF_LOGLEVEL_IS_LOWER(INFO) guards this).
  ad_utility::setRuntimeLogLevel(LogLevel::Enum::INFO);
  EXPECT_EQ(ad_utility::detail::runtimeLogLevel.load(), LogLevel::Enum::INFO);
}

// _____________________________________________________________________________
TEST(LogTest, ExceptionOnTooVerboseLevel) {
  // If the compile-time LOGLEVEL is already TRACE, every runtime level is
  // valid — there is nothing to throw, so we skip.
  if constexpr (LOGLEVEL >= LogLevel::Enum::TRACE) {
    GTEST_SKIP() << "LOGLEVEL is already TRACE; no more-verbose level exists.";
  } else {
    constexpr auto tooVerbose =
        static_cast<LogLevel::Enum>(static_cast<int>(LOGLEVEL) + 1);
    AD_EXPECT_THROW_WITH_MESSAGE(
        ad_utility::setRuntimeLogLevel(LogLevel{tooVerbose}),
        ::testing::HasSubstr("compile-time log level"));
  }
}

// _____________________________________________________________________________
// `Log::imbue` affects the stream that is currently used for logging, and not
// hardcodedly `std::cout`.
TEST(LogTest, Imbue) {
  auto levelCleanup = setLoglevelForTesting(LogLevel::Enum::FATAL);
  auto [streamCleanup, ss] = setGlobalLoggingStreamToStringStream();

  // Without an imbued locale, large numbers are printed without separators.
  AD_LOG_FATAL << 1234567 << '\n';
  EXPECT_THAT(ss.str(), ::testing::HasSubstr("1234567"));

  // After imbuing the locale with the thousands separators, the same number is
  // grouped. Note: The locale is only imbued into the string stream that is
  // currently used for logging, so this doesn't affect any other test.
  ss.str({});
  ad_utility::Log::imbue(ad_utility::commaLocale);
  AD_LOG_FATAL << 1234567 << '\n';
  EXPECT_THAT(ss.str(), ::testing::HasSubstr("1,234,567"));
}

namespace {

// Write a single log argument to `stream`. Invocable arguments are invoked and
// their result is written instead, see `lazyLogArgs` below.
template <typename T>
void streamLogArg(std::ostream& stream, const T& arg) {
  if constexpr (std::is_invocable_v<const T&>) {
    stream << arg();
  } else {
    stream << arg;
  }
}

// A group of log arguments that is only written when it is actually inserted
// into a log stream. Arguments that are invocable are invoked at that point,
// which makes it observable whether a logger evaluates the arguments of a
// suppressed message at all. Only use this as a temporary inside a log
// statement, as it stores references to its arguments.
template <typename... Args>
class LazyLogArgs {
 private:
  std::tuple<const Args&...> args_;

 public:
  explicit LazyLogArgs(const Args&... args) : args_{args...} {}

  friend std::ostream& operator<<(std::ostream& stream,
                                  const LazyLogArgs& lazyArgs) {
    std::apply(
        [&stream](const auto&... args) { (streamLogArg(stream, args), ...); },
        lazyArgs.args_);
    return stream;
  }
};

// Deduce the template arguments of `LazyLogArgs`, see there for details.
template <typename... Args>
LazyLogArgs<Args...> lazyLogArgs(const Args&... args) {
  return LazyLogArgs<Args...>{args...};
}

// The `AD_LOG_...` macros have two implementations (see `util/Log.h`), of which
// only one is active in a given build. The following three function objects
// make all of them accessible at the same time, such that the typed tests below
// run for each of them, independently of the build configuration. Each of them
// logs `args...` at the given `level`, and knows whether it evaluates the
// arguments of a suppressed message.
struct BranchingLogger {
  static constexpr std::string_view name_ = "Branching";
  static constexpr bool evaluatesArgumentsOfSuppressedMessage_ = false;

  template <typename... Args>
  void operator()(LogLevel::Enum level, const Args&... args) const {
    AD_LOG_BRANCHING(level) << lazyLogArgs(args...);
  }

  // Log messages that start with a stream manipulator. Note: The `std::endl`
  // has to appear literally inside the macro, because it is a template and thus
  // requires a special overload of the `operator<<` of the `LogStreamProxy`.
  void logManipulators(LogLevel::Enum level) const {
    AD_LOG_BRANCHING(level) << std::endl;
    AD_LOG_BRANCHING(level) << std::setw(4) << 42 << "\n";
  }
};

struct BranchlessLogger {
  static constexpr std::string_view name_ = "Branchless";
  static constexpr bool evaluatesArgumentsOfSuppressedMessage_ = true;

  template <typename... Args>
  void operator()(LogLevel::Enum level, const Args&... args) const {
    AD_LOG_BRANCHLESS(level) << lazyLogArgs(args...);
  }

  // See `BranchingLogger::logManipulators`.
  void logManipulators(LogLevel::Enum level) const {
    AD_LOG_BRANCHLESS(level) << std::endl;
    AD_LOG_BRANCHLESS(level) << std::setw(4) << 42 << "\n";
  }
};

// The logger that the `AD_LOG_...` macros actually dispatch to, which depends
// on whether `QLEVER_BRANCHLESS_LOGGING` is defined. Testing it makes sure that
// the dispatch works in either configuration.
struct DefaultLogger {
  static constexpr std::string_view name_ = "Default";
  static constexpr bool evaluatesArgumentsOfSuppressedMessage_ =
#ifdef QLEVER_BRANCHLESS_LOGGING
      true;
#else
      false;
#endif

  template <typename... Args>
  void operator()(LogLevel::Enum level, const Args&... args) const {
    AD_LOG(level) << lazyLogArgs(args...);
  }

  // See `BranchingLogger::logManipulators`.
  void logManipulators(LogLevel::Enum level) const {
    AD_LOG(level) << std::endl;
    AD_LOG(level) << std::setw(4) << 42 << "\n";
  }
};

// The fixture for the tests that are run for each of the loggers above.
template <typename Logger>
class LogTestTyped : public ::testing::Test {};

using Loggers =
    ::testing::Types<BranchingLogger, BranchlessLogger, DefaultLogger>;

// Use the name of the logger instead of its index in the names of the typed
// tests.
struct LoggerName {
  template <typename Logger>
  static std::string GetName(int) {
    return std::string{Logger::name_};
  }
};
}  // namespace

TYPED_TEST_SUITE(LogTestTyped, Loggers, LoggerName);

// _____________________________________________________________________________
// A message with a level that passes the runtime log level is logged (including
// the prefix with the timestamp and the log level), a message with a more
// verbose level is completely suppressed.
TYPED_TEST(LogTestTyped, StreamFiltering) {
  // FATAL (0) always passes the compile-time guards. ERROR (1) is suppressed at
  // runtime when the level is set to FATAL.
  auto levelCleanup = setLoglevelForTesting(LogLevel::Enum::FATAL);
  auto [streamCleanup, ss] = setGlobalLoggingStreamToStringStream();

  TypeParam{}(LogLevel::Enum::FATAL, "hello-fatal");
  EXPECT_THAT(ss.str(), ::testing::HasSubstr("FATAL: hello-fatal"));

  ss.str({});
  TypeParam{}(LogLevel::Enum::ERROR, "hello-error");
  EXPECT_THAT(ss.str(), ::testing::IsEmpty());
}

// _____________________________________________________________________________
// Log messages that consist of several arguments of different types are written
// correctly, and a suppressed message produces no output at all.
TYPED_TEST(LogTestTyped, MultipleArguments) {
  auto levelCleanup = setLoglevelForTesting(LogLevel::Enum::FATAL);
  auto [streamCleanup, ss] = setGlobalLoggingStreamToStringStream();

  TypeParam{}(LogLevel::Enum::FATAL, "a=", 42, " b=", std::string{"str"}, '!');
  EXPECT_THAT(ss.str(), ::testing::HasSubstr("a=42 b=str!"));

  ss.str({});
  TypeParam{}(LogLevel::Enum::ERROR, "a=", 42, " b=", std::string{"str"}, '!');
  EXPECT_THAT(ss.str(), ::testing::IsEmpty());
}

// _____________________________________________________________________________
// The arguments of a message that is actually logged are always evaluated, no
// matter which of the loggers is used.
TYPED_TEST(LogTestTyped, ArgumentsOfLoggedMessageAreEvaluated) {
  auto levelCleanup = setLoglevelForTesting(LogLevel::Enum::FATAL);
  auto [streamCleanup, ss] = setGlobalLoggingStreamToStringStream();

  size_t numEvaluations = 0;
  auto expensiveArgument = [&numEvaluations] {
    ++numEvaluations;
    return "expensive";
  };
  TypeParam{}(LogLevel::Enum::FATAL, "value: ", expensiveArgument);
  EXPECT_EQ(numEvaluations, 1);
  EXPECT_THAT(ss.str(), ::testing::HasSubstr("value: expensive"));
}

// _____________________________________________________________________________
// The arguments of a suppressed message are evaluated by the branchless logger
// (which then discards the output), but not by the branching logger, which
// doesn't evaluate them at all.
TYPED_TEST(LogTestTyped, ArgumentsOfSuppressedMessage) {
  auto levelCleanup = setLoglevelForTesting(LogLevel::Enum::FATAL);
  auto [streamCleanup, ss] = setGlobalLoggingStreamToStringStream();

  size_t numEvaluations = 0;
  auto expensiveArgument = [&numEvaluations] {
    ++numEvaluations;
    return "expensive";
  };
  TypeParam{}(LogLevel::Enum::ERROR, "value: ", expensiveArgument);
  EXPECT_THAT(ss.str(), ::testing::IsEmpty());
  EXPECT_EQ(numEvaluations,
            TypeParam::evaluatesArgumentsOfSuppressedMessage_ ? 1u : 0u);
}

// _____________________________________________________________________________
// Messages that start with a stream manipulator are logged correctly, and are
// suppressed if their level doesn't pass the runtime log level.
TYPED_TEST(LogTestTyped, Manipulators) {
  auto levelCleanup = setLoglevelForTesting(LogLevel::Enum::FATAL);
  auto [streamCleanup, ss] = setGlobalLoggingStreamToStringStream();

  TypeParam{}.logManipulators(LogLevel::Enum::FATAL);
  EXPECT_THAT(ss.str(), ::testing::HasSubstr("  42"));
  // Both of the logged messages end with a newline.
  EXPECT_EQ(ql::ranges::count(ss.str(), '\n'), 2);

  ss.str({});
  TypeParam{}.logManipulators(LogLevel::Enum::ERROR);
  EXPECT_THAT(ss.str(), ::testing::IsEmpty());
}

// _____________________________________________________________________________
// Concurrently log many messages, each of them consisting of several `<<`
// arguments, and check that no two messages are interleaved.
TYPED_TEST(LogTestTyped, ThreadSafety) {
  static constexpr size_t numThreads = 8;
  static constexpr size_t msgsPerThread = 200;

  auto [streamCleanup, ss] = setGlobalLoggingStreamToStringStream();

  // Spawn N threads; each logs M lines using multiple `<<` operators. The
  // multi-part write is intentional: if the mutex were absent, partial writes
  // from different threads could interleave within a single line.
  {
    std::vector<ad_utility::JThread> threads;
    threads.reserve(numThreads);
    for (size_t t = 0; t < numThreads; ++t) {
      threads.emplace_back([t] {
        for (size_t m = 0; m < msgsPerThread; ++m) {
          TypeParam{}(LogLevel::Enum::FATAL, "Thread ", t, " message ", m,
                      " end\n");
        }
      });
    }
  }  // All `JThread`s join here on destruction.

  // Pre-populate every expected (thread, msg) pair. Each parsed line must match
  // the log-line prefix pattern, and its pair must still be in the set (first
  // occurrence); the pair is then removed. Any interleaved write would produce
  // a line that fails the regex, and a duplicate would fail the contains check.
  static constexpr ctll::fixed_string kPattern{
      R"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3} - FATAL: Thread (?<thread>\d+) message (?<msg>\d+) end)"};
  std::set<std::pair<size_t, size_t>> expected;
  for (size_t t = 0; t < numThreads; ++t) {
    for (size_t m = 0; m < msgsPerThread; ++m) {
      expected.emplace(t, m);
    }
  }
  for (auto line : absl::StrSplit(ss.str(), '\n', absl::SkipEmpty())) {
    auto match = ctre::match<kPattern>(line);
    ASSERT_TRUE(match) << "Line does not match expected log format: " << line;
    // Note: The `template` keywords are needed because the type of `match` is
    // dependent inside the body of a typed test.
    auto pair = std::make_pair(
        match.template get<"thread">().template to_number<size_t>(),
        match.template get<"msg">().template to_number<size_t>());
    ASSERT_TRUE(expected.contains(pair))
        << "Unexpected or duplicate: thread=" << pair.first
        << " msg=" << pair.second;
    expected.erase(pair);
  }
  EXPECT_TRUE(expected.empty());
}
