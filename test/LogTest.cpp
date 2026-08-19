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

namespace {

// The `AD_LOG_...` macros have two implementations (see `util/Log.h`), of which
// only one is active in a given build. The following three function objects
// make all of them accessible at the same time, such that the tests below can
// test them independently of the build configuration. Each of them logs
// `args...` at the given `level`; arguments that are invocable are only invoked
// when the message is actually written to the log stream (see `lazyLogArgs`).
struct BranchingLogger {
  template <typename... Args>
  void operator()(LogLevel::Enum level, const Args&... args) const {
    AD_LOG_BRANCHING(level) << ad_utility::lazyLogArgs(args...);
  }
};

struct BranchlessLogger {
  template <typename... Args>
  void operator()(LogLevel::Enum level, const Args&... args) const {
    AD_LOG_BRANCHLESS(level) << ad_utility::lazyLogArgs(args...);
  }
};

// The logger that the `AD_LOG_...` macros actually dispatch to, which depends
// on whether `QLEVER_BRANCHLESS_LOGGING` is defined.
struct DefaultLogger {
  template <typename... Args>
  void operator()(LogLevel::Enum level, const Args&... args) const {
    AD_LOG(level) << ad_utility::lazyLogArgs(args...);
  }
};

// True if and only if the `AD_LOG_...` macros use the branchless logger.
constexpr bool defaultLoggerIsBranchless =
#ifdef QLEVER_BRANCHLESS_LOGGING
    true;
#else
    false;
#endif

// A message with a level that passes the runtime log level is logged (including
// the prefix with the timestamp and the log level), a message with a more
// verbose level is completely suppressed.
template <typename Logger>
void testStreamFiltering(
    ad_utility::source_location loc = AD_CURRENT_SOURCE_LOC()) {
  auto trace = generateLocationTrace(loc);
  // FATAL (0) always passes the compile-time guards. ERROR (1) is suppressed at
  // runtime when the level is set to FATAL.
  auto levelCleanup = setLoglevelForTesting(LogLevel::Enum::FATAL);
  auto [streamCleanup, ss] = setGlobalLoggingStreamToStringStream();

  Logger{}(LogLevel::Enum::FATAL, "hello-fatal");
  EXPECT_THAT(ss.str(), ::testing::HasSubstr("FATAL: hello-fatal"));

  ss.str({});
  Logger{}(LogLevel::Enum::ERROR, "hello-error");
  EXPECT_THAT(ss.str(), ::testing::IsEmpty());
}

// Log messages that consist of several arguments of different types are written
// correctly, and a suppressed message produces no output at all.
template <typename Logger>
void testMultipleArguments(
    ad_utility::source_location loc = AD_CURRENT_SOURCE_LOC()) {
  auto trace = generateLocationTrace(loc);
  auto levelCleanup = setLoglevelForTesting(LogLevel::Enum::FATAL);
  auto [streamCleanup, ss] = setGlobalLoggingStreamToStringStream();

  Logger{}(LogLevel::Enum::FATAL, "a=", 42, " b=", std::string{"str"}, '!');
  EXPECT_THAT(ss.str(), ::testing::HasSubstr("a=42 b=str!"));

  ss.str({});
  Logger{}(LogLevel::Enum::ERROR, "a=", 42, " b=", std::string{"str"}, '!');
  EXPECT_THAT(ss.str(), ::testing::IsEmpty());
}

// The arguments of a message that is actually logged are always evaluated, no
// matter which of the two loggers is used.
template <typename Logger>
void testArgumentsOfLoggedMessageAreEvaluated(
    ad_utility::source_location loc = AD_CURRENT_SOURCE_LOC()) {
  auto trace = generateLocationTrace(loc);
  auto levelCleanup = setLoglevelForTesting(LogLevel::Enum::FATAL);
  auto [streamCleanup, ss] = setGlobalLoggingStreamToStringStream();

  size_t numEvaluations = 0;
  auto expensiveArgument = [&numEvaluations] {
    ++numEvaluations;
    return "expensive";
  };
  Logger{}(LogLevel::Enum::FATAL, "value: ", expensiveArgument);
  EXPECT_EQ(numEvaluations, 1);
  EXPECT_THAT(ss.str(), ::testing::HasSubstr("value: expensive"));
}

// Log a suppressed message with an invocable argument and return how often that
// argument was invoked. This is the observable difference between the two
// loggers: the branching logger doesn't evaluate the arguments of a suppressed
// message at all, while the branchless logger evaluates them and discards the
// output.
template <typename Logger>
size_t numEvaluationsForSuppressedMessage() {
  auto levelCleanup = setLoglevelForTesting(LogLevel::Enum::FATAL);
  auto [streamCleanup, ss] = setGlobalLoggingStreamToStringStream();

  size_t numEvaluations = 0;
  auto expensiveArgument = [&numEvaluations] {
    ++numEvaluations;
    return "expensive";
  };
  Logger{}(LogLevel::Enum::ERROR, "value: ", expensiveArgument);
  EXPECT_THAT(ss.str(), ::testing::IsEmpty());
  return numEvaluations;
}

// Concurrently log many messages, each of them consisting of several `<<`
// arguments, and check that no two messages are interleaved.
template <typename Logger>
void testThreadSafety(
    ad_utility::source_location loc = AD_CURRENT_SOURCE_LOC()) {
  auto trace = generateLocationTrace(loc);
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
          Logger{}(LogLevel::Enum::FATAL, "Thread ", t, " message ", m,
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
    auto pair = std::pair{match.get<"thread">().to_number<size_t>(),
                          match.get<"msg">().to_number<size_t>()};
    ASSERT_TRUE(expected.contains(pair))
        << "Unexpected or duplicate: thread=" << pair.first
        << " msg=" << pair.second;
    expected.erase(pair);
  }
  EXPECT_TRUE(expected.empty());
}
}  // namespace

// _____________________________________________________________________________
TEST(LogTest, StreamFilteringBranching) {
  testStreamFiltering<BranchingLogger>();
}

// _____________________________________________________________________________
TEST(LogTest, StreamFilteringBranchless) {
  testStreamFiltering<BranchlessLogger>();
}

// _____________________________________________________________________________
TEST(LogTest, StreamFilteringDefault) { testStreamFiltering<DefaultLogger>(); }

// _____________________________________________________________________________
TEST(LogTest, MultipleArgumentsBranching) {
  testMultipleArguments<BranchingLogger>();
}

// _____________________________________________________________________________
TEST(LogTest, MultipleArgumentsBranchless) {
  testMultipleArguments<BranchlessLogger>();
}

// _____________________________________________________________________________
TEST(LogTest, MultipleArgumentsDefault) {
  testMultipleArguments<DefaultLogger>();
}

// _____________________________________________________________________________
TEST(LogTest, ArgumentsOfLoggedMessageAreEvaluated) {
  testArgumentsOfLoggedMessageAreEvaluated<BranchingLogger>();
  testArgumentsOfLoggedMessageAreEvaluated<BranchlessLogger>();
  testArgumentsOfLoggedMessageAreEvaluated<DefaultLogger>();
}

// _____________________________________________________________________________
TEST(LogTest, ArgumentsOfSuppressedMessage) {
  // The branching logger short-circuits, so the arguments are not evaluated...
  EXPECT_EQ(numEvaluationsForSuppressedMessage<BranchingLogger>(), 0);
  // ... while the branchless logger evaluates them and discards the output.
  EXPECT_EQ(numEvaluationsForSuppressedMessage<BranchlessLogger>(), 1);
  // The `AD_LOG_...` macros behave like exactly one of the two, depending on
  // the `QLEVER_BRANCHLESS_LOGGING` macro. This tests the final dispatch.
  EXPECT_EQ(numEvaluationsForSuppressedMessage<DefaultLogger>(),
            defaultLoggerIsBranchless ? 1u : 0u);
}

// _____________________________________________________________________________
// The `AD_LOG_...` macros can be used directly, in particular also with stream
// manipulators as the first argument, which is a special case for the
// branchless logger.
TEST(LogTest, MacrosAndManipulators) {
  auto levelCleanup = setLoglevelForTesting(LogLevel::Enum::FATAL);
  auto [streamCleanup, ss] = setGlobalLoggingStreamToStringStream();

  AD_LOG_FATAL << std::endl;
  AD_LOG_FATAL << std::setw(4) << 42 << "\n";
  AD_LOG_ERROR << "suppressed";
  AD_LOG_BRANCHING(LogLevel::Enum::FATAL) << std::endl;
  AD_LOG_BRANCHLESS(LogLevel::Enum::FATAL) << std::endl;
  AD_LOG_BRANCHLESS(LogLevel::Enum::ERROR) << std::endl;
  EXPECT_THAT(ss.str(), ::testing::HasSubstr("  42"));
  EXPECT_THAT(ss.str(), ::testing::Not(::testing::HasSubstr("suppressed")));
  // Four of the six log statements above are actually logged, and each of them
  // ends with a newline.
  EXPECT_EQ(ql::ranges::count(ss.str(), '\n'), 4);
}

// _____________________________________________________________________________
TEST(LogTest, ThreadSafetyBranching) { testThreadSafety<BranchingLogger>(); }

// _____________________________________________________________________________
TEST(LogTest, ThreadSafetyBranchless) { testThreadSafety<BranchlessLogger>(); }

// _____________________________________________________________________________
TEST(LogTest, ThreadSafetyDefault) { testThreadSafety<DefaultLogger>(); }
