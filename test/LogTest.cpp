// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <set>
#include <sstream>
#include <vector>

#include "./util/GTestHelpers.h"
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
TEST(LogTest, StreamFiltering) {
  // FATAL (0) always passes compile-time guards. ERROR (1) is suppressed at
  // runtime when the level is set to FATAL.
  auto levelCleanup = setLoglevelForTesting(LogLevel::Enum::FATAL);
  std::stringstream ss;
  ad_utility::setGlobalLoggingStream(&ss);
  auto streamCleanup =
      absl::MakeCleanup([] { ad_utility::setGlobalLoggingStream(&std::cout); });

  AD_LOG_FATAL << "hello-fatal";
  EXPECT_THAT(ss.str(), ::testing::HasSubstr("hello-fatal"));

  ss.str({});
  AD_LOG_ERROR << "hello-error";
  EXPECT_THAT(ss.str(), ::testing::Not(::testing::HasSubstr("hello-error")));
}

// _____________________________________________________________________________
TEST(LogTest, ThreadSafety) {
  static constexpr size_t numThreads = 8;
  static constexpr size_t msgsPerThread = 200;

  // Redirect all logging to a local stringstream for the duration of the test.
  std::stringstream ss;
  ad_utility::setGlobalLoggingStream(&ss);
  auto streamCleanup =
      absl::MakeCleanup([] { ad_utility::setGlobalLoggingStream(&std::cout); });

  // Spawn N threads; each logs M lines using multiple `<<` operators. The
  // multi-part write is intentional: if the mutex were absent, partial writes
  // from different threads could interleave within a single line.
  {
    std::vector<ad_utility::JThread> threads;
    threads.reserve(numThreads);
    for (size_t t = 0; t < numThreads; ++t) {
      threads.emplace_back([t] {
        for (size_t m = 0; m < msgsPerThread; ++m) {
          AD_LOG_FATAL << "Thread " << t << " message " << m << " end\n";
        }
      });
    }
  }  // All `JThread`s join here on destruction.

  // Split the captured output into individual lines.
  std::vector<std::string> lines;
  {
    std::istringstream iss(ss.str());
    std::string line;
    while (std::getline(iss, line)) {
      if (!line.empty()) {
        lines.push_back(std::move(line));
      }
    }
  }

  // Count check: no lines were lost or merged.
  EXPECT_EQ(lines.size(), numThreads * msgsPerThread);

  // Format check: every line must start with the expected log-line prefix. Any
  // interleaving without mutex protection would produce lines that contain a
  // second timestamp in the middle, breaking this pattern.
  static const std::string kLineRegex =
      "[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{3}"
      " - FATAL: .+";
  for (const auto& line : lines) {
    EXPECT_THAT(line, ::testing::MatchesRegex(kLineRegex));
  }

  // Content check: every (threadId, msgIdx) combination appears exactly once.
  std::set<std::string> expectedBodies;
  for (size_t t = 0; t < numThreads; ++t) {
    for (size_t m = 0; m < msgsPerThread; ++m) {
      expectedBodies.insert("Thread " + std::to_string(t) + " message " +
                            std::to_string(m) + " end");
    }
  }
  static constexpr std::string_view kSep = " - FATAL: ";
  for (const auto& line : lines) {
    auto pos = line.find(kSep);
    ASSERT_NE(pos, std::string::npos);
    std::string body = line.substr(pos + kSep.size());
    EXPECT_EQ(expectedBodies.erase(body), 1u)
        << "Unexpected or duplicate log body: " << body;
  }
  EXPECT_TRUE(expectedBodies.empty());
}
