// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <sstream>

#include "./util/GTestHelpers.h"
#include "util/Log.h"

// Fixture that saves and restores the runtime log level and global logging
// stream around each test so tests don't interfere with each other.
class LogTest : public ::testing::Test {
 protected:
  void SetUp() override {
    savedLevel_ = ad_utility::detail::runtimeLogLevel.load();
    ad_utility::setGlobalLoggingStream(&stream_);
  }
  void TearDown() override {
    ad_utility::detail::runtimeLogLevel.store(savedLevel_);
    ad_utility::setGlobalLoggingStream(&std::cout);
  }
  std::stringstream stream_;

 private:
  LogLevel::Enum savedLevel_{};
};

// _____________________________________________________________________________
TEST_F(LogTest, TypeName) { EXPECT_EQ(LogLevel::typeName(), "log level"); }

// _____________________________________________________________________________
TEST_F(LogTest, StringConversions) {
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
TEST_F(LogTest, SetRuntimeLogLevel) {
  ad_utility::setRuntimeLogLevel(LogLevel::Enum::FATAL);
  EXPECT_EQ(ad_utility::detail::runtimeLogLevel.load(), LogLevel::Enum::FATAL);

  ad_utility::setRuntimeLogLevel(LogLevel::Enum::INFO);
  EXPECT_EQ(ad_utility::detail::runtimeLogLevel.load(), LogLevel::Enum::INFO);
}

// _____________________________________________________________________________
TEST_F(LogTest, ExceptionOnTooVerboseLevel) {
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
TEST_F(LogTest, StreamFiltering) {
  // FATAL (0) always passes compile-time guards. ERROR (1) is suppressed when
  // runtime level is FATAL.
  ad_utility::setRuntimeLogLevel(LogLevel::Enum::FATAL);

  AD_LOG_FATAL << "hello-fatal";
  EXPECT_THAT(stream_.str(), ::testing::HasSubstr("hello-fatal"));

  stream_.str({});
  AD_LOG_ERROR << "hello-error";
  EXPECT_THAT(stream_.str(),
              ::testing::Not(::testing::HasSubstr("hello-error")));
}
