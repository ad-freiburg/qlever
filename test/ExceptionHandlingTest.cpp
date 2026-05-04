//  Copyright 2023, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include <absl/cleanup/cleanup.h>
#include <gmock/gmock.h>

#include <array>
#include <atomic>
#include <sstream>
#include <stdexcept>

#include "./util/GTestHelpers.h"
#include "util/ExceptionHandling.h"
#include "util/Log.h"
#include "util/jthread.h"

// _____________________________________________________________________________
TEST(ExceptionHandling, terminateIfThrows) {
  // Avoid warnings and crashes when running all tests at once.
  ::testing::FLAGS_gtest_death_test_style = "threadsafe";
  int numCallsToMockedTerminate = 0;
  auto mockedTerminate = [&numCallsToMockedTerminate]() noexcept {
    ++numCallsToMockedTerminate;
  };
  auto alwaysThrow = []() { throw 42; };

  // Test the default logic (which calls `std::terminate`).
  EXPECT_DEATH_IF_SUPPORTED(
      ad_utility::terminateIfThrows(alwaysThrow, "A function "),
      "A function that should never throw");
  // Replace the call to `std::terminate` by a custom exception to correctly
  // track the coverage.
  ad_utility::terminateIfThrows(alwaysThrow, "A function ", mockedTerminate);
  EXPECT_EQ(numCallsToMockedTerminate, 1);

  auto alwaysThrowException = []() {
    throw std::runtime_error("throwing in test");
  };
  EXPECT_DEATH_IF_SUPPORTED(ad_utility::terminateIfThrows(
                                alwaysThrowException, "test for terminating"),
                            "A function that should never throw");
  ad_utility::terminateIfThrows(alwaysThrowException, "A function ",
                                mockedTerminate);
  EXPECT_EQ(numCallsToMockedTerminate, 2);

  auto noThrowThenExit = []() {
    ad_utility::terminateIfThrows([]() {}, "");
    std::exit(42);
  };
  EXPECT_EXIT(noThrowThenExit(), ::testing::ExitedWithCode(42), ::testing::_);

  ad_utility::terminateIfThrows([]() {}, "", mockedTerminate);
  EXPECT_EQ(numCallsToMockedTerminate, 2);
}

// ________________________________________________________________
TEST(ExceptionHandling, ignoreExceptionIfThrows) {
  int i = 0;
  ASSERT_NO_THROW(ad_utility::ignoreExceptionIfThrows([&i]() {
    i = 42;
    throw std::runtime_error{"blim"};
  }));
  EXPECT_EQ(i, 42);

  ASSERT_NO_THROW(
      ad_utility::ignoreExceptionIfThrows([&i]() noexcept { i = -4; }));
  EXPECT_EQ(i, -4);
}

namespace {
template <auto throwingFunction>
struct S {
  ad_utility::ThrowInDestructorIfSafe t_;
  ~S() noexcept(false) { t_(throwingFunction); }
};

// This function should as part of the `S` destructor invoke the
// `innerThrowingFunction` and therefore throw the exception.
template <auto innerThrowingFunction>
void throwInnerException() {
  S<innerThrowingFunction> s;
};

// This function should ignore the function thrown by the
// `innerThrowingFunction` and only propagate the `system_error` to the outside.
template <auto innerThrowingFunction>
[[noreturn]] void ignoreInnerException() {
  S<innerThrowingFunction> s;
  throw std::system_error{std::error_code{}};
};
}  // namespace

// _____________________________________________________________________________
TEST(ExceptionCollector, isNoOpWhenEmpty) {
  ad_utility::ExceptionCollector collector;
  EXPECT_NO_THROW(collector.rethrow());
  collector(std::exception_ptr{});
  EXPECT_NO_THROW(collector.rethrow());
}

// _____________________________________________________________________________
TEST(ExceptionCollector, asCompletionHandler) {
  std::ostringstream logStream;
  ad_utility::setGlobalLoggingStream(&logStream);
  absl::Cleanup cleanup{
      []() { ad_utility::setGlobalLoggingStream(&std::cout); }};

  ad_utility::ExceptionCollector collector;
  try {
    throw std::runtime_error{"first"};
  } catch (...) {
    collector(std::current_exception());
  }
  // Additional `std::exception`s are logged with their message; only the first
  // one is rethrown.
  try {
    throw std::logic_error{"second"};
  } catch (...) {
    collector(std::current_exception());
  }
  EXPECT_THAT(
      logStream.str(),
      ::testing::AllOf(::testing::HasSubstr("Additional exception captured"),
                       ::testing::HasSubstr("second")));

  // Additional exceptions that do not derive from `std::exception` exercise
  // the catch-all branch and are logged with a generic message.
  try {
    throw 42;
  } catch (...) {
    collector(std::current_exception());
  }
  EXPECT_THAT(
      logStream.str(),
      ::testing::HasSubstr("Additional exception of unknown type captured"));

  AD_EXPECT_THROW_WITH_MESSAGE_AND_TYPE(
      collector.rethrow(), ::testing::StrEq("first"), std::runtime_error);
}

// _____________________________________________________________________________
TEST(ExceptionCollector, wrap) {
  ad_utility::ExceptionCollector collector;
  int sideEffect = 0;
  auto noThrowing = collector.wrap([&] { sideEffect = 7; });
  noThrowing();
  EXPECT_EQ(sideEffect, 7);
  EXPECT_NO_THROW(collector.rethrow());

  auto throwing = collector.wrap([] { throw std::runtime_error{"from wrap"}; });
  EXPECT_NO_THROW(throwing());
  AD_EXPECT_THROW_WITH_MESSAGE_AND_TYPE(
      collector.rethrow(), ::testing::StrEq("from wrap"), std::runtime_error);
}

// _____________________________________________________________________________
TEST(ExceptionCollector, isThreadSafe) {
  ad_utility::ExceptionCollector collector;
  {
    constexpr int numThreads = 16;
    std::atomic<int> ready = 0;
    std::array<ad_utility::JThread, numThreads> threads;
    for (int i = 0; i < numThreads; ++i) {
      threads.at(i) = ad_utility::JThread{[&collector, &ready, i] {
        ++ready;
        while (ready.load() < numThreads) {
          // Spin until all threads are ready, to maximise contention.
        }
        try {
          throw std::runtime_error{"t" + std::to_string(i)};
        } catch (...) {
          collector(std::current_exception());
        }
      }};
    }
    // Wait for threads to complete.
  }
  AD_EXPECT_THROW_WITH_MESSAGE_AND_TYPE(collector.rethrow(),
                                        ::testing::MatchesRegex("t[0-9]+"),
                                        std::runtime_error);
}

// _____________________________________________________________________________
TEST(ExceptionCollector, destructorRethrowsWhenRethrowWasNeverCalled) {
  // If the user forgets to call `rethrow()`, the captured exception is
  // surfaced from the destructor instead of being silently swallowed.
  AD_EXPECT_THROW_WITH_MESSAGE_AND_TYPE(
      {
        ad_utility::ExceptionCollector collector;
        try {
          throw std::runtime_error{"forgotten"};
        } catch (...) {
          collector(std::current_exception());
        }
        // No call to `rethrow()` — the destructor must rethrow.
      },
      ::testing::StrEq("forgotten"), std::runtime_error);
}

// _____________________________________________________________________________
TEST(ExceptionCollector, destructorLogsWhenRethrowWouldTerminate) {
  // If the destructor runs while another exception is already propagating,
  // rethrowing would call `std::terminate`. The captured exception must be
  // logged instead, and the original exception must continue to propagate.
  AD_EXPECT_THROW_WITH_MESSAGE_AND_TYPE(
      {
        ad_utility::ExceptionCollector collector;
        try {
          throw std::runtime_error{"silently dropped"};
        } catch (...) {
          collector(std::current_exception());
        }
        // Throw a different exception. The collector destructor will run while
        // this one is in flight and must not call `std::terminate`.
        throw std::logic_error{"propagated to caller"};
      },
      ::testing::StrEq("propagated to caller"), std::logic_error);
}

// _____________________________________________________________________________
TEST(ExceptionCollector, rethrowClearsException) {
  ad_utility::ExceptionCollector collector;
  try {
    throw std::runtime_error{"my error"};
  } catch (...) {
    collector(std::current_exception());
  }
  AD_EXPECT_THROW_WITH_MESSAGE_AND_TYPE(
      collector.rethrow(), ::testing::StrEq("my error"), std::runtime_error);
  EXPECT_NO_THROW(collector.rethrow());
}

// _____________________________________________________________________________
TEST(ExceptionHandling, throwIfSafe) {
  using namespace ad_utility;
  ThrowInDestructorIfSafe t;
  static constexpr auto throwException = []() {
    throw std::runtime_error{"haha"};
  };
  static constexpr auto throwInt = []() { throw 42; };
  EXPECT_THROW(throwInnerException<throwException>(), std::runtime_error);
  EXPECT_THROW(throwInnerException<throwInt>(), int);
  EXPECT_THROW((ignoreInnerException<throwException>()), std::system_error);
  EXPECT_THROW((ignoreInnerException<throwInt>()), std::system_error);
}
