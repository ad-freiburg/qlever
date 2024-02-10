//  Copyright 2023, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "util/ExceptionHandling.h"

// ________________________________________________________________
TEST(ExceptionHandling, terminateIfThrows) {
  // Avoid warnings and crashes when running all tests at once.
  ::testing::FLAGS_gtest_death_test_style = "threadsafe";
  int numCallsToMockedTerminate = 0;
  auto mockedTerminate = [&numCallsToMockedTerminate]() noexcept {
    ++numCallsToMockedTerminate;
  };
  auto alwaysThrow = []() { throw 42; };

  // Test the default logic (which calls `std::terminate`).
  EXPECT_DEATH(ad_utility::terminateIfThrows(alwaysThrow, "A function "),
               "A function that should never throw");
  // Replace the call to `std::terminate` by a custom exception to correctly
  // track the coverage.
  ad_utility::terminateIfThrows(alwaysThrow, "A function ", mockedTerminate);
  EXPECT_EQ(numCallsToMockedTerminate, 1);

  auto alwaysThrowException = []() {
    throw std::runtime_error("throwing in test");
  };
  EXPECT_DEATH(ad_utility::terminateIfThrows(alwaysThrowException,
                                             "test for terminating"),
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
  ~S() { t_(throwingFunction); }
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
// ________________________________________________________________
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
