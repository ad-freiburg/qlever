//  Copyright 2022, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "util/OnDestruction.h"

// This global constant is needed for the `terminateIfThrows` test.
namespace {
int mockedTerminateNumCalls = 0;
}

TEST(OnDestruction, terminateIfThrows) {
  // We need a lambda that is convertible to a function pointer for
  // `ad_utility::terminateIfThrows`, so we have to use a global variable to
  // communicate that the mocked "termination" was called.
  auto terminateReplacement = []() noexcept { ++mockedTerminateNumCalls; };
  auto alwaysThrow = []() { throw 42; };

  // Test the default logic (which calls `std::terminate`
  EXPECT_DEATH(ad_utility::terminateIfThrows(alwaysThrow, "A function "),
               "A function that should never throw");
  // Replace the call to `std::terminate` by a custom exception to correctly
  // track the coverage.
  ad_utility::terminateIfThrows(alwaysThrow, "A function ",
                                terminateReplacement);
  EXPECT_EQ(mockedTerminateNumCalls, 1);

  auto alwaysThrowException = []() {
    throw std::runtime_error("throwing in test");
  };
  EXPECT_DEATH(ad_utility::terminateIfThrows(alwaysThrowException,
                                             "test for terminating"),
               "A function that should never throw");
  ad_utility::terminateIfThrows(alwaysThrowException, "A function ",
                                terminateReplacement);
  EXPECT_EQ(mockedTerminateNumCalls, 2);

  auto noThrowThenExit = []() {
    ad_utility::terminateIfThrows([]() {}, "");
    std::exit(42);
  };
  EXPECT_EXIT(noThrowThenExit(), ::testing::ExitedWithCode(42), ::testing::_);

  ad_utility::terminateIfThrows([]() {}, "", terminateReplacement);
  EXPECT_EQ(mockedTerminateNumCalls, 2);
}

TEST(OnDestruction, OnDestructionDontThrowDuringStackUnwinding) {
  int i = 0;

  // The basic case: The destructor of `cleanup` (at the end of the {} scope)
  // sets `i` to 42;
  {
    auto cleanup = ad_utility::makeOnDestructionDontThrowDuringStackUnwinding(
        [&i]() { i = 42; });
  }
  ASSERT_EQ(i, 42);

  // The basic throwing case: The destructor of `cleanup` inside the lambda
  // throws an exception, which is then propagated to the `ASSERT_THROW` macro.
  auto runCleanup = []() {
    auto cleanup = ad_utility::makeOnDestructionDontThrowDuringStackUnwinding(
        []() { throw std::runtime_error("inside cleanup"); });
  };
  ASSERT_THROW(runCleanup(), std::runtime_error);

  // First the `out_of_range` is thrown. During the stack unwinding, the
  // destructor of `cleanup` is called which detects that it is not safe to
  // throw and thus catches and logs the inner `runtime_error`. Thus the
  // `ASSERT_THROW` macro sees the `out_of_range` exception.
  auto runCleanupDuringUnwinding = [&i]() {
    auto cleanup =
        ad_utility::makeOnDestructionDontThrowDuringStackUnwinding([&i]() {
          i = 12;
          throw std::runtime_error("inside cleanup");
        });
    throw std::out_of_range{"outer exception"};
  };
  ASSERT_THROW(runCleanupDuringUnwinding(), std::out_of_range);
  ASSERT_EQ(i, 12);

  auto runCleanupNested = [&i]() {
    auto innerCleanup =
        ad_utility::makeOnDestructionDontThrowDuringStackUnwinding([&i]() {
          try {
            auto innerCleanup =
                ad_utility::makeOnDestructionDontThrowDuringStackUnwinding(
                    [&i]() {
                      i = 12;
                      throw std::runtime_error("inside inner cleanup");
                    });
          } catch (const std::runtime_error& rt) {
            i = 123;
            throw std::runtime_error("inside outer cleanup");
          }
        });
    throw std::out_of_range("bim");
  };
  ASSERT_THROW(runCleanupNested(), std::out_of_range);
  ASSERT_EQ(i, 123);
  auto runCleanupNested2 = [&i]() {
    auto outerCleanup =
        ad_utility::makeOnDestructionDontThrowDuringStackUnwinding([&i]() {
          try {
            auto outerCleanup =
                ad_utility::makeOnDestructionDontThrowDuringStackUnwinding(
                    [&i]() {
                      i = 18;
                      throw std::runtime_error("inside inner cleanup");
                    });
          } catch (std::out_of_range& rt) {
            i = 234;
          }
        });
    throw std::out_of_range("bim");
  };
  ASSERT_THROW(runCleanupNested2(), std::out_of_range);
  ASSERT_EQ(i, 18);
}
