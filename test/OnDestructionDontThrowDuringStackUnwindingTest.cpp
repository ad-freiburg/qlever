//  Copyright 2023, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include "gtest/gtest.h"
#include "util/OnDestructionDontThrowDuringStackUnwinding.h"

// ________________________________________________________________
TEST(OnDestruction, OnDestructionDontThrowDuringStackUnwinding) {
  int i = 0;

  // The basic case: The destructor of `cleanup` (at the end of the {} scope)
  // sets `i` to 42;
  {
    auto cleanup = ad_utility::makeOnDestructionDontThrowDuringStackUnwinding(
        [&i]() { i += 32; });
    i = 10;
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
  // destructor of `cleanup` is called, which detects that it is not safe to
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

  // First the `std::out_of_range` at the end is thrown. The destructor of
  // `outerCleanup` is called, which again calls the destructor of
  // `innerCleanup`. This destructor throws an exception, but it is actually
  // safe to throw this exception (because it is immediately caught by the
  // `outerCleanup`). That is why we can observe the effect of the catch clause
  // (`i` is set). This means that the logic of the `OnDestruction...` object
  // `innerCleanup` does not catch the `runtime_error`, because it is safe to
  // let it propagate, although stack unwinding is in progress.
  auto runCleanupNested = [&i]() {
    auto outerCleanup =
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

  // Just for completenesss/ documentation:
  // Similar to the previous test, but the wrong exception is caught.
  // This means that the `OnDestructionDont...` object will again catch
  // the inner `runtime_error` and the program will not terminate, but simply
  // observe the outer `out_of_range` error.
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

// ________________________________________________________________
TEST(OnDestruction, cancel) {
  int i = 12;
  {
    auto cl = ad_utility::makeOnDestructionDontThrowDuringStackUnwinding(
        [&i] { i = 24; });
  }
  ASSERT_EQ(i, 24);
  {
    auto cl = ad_utility::makeOnDestructionDontThrowDuringStackUnwinding(
        [&i] { i = 123; });
    cl.cancel();
  }
  ASSERT_EQ(i, 24);
}
