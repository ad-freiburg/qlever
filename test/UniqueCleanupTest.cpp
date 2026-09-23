//   Copyright 2023, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <memory>
#include <stdexcept>
#include <vector>

#include "util/Exception.h"
#include "util/UniqueCleanup.h"

using ad_utility::unique_cleanup::UniqueCleanup;
using ::testing::Pointee;
using namespace std::string_literals;

TEST(UniqueCleanup, CorrectCallbackOnDestruction) {
  bool run = false;
  {
    UniqueCleanup uniqueCleanup{1337, [&run](auto value) {
                                  EXPECT_EQ(value, 1337);
                                  run = true;
                                }};
    ASSERT_FALSE(run) << "Callback was called too early";
  }
  ASSERT_TRUE(run) << "Callback was not called on destruction";
}

// _____________________________________________________________________________

TEST(UniqueCleanup, CorrectCallbackAfterMove) {
  uint32_t counter = 0;
  {
    UniqueCleanup<int, std::function<void(int)>> a{1337,
                                                   [&counter](auto value) {
                                                     EXPECT_EQ(value, 1337);
                                                     counter++;
                                                   }};
    {
      auto b = std::move(a);
      auto c = std::move(b);
      // verify move assignment works as well
      b = std::move(c);
      ASSERT_EQ(counter, 0) << "Callback was called too early";
    }
    ASSERT_EQ(counter, 1)
        << "Callback was not called, or called too many times";
  }
  ASSERT_EQ(counter, 1)
      << "Callback was run on instance which was moved out of";
}

// _____________________________________________________________________________

TEST(UniqueCleanup, VerifyCorrectValueAccess) {
  std::unique_ptr<std::string> pointer;
  UniqueCleanup uniqueCleanup{std::make_unique<std::string>("42"), [](auto) {}};
  const auto& constView = uniqueCleanup;

  ASSERT_NE(*uniqueCleanup, nullptr);
  EXPECT_EQ(**uniqueCleanup, "42");
  // Check const implementations
  ASSERT_NE(constView->get(), nullptr);
  EXPECT_EQ(**constView, "42");

  uniqueCleanup->swap(pointer);

  EXPECT_EQ(*uniqueCleanup, nullptr);
  ASSERT_NE(pointer, nullptr);
  EXPECT_EQ(*pointer, "42");
}

// _____________________________________________________________________________
TEST(UniqueCleanup, MoveAssignmentRunsCleanupOfOverwrittenValue) {
  std::vector<int> cleanedUp;
  auto cleanup = [&cleanedUp](int value) { cleanedUp.push_back(value); };
  {
    UniqueCleanup<int, std::function<void(int)>> a{1, cleanup};
    UniqueCleanup<int, std::function<void(int)>> b{2, cleanup};
    a = std::move(b);
    EXPECT_THAT(cleanedUp, ::testing::ElementsAre(1));
    EXPECT_EQ(*a, 2);
    EXPECT_TRUE(a.isActive());
    // NOLINTNEXTLINE(bugprone-use-after-move)
    EXPECT_FALSE(b.isActive());

    // Self-assignment neither runs nor disables the cleanup.
    auto& alias = a;
    a = std::move(alias);
    EXPECT_THAT(cleanedUp, ::testing::ElementsAre(1));
    EXPECT_TRUE(a.isActive());
  }
  EXPECT_THAT(cleanedUp, ::testing::ElementsAre(1, 2));
}

// _____________________________________________________________________________
TEST(UniqueCleanup, MoveAssignmentIntoCancelledObject) {
  std::vector<int> cleanedUp;
  auto cleanup = [&cleanedUp](int value) { cleanedUp.push_back(value); };
  {
    UniqueCleanup<int, std::function<void(int)>> a{1, cleanup};
    UniqueCleanup<int, std::function<void(int)>> b{2, cleanup};
    std::move(a).cancel();
    EXPECT_FALSE(a.isActive());
    a = std::move(b);
    EXPECT_TRUE(cleanedUp.empty());
  }
  EXPECT_THAT(cleanedUp, ::testing::ElementsAre(2));
}

// _____________________________________________________________________________
TEST(UniqueCleanup, RunNow) {
  size_t counter = 0;
  auto cleanup = [&counter](std::unique_ptr<int>&& value) {
    ++counter;
    return *value;
  };
  {
    UniqueCleanup<std::unique_ptr<int>, decltype(cleanup)> a{
        std::make_unique<int>(42), cleanup};
    EXPECT_EQ(std::move(a).runNow(), 42);
    EXPECT_EQ(counter, 1);
    EXPECT_FALSE(a.isActive());
    // The cleanup took the value by reference, so it is still there.
    ASSERT_NE(*a, nullptr);
    EXPECT_EQ(**a, 42);
    // Running it a second time is not allowed.
    EXPECT_THROW(std::move(a).runNow(), ad_utility::Exception);
  }
  EXPECT_EQ(counter, 1);
}

// _____________________________________________________________________________
TEST(UniqueCleanup, RunNowPropagatesExceptions) {
  UniqueCleanup<int, std::function<void(int)>> a{
      1, [](int) { throw std::runtime_error{"cleanup failed"}; }};
  EXPECT_THROW(std::move(a).runNow(), std::runtime_error);
  EXPECT_FALSE(a.isActive());
}

// _____________________________________________________________________________
TEST(UniqueCleanup, ThrowingCleanupInDestructorTerminates) {
  EXPECT_DEATH_IF_SUPPORTED(
      (UniqueCleanup<int, std::function<void(int)>>{
          1, [](int) { throw std::runtime_error{"cleanup failed"}; }}),
      "The cleanup of a `UniqueCleanup` failed");
}
