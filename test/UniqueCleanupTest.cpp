//   Copyright 2023, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <memory>

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

TEST(UniqueCleanup, CorrectCallbackOnInvokeManuallyAndCancel) {
  uint32_t counter = 0;
  {
    UniqueCleanup a{1337, [&counter](auto value) {
                      EXPECT_EQ(value, 1337);
                      counter++;
                    }};
    ASSERT_EQ(counter, 0) << "Callback was called too early";
    std::move(a).invokeManuallyAndCancel();
    ASSERT_EQ(counter, 1)
        << "Callback was not called, or called too many times";
  }
  ASSERT_EQ(counter, 1)
      << "Callback was run on instance which was moved out of";
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
