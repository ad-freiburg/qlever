//   Copyright 2023, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "util/CancellationHandle.h"

using ad_utility::CancellationException;
using ad_utility::CancellationHandle;
using ad_utility::CancellationState;
using ::testing::HasSubstr;

using namespace std::chrono_literals;

TEST(CancellationException, verifyConstructorMessageIsPassed) {
  auto message = "Message";
  CancellationException exception{message};
  EXPECT_STREQ(message, exception.what());
}

TEST(CancellationException, verifyConstructorDoesNotAcceptNoReason) {
  EXPECT_THROW(
      CancellationException exception(CancellationState::NOT_CANCELLED, ""),
      ad_utility::Exception);
}

TEST(CancellationHandle, verifyNotCancelledByDefault) {
  CancellationHandle handle;

  EXPECT_FALSE(handle.isCancelled());
  EXPECT_NO_THROW(handle.throwIfCancelled(""));
  EXPECT_NO_THROW(handle.throwIfCancelled([]() { return ""; }));
}

TEST(CancellationHandle, verifyCancelWithWrongReasonThrows) {
  CancellationHandle handle;

  EXPECT_THROW(handle.cancel(CancellationState::NOT_CANCELLED),
               ad_utility::Exception);
}

TEST(CancellationHandle, verifyTimeoutCancellationWorks) {
  CancellationHandle handle;

  handle.cancel(CancellationState::TIMEOUT);

  EXPECT_TRUE(handle.isCancelled());
  EXPECT_THROW(
      {
        try {
          handle.throwIfCancelled("Some Detail");
        } catch (const CancellationException& exception) {
          EXPECT_THAT(exception.what(), HasSubstr("Some Detail"));
          EXPECT_THAT(exception.what(), HasSubstr("timeout"));
          throw;
        }
      },
      CancellationException);
  EXPECT_THROW(
      {
        try {
          handle.throwIfCancelled([]() { return "Some Detail"; });
        } catch (const CancellationException& exception) {
          EXPECT_THAT(exception.what(), HasSubstr("Some Detail"));
          EXPECT_THAT(exception.what(), HasSubstr("timeout"));
          throw;
        }
      },
      CancellationException);
}

TEST(CancellationHandle, verifyManualCancellationWorks) {
  CancellationHandle handle;

  handle.cancel(CancellationState::MANUAL);

  EXPECT_TRUE(handle.isCancelled());
  EXPECT_THROW(
      {
        try {
          handle.throwIfCancelled("Some Detail");
        } catch (const CancellationException& exception) {
          EXPECT_THAT(exception.what(), HasSubstr("Some Detail"));
          EXPECT_THAT(exception.what(), HasSubstr("manual cancellation"));
          throw;
        }
      },
      CancellationException);
  EXPECT_THROW(
      {
        try {
          handle.throwIfCancelled([]() { return "Some Detail"; });
        } catch (const CancellationException& exception) {
          EXPECT_THAT(exception.what(), HasSubstr("Some Detail"));
          EXPECT_THAT(exception.what(), HasSubstr("manual cancellation"));
          throw;
        }
      },
      CancellationException);
}

TEST(CancellationHandle, verifyCancellationWorksWithMultipleThreads) {
  CancellationHandle handle;

  std::thread thread{[&]() {
    std::this_thread::sleep_for(5ms);
    handle.cancel(CancellationState::TIMEOUT);
  }};

  EXPECT_THROW(
      {
        auto end = std::chrono::steady_clock::now() + 100ms;
        while (std::chrono::steady_clock::now() < end) {
          handle.throwIfCancelled("Some Detail");
        }
      },
      CancellationException);
  EXPECT_TRUE(handle.isCancelled());
  thread.join();
}
