//   Copyright 2023, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "util/CancellationHandle.h"
#include "util/GTestHelpers.h"
#include "util/jthread.h"

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

// _____________________________________________________________________________

TEST(CancellationException, verifyConstructorDoesNotAcceptNoReason) {
  EXPECT_THROW(
      CancellationException exception(CancellationState::NOT_CANCELLED, ""),
      ad_utility::Exception);
}

// _____________________________________________________________________________

TEST(CancellationHandle, verifyNotCancelledByDefault) {
  CancellationHandle handle;

  EXPECT_FALSE(handle.isCancelled());
  EXPECT_NO_THROW(handle.throwIfCancelled(""));
  EXPECT_NO_THROW(handle.throwIfCancelled([]() { return ""; }));
}

// _____________________________________________________________________________

TEST(CancellationHandle, verifyCancelWithWrongReasonThrows) {
  CancellationHandle handle;

  EXPECT_THROW(handle.cancel(CancellationState::NOT_CANCELLED),
               ad_utility::Exception);
}

// _____________________________________________________________________________

auto detail = "Some Detail";

TEST(CancellationHandle, verifyTimeoutCancellationWorks) {
  CancellationHandle handle;

  handle.cancel(CancellationState::TIMEOUT);

  auto timeoutMessageMatcher = AllOf(HasSubstr(detail), HasSubstr("timeout"));
  EXPECT_TRUE(handle.isCancelled());
  AD_EXPECT_THROW_WITH_MESSAGE_AND_TYPE(handle.throwIfCancelled(detail),
                                        timeoutMessageMatcher,
                                        CancellationException);
  AD_EXPECT_THROW_WITH_MESSAGE_AND_TYPE(
      handle.throwIfCancelled([]() { return detail; }), timeoutMessageMatcher,
      CancellationException);
}

// _____________________________________________________________________________

TEST(CancellationHandle, verifyManualCancellationWorks) {
  CancellationHandle handle;

  handle.cancel(CancellationState::MANUAL);

  auto cancellationMessageMatcher =
      AllOf(HasSubstr(detail), HasSubstr("manual cancellation"));
  EXPECT_TRUE(handle.isCancelled());
  AD_EXPECT_THROW_WITH_MESSAGE_AND_TYPE(handle.throwIfCancelled(detail),
                                        cancellationMessageMatcher,
                                        CancellationException);
  AD_EXPECT_THROW_WITH_MESSAGE_AND_TYPE(
      handle.throwIfCancelled([]() { return detail; }),
      cancellationMessageMatcher, CancellationException);
}

// _____________________________________________________________________________

TEST(CancellationHandle, verifyCancellationWorksWithMultipleThreads) {
  CancellationHandle handle;

  ad_utility::JThread thread{[&]() {
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
}
