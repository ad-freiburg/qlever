//   Copyright 2023, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#include <absl/cleanup/cleanup.h>
#include <gmock/gmock.h>

#include "util/CancellationHandle.h"
#include "util/GTestHelpers.h"
#include "util/jthread.h"

using ad_utility::CancellationException;
using ad_utility::CancellationHandle;
using ad_utility::CancellationState;
using ::testing::AllOf;
using ::testing::ContainsRegex;
using ::testing::HasSubstr;

using namespace std::chrono_literals;

template <typename CancellationHandle>
struct CancellationHandleFixture : public ::testing::Test {
  CancellationHandle handle_;
};
using WithAndWithoutWatchDog =
    ::testing::Types<CancellationHandle<true>, CancellationHandle<false>>;
TYPED_TEST_SUITE(CancellationHandleFixture, WithAndWithoutWatchDog);

// _____________________________________________________________________________

TEST(CancellationHandle, verifyConstructorMessageIsPassed) {
  auto message = "Message";
  CancellationException exception{message};
  EXPECT_STREQ(message, exception.what());
}

// _____________________________________________________________________________

TEST(CancellationHandle, verifyConstructorDoesNotAcceptNoReason) {
  EXPECT_THROW(
      CancellationException exception(CancellationState::NOT_CANCELLED, ""),
      ad_utility::Exception);
}

// _____________________________________________________________________________

TYPED_TEST(CancellationHandleFixture, verifyNotCancelledByDefault) {
  auto& handle = this->handle_;

  EXPECT_FALSE(handle.isCancelled());
  EXPECT_NO_THROW(handle.throwIfCancelled(""));
  EXPECT_NO_THROW(handle.throwIfCancelled([]() { return ""; }));
}

// _____________________________________________________________________________

TYPED_TEST(CancellationHandleFixture, verifyCancelWithWrongReasonThrows) {
  auto& handle = this->handle_;
  EXPECT_THROW(handle.cancel(CancellationState::NOT_CANCELLED),
               ad_utility::Exception);
}

// _____________________________________________________________________________

auto detail = "Some Detail";

TYPED_TEST(CancellationHandleFixture, verifyTimeoutCancellationWorks) {
  auto& handle = this->handle_;

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

TYPED_TEST(CancellationHandleFixture, verifyManualCancellationWorks) {
  auto& handle = this->handle_;

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

TYPED_TEST(CancellationHandleFixture,
           verifyCancellationWorksWithMultipleThreads) {
  auto& handle = this->handle_;

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

// _____________________________________________________________________________

TEST(CancellationHandle, ensureObjectLifetimeIsValidWithoutWatchDogStarted) {
  EXPECT_NO_THROW(CancellationHandle<true>{});
}

// _____________________________________________________________________________

namespace ad_utility {

TEST(CancellationHandle, verifyWatchDogDoesChangeState) {
  CancellationHandle<true> handle;

  EXPECT_EQ(handle.cancellationState_, CancellationState::NOT_CANCELLED);
  handle.startWatchDog();

  // Give thread some time to start
  std::this_thread::sleep_for(10ms);
  EXPECT_EQ(handle.cancellationState_, CancellationState::WAITING_FOR_CHECK);

  std::this_thread::sleep_for(detail::DESIRED_CHECK_INTERVAL);
  EXPECT_EQ(handle.cancellationState_, CancellationState::CHECK_WINDOW_MISSED);
}

// _____________________________________________________________________________

TEST(CancellationHandle, verifyResetWatchDogStateDoesProperlyResetState) {
  CancellationHandle<true> handle;

  handle.cancellationState_ = CancellationState::NOT_CANCELLED;
  handle.resetWatchDogState();
  EXPECT_EQ(handle.cancellationState_, CancellationState::NOT_CANCELLED);

  handle.cancellationState_ = CancellationState::WAITING_FOR_CHECK;
  handle.resetWatchDogState();
  EXPECT_EQ(handle.cancellationState_, CancellationState::NOT_CANCELLED);

  handle.cancellationState_ = CancellationState::CHECK_WINDOW_MISSED;
  handle.resetWatchDogState();
  EXPECT_EQ(handle.cancellationState_, CancellationState::NOT_CANCELLED);

  handle.cancellationState_ = CancellationState::MANUAL;
  handle.resetWatchDogState();
  EXPECT_EQ(handle.cancellationState_, CancellationState::MANUAL);

  handle.cancellationState_ = CancellationState::TIMEOUT;
  handle.resetWatchDogState();
  EXPECT_EQ(handle.cancellationState_, CancellationState::TIMEOUT);
}

// _____________________________________________________________________________

TEST(CancellationHandle, verifyResetWatchDogStateIsNoOpWithoutWatchDog) {
  CancellationHandle<false> handle;

  handle.cancellationState_ = CancellationState::NOT_CANCELLED;
  handle.resetWatchDogState();
  EXPECT_EQ(handle.cancellationState_, CancellationState::NOT_CANCELLED);

  handle.cancellationState_ = CancellationState::WAITING_FOR_CHECK;
  handle.resetWatchDogState();
  EXPECT_EQ(handle.cancellationState_, CancellationState::WAITING_FOR_CHECK);

  handle.cancellationState_ = CancellationState::CHECK_WINDOW_MISSED;
  handle.resetWatchDogState();
  EXPECT_EQ(handle.cancellationState_, CancellationState::CHECK_WINDOW_MISSED);

  handle.cancellationState_ = CancellationState::MANUAL;
  handle.resetWatchDogState();
  EXPECT_EQ(handle.cancellationState_, CancellationState::MANUAL);

  handle.cancellationState_ = CancellationState::TIMEOUT;
  handle.resetWatchDogState();
  EXPECT_EQ(handle.cancellationState_, CancellationState::TIMEOUT);
}

// _____________________________________________________________________________

TEST(CancellationHandle, verifyCheckDoesPleaseWatchDog) {
  CancellationHandle<true> handle;
  // Because watch dog operates async, simulate it here for stable tests.
  handle.watchDogRunning_ = true;

  handle.cancellationState_ = CancellationState::WAITING_FOR_CHECK;
  EXPECT_NO_THROW(handle.throwIfCancelled(""));
  EXPECT_EQ(handle.cancellationState_, CancellationState::NOT_CANCELLED);

  handle.cancellationState_ = CancellationState::CHECK_WINDOW_MISSED;
  EXPECT_NO_THROW(handle.throwIfCancelled(""));
  EXPECT_EQ(handle.cancellationState_, CancellationState::NOT_CANCELLED);
}

// _____________________________________________________________________________

TEST(CancellationHandle, verifyCheckDoesNotOverrideCancelledState) {
  CancellationHandle<true> handle;

  handle.cancellationState_ = CancellationState::MANUAL;
  EXPECT_THROW(handle.throwIfCancelled(""), CancellationException);
  EXPECT_EQ(handle.cancellationState_, CancellationState::MANUAL);

  handle.cancellationState_ = CancellationState::TIMEOUT;
  EXPECT_THROW(handle.throwIfCancelled(""), CancellationException);
  EXPECT_EQ(handle.cancellationState_, CancellationState::TIMEOUT);
}

// _____________________________________________________________________________

TEST(CancellationHandle, verifyCheckAfterDeadlineMissDoesReportProperly) {
  auto& choice = ad_utility::LogstreamChoice::get();
  CancellationHandle<true> handle;
  // Because watch dog operates async, simulate it here for stable tests.
  handle.watchDogRunning_ = true;

  auto& originalOStream = choice.getStream();
  absl::Cleanup cleanup{[&]() { choice.setStream(&originalOStream); }};

  std::ostringstream testStream;
  choice.setStream(&testStream);

  handle.startTimeoutWindow_ =
      std::chrono::steady_clock::now().time_since_epoch().count();
  handle.cancellationState_ = CancellationState::CHECK_WINDOW_MISSED;
  EXPECT_NO_THROW(handle.throwIfCancelled("my-detail"));
  EXPECT_EQ(handle.cancellationState_, CancellationState::NOT_CANCELLED);

  EXPECT_THAT(
      std::move(testStream).str(),
      AllOf(HasSubstr("my-detail"),
            HasSubstr(
                ParseableDuration{detail::DESIRED_CHECK_INTERVAL}.toString()),
            // Check for small miss window
            ContainsRegex("by [0-9]ms")));
}

// Make sure member functions still exist when no watch dog functionality
// is available to make the code simpler. In this case the functions should
// be no-op.
static_assert(std::is_member_function_pointer_v<
              decltype(&CancellationHandle<false>::startWatchDog)>);

}  // namespace ad_utility
