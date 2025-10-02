//   Copyright 2023, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>
//
// Copyright 2025, Bayerische Motoren Werke Aktiengesellschaft (BMW AG)

#include <absl/cleanup/cleanup.h>
#include <absl/strings/str_cat.h>
#include <gmock/gmock.h>

#include "backports/keywords.h"
#include "util/CancellationHandle.h"
#include "util/GTestHelpers.h"
#include "util/jthread.h"

using ad_utility::CancellationException;
using ad_utility::CancellationHandle;
using enum ad_utility::CancellationState;
using enum ad_utility::detail::CancellationMode;
using ::testing::AllOf;
using ::testing::ContainsRegex;
using ::testing::HasSubstr;

using namespace std::chrono_literals;

ad_utility::source_location location = AD_CURRENT_SOURCE_LOC();
const int expectedLocationLine = __LINE__ - 1;
const auto expectedLocation =
    absl::StrCat("CancellationHandleTest.cpp:", expectedLocationLine);

template <typename CancellationHandle>
struct CancellationHandleFixture : public ::testing::Test {
  CancellationHandle handle_;
};
using WithAndWithoutWatchDog =
    ::testing::Types<CancellationHandle<ENABLED>,
                     CancellationHandle<NO_WATCH_DOG>>;
TYPED_TEST_SUITE(CancellationHandleFixture, WithAndWithoutWatchDog);

// _____________________________________________________________________________

TEST(CancellationException, verifyConstructorMessageIsPassed) {
  auto message = "Message";
  CancellationException exception{message};
  EXPECT_STREQ(message, exception.what());
}

// _____________________________________________________________________________

TEST(CancellationException, verifyConstructorDoesNotAcceptNoReason) {
  EXPECT_THROW(CancellationException exception(NOT_CANCELLED),
               ad_utility::Exception);
}

// _____________________________________________________________________________

TEST(CancellationException, verifySetOperationModifiedTheMessageAsExpected) {
  auto message = "Message";
  auto operation = "Operation";
  auto otherThing = "Other Thing";
  {
    CancellationException exception{message};

    exception.setOperation(operation);
    EXPECT_THAT(exception.what(),
                AllOf(HasSubstr(message), HasSubstr(operation)));

    // Verify double call does not overwrite initial operation.
    exception.setOperation(otherThing);
    EXPECT_THAT(exception.what(),
                AllOf(HasSubstr(message), HasSubstr(operation),
                      Not(HasSubstr(otherThing))));
  }
  {
    CancellationException exception{MANUAL};

    exception.setOperation(operation);
    EXPECT_THAT(exception.what(), HasSubstr(operation));

    // Verify double call does not overwrite initial operation.
    exception.setOperation(otherThing);
    EXPECT_THAT(exception.what(),
                AllOf(HasSubstr(operation), Not(HasSubstr(otherThing))));
  }
}

// _____________________________________________________________________________

TYPED_TEST(CancellationHandleFixture, verifyNotCancelledByDefault) {
  auto& handle = this->handle_;

  EXPECT_FALSE(handle.isCancelled());
  EXPECT_NO_THROW(handle.throwIfCancelled());
  EXPECT_NO_THROW(handle.throwIfCancelled());
}

// _____________________________________________________________________________

TYPED_TEST(CancellationHandleFixture, verifyCancelWithWrongReasonThrows) {
  auto& handle = this->handle_;
  EXPECT_THROW(handle.cancel(NOT_CANCELLED), ad_utility::Exception);
}

// _____________________________________________________________________________

TYPED_TEST(CancellationHandleFixture, verifyTimeoutCancellationWorks) {
  auto& handle = this->handle_;

  handle.cancel(TIMEOUT);

  EXPECT_TRUE(handle.isCancelled());
  AD_EXPECT_THROW_WITH_MESSAGE_AND_TYPE(handle.throwIfCancelled(location),
                                        HasSubstr("timed out"),
                                        CancellationException);
}

// _____________________________________________________________________________

TYPED_TEST(CancellationHandleFixture, verifyManualCancellationWorks) {
  auto& handle = this->handle_;

  handle.cancel(MANUAL);

  EXPECT_TRUE(handle.isCancelled());
  AD_EXPECT_THROW_WITH_MESSAGE_AND_TYPE(handle.throwIfCancelled(location),
                                        HasSubstr("manually cancelled"),
                                        CancellationException);
}

// _____________________________________________________________________________

TYPED_TEST(CancellationHandleFixture,
           verifyCancellationWorksWithMultipleThreads) {
  auto& handle = this->handle_;

  ad_utility::JThread thread{[&]() {
    std::this_thread::sleep_for(5ms);
    handle.cancel(TIMEOUT);
  }};

  EXPECT_THROW(
      {
        auto end = std::chrono::steady_clock::now() + 100ms;
        while (std::chrono::steady_clock::now() < end) {
          handle.throwIfCancelled();
        }
      },
      CancellationException);
  EXPECT_TRUE(handle.isCancelled());
}

// _____________________________________________________________________________

TEST(CancellationHandle, ensureObjectLifetimeIsValidWithoutWatchDogStarted) {
  EXPECT_NO_THROW(CancellationHandle<ENABLED>{});
}

// _____________________________________________________________________________

namespace ad_utility {

TEST(CancellationHandle, verifyWatchDogDoesChangeState) {
#ifdef _QLEVER_NO_TIMING_TESTS
  GTEST_SKIP_("because _QLEVER_NO_TIMING_TESTS defined");
#endif
  CancellationHandle<ENABLED> handle;

  EXPECT_EQ(handle.cancellationState_, NOT_CANCELLED);
  handle.startWatchDog();

  // Give thread some time to start
  std::this_thread::sleep_for(10ms);
  EXPECT_EQ(handle.cancellationState_, WAITING_FOR_CHECK);

  std::this_thread::sleep_for(DESIRED_CANCELLATION_CHECK_INTERVAL);
  EXPECT_EQ(handle.cancellationState_, CHECK_WINDOW_MISSED);
}

// _____________________________________________________________________________

TEST(CancellationHandle, verifyWatchDogDoesNotChangeStateAfterCancel) {
#ifdef _QLEVER_NO_TIMING_TESTS
  GTEST_SKIP_("because _QLEVER_NO_TIMING_TESTS defined");
#endif
  CancellationHandle<ENABLED> handle;
  handle.startWatchDog();

  // Give thread some time to start
  std::this_thread::sleep_for(10ms);

  handle.cancellationState_ = MANUAL;
  std::this_thread::sleep_for(DESIRED_CANCELLATION_CHECK_INTERVAL);
  EXPECT_EQ(handle.cancellationState_, MANUAL);

  handle.cancellationState_ = TIMEOUT;
  std::this_thread::sleep_for(DESIRED_CANCELLATION_CHECK_INTERVAL);
  EXPECT_EQ(handle.cancellationState_, TIMEOUT);
}

// _____________________________________________________________________________

TEST(CancellationHandle, ensureDestructorReturnsFastWithActiveWatchDog) {
  std::chrono::steady_clock::time_point start;
  {
    CancellationHandle<ENABLED> handle;
    handle.startWatchDog();
    start = std::chrono::steady_clock::now();
  }
  auto duration = std::chrono::steady_clock::now() - start;
  // Ensure we don't need to wait for the entire interval to finish.
  EXPECT_LT(duration, DESIRED_CANCELLATION_CHECK_INTERVAL);
}

// _____________________________________________________________________________

TEST(CancellationHandle, verifyResetWatchDogStateDoesProperlyResetState) {
  CancellationHandle<ENABLED> handle;

  handle.cancellationState_ = NOT_CANCELLED;
  handle.resetWatchDogState();
  EXPECT_EQ(handle.cancellationState_, NOT_CANCELLED);

  handle.cancellationState_ = WAITING_FOR_CHECK;
  handle.resetWatchDogState();
  EXPECT_EQ(handle.cancellationState_, NOT_CANCELLED);

  handle.cancellationState_ = CHECK_WINDOW_MISSED;
  handle.resetWatchDogState();
  EXPECT_EQ(handle.cancellationState_, NOT_CANCELLED);

  handle.cancellationState_ = MANUAL;
  handle.resetWatchDogState();
  EXPECT_EQ(handle.cancellationState_, MANUAL);

  handle.cancellationState_ = TIMEOUT;
  handle.resetWatchDogState();
  EXPECT_EQ(handle.cancellationState_, TIMEOUT);
}

// _____________________________________________________________________________

TEST(CancellationHandle, verifyResetWatchDogStateIsNoOpWithoutWatchDog) {
  CancellationHandle<NO_WATCH_DOG> handle;

  handle.cancellationState_ = NOT_CANCELLED;
  handle.resetWatchDogState();
  EXPECT_EQ(handle.cancellationState_, NOT_CANCELLED);

  handle.cancellationState_ = WAITING_FOR_CHECK;
  handle.resetWatchDogState();
  EXPECT_EQ(handle.cancellationState_, WAITING_FOR_CHECK);

  handle.cancellationState_ = CHECK_WINDOW_MISSED;
  handle.resetWatchDogState();
  EXPECT_EQ(handle.cancellationState_, CHECK_WINDOW_MISSED);

  handle.cancellationState_ = MANUAL;
  handle.resetWatchDogState();
  EXPECT_EQ(handle.cancellationState_, MANUAL);

  handle.cancellationState_ = TIMEOUT;
  handle.resetWatchDogState();
  EXPECT_EQ(handle.cancellationState_, TIMEOUT);
}

// _____________________________________________________________________________

TEST(CancellationHandle, verifyCheckDoesPleaseWatchDog) {
  CancellationHandle<ENABLED> handle;

  handle.cancellationState_ = WAITING_FOR_CHECK;
  EXPECT_NO_THROW(handle.throwIfCancelled());
  EXPECT_EQ(handle.cancellationState_, NOT_CANCELLED);

  handle.cancellationState_ = CHECK_WINDOW_MISSED;
  EXPECT_NO_THROW(handle.throwIfCancelled());
  EXPECT_EQ(handle.cancellationState_, NOT_CANCELLED);
}

// _____________________________________________________________________________

TEST(CancellationHandle, verifyCheckDoesNotOverrideCancelledState) {
  CancellationHandle<ENABLED> handle;

  handle.cancellationState_ = MANUAL;
  EXPECT_THROW(handle.throwIfCancelled(), CancellationException);
  EXPECT_EQ(handle.cancellationState_, MANUAL);

  handle.cancellationState_ = TIMEOUT;
  EXPECT_THROW(handle.throwIfCancelled(), CancellationException);
  EXPECT_EQ(handle.cancellationState_, TIMEOUT);
}

// _____________________________________________________________________________

TEST(CancellationHandle, verifyCheckAfterDeadlineMissDoesReportProperly) {
  // If the log level is not high enough this test will fail
  static_assert(LOGLEVEL >= WARN);
  auto& choice = ad_utility::LogstreamChoice::get();
  CancellationHandle<ENABLED> handle;

  auto& originalOStream = choice.getStream();
  absl::Cleanup cleanup{[&]() { choice.setStream(&originalOStream); }};

  std::ostringstream testStream;
  choice.setStream(&testStream);

  handle.startTimeoutWindow_ = std::chrono::steady_clock::now();
  handle.cancellationState_ = CHECK_WINDOW_MISSED;
  EXPECT_NO_THROW(handle.throwIfCancelled(location));
  EXPECT_EQ(handle.cancellationState_, NOT_CANCELLED);

  EXPECT_THAT(
      std::move(testStream).str(),
      AllOf(HasSubstr(expectedLocation),
            HasSubstr(ParseableDuration{DESIRED_CANCELLATION_CHECK_INTERVAL}
                          .toString()),
            // Check for small miss window
            ContainsRegex("least 5[0-9]ms")));
  // This test assumes this interval to be 50ms to build the regex
  static_assert(DESIRED_CANCELLATION_CHECK_INTERVAL == 50ms);
}

// _____________________________________________________________________________

TEST(CancellationHandle, verifyPleaseWatchDogReportsOnlyWhenNecessary) {
  // If the log level is not high enough this test will fail
  static_assert(LOGLEVEL >= WARN);
  auto& choice = ad_utility::LogstreamChoice::get();
  CancellationHandle<ENABLED> handle;

  auto& originalOStream = choice.getStream();
  absl::Cleanup cleanup{[&]() { choice.setStream(&originalOStream); }};

  std::ostringstream testStream;
  choice.setStream(&testStream);

  handle.startTimeoutWindow_ = std::chrono::steady_clock::now();
  handle.cancellationState_ = CHECK_WINDOW_MISSED;

  // The first call should trigger a log
  handle.pleaseWatchDog(CHECK_WINDOW_MISSED, location, detail::printNothing);

  EXPECT_EQ(handle.cancellationState_, NOT_CANCELLED);
  EXPECT_THAT(std::move(testStream).str(), HasSubstr(expectedLocation));

  testStream.str("");

  // The second call should not trigger a log because the state has already
  // been reset
  handle.pleaseWatchDog(CHECK_WINDOW_MISSED, location, detail::printNothing);

  EXPECT_EQ(handle.cancellationState_, NOT_CANCELLED);
  EXPECT_THAT(std::move(testStream).str(), Not(HasSubstr(expectedLocation)));

  handle.cancellationState_ = CHECK_WINDOW_MISSED;
  testStream.str("");

  // WAITING_FOR_CHECK should not trigger a log
  handle.pleaseWatchDog(WAITING_FOR_CHECK, location, detail::printNothing);

  EXPECT_EQ(handle.cancellationState_, NOT_CANCELLED);
  EXPECT_THAT(std::move(testStream).str(), Not(HasSubstr(expectedLocation)));

  handle.cancellationState_ = CHECK_WINDOW_MISSED;

  constexpr auto printSomething = []() { return "something"; };
  // The first call should trigger a log with more details
  handle.pleaseWatchDog(CHECK_WINDOW_MISSED, location, printSomething);

  EXPECT_EQ(handle.cancellationState_, NOT_CANCELLED);
  EXPECT_THAT(std::move(testStream).str(),
              AllOf(HasSubstr(expectedLocation), HasSubstr(printSomething())));

  testStream.str("");

  // The second call should not trigger a log
  handle.pleaseWatchDog(CHECK_WINDOW_MISSED, location, printSomething);

  EXPECT_EQ(handle.cancellationState_, NOT_CANCELLED);
  EXPECT_THAT(
      std::move(testStream).str(),
      Not(AllOf(HasSubstr(expectedLocation), HasSubstr(printSomething()))));

  handle.cancellationState_ = CHECK_WINDOW_MISSED;
  testStream.str("");

  // WAITING_FOR_CHECK should not trigger a log
  handle.pleaseWatchDog(WAITING_FOR_CHECK, location, printSomething);

  EXPECT_EQ(handle.cancellationState_, NOT_CANCELLED);
  EXPECT_THAT(
      std::move(testStream).str(),
      Not(AllOf(HasSubstr(expectedLocation), HasSubstr(printSomething()))));
}

// _____________________________________________________________________________

TEST(CancellationHandle, verifyPleaseWatchDogDoesNotAcceptInvalidState) {
  using detail::printNothing;
  CancellationHandle<ENABLED> handle;
  EXPECT_THROW(handle.pleaseWatchDog(NOT_CANCELLED, location, printNothing),
               ad_utility::Exception);
  EXPECT_THROW(handle.pleaseWatchDog(MANUAL, location, printNothing),
               ad_utility::Exception);
  EXPECT_THROW(handle.pleaseWatchDog(TIMEOUT, location, printNothing),
               ad_utility::Exception);
}

// _____________________________________________________________________________

TEST(CancellationHandle, verifyIsCancelledDoesPleaseWatchDog) {
  // If the log level is not high enough this test will fail
  static_assert(LOGLEVEL >= WARN);
  auto& choice = ad_utility::LogstreamChoice::get();
  CancellationHandle<ENABLED> handle;

  auto& originalOStream = choice.getStream();
  absl::Cleanup cleanup{[&]() { choice.setStream(&originalOStream); }};

  std::ostringstream testStream;
  choice.setStream(&testStream);

  handle.startTimeoutWindow_ = std::chrono::steady_clock::now();
  handle.cancellationState_ = CHECK_WINDOW_MISSED;

  handle.isCancelled(location);

  EXPECT_EQ(handle.cancellationState_, NOT_CANCELLED);
  EXPECT_THAT(std::move(testStream).str(), HasSubstr(expectedLocation));

  handle.cancellationState_ = WAITING_FOR_CHECK;
  testStream.str("");

  handle.isCancelled(location);

  EXPECT_EQ(handle.cancellationState_, NOT_CANCELLED);
  EXPECT_THAT(std::move(testStream).str(), Not(HasSubstr(expectedLocation)));
}

// _____________________________________________________________________________

TEST(CancellationHandle, verifyWatchDogEndsEarlyIfCancelled) {
  CancellationHandle<ENABLED> handle;
  handle.cancel(MANUAL);

  handle.startWatchDog();
  // Wait for Watchdog to start
  std::this_thread::sleep_for(1ms);

  handle.cancellationState_ = WAITING_FOR_CHECK;

  // Wait for one watchdog cycle + tolerance
  std::this_thread::sleep_for(DESIRED_CANCELLATION_CHECK_INTERVAL + 1ms);
  // If the watchdog were running it would've set this to CHECK_WINDOW_MISSED
  EXPECT_EQ(handle.cancellationState_, WAITING_FOR_CHECK);
}

// _____________________________________________________________________________

TEST(CancellationHandle, expectDisabledHandleIsAlwaysFalse) {
  CancellationHandle<DISABLED> handle;

  EXPECT_FALSE(handle.isCancelled());
  EXPECT_NO_THROW(handle.throwIfCancelled());
}

template <typename T>
QL_CONSTEVAL bool isMemberFunction([[maybe_unused]] T funcPtr) {
  return std::is_member_function_pointer_v<T>;
}

// Make sure member functions still exist when no watch dog functionality
// is available to make the code simpler. In this case the functions should
// be no-op.
static_assert(
    isMemberFunction(&CancellationHandle<NO_WATCH_DOG>::startWatchDog));
static_assert(
    isMemberFunction(&CancellationHandle<NO_WATCH_DOG>::resetWatchDogState));
static_assert(isMemberFunction(&CancellationHandle<DISABLED>::startWatchDog));
static_assert(
    isMemberFunction(&CancellationHandle<DISABLED>::resetWatchDogState));
static_assert(isMemberFunction(&CancellationHandle<DISABLED>::cancel));
static_assert(isMemberFunction(&CancellationHandle<DISABLED>::isCancelled));
// Ideally we'd add a static assertion for throwIfCancelled here too, but
// because the function is overloaded, we can't get a function pointer for it.

// Constexpr test cases
static_assert(trimFileName("") == "");
static_assert(trimFileName("/") == "");
static_assert(trimFileName("folder/") == "");
static_assert(trimFileName("//////") == "");
static_assert(trimFileName("../Test.cpp") == "Test.cpp");
static_assert(trimFileName("Test.cpp") == "Test.cpp");
static_assert(trimFileName("./folder/Test.cpp") == "Test.cpp");

}  // namespace ad_utility
