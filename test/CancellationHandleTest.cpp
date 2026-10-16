// Copyright 2023 - 2026 The QLever Authors, in particular:
//
// 2023 - 2026 Robin Textor-Falconi <textorr@cs.uni-freiburg.de>, UFR
// 2025 Bayerische Motoren Werke Aktiengesellschaft (BMW AG)
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

#include <absl/cleanup/cleanup.h>
#include <absl/functional/any_invocable.h>
#include <absl/strings/str_cat.h>
#include <gmock/gmock.h>

#include <vector>

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

// Record the tasks scheduled by the watch dog instead of running them, such
// that the tests can run its steps one by one via `runNext`.
struct FakeRunAfter {
  std::vector<absl::AnyInvocable<void()>> tasks_;

  // Return a `RunAfter` that schedules its tasks on this object.
  CancellationHandle<>::RunAfter runAfter() {
    return [this](std::chrono::milliseconds delay,
                  absl::AnyInvocable<void()> task) {
      EXPECT_EQ(delay, DESIRED_CANCELLATION_CHECK_INTERVAL);
      tasks_.push_back(std::move(task));
    };
  }

  // Run the oldest scheduled task, which might schedule a new one.
  void runNext() {
    ASSERT_FALSE(tasks_.empty());
    auto task = std::move(tasks_.front());
    tasks_.erase(tasks_.begin());
    task();
  }
};

// _____________________________________________________________________________

TEST(CancellationException, verifyConstructorMessageIsPassed) {
  auto message = "Message";
  CancellationException exception{MANUAL, message};
  EXPECT_STREQ(message, exception.what());
}

// _____________________________________________________________________________

TEST(CancellationException, verifyConstructorDoesNotAcceptNoReason) {
  EXPECT_THROW(CancellationException exception(NOT_CANCELLED),
               ad_utility::Exception);
}

// _____________________________________________________________________________

TEST(CancellationException, stateAccessorReturnsConstructorReason) {
  EXPECT_EQ(CancellationException{TIMEOUT}.state(), TIMEOUT);
  EXPECT_EQ(CancellationException{MANUAL}.state(), MANUAL);
  // Custom message paired with a known reason keeps both.
  CancellationException withReason{TIMEOUT, "estimate exceeded remaining time"};
  EXPECT_EQ(withReason.state(), TIMEOUT);
  EXPECT_STREQ(withReason.what(), "estimate exceeded remaining time");
}

// _____________________________________________________________________________

TEST(CancellationException, verifySetOperationModifiedTheMessageAsExpected) {
  auto message = "Message";
  auto operation = "Operation";
  auto otherThing = "Other Thing";
  {
    CancellationException exception{MANUAL, message};

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

namespace ad_utility {

// Test that the watch dog advances the state step by step and that a check
// resets it.
TEST(CancellationHandle, verifyWatchDogDoesChangeState) {
  auto handle = std::make_shared<CancellationHandle<ENABLED>>();
  FakeRunAfter fake;

  EXPECT_EQ(handle->cancellationState_, NOT_CANCELLED);
  CancellationHandle<ENABLED>::registerWatchDog(handle, fake.runAfter());

  // The first step is run immediately.
  EXPECT_EQ(handle->cancellationState_, WAITING_FOR_CHECK);
  EXPECT_EQ(fake.tasks_.size(), 1);

  fake.runNext();
  EXPECT_EQ(handle->cancellationState_, CHECK_WINDOW_MISSED);
  EXPECT_EQ(fake.tasks_.size(), 1);

  fake.runNext();
  EXPECT_EQ(handle->cancellationState_, CHECK_WINDOW_MISSED);
  EXPECT_EQ(fake.tasks_.size(), 1);

  // A check resets the state, the next step starts the window again.
  handle->throwIfCancelled();
  EXPECT_EQ(handle->cancellationState_, NOT_CANCELLED);
  fake.runNext();
  EXPECT_EQ(handle->cancellationState_, WAITING_FOR_CHECK);
  EXPECT_EQ(fake.tasks_.size(), 1);
}

// Test that the watch dog leaves a cancelled handle alone and stops.
TEST(CancellationHandle, verifyWatchDogDoesNotChangeStateAfterCancel) {
  for (auto reason : {MANUAL, TIMEOUT}) {
    auto handle = std::make_shared<CancellationHandle<ENABLED>>();
    FakeRunAfter fake;
    CancellationHandle<ENABLED>::registerWatchDog(handle, fake.runAfter());

    handle->cancellationState_ = reason;
    fake.runNext();
    EXPECT_EQ(handle->cancellationState_, reason);
    // The watch dog has stopped.
    EXPECT_TRUE(fake.tasks_.empty());
  }
}

// Test that the watch dog neither keeps the handle alive nor runs on after the
// handle is destroyed.
TEST(CancellationHandle, verifyWatchDogEndsIfHandleIsDestroyed) {
  auto handle = std::make_shared<CancellationHandle<ENABLED>>();
  std::weak_ptr<CancellationHandle<ENABLED>> weakHandle = handle;
  FakeRunAfter fake;
  CancellationHandle<ENABLED>::registerWatchDog(handle, fake.runAfter());

  // The watch dog doesn't keep the handle alive.
  handle.reset();
  EXPECT_TRUE(weakHandle.expired());
  fake.runNext();
  EXPECT_TRUE(fake.tasks_.empty());
}

// Test that the watch dog can be registered only once and only for a live
// handle.
TEST(CancellationHandle, verifyWatchDogCanOnlyBeRegisteredOnce) {
  auto handle = std::make_shared<CancellationHandle<ENABLED>>();
  FakeRunAfter fake;
  CancellationHandle<ENABLED>::registerWatchDog(handle, fake.runAfter());
  EXPECT_THROW(
      CancellationHandle<ENABLED>::registerWatchDog(handle, fake.runAfter()),
      ad_utility::Exception);
  EXPECT_EQ(fake.tasks_.size(), 1);

  // An expired handle can't be registered.
  EXPECT_THROW(
      CancellationHandle<ENABLED>::registerWatchDog(
          std::weak_ptr<CancellationHandle<ENABLED>>{}, fake.runAfter()),
      ad_utility::Exception);
  EXPECT_EQ(fake.tasks_.size(), 1);
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
  QLEVER_SKIP_TEST_IF_FLAKY_TIMING;
  ENFORCE_LOG_LEVEL_OR_SKIP(DEBUG);
  CancellationHandle<ENABLED> handle;

  auto [cleanup, testStream] = setGlobalLoggingStreamToStringStream();

  handle.startTimeoutWindow_ = std::chrono::steady_clock::now();
  handle.cancellationState_ = CHECK_WINDOW_MISSED;
  EXPECT_NO_THROW(handle.throwIfCancelled(location));
  EXPECT_EQ(handle.cancellationState_, NOT_CANCELLED);

  EXPECT_THAT(
      std::move(testStream).str(),
      AllOf(HasSubstr(expectedLocation),
            HasSubstr(ParseableDuration{DESIRED_CANCELLATION_CHECK_INTERVAL}
                          .toString()),
            // Check for small miss window.
            ContainsRegex("least 5[0-9]ms")));
  // This test assumes this interval to be 50ms to build the regex.
  static_assert(DESIRED_CANCELLATION_CHECK_INTERVAL == 50ms);
}

// _____________________________________________________________________________

TEST(CancellationHandle, verifyPleaseWatchDogReportsOnlyWhenNecessary) {
  ENFORCE_LOG_LEVEL_OR_SKIP(DEBUG);
  CancellationHandle<ENABLED> handle;

  auto [cleanup, testStream] = setGlobalLoggingStreamToStringStream();

  handle.startTimeoutWindow_ = std::chrono::steady_clock::now();
  handle.cancellationState_ = CHECK_WINDOW_MISSED;

  // The first call should trigger a log.
  handle.pleaseWatchDog(CHECK_WINDOW_MISSED, location, detail::printNothing);

  EXPECT_EQ(handle.cancellationState_, NOT_CANCELLED);
  EXPECT_THAT(std::move(testStream).str(), HasSubstr(expectedLocation));

  testStream.str("");

  // The second call should not trigger a log because the state has already
  // been reset.
  handle.pleaseWatchDog(CHECK_WINDOW_MISSED, location, detail::printNothing);

  EXPECT_EQ(handle.cancellationState_, NOT_CANCELLED);
  EXPECT_THAT(std::move(testStream).str(), Not(HasSubstr(expectedLocation)));

  handle.cancellationState_ = CHECK_WINDOW_MISSED;
  testStream.str("");

  // WAITING_FOR_CHECK should not trigger a log.
  handle.pleaseWatchDog(WAITING_FOR_CHECK, location, detail::printNothing);

  EXPECT_EQ(handle.cancellationState_, NOT_CANCELLED);
  EXPECT_THAT(std::move(testStream).str(), Not(HasSubstr(expectedLocation)));

  handle.cancellationState_ = CHECK_WINDOW_MISSED;

  constexpr auto printSomething = []() { return "something"; };
  // The first call should trigger a log with more details.
  handle.pleaseWatchDog(CHECK_WINDOW_MISSED, location, printSomething);

  EXPECT_EQ(handle.cancellationState_, NOT_CANCELLED);
  EXPECT_THAT(std::move(testStream).str(),
              AllOf(HasSubstr(expectedLocation), HasSubstr(printSomething())));

  testStream.str("");

  // The second call should not trigger a log.
  handle.pleaseWatchDog(CHECK_WINDOW_MISSED, location, printSomething);

  EXPECT_EQ(handle.cancellationState_, NOT_CANCELLED);
  EXPECT_THAT(
      std::move(testStream).str(),
      Not(AllOf(HasSubstr(expectedLocation), HasSubstr(printSomething()))));

  handle.cancellationState_ = CHECK_WINDOW_MISSED;
  testStream.str("");

  // WAITING_FOR_CHECK should not trigger a log.
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
  ENFORCE_LOG_LEVEL_OR_SKIP(DEBUG);
  CancellationHandle<ENABLED> handle;

  auto [cleanup, testStream] = setGlobalLoggingStreamToStringStream();

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

// Test that nothing is scheduled for a handle that is already cancelled.
TEST(CancellationHandle, verifyWatchDogEndsEarlyIfCancelled) {
  auto handle = std::make_shared<CancellationHandle<ENABLED>>();
  handle->cancel(MANUAL);

  FakeRunAfter fake;
  CancellationHandle<ENABLED>::registerWatchDog(handle, fake.runAfter());
  // Nothing is scheduled if the handle was cancelled already.
  EXPECT_TRUE(fake.tasks_.empty());
  EXPECT_EQ(handle->cancellationState_, MANUAL);
}

// _____________________________________________________________________________

TEST(CancellationHandle, expectDisabledHandleIsAlwaysFalse) {
  CancellationHandle<DISABLED> handle;

  EXPECT_FALSE(handle.isCancelled());
  EXPECT_NO_THROW(handle.throwIfCancelled());
}

// Test that `registerWatchDog` is a no-op in the `DISABLED` mode.
TEST(CancellationHandle, expectDisabledHandleDoesNotRegisterWatchDog) {
  auto handle = std::make_shared<CancellationHandle<DISABLED>>();
  FakeRunAfter fake;
  CancellationHandle<DISABLED>::registerWatchDog(handle, fake.runAfter());
  EXPECT_TRUE(fake.tasks_.empty());
}

// Test that `registerWatchDog` is a no-op in the `NO_WATCH_DOG` mode, also when
// called twice.
TEST(CancellationHandle, expectNoWatchDogHandleDoesNotRegisterWatchDog) {
  auto handle = std::make_shared<CancellationHandle<NO_WATCH_DOG>>();
  FakeRunAfter fake;
  CancellationHandle<NO_WATCH_DOG>::registerWatchDog(handle, fake.runAfter());
  EXPECT_TRUE(fake.tasks_.empty());
  // Registering twice is fine, because nothing is registered.
  CancellationHandle<NO_WATCH_DOG>::registerWatchDog(handle, fake.runAfter());
  EXPECT_TRUE(fake.tasks_.empty());
}

template <typename T>
QL_CONSTEVAL bool isMemberFunction([[maybe_unused]] T funcPtr) {
  return std::is_member_function_pointer_v<T>;
}

// Make sure member functions still exist when no watch dog functionality
// is available to make the code simpler. In this case the functions should
// be no-op.
static_assert(isMemberFunction(&CancellationHandle<DISABLED>::cancel));
static_assert(isMemberFunction(&CancellationHandle<DISABLED>::isCancelled));
// Ideally we'd add a static assertion for throwIfCancelled here too, but
// because the function is overloaded, we can't get a function pointer for it.

// Constexpr test cases.
static_assert(trimFileName("") == "");
static_assert(trimFileName("/") == "");
static_assert(trimFileName("folder/") == "");
static_assert(trimFileName("//////") == "");
static_assert(trimFileName("../Test.cpp") == "Test.cpp");
static_assert(trimFileName("Test.cpp") == "Test.cpp");
static_assert(trimFileName("./folder/Test.cpp") == "Test.cpp");

}  // namespace ad_utility
