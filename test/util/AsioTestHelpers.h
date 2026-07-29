// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_TEST_UTIL_ASIOTESTHELPERS_H
#define QLEVER_TEST_UTIL_ASIOTESTHELPERS_H

#include <boost/asio/co_spawn.hpp>
#include <boost/asio/deferred.hpp>
#include <boost/asio/use_future.hpp>
#include <exception>
#include <future>
#include <optional>
#include <string>
#include <utility>

#include "util/Forward.h"

// Helpers for tests that run an asynchronous operation (typically on a
// `boost::asio::thread_pool`) and expect that operation to fail via an
// `exception_ptr`.
//
// Such a test must not let the exception itself cross the thread boundary, in
// particular not via `boost::asio::use_future` together with
// `AD_EXPECT_THROW_WITH_MESSAGE(future.get(), ...)`. That pattern is correct
// C++, but the thread sanitizer reports a data race for it, because it cannot
// see the synchronization that makes it correct:
//
// 1. `std::future::get()` rethrows the exception that the completion handler
//    has stored in the shared state of the corresponding `std::promise`.
//    `std::rethrow_exception` does not copy the exception; it increments the
//    reference count of the exception object (see `__cxa_refcounted_exception`
//    of the Itanium ABI) and propagates that very object. The `catch` block of
//    the test therefore reads `what()` directly from the exception object that
//    was constructed on the thread pool.
// 2. That object, and with it the buffer of the `std::string` that holds its
//    message, is destroyed by whichever thread performs the last decrement of
//    that reference count. Once the test has left its `catch` block, this can
//    be the thread pool, which drops the last reference when it destroys the
//    completion handler, the promise, and the shared state.
// 3. The decrements are atomic, so the destruction is properly ordered after
//    the reading of the message and there is no undefined behavior. But
//    `__cxa_decrement_exception_refcount` lives in `libstdc++`, which is not
//    compiled with `-fsanitize=thread`, so the sanitizer never observes those
//    atomics. It does observe the `operator delete` of the message buffer
//    (allocating functions are always intercepted), and hence reports a race
//    between that `operator delete` and the reading of the message.
//
// The helpers below avoid this by converting the exception into a `std::string`
// on the thread on which the operation completes, and by transferring only that
// string. The conversion is expressed as a `boost::asio::deferred`
// transformation, which Asio invokes inline in the completion handler of the
// operation. The transfer to the calling thread is done by
// `boost::asio::use_future`: for the transfer of a value, the
// `std::promise`/`std::future` pair establishes synchronization that the thread
// sanitizer does understand, because its mutex is intercepted.
//
// NOTE: There are two alternatives to the approach of these helpers, both of
// which we have discarded:
// 1. Build the standard library (`libc++` supports this via
//    `LLVM_USE_SANITIZER=Thread`) as well as all our other dependencies with
//    thread sanitizer instrumentation and use that in our CI. The sanitizer
//    would then see the reference counting and the report would disappear.
//    There are no prebuilt packages for such a setup, so this would be a large
//    change to the CI.
// 2. Exclude the affected places from the instrumentation, either via a
//    suppression file (an entry like `called_from_lib:libstdc++.so.6`) or via
//    `__attribute__((no_sanitize("thread")))` on the code that reads the
//    message. Both are far coarser than the problem: they also hide real races,
//    the suppression in everything that goes through the standard library, the
//    attribute in the whole annotated function.
namespace ad_utility::testing {

// Return the message of the exception that `exception` refers to, or
// `std::nullopt` if `exception` is empty. Call this on the thread that
// completes the corresponding operation, see the comment above.
inline std::optional<std::string> getMessageOfException(
    const std::exception_ptr& exception) {
  if (!exception) {
    return std::nullopt;
  }
  try {
    std::rethrow_exception(exception);
  } catch (const std::exception& e) {
    return e.what();
  } catch (...) {
    return "Unknown exception that does not derive from `std::exception`";
  }
}

// The outcome of an asynchronous operation with the completion signature
// `void(std::exception_ptr, Result)`: the message of the exception that the
// operation has failed with (`std::nullopt` if it has succeeded), and the
// result that it has completed with (value-initialized in the case of a
// failure).
template <typename Result>
struct AsyncOutcome {
  std::optional<std::string> errorMessage_;
  Result result_;

  // Return true if the operation has failed.
  bool failed() const { return errorMessage_.has_value(); }
};

// A `boost::asio::deferred` transformation that replaces the
// `std::exception_ptr` of a completion by the message of that exception and
// discards the result of the operation. Pipe it onto an operation that was
// initiated with the `boost::asio::deferred` completion token, for example
//
//   auto message = (source.asyncGetNextBlock(boost::asio::deferred)
//                   | asErrorMessage() | boost::asio::use_future).get();
//
// Asio invokes the transformation inline in the completion handler of the
// operation, which is exactly what the comment above requires.
inline auto asErrorMessage() {
  return boost::asio::deferred([](std::exception_ptr exception, auto&&...) {
    return boost::asio::deferred.values(getMessageOfException(exception));
  });
}

// Same as `asErrorMessage` above, but keep the result of the operation and
// combine it with the message into an `AsyncOutcome<Result>`. The `Result` has
// to be specified explicitly, because the arguments of the completion are
// forwarded to the transformation exactly as the operation has passed them,
// without a conversion to the types from its completion signature.
template <typename Result>
auto asOutcome() {
  return boost::asio::deferred(
      [](std::exception_ptr exception, auto&&... results) {
        return boost::asio::deferred.values(AsyncOutcome<Result>{
            getMessageOfException(exception), Result{AD_FWD(results)...}});
      });
}

// Run the `deferredOperation` (an asynchronous operation that was initiated
// with the `boost::asio::deferred` completion token), wait for its completion,
// and return the corresponding `AsyncOutcome<Result>`.
template <typename Result, typename DeferredOperation>
AsyncOutcome<Result> runAsyncOperationAndGetOutcome(
    DeferredOperation deferredOperation) {
  return (std::move(deferredOperation) | asOutcome<Result>() |
          boost::asio::use_future)
      .get();
}

// Same as `runAsyncOperationAndGetOutcome` above, but only return the message
// of the exception that the operation has failed with (`std::nullopt` if it has
// succeeded) and discard the result. This also works for operations that
// complete without a result.
template <typename DeferredOperation>
std::optional<std::string> getErrorMessageOfAsyncOperation(
    DeferredOperation deferredOperation) {
  return (std::move(deferredOperation) | asErrorMessage() |
          boost::asio::use_future)
      .get();
}

// `co_spawn` the coroutine `awaitable` onto the `executor` (which may also be
// an execution context, for example a `boost::asio::thread_pool`), wait for it
// to complete, and return the message of the exception that it has failed with,
// or `std::nullopt` if it has succeeded. The result of the coroutine is
// discarded.
template <typename Executor, typename Awaitable>
std::optional<std::string> getErrorMessageOfCoroutine(Executor&& executor,
                                                      Awaitable awaitable) {
  return getErrorMessageOfAsyncOperation(boost::asio::co_spawn(
      AD_FWD(executor), std::move(awaitable), boost::asio::deferred));
}
}  // namespace ad_utility::testing

#endif  // QLEVER_TEST_UTIL_ASIOTESTHELPERS_H
