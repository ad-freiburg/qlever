//   Copyright 2023, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#ifndef QLEVER_ASIOHELPERS_H
#define QLEVER_ASIOHELPERS_H

#include <boost/asio/awaitable.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <boost/asio/cancellation_signal.hpp>
#include <boost/asio/bind_cancellation_slot.hpp>


#include "util/Exception.h"
#include "util/Log.h"

namespace ad_utility {

namespace net = boost::asio;
template <typename Strand>
struct Canceller {
  Strand strand_;
  net::cancellation_signal signal_{};

  void operator()(net::cancellation_type type) {
    // LOG(INFO) << "Got a cancellation request" << std::endl;
    net::dispatch(strand_, [this, type]() { signal_.emit(type); });
  }

  Canceller(Strand strand, std::optional<net::cancellation_slot>& slotTarget)
      : strand_{strand} {
    slotTarget = signal_.slot();
  }
};

template <ad_utility::isInstantiation<net::strand> Strand>
auto changeStrandInitiation = [](auto handler, Strand strand, auto&& initiation,
                                 auto&&... initArgs) {
  auto inputSlot = net::get_associated_cancellation_slot(handler);
  std::optional<net::cancellation_slot> slotTarget;
  // LOG(INFO) << "Size of signatures == " << sizeof...(Signatures) << std::endl;
  if (inputSlot.is_connected()) {
    inputSlot.template emplace<Canceller<Strand>>(strand, slotTarget);
    auto actualHandler = net::bind_executor(
        strand,
        net::bind_cancellation_slot(slotTarget.value(), AD_FWD(handler)));
    return AD_FWD(initiation)(std::move(actualHandler), AD_FWD(initArgs)...);
  } else {
    return AD_FWD(initiation)(std::move(handler), AD_FWD(initArgs)...);
  }
};

template <ad_utility::isInstantiation<net::strand> Strand>
auto awaitableOnStrandInitiation =
    [](auto handler, Strand strand, auto awaitable) {
      auto inputSlot = net::get_associated_cancellation_slot(handler);
      std::optional<net::cancellation_slot> slotTarget;
      // LOG(INFO) << "Size of signatures == " << sizeof...(Signatures) <<
      // std::endl;

      if (inputSlot.is_connected()) {
        inputSlot.template emplace<Canceller<Strand>>(strand, slotTarget);
        net::co_spawn(
            strand, std::move(awaitable),
            net::bind_cancellation_slot(slotTarget.value(), AD_FWD(handler)));
      } else {
        net::co_spawn(strand, std::move(awaitable), AD_FWD(handler));
      }
    };

template <typename Strand, typename CompletionToken>
struct ChangeStrandToken {
  Strand strand;
  CompletionToken& token;
};

template <typename Strand, typename CompletionToken>
ChangeStrandToken(Strand, CompletionToken&)
    -> ChangeStrandToken<Strand, CompletionToken>;
}  // namespace ad_utility;

namespace boost::asio {
template <typename Strand, typename CompletionToken, typename... Signatures>
struct async_result<ad_utility::ChangeStrandToken<Strand, CompletionToken>,
                    Signatures...> {
  template <typename Initiation, typename... InitArgs>
  static auto initiate(
      Initiation&& init,
      ad_utility::ChangeStrandToken<Strand, CompletionToken> token,
      InitArgs&&... initArgs) {
    return async_initiate<CompletionToken, Signatures...>(
        ad_utility::changeStrandInitiation<Strand>, token.token, token.strand,
        AD_FWD(init), AD_FWD(initArgs)...);
  }
};

}  // namespace boost::asio

namespace ad_utility {

template <typename Strand, typename CompletionToken>
auto runOnStrand(Strand strand, CompletionToken& token) {
  //return net::post(net::bind_executor(strand, token));
  return net::post(ChangeStrandToken{strand, token});
}

template <typename Strand, typename CompletionToken>
auto runAwaitableOnStrand(Strand strand, net::awaitable<void> awaitable,
                          CompletionToken& token) {
  auto innerAwaitable = [](auto awaitable, auto strand) -> net::awaitable<void> {
      co_await net::post(net::bind_executor(strand, net::use_awaitable));
      co_await net::co_spawn(strand, std::move(awaitable), net::bind_executor(strand, net::use_awaitable));
      co_await net::post(net::use_awaitable);
  };
  return net::co_spawn(strand, innerAwaitable(std::move(awaitable), strand), net::bind_executor(strand, token));
  //return net::bind_executor(strand, net::co_spawn(strand, std::move(awaitable), net::bind_executor(strand, token)));
  /*
  return net::async_initiate<CompletionToken, void()>(
      awaitableOnStrandInitiation<Strand>, token, strand, std::move(awaitable));
      */
}
template <typename Strand>
net::awaitable<void> runAwaitableOnStrandAwaitable(
    Strand strand, net::awaitable<void> awaitable) {
  co_await net::post(net::bind_executor(strand, net::use_awaitable));
  co_await net::co_spawn(strand, std::move(awaitable), net::bind_executor(strand, net::use_awaitable));
  co_await net::post(net::use_awaitable);
  /*
  return net::async_initiate<CompletionToken, void()>(
      awaitableOnStrandInitiation<Strand>, token, strand, std::move(awaitable));
      */
}

/// Helper function that ensures that co_await resumes on the executor
/// this coroutine was co_spawned on.
/// IMPORTANT: If the coroutine is cancelled, no guarantees are given. Make
/// sure to keep that in mind when handling cancellation errors!
template <typename T>
inline net::awaitable<T> resumeOnOriginalExecutor(net::awaitable<T> awaitable) {
  std::exception_ptr exceptionPtr;
  try {
    T result = co_await std::move(awaitable);
    co_await net::post(net::use_awaitable);
    co_return result;
  } catch (...) {
    exceptionPtr = std::current_exception();
  }
  auto cancellationState = co_await net::this_coro::cancellation_state;
  if (cancellationState.cancelled() == net::cancellation_type::none) {
    // use_awaitable always resumes the coroutine on the executor the coroutine
    // was co_spawned on
    co_await net::post(net::use_awaitable);
  }
  AD_CORRECTNESS_CHECK(exceptionPtr);
  std::rethrow_exception(exceptionPtr);
}





/// Helper function that ensures that co_await resumes on the executor
/// this coroutine was co_spawned on. Overload for void.
inline net::awaitable<void> resumeOnOriginalExecutor(
    net::awaitable<void> awaitable) {
  std::exception_ptr exceptionPtr;
  try {
    co_await std::move(awaitable);
  } catch (...) {
    exceptionPtr = std::current_exception();
  }
  if ((co_await net::this_coro::cancellation_state).cancelled() ==
      net::cancellation_type::none) {
    // use_awaitable always resumes the coroutine on the executor the coroutine
    // was co_spawned on
    co_await net::post(net::use_awaitable);
  }
  if (exceptionPtr) {
    std::rethrow_exception(exceptionPtr);
  }
}
}  // namespace ad_utility

#endif  // QLEVER_ASIOHELPERS_H
