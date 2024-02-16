//   Copyright 2023, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#ifndef QLEVER_ASIOHELPERS_H
#define QLEVER_ASIOHELPERS_H

#include <absl/cleanup/cleanup.h>

#include <boost/asio/awaitable.hpp>
#include <boost/asio/bind_cancellation_slot.hpp>
#include <boost/asio/cancellation_signal.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/deferred.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/use_awaitable.hpp>

#include "util/Exception.h"
#include "util/Log.h"

namespace ad_utility {

namespace net = boost::asio;

template <typename Strand, typename CompletionToken, std::invocable Function>
auto runFunctionOnStrand(Strand strand, Function function,
                         CompletionToken& token) {
  LOG(INFO) << "Run on strandd" << std::endl;
  auto awaitable =
      [](auto function) -> net::awaitable<std::invoke_result_t<Function>> {
    co_return std::invoke(function);
  };
  auto innerAwaitable =
      [](auto awaitable,
         auto strand) -> net::awaitable<std::invoke_result_t<Function>> {
    co_return (co_await net::co_spawn(strand, std::move(awaitable),
                                      net::use_awaitable));
  };
  return net::co_spawn(
      strand, innerAwaitable(awaitable(std::move(function)), strand), token);
}

/*
template <typename Strand, typename T>
net::awaitable<T> runAwaitableOnStrandAwaitable(Strand strand,
                                                net::awaitable<T> awaitable) {
  auto state = co_await net::this_coro::cancellation_state;
  auto slot = state.slot();
  net::cancellation_signal signal;
  if (slot.is_connected()) {
    slot.assign([&signal, &strand](auto type) {
      net::dispatch(strand, [&signal, type]() { signal.emit(type); });
    });
  }
  absl::Cleanup c{[&slot]() { slot.clear(); }};
  auto token = net::bind_executor(
      strand, net::bind_cancellation_slot(signal.slot(), net::use_awaitable));
  co_await net::post(token);
  if constexpr (std::is_void_v<T>) {
    co_await std::move(awaitable);
    slot.clear();
    co_await net::post(net::use_awaitable);
    co_return;
  } else {
    auto res = co_await std::move(awaitable);
    slot.clear();
    co_await net::post(net::use_awaitable);
    co_return res;
  }
}
 */

template <typename Strand, typename T>
net::awaitable<T> runAwaitableOnStrandAwaitable(Strand strand,
                                                net::awaitable<T> awaitable) {
  auto inner = [](auto strand, auto f) -> net::awaitable<T> {
    co_await net::post(net::use_awaitable);
    co_return (
        co_await net::co_spawn(strand, std::move(f), net::use_awaitable));
  };
  if constexpr (std::is_void_v<T>) {
    co_await net::co_spawn(strand, inner(strand, std::move(awaitable)),
                           net::use_awaitable);
    co_await net::post(net::use_awaitable);
  } else {
    decltype(auto) res = co_await net::co_spawn(
        strand, inner(strand, std::move(awaitable)), net::use_awaitable);
    co_await net::post(net::use_awaitable);
    co_return res;
  }
}
}  // namespace ad_utility

#endif  // QLEVER_ASIOHELPERS_H
