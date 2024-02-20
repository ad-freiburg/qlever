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

// Run
template <typename Strand, typename CompletionToken, std::invocable Function>
auto runFunctionOnExecutorUncancellable(Strand strand, Function function,
                                        CompletionToken& token) {
  // TODO<joka921> Use the generic facilities of boost asio.
  // TODO<joka921> Add tests that strands are correctly given up etc.
  auto awaitable =
      [](auto function) -> net::awaitable<std::invoke_result_t<Function>> {
    co_return std::invoke(function);
  };
  auto innerAwaitable =
      [](auto awaitable,
         auto strand) -> net::awaitable<std::invoke_result_t<Function>> {
    co_return (co_await net::co_spawn(
        strand, std::move(awaitable),
        net::bind_cancellation_slot(net::cancellation_slot{},
                                    net::use_awaitable)));
  };
  return net::co_spawn(
      strand, innerAwaitable(awaitable(std::move(function)), strand),
      net::bind_cancellation_slot(net::cancellation_slot{}, token));
}
}  // namespace ad_utility

#endif  // QLEVER_ASIOHELPERS_H
