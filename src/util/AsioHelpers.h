//   Copyright 2023, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Authors: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>
//            Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

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
auto runFunctionOnExecutorUncancellable(Strand strand, Function function,
                                        CompletionToken& token) {
  using Value = std::invoke_result_t<Function>;
  using Signature = void(std::exception_ptr, Value);

  auto initiatingFunction = [function = std::move(function),
                             strand](auto&& handler) mutable {
    auto onExecutor = [function = std::move(function),
                       handler = AD_FWD(handler)]() mutable {
      try {
        // If `function()` throws no exception, we have no
        // exception_ptr and the return value as the second argument.
        handler(nullptr, function());
      } catch (...) {
        // If `function()` throws, we propagate the exception to the
        // handler, and have no return value.
        // TODO<C++23/26> When we get networking+executors, we
        // hopefully have `setError` method which doesn't need a dummy
        // return value.
        Value* ptr = nullptr;
        handler(std::current_exception(), std::move(*ptr));
      }
    };

    net::post(strand, std::move(onExecutor));
  };
  return net::async_initiate<CompletionToken, Signature>(
      std::move(initiatingFunction), token);
}
}  // namespace ad_utility

#endif  // QLEVER_ASIOHELPERS_H
