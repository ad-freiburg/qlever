//  Copyright 2021, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#ifndef QLEVER_ASYNCWAITFORFUTURE_H
#define QLEVER_ASYNCWAITFORFUTURE_H
// Inspired by https://gist.github.com/inetic/dc9081baf45ec4b60037

#include <future>
#include "../HttpServer/beast.h"

namespace ad_utility::asio_helpers {

template<typename F>
using ResultOrException = std::variant<std::exception_ptr, std::invoke_result_t<F>>;

template <typename CompletionToken, typename F>
auto async_on_external_thread(F function,
                         CompletionToken&& token) {
  namespace asio = boost::asio;
  namespace system = boost::system;

  using Value = ResultOrException<F>;
  using Sig = void(system::error_code, Value);
  /*
  using Result = asio::async_result<std::decay_t<CompletionToken>, Sig>;
  // TODO<joka921> It should be `completion_handler_type`, but there
  // is currently a bug in boost::asio::use_awaitable_t that only specifies
  // the completion_handler_type
  using Handler = typename Result::handler_type;
   */

  /*
  Handler handler(std::forward<decltype(token)>(token));
   */
  auto everything = [function = std::move(function)] (auto&& handler) mutable {
    auto call = [function = std::move(function), handler = std::forward<decltype(handler)>(handler)]() mutable {
      try {
        auto&& result = function();
        handler(system::error_code(), std::forward<decltype(result)>(result));
      } catch (...) {
        handler(system::error_code(), std::current_exception());
      }
    };

    std::thread thread(std::move(call));
    thread.detach();
  };
  return boost::asio::async_initiate<CompletionToken, Sig>(std::move(everything), token);
}
}

#endif  // QLEVER_ASYNCWAITFORFUTURE_H
