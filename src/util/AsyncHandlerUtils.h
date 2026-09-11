// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_UTIL_ASYNCHANDLERUTILS_H
#define QLEVER_SRC_UTIL_ASYNCHANDLERUTILS_H

#include <boost/asio/associated_executor.hpp>
#include <boost/asio/post.hpp>
#include <exception>
#include <utility>

#include "backports/asio.h"

namespace ad_utility {

// Wrap a completion `handler` with the signature
// `void(std::exception_ptr, Payload)` such that it is never run inline, but
// always `post`ed onto the executor that is associated with it (or onto
// `defaultExecutor`, if it has none of its own). The resulting handler may
// hence safely be invoked directly, in particular from within a strand,
// because the actual work of `handler` then runs outside of that strand.
//
// NOTE: This is not the same as `boost::asio::bind_executor`, for three
// reasons. First, `bind_executor` only *associates* an executor with a
// handler; it is up to the initiating function of an asynchronous operation to
// honor that association. The handlers here are invoked directly by hand (see
// `AsyncSerialParserAdapter::asyncGetBatchImpl` for an example), so the
// association would simply be ignored. Second, `bind_executor` *overrides* the
// executor that the handler already has, whereas the wrapper below reads that
// executor and only falls back to `defaultExecutor` if there is none. Third,
// even an honored association only leads to a `dispatch`, which may still run
// the handler inline if the current thread already runs on that executor,
// which would defeat the very purpose of this function.
template <typename Payload, typename Handler>
auto makeHandlerExecutorAware(Handler handler,
                              const ql::any_io_executor& defaultExecutor) {
  auto executor =
      boost::asio::get_associated_executor(handler, defaultExecutor);
  return [handler = std::move(handler), executor](std::exception_ptr exception,
                                                  Payload payload) mutable {
    boost::asio::post(executor, [handler = std::move(handler),
                                 exception = std::move(exception),
                                 payload = std::move(payload)]() mutable {
      std::move(handler)(std::move(exception), std::move(payload));
    });
  };
}

}  // namespace ad_utility

#endif  // QLEVER_SRC_UTIL_ASYNCHANDLERUTILS_H
