//   Copyright 2023, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#ifndef QLEVER_ASIOHELPERS_H
#define QLEVER_ASIOHELPERS_H

#include <boost/asio/awaitable.hpp>
#include <boost/asio/dispatch.hpp>
#include <boost/asio/use_awaitable.hpp>

namespace ad_utility {

namespace net = boost::asio;

/// Helper function that ensures that co_await resumes on the same
/// executor it started with.
/// IMPORTANT: If the inner awaitable throws, no guarantees are given. Make
/// sure to keep that in mind when handling errors inside coroutines!
template <typename T>
inline net::awaitable<T> sameExecutor(net::awaitable<T> awaitable) {
  auto initialExecutor = co_await net::this_coro::executor;
  T result = co_await std::move(awaitable);
  co_await net::dispatch(initialExecutor, net::use_awaitable);
  co_return result;
}

// _____________________________________________________________________________

/// Helper function that ensures that co_await resumes on the same
/// executor it started with. Overload for void.
inline net::awaitable<void> sameExecutor(net::awaitable<void> awaitable) {
  auto initialExecutor = co_await net::this_coro::executor;
  co_await std::move(awaitable);
  co_await net::dispatch(initialExecutor, net::use_awaitable);
}
}  // namespace ad_utility

#endif  // QLEVER_ASIOHELPERS_H
