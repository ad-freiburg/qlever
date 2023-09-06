//   Copyright 2023, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#ifndef QLEVER_ASIO_H
#define QLEVER_ASIO_H

#include <boost/asio/awaitable.hpp>

namespace ad_utility {

namespace net = boost::asio;

/// Helper function that ensures that co_await resumes on the same
/// executor it started with.
template <typename T>
inline net::awaitable<T> sameExecutor(net::awaitable<T> awaitable) {
  auto initialExecutor = co_await net::this_coro::executor;
  auto result = co_await std::move(awaitable);
  co_await net::dispatch(initialExecutor, net::use_awaitable);
  co_return result;
}
}  // namespace ad_utility

#endif  // QLEVER_ASIO_H
