//   Copyright 2023, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#include "util/http/websocket/UpdateWrapper.h"

namespace ad_utility::websocket {

net::awaitable<UpdateWrapper> UpdateWrapper::create(QueryId queryId,
                                                    QueryHub& queryHub) {
  co_return UpdateWrapper{
      co_await queryHub.createOrAcquireDistributor(std::move(queryId), true),
      co_await net::this_coro::executor};
}

void UpdateWrapper::operator()(std::string json) const {
  auto lambda = [](auto distributor,
                   auto json) mutable -> net::awaitable<void> {
    // It is important `distributor` is kept alive during this call!
    co_await distributor->addQueryStatusUpdate(std::move(json));
  };
  net::co_spawn(executor_, lambda(*distributor_, std::move(json)),
                net::detached);
}
}  // namespace ad_utility::websocket
