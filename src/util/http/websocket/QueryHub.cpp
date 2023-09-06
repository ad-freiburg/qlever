//   Copyright 2023, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#include "util/http/websocket/QueryHub.h"

#include "util/Asio.h"

namespace ad_utility::websocket {
net::awaitable<std::shared_ptr<QueryToSocketDistributor>>
QueryHub::createOrAcquireDistributorInternal(QueryId queryId) {
  co_await net::dispatch(globalStrand_, net::use_awaitable);
  if (socketDistributors_.contains(queryId)) {
    if (auto ptr = socketDistributors_.at(queryId).pointer_.lock()) {
      co_return ptr;
    }
    // There's a scenario where the object has already been destructed, but not
    // yet removed from the list. In this case remove this here and proceed
    // to build a new instance. This is safe because the id is checked for
    // equality before removal.
    socketDistributors_.erase(queryId);
  }

  auto id = counter++;
  auto coroutine = [](auto strand, auto& distributors, auto queryId,
                      auto id) -> net::awaitable<void> {
    co_await net::dispatch(strand, net::use_awaitable);
    // Only erase object if we created it, otherwise we have a race condition
    if (distributors.contains(queryId) && distributors.at(queryId).id_ == id) {
      distributors.erase(queryId);
    }
  };
  auto distributor = std::make_shared<QueryToSocketDistributor>(
      ioContext_,
      [this, queryId, coroutine = std::move(coroutine), id]() mutable {
        net::co_spawn(globalStrand_,
                      // TODO check if references are safe here?
                      coroutine(globalStrand_, socketDistributors_,
                                std::move(queryId), id),
                      net::detached);
      });
  socketDistributors_.emplace(
      queryId, IdentifiablePointer{.pointer_ = distributor, .id_ = id});
  co_return distributor;
}

// _____________________________________________________________________________

net::awaitable<std::shared_ptr<QueryToSocketDistributor>>
QueryHub::createOrAcquireDistributor(QueryId queryId) {
  return sameExecutor(createOrAcquireDistributorInternal(std::move(queryId)));
}
}  // namespace ad_utility::websocket
