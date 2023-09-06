//   Copyright 2023, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#include "util/http/websocket/WebSocketTracker.h"

namespace ad_utility::websocket {
net::awaitable<std::shared_ptr<QueryToSocketDistributor>>
WebSocketTracker::createOrAcquireDistributor(QueryId queryId) {
  co_await net::dispatch(globalStrand_, net::use_awaitable);
  if (socketDistributors_.contains(queryId)) {
    if (auto ptr = socketDistributors_.at(queryId).lock()) {
      co_return ptr;
    }
    // There's a case where the object has already been destructed, but not yet
    // Removed from the list. In this case queue to the end of the strand
    // until the destructor was executed.
    size_t sanityCounter = 0;
    while (socketDistributors_.contains(queryId)) {
      co_await net::post(globalStrand_, net::use_awaitable);
      sanityCounter++;
      // If this loop is run too many times something is really wrong!
      // This would indicate that the pointer is never scheduled to be removed
      // from the list.
      AD_CORRECTNESS_CHECK(sanityCounter < 10);
    }
  }
  auto distributor = std::make_shared<QueryToSocketDistributor>(
      ioContext_, [this, queryId]() mutable {
        auto coroutine = [](auto& strand, auto& distributors,
                            auto queryId) -> net::awaitable<void> {
          // TODO check if references are safe here?
          //  also maybe prefer using a QueryToSocketDistributorWrapper?
          //  which does the cleanup, but also can be access like a regular
          //  shared ptr
          co_await net::dispatch(strand, net::use_awaitable);
          distributors.erase(queryId);
        };
        net::co_spawn(
            globalStrand_,
            coroutine(globalStrand_, socketDistributors_, std::move(queryId)),
            net::detached);
      });
  socketDistributors_.emplace(queryId, distributor);
  co_return distributor;
}
}  // namespace ad_utility::websocket
