//   Copyright 2023, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#include "util/http/websocket/QueryHub.h"

#include "util/AsioHelpers.h"

namespace ad_utility::websocket {

net::awaitable<std::shared_ptr<QueryToSocketDistributor>>
QueryHub::createOrAcquireDistributorInternal(QueryId queryId) {
  co_await net::dispatch(net::bind_executor(globalStrand_, net::use_awaitable));
  while (socketDistributors_.contains(queryId)) {
    if (auto ptr = socketDistributors_.at(queryId).lock()) {
      co_return ptr;
    }
    // There's the unlikely case where the reference counter reached zero and
    // the weak pointer can no longer create a shared pointer, but the
    // destructor is waiting for execution on `singleThreadPool_`. In this case
    // re-schedule this coroutine to be executed after destruction.
    co_await net::post(net::bind_executor(globalStrand_, net::use_awaitable));
  }

  auto distributor =
      std::make_shared<QueryToSocketDistributor>(ioContext_, [this, queryId]() {
        auto future = net::dispatch(net::bind_executor(
            globalStrand_, std::packaged_task<void()>([this, &queryId]() {
              AD_CORRECTNESS_CHECK(socketDistributors_.erase(queryId));
            })));
        // Make sure we never block, just run some tasks on the `ioContext_`
        // until the task is executed
        while (future.wait_for(std::chrono::seconds(0)) !=
               std::future_status::ready) {
          ioContext_.run_one();
        }
      });
  socketDistributors_.emplace(queryId, distributor);
  co_return distributor;
}

// _____________________________________________________________________________

net::awaitable<std::shared_ptr<QueryToSocketDistributor>>
QueryHub::createOrAcquireDistributorForSending(QueryId queryId) {
  return sameExecutor(createOrAcquireDistributorInternal(std::move(queryId)));
}

// _____________________________________________________________________________

net::awaitable<std::shared_ptr<const QueryToSocketDistributor>>
QueryHub::createOrAcquireDistributorForReceiving(QueryId queryId) {
  auto result = co_await sameExecutor(
      createOrAcquireDistributorInternal(std::move(queryId)));
  co_return result;
}
}  // namespace ad_utility::websocket
