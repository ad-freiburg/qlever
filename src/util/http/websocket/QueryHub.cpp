//   Copyright 2023, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#include "util/http/websocket/QueryHub.h"

#include "util/AsioHelpers.h"

namespace ad_utility::websocket {

template <bool isSender>
net::awaitable<std::shared_ptr<
    QueryHub::ConditionalConst<isSender, QueryToSocketDistributor>>>
QueryHub::createOrAcquireDistributorInternal(QueryId queryId) {
  co_await net::dispatch(net::bind_executor(globalStrand_, net::use_awaitable));
  while (socketDistributors_.contains(queryId)) {
    auto& reference = socketDistributors_.at(queryId);
    if (auto ptr = reference.pointer_.lock()) {
      if constexpr (isSender) {
        // Ensure only single sender reference is acquired for a single session
        AD_CONTRACT_CHECK(!reference.started_);
        reference.started_ = true;
      }
      co_return ptr;
    }
    // There's the unlikely case where the reference counter reached zero and
    // the weak pointer can no longer create a shared pointer, but the
    // destructor is waiting for execution on `globalStrand_`. In this case
    // re-schedule this coroutine to be executed after destruction. So it is
    // crucial to use post over dispatch here.
    co_await net::post(net::bind_executor(globalStrand_, net::use_awaitable));
  }

  auto distributor =
      std::make_shared<QueryToSocketDistributor>(ioContext_, [this, queryId]() {
        auto future = net::dispatch(net::bind_executor(
            globalStrand_, std::packaged_task<void()>([this, &queryId]() {
              bool wasErased = socketDistributors_.erase(queryId);
              AD_CORRECTNESS_CHECK(wasErased);
            })));
        // Make sure we never block, just run some tasks on the `ioContext_`
        // until the task is executed
        while (future.wait_for(std::chrono::seconds(0)) !=
               std::future_status::ready) {
          ioContext_.poll_one();
        }
      });
  socketDistributors_.emplace(queryId,
                              WeakReferenceHolder{distributor, isSender});
  co_return distributor;
}

// _____________________________________________________________________________

net::awaitable<std::shared_ptr<QueryToSocketDistributor>>
QueryHub::createOrAcquireDistributorForSending(QueryId queryId) {
  return resumeOnOriginalExecutor(
      createOrAcquireDistributorInternal<true>(std::move(queryId)));
}

// _____________________________________________________________________________

net::awaitable<std::shared_ptr<const QueryToSocketDistributor>>
QueryHub::createOrAcquireDistributorForReceiving(QueryId queryId) {
  return resumeOnOriginalExecutor(
      createOrAcquireDistributorInternal<false>(std::move(queryId)));
}
}  // namespace ad_utility::websocket
