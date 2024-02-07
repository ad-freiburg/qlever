//   Copyright 2023, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#include "util/http/websocket/QueryHub.h"

#include "util/AsioHelpers.h"

namespace ad_utility::websocket {

// Return a lambda that deletes the `queryId` from the `socketDistributors`, but
// only if it is either expired or `alwaysDelete` was specified.
inline auto makeDeleteFromDistributors = [](auto& socketDistributors,
                                            QueryId queryId,
                                            bool alwaysDelete) {
  return [&socketDistributors, queryId = std::move(queryId), alwaysDelete]() {
    auto it = socketDistributors.find(queryId);
    if (it == socketDistributors.end()) {
      return;
    }
    bool expired = it->second.pointer_.expired();
    // The branch `both of them are true` is currently not covered by
    // tests and also not coverable, because the manual `signalEnd` call
    // always comes before the destructor.
    if (alwaysDelete || expired) {
      socketDistributors.erase(it);
    }
  };
};

// _____________________________________________________________________________
std::shared_ptr<QueryToSocketDistributor>
QueryHub::createOrAcquireDistributorInternalUnsafe(QueryId queryId,
                                                   bool isSender) {
  auto& distributors = socketDistributors_;
  if (distributors.contains(queryId)) {
    auto& reference = distributors.at(queryId);
    if (auto ptr = reference.pointer_.lock()) {
      if (isSender) {
        // Ensure only single sender reference is acquired for a single session
        AD_CONTRACT_CHECK(!reference.started_);
        reference.started_ = true;
      }
      return ptr;
    } else {
      distributors.erase(queryId);
    }
  }

  // The cleanup call for the distributor.
  // We pass a weak_ptr version of `shared_pointer socketDistributors_` here,
  // because in unit tests the callback might be invoked after this
  // `QueryHub` was destroyed.
  auto cleanupCall = [this, queryId,
                      alreadyCalled = false](bool alwaysDelete) mutable {
    AD_CORRECTNESS_CHECK(!alreadyCalled);
    alreadyCalled = true;
    // We need to keep the mutex alive (at least for unit tests).
    mutex_.asyncLockGuard([this, queryId = std::move(queryId),
                           alwaysDelete]([[maybe_unused]] auto guard) {
      makeDeleteFromDistributors(socketDistributors_, std::move(queryId),
                                 alwaysDelete)();
    });
    // We don't wait for the deletion to complete here, but only for its
    // scheduling. This currently might lead to the deletion of a queryId from
    // the registry to be a little bit delayed, but we don't suppose that this
    // is much of an issue
  };

  auto distributor = std::make_shared<QueryToSocketDistributor>(
      ioContext_, std::move(cleanupCall));
  distributors.emplace(queryId, WeakReferenceHolder{distributor, isSender});
  return distributor;
}

// _____________________________________________________________________________
template <bool isSender>
net::awaitable<std::shared_ptr<
    QueryHub::ConditionalConst<isSender, QueryToSocketDistributor>>>
QueryHub::createOrAcquireDistributorInternal(QueryId queryId) {
  auto guard = co_await mutex_.asyncLockGuard(net::use_awaitable);
  co_return createOrAcquireDistributorInternalUnsafe(std::move(queryId),
                                                     isSender);
}

// _____________________________________________________________________________
net::awaitable<std::shared_ptr<QueryToSocketDistributor>>
QueryHub::createOrAcquireDistributorForSending(QueryId queryId) {
  return createOrAcquireDistributorInternal<true>(std::move(queryId));
}

// _____________________________________________________________________________

net::awaitable<std::shared_ptr<const QueryToSocketDistributor>>
QueryHub::createOrAcquireDistributorForReceiving(QueryId queryId) {
  return createOrAcquireDistributorInternal<false>(std::move(queryId));
}

}  // namespace ad_utility::websocket
