//   Copyright 2023, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#include "util/http/websocket/QueryHub.h"

#include "util/AsioHelpers.h"

namespace ad_utility::websocket {

// Return a lambda that deletes the `queryId` from the `socketDistributors`, but
// only if it is either expired or `alwaysDelete` was specified.
void deleteFromDistributors(auto socketDistributors, QueryId queryId,
                            bool alwaysDelete) {
  auto it = socketDistributors->find(queryId);
  if (it == socketDistributors->end()) {
    return;
  }
  bool expired = it->second.pointer_.expired();
  // The branch `both of them are true` is currently not covered by
  // tests and also not coverable, because the manual `signalEnd` call
  // always comes before the destructor.
  if (alwaysDelete || expired) {
    socketDistributors->erase(it);
  }
}

// _____________________________________________________________________________
template <bool isSender>
std::shared_ptr<QueryHub::ConditionalConst<isSender, QueryToSocketDistributor>>
QueryHub::createOrAcquireDistributorInternal(QueryId queryId) {
  std::lock_guard l{mutex_};
  auto& distributors = socketDistributors_.get();
  if (distributors.contains(queryId)) {
    auto& reference = distributors.at(queryId);
    if (auto ptr = reference.pointer_.lock()) {
      if (isSender) {
        // Ensure only single sender reference is acquired for a single session
        AD_CONTRACT_CHECK(!reference.started_);
        reference.started_ = true;
      }
      return ptr;
    }
  }

  // The cleanup call for the distributor.
  // We pass a weak_ptr version of `shared_pointer socketDistributors_` here,
  // because in unit tests the callback might be invoked after this
  // `QueryHub` was destroyed.
  auto cleanupCall =
      [&mutex = mutex_, socketDistributors = socketDistributors_.getWeak(),
       queryId, alreadyCalled = false](bool alwaysDelete) mutable {
        AD_CORRECTNESS_CHECK(!alreadyCalled);
        alreadyCalled = true;
        // This if-clause causes `guard_` to prevent destruction of
        // the complete `this` object and therefore also the `mutex_`.
        // Otherwise, if the QueryHub is already destroyed, then
        // just do nothing, no need for cleanup in this case.
        auto pointer = socketDistributors.lock();
        if (!pointer) {
          return;
        }
        std::lock_guard l{mutex};
        deleteFromDistributors(std::move(pointer), std::move(queryId),
                               alwaysDelete);
      };

  auto distributor = std::make_shared<QueryToSocketDistributor>(
      ioContext_, std::move(cleanupCall));
  distributors.insert_or_assign(queryId,
                                WeakReferenceHolder{distributor, isSender});
  return distributor;
}

// _____________________________________________________________________________
std::shared_ptr<QueryToSocketDistributor>
QueryHub::createOrAcquireDistributorForSending(QueryId queryId) {
  return createOrAcquireDistributorInternal<true>(std::move(queryId));
}

// _____________________________________________________________________________

std::shared_ptr<const QueryToSocketDistributor>
QueryHub::createOrAcquireDistributorForReceiving(QueryId queryId) {
  return createOrAcquireDistributorInternal<false>(std::move(queryId));
}

}  // namespace ad_utility::websocket
