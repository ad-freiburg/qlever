//   Copyright 2023, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#include "util/http/websocket/QueryHub.h"

#include "util/AsioHelpers.h"

namespace ad_utility::websocket {

// Return a lambda that deletes the `queryId` from the `socketDistributors`, but
// only if it is either expired or `alwaysDelete` was specified.
inline auto makeDeleteFromDistributors =
    [](auto socketDistributors, QueryId queryId, bool alwaysDelete) {
      return [socketDistributors = std::move(socketDistributors),
              queryId = std::move(queryId), alwaysDelete]() {
        auto it = socketDistributors->find(queryId);
        if (it == socketDistributors->end()) {
          return;
        }
        bool expired = it->second.pointer_.expired();
        // The branch `both of them are true` is currently not covered by tests
        // and also not coverable, because the manual `signalEnd` call always
        // comes before the destructor.
        if (alwaysDelete || expired) {
          socketDistributors->erase(it);
        }
      };
    };

// _____________________________________________________________________________
net::awaitable<std::shared_ptr<QueryToSocketDistributor>>
QueryHub::createOrAcquireDistributorInternalUnsafe(QueryId queryId,
                                                   bool isSender) {
  if (socketDistributors_->contains(queryId)) {
    auto& reference = socketDistributors_->at(queryId);
    if (auto ptr = reference.pointer_.lock()) {
      if (isSender) {
        // Ensure only single sender reference is acquired for a single session
        AD_CONTRACT_CHECK(!reference.started_);
        reference.started_ = true;
      }
      co_return ptr;
    } else {
      socketDistributors_->erase(queryId);
    }
  }

  // The cleanup call for the distributor.
  // We pass a copy of the `shared_pointer socketDistributors_` here,
  // because in unit tests the callback might be invoked after this
  // `QueryHub` was destroyed.
  auto cleanupCall = [globalStrand = globalStrand_,
                      socketDistributors = socketDistributors_, queryId,
                      alreadyCalled = false](bool alwaysDelete) mutable {
    AD_CORRECTNESS_CHECK(!alreadyCalled);
    alreadyCalled = true;
    net::dispatch(globalStrand,
                  makeDeleteFromDistributors(std::move(socketDistributors),
                                             std::move(queryId), alwaysDelete));
    // We don't wait for the deletion to complete here, but only for its
    // scheduling. We still get the expected behavior because all accesses
    // to the `socketDistributor` are synchronized via a strand and
    // BOOST::asio schedules in a FIFO manner.
  };

  auto distributor = std::make_shared<QueryToSocketDistributor>(
      ioContext_, std::move(cleanupCall));
  socketDistributors_->emplace(queryId,
                               WeakReferenceHolder{distributor, isSender});
  co_return distributor;
}

// _____________________________________________________________________________
template <bool isSender>
net::awaitable<std::shared_ptr<
    QueryHub::ConditionalConst<isSender, QueryToSocketDistributor>>>
QueryHub::createOrAcquireDistributorInternal(QueryId queryId) {
  co_await net::post(net::bind_executor(globalStrand_, net::use_awaitable));
  co_return co_await createOrAcquireDistributorInternalUnsafe(
      std::move(queryId), isSender);
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
