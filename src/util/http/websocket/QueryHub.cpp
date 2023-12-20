//   Copyright 2023, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#include "util/http/websocket/QueryHub.h"

#include "util/AsioHelpers.h"

namespace ad_utility::websocket {

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

  auto makeCleanupCall = [&]() {
    // We pass a copy of the `shared_pointer socketDistributors_` here,
    // because in unit tests the callback might be invoked after this
    // `QueryHub` was destroyed.
    return [globalStrand = globalStrand_,
            socketDistributors = socketDistributors_, queryId,
            alreadyCalled = false](bool alwaysDelete) mutable {
      AD_CORRECTNESS_CHECK(!alreadyCalled);
      alreadyCalled = true;
      net::dispatch(globalStrand, [alwaysDelete,
                                   socketDistributors =
                                       std::move(socketDistributors),
                                   queryId = std::move(queryId)]() {
        auto it = socketDistributors->find(queryId);
        if (it != socketDistributors->end()) {
          // Either this is the call to `signalEnd` (pointer still valid), or it
          // is really the last reference. Other cases cannot happen because the
          // call to `signalEnd` also cancels the cleanup for all the involved
          // receivers.
          AD_CORRECTNESS_CHECK(alwaysDelete ^ it->second.pointer_.expired());
          socketDistributors->erase(it);
        }
      });
      // We don't wait for the deletion to complete here, but only for its
      // scheduling. We still get the expected behavior because all accesses
      // to the `socketDistributor` are synchronized via a strand and
      // BOOST::asio schedules ina FIFO manner.
    };
  };

  auto distributor =
      std::make_shared<QueryToSocketDistributor>(ioContext_, makeCleanupCall());
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
