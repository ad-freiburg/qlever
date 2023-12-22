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

  // A coroutine that deletes the `queryId` from the distributors, but only if
  // it is either expired or `alwaysDelete` was specified.
  auto deleteFromDistributors = [](auto socketDistributors, QueryId queryId,
                                   bool alwaysDelete) -> net::awaitable<void> {
    auto it = socketDistributors->find(queryId);
    if (it == socketDistributors->end()) {
      co_return;
    }
    bool expired = it->second.pointer_.expired();
    if (alwaysDelete || expired) {
      socketDistributors->erase(it);
    }
    co_return;
  };

  // The cleanup call for the distributor.
  // We pass a copy of the `shared_pointer socketDistributors_` here,
  // because in unit tests the callback might be invoked after this
  // `QueryHub` was destroyed.
  auto cleanupCall = [globalStrand = globalStrand_, &deleteFromDistributors,
                      socketDistributors = socketDistributors_, queryId,
                      alreadyCalled = false](bool alwaysDelete) mutable {
    AD_CORRECTNESS_CHECK(!alreadyCalled);
    alreadyCalled = true;
    net::co_spawn(globalStrand,
                  deleteFromDistributors(std::move(socketDistributors),
                                         std::move(queryId), alwaysDelete),
                  net::detached);
    // We don't wait for the deletion to complete here, but only for its
    // scheduling. We still get the expected behavior because all accesses
    // to the `socketDistributor` are synchronized via a strand and
    // BOOST::asio schedules ina FIFO manner.
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
