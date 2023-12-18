//   Copyright 2023, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#include "util/http/websocket/QueryHub.h"

#include "util/AsioHelpers.h"

namespace ad_utility::websocket {

template <bool isSender>
net::awaitable<std::shared_ptr<
    QueryHub::ConditionalConst<isSender, QueryToSocketDistributor>>>
QueryHub::createOrAcquireDistributorInternalUnsafe(QueryId queryId) {
  if (socketDistributors_->contains(queryId)) {
    auto& reference = socketDistributors_->at(queryId);
    if (auto ptr = reference.pointer_.lock()) {
      if constexpr (isSender) {
        // Ensure only single sender reference is acquired for a single session
        AD_CONTRACT_CHECK(!reference.started_);
        reference.started_ = true;
      }
      co_return ptr;
    } else {
      socketDistributors_->erase(queryId);
    }
  }

  // TODO<joka921> Factor this out.
  auto makeCleanupCall = [&](bool alwaysDelete) {
    return [alwaysDelete, globalStrand = globalStrand_,
            socketDistributors = socketDistributors_, queryId,
            alreadyCalled = false]() mutable {
      AD_CORRECTNESS_CHECK(!alreadyCalled);
      alreadyCalled = true;
      net::dispatch(
          globalStrand,
          [alwaysDelete, socketDistributors = std::move(socketDistributors),
           queryId = std::move(queryId)]() {
            auto it = socketDistributors->find(queryId);
            // Always erase the `queryId` when the corresponding
            // sender is destroyed. For listeners, we only delete it
            // if it was the last reference.
            if (it != socketDistributors->end() &&
                (alwaysDelete || it->second.pointer_.expired())) {
              socketDistributors->erase(it);
            }
          });
      // We don't wait for the deletion to complete here, but only for its
      // scheduling. We still get the expected behavior because all accesses
      // to the `socketDistributor` are synchronized via a strand and
      // BOOST::asio schedules ina FIFO manner.
    };
  };

  auto distributor = std::make_shared<QueryToSocketDistributor>(
      // We pass a copy of the `shared_pointer socketDistributors_` here,
      // because in unit tests the callback might be invoked after this
      // `QueryHub` was destroyed.
      ioContext_, makeCleanupCall(false), makeCleanupCall(true));
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
  co_return co_await createOrAcquireDistributorInternalUnsafe<isSender>(
      std::move(queryId));
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

// _____________________________________________________________________________

// Clang does not seem to keep this instantiation around when called directly,
// so we need to explicitly instantiate it here for the
// QueryHub_testCorrectReschedulingForEmptyPointerOnDestruct test to compile
// properly
template net::awaitable<std::shared_ptr<const QueryToSocketDistributor>>
    QueryHub::createOrAcquireDistributorInternalUnsafe<false>(QueryId);
}  // namespace ad_utility::websocket
