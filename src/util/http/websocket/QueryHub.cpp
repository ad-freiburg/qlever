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
  while (socketDistributors_->contains(queryId)) {
    auto& reference = socketDistributors_->at(queryId);
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

  auto distributor = std::make_shared<QueryToSocketDistributor>(
      // We pass a copy of the `shared_pointer socketDistributors_` here,
      // because in unit tests the callback might be invoked after this
      // `QueryHub` was destroyed.
      ioContext_, [&ioContext = ioContext_, globalStrand = globalStrand_,
                   socketDistributors = socketDistributors_, queryId]() {
        auto future = net::dispatch(net::bind_executor(
            globalStrand,
            std::packaged_task<void()>([&socketDistributors, &queryId]() {
              bool wasErased = socketDistributors->erase(queryId);
              AD_CORRECTNESS_CHECK(wasErased);
            })));
        // As long as the destructor would have to block anyway, perform work
        // on the `ioContext_`. This avoids blocking in case the destructor
        // already runs inside the `ioContext_`.
        // Note: When called on a strand this may block the current strand.
        // If the ioContext has been stopped for some reason don't wait
        // for the result, or this will never terminate.
        while (future.wait_for(std::chrono::seconds(0)) !=
                   std::future_status::ready &&
               !ioContext.stopped()) {
          ioContext.poll_one();
        }
      });
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
