//   Copyright 2023, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#include "util/http/websocket/QueryHub.h"

#include "util/AsioHelpers.h"

namespace ad_utility::websocket {

/// Helper function to check if two weak pointers point to the same control
/// block. This is the case for all weak pointers created from the same shared
/// pointer or aliases of that shared pointer.
template <typename T, typename U>
bool equals(const std::weak_ptr<T>& t, const std::weak_ptr<U>& u) noexcept {
  return !t.owner_before(u) && !u.owner_before(t);
}

// _____________________________________________________________________________

net::awaitable<std::shared_ptr<QueryToSocketDistributor>>
QueryHub::createOrAcquireDistributorInternal(QueryId queryId) {
  co_await net::dispatch(globalStrand_, net::use_awaitable);
  if (socketDistributors_.contains(queryId)) {
    auto ptr = socketDistributors_.at(queryId).lock();
    // Destructor must block until distributor is unregistered
    AD_CORRECTNESS_CHECK(ptr);
    co_return ptr;
  }

  auto pointerHolder =
      std::make_shared<std::optional<PointerType>>(std::nullopt);
  auto cleanupAction = [this, queryId, pointerHolder]() {
    AD_CORRECTNESS_CHECK(pointerHolder->has_value());
    auto referencePointer = std::move(pointerHolder->value());
    // Only erase object if we created it, otherwise we have a race condition
    if (socketDistributors_.contains(queryId) &&
        equals(referencePointer, socketDistributors_.at(queryId))) {
      socketDistributors_.erase(queryId);
    }
  };
  auto distributor = std::make_shared<QueryToSocketDistributor>(
      ioContext_, [this, cleanupAction = std::move(cleanupAction)]() mutable {
        auto future =
            net::dispatch(globalStrand_,
                          std::packaged_task<void()>(std::move(cleanupAction)));
        // Block until cleanup action is executed on different strand.
        // This is ugly because it might block a worker thread that is supposed
        // to handle other requests, but destructors can't be coroutines!
        future.wait();
      });
  *pointerHolder = distributor;
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
