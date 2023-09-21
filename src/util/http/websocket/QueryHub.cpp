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

template <bool isProvider>
second_t<isProvider, net::awaitable<std::shared_ptr<QueryToSocketDistributor>>>
QueryHub::createOrAcquireDistributorInternal(QueryId queryId) {
  co_await net::dispatch(globalStrand_, net::use_awaitable);
  if (socketDistributors_.contains(queryId)) {
    if (auto ptr = socketDistributors_.at(queryId).lock()) {
      if constexpr (isProvider) {
        if (!co_await sameExecutor(ptr->signalStartIfNotStarted())) {
          co_return ptr;
        }
      } else {
        co_return ptr;
      }
    }
    // There's a scenario where the object has is about to be removed from the
    // list, but wasn't so far because of concurrency. In this case remove it
    // here and proceed to build a new instance. This is safe because the id is
    // checked for "equality" before removal.
    socketDistributors_.erase(queryId);
  }

  auto pointerHolder =
      std::make_shared<std::optional<PointerType>>(std::nullopt);
  auto cleanupAction = [](auto& distributors, auto queryId,
                          auto referencePointer) -> net::awaitable<void> {
    // Only erase object if we created it, otherwise we have a race condition
    if (distributors.contains(queryId) &&
        equals(referencePointer, distributors.at(queryId))) {
      distributors.erase(queryId);
    }
    // Make sure this is treated as coroutine
    co_return;
  };
  auto distributor = std::make_shared<QueryToSocketDistributor>(
      ioContext_, [this, queryId, coroutine = std::move(cleanupAction),
                   pointerHolder]() mutable {
        AD_CORRECTNESS_CHECK(pointerHolder->has_value());
        net::co_spawn(globalStrand_,
                      coroutine(socketDistributors_, std::move(queryId),
                                std::move(pointerHolder->value())),
                      net::detached);
      });
  *pointerHolder = distributor;
  socketDistributors_.emplace(queryId, distributor);
  if constexpr (isProvider) {
    co_await sameExecutor(distributor->signalStartIfNotStarted());
  }
  co_return distributor;
}

// _____________________________________________________________________________

net::awaitable<std::shared_ptr<QueryToSocketDistributor>>
QueryHub::createOrAcquireDistributorForSending(QueryId queryId) {
  return sameExecutor(
      createOrAcquireDistributorInternal<true>(std::move(queryId)));
}

// _____________________________________________________________________________

net::awaitable<std::shared_ptr<QueryToSocketDistributor>>
QueryHub::createOrAcquireDistributorForReceiving(QueryId queryId) {
  return sameExecutor(
      createOrAcquireDistributorInternal<false>(std::move(queryId)));
}
}  // namespace ad_utility::websocket
