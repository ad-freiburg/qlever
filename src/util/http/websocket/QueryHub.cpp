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
    // On all code paths a finished `socketDistributor` is first deleted from
    // the `QueryHub` and only then destroyed.
    // This is asserted by the following check.
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
        // If we blocked until the cleanup action is executed on different
        // strand, this could theoretically lead to deadlocks when globalStrand_
        // is on a different execution context. This is currently not the case,
        // but it might be in the future, resulting in deadlocks at distance
        // which is bad. Unfortunately destructors can't be coroutines, which
        // would just allow us to co_await execution without issues. To prevent
        // deadlocks, we wait at most 1 second (arbitrarily chosen interval)
        // for the cleanup to complete, then print a warning, and wait for 9
        // more seconds before we call std::terminate instead of running into
        // a deadlock.
        auto status = future.wait_for(std::chrono::seconds(1));
        if (status != std::future_status::ready) {
          LOG(WARN)
              << "Distributor cleanup in the websocket module taking longer "
                 "than usual. This might be due to heavy load or a deadlock."
              << std::endl;
        }
        status = future.wait_for(std::chrono::seconds(9));
        if (status != std::future_status::ready) {
          LOG(FATAL)
              << "Distributor cleanup in the websocket module taking longer "
                 "than 10 seconds. Terminating to avoid potential deadlock."
              << std::endl;
          std::terminate();
        }
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
