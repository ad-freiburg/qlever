//   Copyright 2023, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#include "util/http/websocket/UpdateWrapper.h"

namespace ad_utility::websocket {

UpdateWrapper::UpdateWrapper(DistributorWithFixedLifetime distributor,
                             net::any_io_executor executor)
    : distributor_{std::move(distributor),
                   [executor](auto&& distributor) {
                     auto coroutine =
                         [](auto distributor) -> net::awaitable<void> {
                       co_await distributor.distributor_->signalEnd();
                     };
                     net::co_spawn(executor, coroutine(AD_FWD(distributor)),
                                   net::detached);
                   }},
      executor_{std::move(executor)} {}

// _____________________________________________________________________________

net::awaitable<UpdateWrapper> UpdateWrapper::create(OwningQueryId owningQueryId,
                                                    QueryHub& queryHub) {
  co_return UpdateWrapper{
      DistributorWithFixedLifetime{
          co_await queryHub.createOrAcquireDistributorForSending(
              owningQueryId.toQueryId()),
          std::move(owningQueryId)},
      co_await net::this_coro::executor};
}

// _____________________________________________________________________________

void UpdateWrapper::operator()(std::string json) const {
  auto lambda = [](std::shared_ptr<QueryToSocketDistributor> distributor,
                   std::string json) mutable -> net::awaitable<void> {
    // It is important `distributor` is kept alive during this call, thus
    // it is passed by value
    co_await distributor->addQueryStatusUpdate(std::move(json));
  };
  net::co_spawn(executor_, lambda(distributor_->distributor_, std::move(json)),
                net::detached);
}
}  // namespace ad_utility::websocket
