//   Copyright 2023, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#include "util/http/websocket/MessageSender.h"

namespace ad_utility::websocket {

MessageSender::MessageSender(
    DistributorAndOwningQueryId distributorAndOwningQueryId,
    net::any_io_executor executor)
    : distributorAndOwningQueryId_{std::move(distributorAndOwningQueryId),
                                   [executor](
                                       auto&& distributorAndOwningQueryId) {
                                     auto coroutine =
                                         [](auto distributorAndOwningQueryId)
                                         -> net::awaitable<void> {
                                       // signalEnd() removes the
                                       // distributorAndOwningQueryId from the
                                       // QueryHub. When the coroutine is
                                       // destroyed afterwards the query id is
                                       // unregistered from the registry by the
                                       // destructor of `OwningQueryId`! This is
                                       // the reason why the `OwningQueryId` is
                                       // part of the struct but never actually
                                       // accessed.
                                       co_await distributorAndOwningQueryId
                                           .distributor_->signalEnd();
                                     };
                                     net::co_spawn(
                                         executor,
                                         coroutine(AD_FWD(
                                             distributorAndOwningQueryId)),
                                         net::detached);
                                   }},
      executor_{std::move(executor)} {}

// _____________________________________________________________________________

net::awaitable<MessageSender> MessageSender::create(OwningQueryId owningQueryId,
                                                    QueryHub& queryHub) {
  co_return MessageSender{
      DistributorAndOwningQueryId{
          co_await queryHub.createOrAcquireDistributorForSending(
              owningQueryId.toQueryId()),
          std::move(owningQueryId)},
      co_await net::this_coro::executor};
}

// _____________________________________________________________________________

void MessageSender::operator()(std::string json) const {
  auto lambda = [](std::shared_ptr<QueryToSocketDistributor> distributor,
                   std::string json) mutable -> net::awaitable<void> {
    // It is important `distributor` is kept alive during this call, thus
    // it is passed by value
    co_await distributor->addQueryStatusUpdate(std::move(json));
  };
  net::co_spawn(
      executor_,
      lambda(distributorAndOwningQueryId_->distributor_, std::move(json)),
      net::detached);
}
}  // namespace ad_utility::websocket
