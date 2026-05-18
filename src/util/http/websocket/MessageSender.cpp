//   Copyright 2023, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#include "util/http/websocket/MessageSender.h"

namespace ad_utility::websocket {

// _____________________________________________________________________________
MessageSender::MessageSender(OwningQueryId owningQueryId, QueryHub& queryHub)
    : owningQueryId_{std::move(owningQueryId)},
      distributor_{queryHub.createOrAcquireDistributorForSending(
                       owningQueryId_.toQueryId()),
                   [](std::shared_ptr<QueryToSocketDistributor> distributor) {
                     distributor->signalEnd();
                   }} {}

// _____________________________________________________________________________
void MessageSender::operator()(std::string json) const {
  (*distributor_)->addQueryStatusUpdate(std::move(json));
}

// _____________________________________________________________________________
std::function<void(std::string)> MessageSender::extractUpdateCallback() && {
  // std::function needs a copyable target, but UniqueCleanup is move-only;
  // wrap it in a shared_ptr so the lambda is copyable. signalEnd() runs when
  // the last copy of this callback is destroyed.
  auto distributor = std::make_shared<
      UniqueCleanup<std::shared_ptr<QueryToSocketDistributor>>>(
      std::move(distributor_));
  return [distributor = std::move(distributor)](std::string json) {
    (**distributor)->addQueryStatusUpdate(std::move(json));
  };
}
}  // namespace ad_utility::websocket
