//   Copyright 2023, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#include "util/http/websocket/MessageSender.h"

namespace ad_utility::websocket {

// _____________________________________________________________________________
MessageSender::MessageSender(
    DistributorAndOwningQueryId distributorAndOwningQueryId)
    : distributorAndOwningQueryId_{
          std::move(distributorAndOwningQueryId),
          [](auto&& distributorAndOwningQueryId) {
            distributorAndOwningQueryId.distributor_->signalEnd();
          }} {}

// _____________________________________________________________________________
MessageSender MessageSender::create(OwningQueryId owningQueryId,
                                    QueryHub& queryHub) {
  return MessageSender{DistributorAndOwningQueryId{
      queryHub.createOrAcquireDistributorForSending(owningQueryId.toQueryId()),
      std::move(owningQueryId)}};
}

// _____________________________________________________________________________

void MessageSender::operator()(std::string json) const {
  distributorAndOwningQueryId_->distributor_->addQueryStatusUpdate(
      std::move(json));
}
}  // namespace ad_utility::websocket
