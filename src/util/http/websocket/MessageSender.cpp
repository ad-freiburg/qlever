//   Copyright 2023, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#include "util/http/websocket/MessageSender.h"

namespace ad_utility::websocket {

// _____________________________________________________________________________
MessageSender::MessageSender(OwningQueryId owningQueryId, QueryHub& queryHub)
    : distributorAndOwningQueryId_{
          {queryHub.createOrAcquireDistributorForSending(
               owningQueryId.toQueryId()),
           std::move(owningQueryId)},
          [](DistributorAndOwningQueryId distributorAndOwningQueryId) {
            distributorAndOwningQueryId.distributor_->signalEnd();
          }} {}

// _____________________________________________________________________________
void MessageSender::operator()(std::string json) const {
  distributorAndOwningQueryId_->distributor_->addQueryStatusUpdate(
      std::move(json));
}
}  // namespace ad_utility::websocket
