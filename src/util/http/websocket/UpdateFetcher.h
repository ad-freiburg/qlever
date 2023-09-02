//   Copyright 2023, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#ifndef QLEVER_UPDATEFETCHER_H
#define QLEVER_UPDATEFETCHER_H

#include <boost/asio/awaitable.hpp>
#include <boost/asio/use_awaitable.hpp>

#include "util/http/websocket/WebSocketTracker.h"

namespace ad_utility::websocket {
using StrandType = net::strand<net::any_io_executor>;

/// Class that provides an interface for boost::asio, so a websocket connection
/// can "asynchronously wait" for an update of a specified query to occur.
/// There is one instance of this class for every connected websocket.
class UpdateFetcher {
  using PayloadType = std::shared_ptr<const std::string>;

  WebSocketTracker& webSocketTracker_;
  const QueryId& queryId_;
  StrandType& socketStrand_;
  std::shared_ptr<QueryToSocketDistributor> distributor_{nullptr};
  // Counter to ensure sequential processing
  size_t nextIndex_ = 0;

 public:
  UpdateFetcher(const QueryId& queryId, WebSocketTracker& webSocketTracker,
                StrandType& strand)
      : webSocketTracker_{webSocketTracker},
        queryId_{queryId},
        socketStrand_{strand} {}

  /// If an update occurred for the given query since the last time this was
  /// called this resumes immediately. Otherwise it will wait
  /// (in the boost::asio world) for an update to occur and resume then.
  net::awaitable<PayloadType> waitForEvent();
};

}  // namespace ad_utility::websocket

#endif  // QLEVER_UPDATEFETCHER_H
