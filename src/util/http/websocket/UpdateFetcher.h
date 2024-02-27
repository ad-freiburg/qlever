//   Copyright 2023, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#ifndef QLEVER_UPDATEFETCHER_H
#define QLEVER_UPDATEFETCHER_H

#include <boost/asio/awaitable.hpp>

#include "util/http/websocket/QueryHub.h"
#include "util/http/websocket/QueryId.h"

namespace ad_utility::websocket {
/// Class that provides an interface for boost::asio, so a websocket connection
/// can "asynchronously wait" for an update of a specified query to occur.
/// There is one instance of this class for every connected websocket.
class UpdateFetcher {
  using PayloadType = std::shared_ptr<const std::string>;

  QueryHub& queryHub_;
  QueryId queryId_;
  std::shared_ptr<const QueryToSocketDistributor> distributor_ =
      queryHub_.createOrAcquireDistributorForReceiving(queryId_);
  // Counter to ensure sequential processing
  size_t nextIndex_ = 0;

 public:
  UpdateFetcher(QueryHub& queryHub, QueryId queryId)
      : queryHub_{queryHub}, queryId_{std::move(queryId)} {}

  /// Return the strand on which the calls to `waitForEvent` must be awaited.
  auto strand() const { return distributor_->strand(); }

  /// If an update occurred for the given query since the last time this was
  /// called this resumes immediately. Otherwise it will wait
  /// (in the boost::asio world) for an update to occur and resume then.
  /// Must be awaited on the strand returned by `strand()`, otherwise an
  /// exception is thrown (see `QueryToSocketDistributor.h` for details)
  net::awaitable<PayloadType> waitForEvent();
};

}  // namespace ad_utility::websocket

#endif  // QLEVER_UPDATEFETCHER_H
