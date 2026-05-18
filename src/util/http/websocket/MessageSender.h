//   Copyright 2023, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#ifndef QLEVER_MESSAGESENDER_H
#define QLEVER_MESSAGESENDER_H

#include <functional>

#include "util/UniqueCleanup.h"
#include "util/http/websocket/QueryHub.h"
#include "util/http/websocket/QueryId.h"
#include "util/http/websocket/QueryToSocketDistributor.h"

namespace ad_utility::websocket {
using unique_cleanup::UniqueCleanup;

/// This class provides a convenient wrapper that extracts the proper
/// `QueryToSocketDistributor` of the passed `QueryHub` reference
/// and provides a generic operator() interface to call addQueryStatusUpdate()
/// from the non-asio world.
class MessageSender {
 private:
  // Job B: registry ticket. Its own cleanup unregisters and logs "end".
  OwningQueryId owningQueryId_;
  // Job A: send channel; cleanup calls signalEnd() once. Declared after
  // owningQueryId_ so the constructor can use the query id.
  UniqueCleanup<std::shared_ptr<QueryToSocketDistributor>> distributor_;
  net::any_io_executor executor_;

 public:
  // Construct from a query ID and a `QueryHub`.
  MessageSender(OwningQueryId owningQueryId, QueryHub& queryHub);

  MessageSender(MessageSender&&) noexcept = default;
  MessageSender& operator=(MessageSender&&) noexcept = default;

  // Broadcast the string to all listeners. Precondition: the channel was
  // not moved out via extractUpdateCallback().
  void operator()(std::string) const;

  // Get read only view of underlying `QueryId`.
  const QueryId& getQueryId() const noexcept {
    return owningQueryId_.toQueryId();
  }

  // Move the send channel (Job A) out into a standalone callback; this
  // MessageSender then keeps only Job B. signalEnd() fires when the returned
  // callback is destroyed (after the plan's last late update).
  std::function<void(std::string)> extractUpdateCallback() &&;
};

}  // namespace ad_utility::websocket

#endif  // QLEVER_MESSAGESENDER_H
