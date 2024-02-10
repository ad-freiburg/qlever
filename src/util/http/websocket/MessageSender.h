//   Copyright 2023, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#ifndef QLEVER_MESSAGESENDER_H
#define QLEVER_MESSAGESENDER_H

#include <boost/asio/co_spawn.hpp>

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
  /// Keep the OwningQueryId alive until
  /// distributorAndOwningQueryId_->signalEnd() is called (see the constructor
  /// of `MessageSender` for details).
  struct DistributorAndOwningQueryId {
    std::shared_ptr<QueryToSocketDistributor> distributor_;
    OwningQueryId owningQueryId_;

    // There seems to be a bug with gcc < 13 where aggregate-initializing
    // this struct in combination with coroutines leads to segfaults
    DistributorAndOwningQueryId(
        std::shared_ptr<QueryToSocketDistributor> distributor,
        OwningQueryId owningQueryId)
        : distributor_{std::move(distributor)},
          owningQueryId_{std::move(owningQueryId)} {}
  };
  UniqueCleanup<DistributorAndOwningQueryId> distributorAndOwningQueryId_;
  net::any_io_executor executor_;

  // This constructor is private because this instance should only ever be
  // created asynchronously. Use the public factory function `create` instead.
  MessageSender(DistributorAndOwningQueryId, net::any_io_executor);

 public:
  MessageSender(MessageSender&&) noexcept = default;
  MessageSender& operator=(MessageSender&&) noexcept = default;

  /// Asynchronously creates an instance of this class. This is because because
  /// creating a distributor for this class needs to be done in a synchronized
  /// way.
  static net::awaitable<MessageSender> create(OwningQueryId owningQueryId,
                                              QueryHub& queryHub);

  /// Broadcast the string to all listeners of this query asynchronously.
  void operator()(std::string) const;

  /// Get read only view of underlying `QueryId`.
  const QueryId& getQueryId() const noexcept {
    return distributorAndOwningQueryId_->owningQueryId_.toQueryId();
  }
};

}  // namespace ad_utility::websocket

#endif  // QLEVER_MESSAGESENDER_H
