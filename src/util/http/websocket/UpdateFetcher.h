//   Copyright 2023, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#ifndef QLEVER_UPDATEFETCHER_H
#define QLEVER_UPDATEFETCHER_H

#include "util/http/websocket/WebSocketTracker.h"

namespace ad_utility::websocket {

/// Class that provides an interface for boost::asio, so a websocket connection
/// can "asynchronously wait" for an update of a specified query to occur.
class UpdateFetcher {
  using payload_type = std::shared_ptr<const std::string>;

  WebSocketTracker& webSocketTracker_;
  const QueryId& queryId_;
  net::strand<net::io_context::executor_type>& socketStrand_;
  std::shared_ptr<QueryToSocketDistributor> distributor_{nullptr};
  // Counter to ensure sequential processing
  size_t nextIndex_ = 0;

  // Return the next element from queryEventQueue and increment nextIndex_ by 1
  // if nextIndex_ is still within bounds and returns a nullptr otherwise
  payload_type optionalFetchAndAdvance();

 public:
  UpdateFetcher(const QueryId& queryId, WebSocketTracker& webSocketTracker,
                net::strand<net::io_context::executor_type>& strand)
      : webSocketTracker_{webSocketTracker},
        queryId_{queryId},
        socketStrand_{strand} {}

  /// If an update occurred for the given query since the last time this was
  /// called this resumes immediately. Otherwise it will wait
  /// (in the boost::asio world) for an update to occur and resume then.
  // Based on https://stackoverflow.com/a/69285751
  template <typename CompletionToken>
  auto waitForEvent(CompletionToken&& token) {
    auto initiate = [this]<typename Handler>(Handler&& self) mutable {
      auto sharedSelf = std::make_shared<Handler>(std::forward<Handler>(self));

      net::post(
          socketStrand_, [this, sharedSelf = std::move(sharedSelf)]() mutable {
            auto waitingHook = [this, sharedSelf = std::move(sharedSelf)]() {
              AD_CORRECTNESS_CHECK(distributor_);
              if (auto data = optionalFetchAndAdvance()) {
                (*sharedSelf)(std::move(data));
              } else if (distributor_->isFinished()) {
                (*sharedSelf)(nullptr);
              } else {
                distributor_->callOnUpdate(
                    [this, sharedSelf = std::move(sharedSelf)]() {
                      if (auto data = optionalFetchAndAdvance()) {
                        (*sharedSelf)(std::move(data));
                      } else if (distributor_->isFinished()) {
                        (*sharedSelf)(nullptr);
                      }
                    });
              }
            };
            if (distributor_) {
              waitingHook();
            } else {
              // TODO this should return a cancel callback
              webSocketTracker_.invokeOnQueryStart(
                  queryId_, [this, waitingHook = std::move(waitingHook)](
                                auto distributor) mutable {
                    distributor_ = std::move(distributor);
                    waitingHook();
                  });
            }
          });
    };
    return net::async_initiate<CompletionToken, void(payload_type)>(
        initiate, std::forward<CompletionToken>(token));
  }
};

}  // namespace ad_utility::websocket

#endif  // QLEVER_UPDATEFETCHER_H
