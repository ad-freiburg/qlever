//  Copyright 2023, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#pragma once
#include <absl/container/flat_hash_map.h>

#include <boost/beast/websocket.hpp>
#include <vector>

#include "util/http/beast.h"
#include "util/http/websocket/Common.h"
#include "util/http/websocket/EphemeralWaitingList.h"
#include "util/http/websocket/QueryToSocketDistributor.h"

namespace ad_utility::websocket {
namespace net = boost::asio;
namespace beast = boost::beast;
namespace http = beast::http;
using boost::asio::ip::tcp;
using websocket::common::QueryId;
using websocket = beast::websocket::stream<tcp::socket>;

/// Central class to keep track of all currently connected websockets
/// to provide them with status updates.
class WebSocketManager {
  net::io_context& ioContext_;
  std::optional<net::strand<net::io_context::executor_type>> registryStrand_{};
  absl::flat_hash_map<QueryId, std::shared_ptr<QueryToSocketDistributor>>
      socketDistributors_{};
  EphemeralWaitingList waitingList_;

  /// Loop that waits for input from the client
  net::awaitable<void> handleClientCommands(websocket&);

  /// Loop that waits for an update of the given query to occur and send it
  /// to the client once it happens
  net::awaitable<void> waitForServerEvents(websocket&, const QueryId&);

  /// Accepts the websocket connection and "blocks" for the lifetime of the
  /// websocket connection
  net::awaitable<void> connectionLifecycle(tcp::socket,
                                           http::request<http::string_body>);

 public:
  /// Constructs an instance of this class using the provided io context
  /// reference to create a thread-safe strand for all cross-thread operations.
  /// Any instance needs to be destructed before the io context.
  explicit WebSocketManager(net::io_context& ioContext)
      : ioContext_{ioContext}, registryStrand_(net::make_strand(ioContext)) {}

  /// The main interface for this class. The HTTP server is supposed to check
  /// if an HTTP request is a websocket upgrade request and delegate further
  /// handling to this method if it is the case.
  net::awaitable<void> manageConnection(tcp::socket,
                                        http::request<http::string_body>);

  /// Notifies this class that the given query will no longer receive any
  /// updates, so all waiting connections will be closed.
  void releaseQuery(QueryId queryId);

  std::shared_ptr<QueryToSocketDistributor> createDistributor(const QueryId&);
  void invokeOnQueryStart(
      const QueryId&,
      std::function<void(std::shared_ptr<QueryToSocketDistributor>)>);
};

/// Class that provides an interface for boost::asio, so a websocket connection
/// can "asynchronously wait" for an update of a specified query to occur.
class UpdateFetcher {
  using payload_type = std::shared_ptr<const std::string>;

  WebSocketManager& webSocketManager_;
  const QueryId& queryId_;
  net::strand<net::io_context::executor_type>& socketStrand_;
  std::shared_ptr<QueryToSocketDistributor> distributor_{nullptr};
  // Counter to ensure sequential processing
  size_t nextIndex_ = 0;

  // Return the next element from queryEventQueue and increment nextIndex_ by 1
  // if nextIndex_ is still within bounds and returns a nullptr otherwise
  payload_type optionalFetchAndAdvance();

 public:
  UpdateFetcher(const QueryId& queryId, WebSocketManager& webSocketManager,
                net::strand<net::io_context::executor_type>& strand)
      : webSocketManager_{webSocketManager},
        queryId_{queryId},
        socketStrand_{strand} {}

  /// If an update occurred for the given query since the last time this was
  /// called this resumes immediately. Otherwise it will wait
  /// (in the boost::asio world) for an update to occur and resume then.
  template <typename CompletionToken>
  auto waitForEvent(CompletionToken&& token);
};

/// Helper function to provide a proper error response if the provided URL
/// path is not accepted by the server.
std::optional<http::response<http::string_body>> getErrorResponseIfPathIsValid(
    const http::request<http::string_body>&);
};  // namespace ad_utility::websocket
