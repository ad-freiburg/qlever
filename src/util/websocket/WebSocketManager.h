//  Copyright 2023, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#pragma once
#include <absl/container/btree_map.h>
#include <absl/container/flat_hash_map.h>

#include <boost/beast/websocket.hpp>

#include "util/LinkedList.h"
#include "util/http/beast.h"
#include "util/websocket/Common.h"

namespace ad_utility::websocket {
namespace net = boost::asio;
namespace beast = boost::beast;
namespace http = beast::http;
using boost::asio::ip::tcp;
using websocket::common::QueryId;
using websocket = beast::websocket::stream<tcp::socket>;

class UpdateFetcher {
  using payload_type = std::shared_ptr<const std::string>;
  absl::flat_hash_map<QueryId, ad_utility::LinkedList<payload_type>>&
      informationQueues_;
  absl::btree_multimap<QueryId, std::function<void()>>& wakeupCalls_;
  const QueryId& queryId_;
  net::strand<net::io_context::executor_type>& registryStrand_;
  std::shared_ptr<ad_utility::Node<payload_type>> currentNode_{nullptr};

 public:
  UpdateFetcher(
      absl::flat_hash_map<QueryId, ad_utility::LinkedList<payload_type>>&
          informationQueues,
      absl::btree_multimap<QueryId, std::function<void()>>& wakeupCalls,
      const QueryId& queryId,
      net::strand<net::io_context::executor_type>& registryStrand)
      : informationQueues_{informationQueues},
        wakeupCalls_{wakeupCalls},
        queryId_{queryId},
        registryStrand_{registryStrand} {}

  template <typename CompletionToken>
  auto waitForEvent(CompletionToken&& token);
};

class WebSocketManager {
  absl::flat_hash_map<
      QueryId, ad_utility::LinkedList<std::shared_ptr<const std::string>>>
      informationQueues_{};
  absl::btree_multimap<QueryId, std::function<void()>> wakeupCalls_{};

  std::optional<net::strand<net::io_context::executor_type>> registryStrand_{};

  net::awaitable<void> handleClientCommands(websocket&);

  net::awaitable<void> waitForServerEvents(websocket&, const QueryId&);

  net::awaitable<void> connectionLifecycle(tcp::socket,
                                           http::request<http::string_body>);

  void wakeUpWebSocketsForQuery(const QueryId& queryId);

 public:
  void setIoContext(net::io_context& ioContext) {
    registryStrand_ = net::make_strand(ioContext);
  }
  net::awaitable<void> manageConnection(tcp::socket,
                                        http::request<http::string_body>);
  void addQueryStatusUpdate(const QueryId& queryId, std::string);

  void releaseQuery(QueryId queryId);
};

std::optional<http::response<http::string_body>> checkPathIsValid(
    const http::request<http::string_body>&);
};  // namespace ad_utility::websocket
