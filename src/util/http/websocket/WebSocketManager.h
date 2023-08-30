//  Copyright 2023, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#pragma once
#include <absl/container/flat_hash_map.h>

#include <boost/beast/websocket.hpp>
#include <unordered_map>
#include <vector>

#include "Common.h"
#include "util/http/beast.h"

namespace ad_utility::websocket {
namespace net = boost::asio;
namespace beast = boost::beast;
namespace http = beast::http;
using boost::asio::ip::tcp;
using websocket::common::QueryId;
using websocket = beast::websocket::stream<tcp::socket>;

class UpdateFetcher {
  using payload_type = std::shared_ptr<const std::string>;
  using QueryToCallback =
      std::unordered_multimap<QueryId, std::function<void()>,
                              absl::Hash<QueryId>>;
  absl::flat_hash_map<QueryId, std::shared_ptr<std::vector<payload_type>>>&
      informationQueues_;
  QueryToCallback& wakeupCalls_;
  const QueryId& queryId_;
  net::strand<net::io_context::executor_type>& registryStrand_;
  std::shared_ptr<std::vector<payload_type>> queryEventQueue_{nullptr};
  size_t nextIndex_ = 0;

  // Return the next element from queryEventQueue and increment nextIndex_ by 1
  // if nextIndex_ is still within bounds and returns a nullptr otherwise
  payload_type optionalFetchAndAdvance();

 public:
  UpdateFetcher(
      absl::flat_hash_map<QueryId, std::shared_ptr<std::vector<payload_type>>>&
          informationQueues,
      QueryToCallback& wakeupCalls, const QueryId& queryId,
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
      QueryId, std::shared_ptr<std::vector<std::shared_ptr<const std::string>>>>
      informationQueues_{};
  std::unordered_multimap<QueryId, std::function<void()>, absl::Hash<QueryId>>
      wakeupCalls_{};

  std::optional<net::strand<net::io_context::executor_type>> registryStrand_{};

  net::awaitable<void> handleClientCommands(websocket&);

  net::awaitable<void> waitForServerEvents(websocket&, const QueryId&);

  net::awaitable<void> connectionLifecycle(tcp::socket,
                                           http::request<http::string_body>);

  void wakeUpWebSocketsForQuery(const QueryId& queryId);
  void runAndEraseWakeUpCallsSynchronously(const QueryId& queryId);

 public:
  explicit WebSocketManager(net::io_context& ioContext)
      : registryStrand_(net::make_strand(ioContext)) {}
  net::awaitable<void> manageConnection(tcp::socket,
                                        http::request<http::string_body>);
  void addQueryStatusUpdate(const QueryId& queryId, std::string);

  void releaseQuery(QueryId queryId);
};

std::optional<http::response<http::string_body>> getErrorResponseIfPathIsValid(
    const http::request<http::string_body>&);
};  // namespace ad_utility::websocket
