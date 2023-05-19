//  Copyright 2023, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#include "WebSocketManager.h"

#include <ctre/ctre.h>

#include <boost/asio/experimental/awaitable_operators.hpp>
#include <mutex>
#include <optional>

#include "util/MultiMap.h"
#include "util/http/HttpUtils.h"
#include "util/websocket/Common.h"

namespace ad_utility::websocket {
using namespace boost::asio::experimental::awaitable_operators;

void WebSocketManager::addQueryStatusUpdate(const QueryId& queryId,
                                            std::string payload) {
  auto sharedPayload = std::make_shared<const std::string>(std::move(payload));
  net::post(registryStrand_.value(),
            [this, sharedPayload = std::move(sharedPayload), &queryId]() {
              if (informationQueues_.contains(queryId)) {
                informationQueues_.at(queryId).append(std::move(sharedPayload));
              } else {
                informationQueues_.emplace(
                    queryId,
                    ad_utility::LinkedList<std::shared_ptr<const std::string>>{
                        std::move(sharedPayload)});
              }
              wakeUpWebSocketsForQuery(queryId);
            });
}

void WebSocketManager::wakeUpWebSocketsForQuery(const QueryId& queryId) {
  net::post(registryStrand_.value(), [this, &queryId]() {
    auto range = wakeupCalls_.equal_range(queryId);
    for (auto it = range.first; it != range.second; ++it) {
      it->second();
    }
    wakeupCalls_.erase(queryId);
  });
}

void WebSocketManager::releaseQuery(QueryId queryId) {
  net::post(registryStrand_.value(), [this, queryId = std::move(queryId)]() {
    informationQueues_.erase(queryId);
    auto range = wakeupCalls_.equal_range(queryId);
    for (auto it = range.first; it != range.second; ++it) {
      it->second();
    }
    wakeupCalls_.erase(queryId);
  });
}

// Based on https://stackoverflow.com/a/69285751
template <typename CompletionToken>
auto UpdateFetcher::waitForEvent(CompletionToken&& token) {
  auto initiate = [this]<typename Handler>(Handler&& self) mutable {
    auto sharedSelf = std::make_shared<Handler>(std::forward<Handler>(self));

    if (currentNode_ && currentNode_->hasNext()) {
      currentNode_ = currentNode_->next();
      (*sharedSelf)(currentNode_->getPayload());
    } else {
      net::post(registryStrand_, [this, sharedSelf]() {
        wakeupCalls_.insert(
            {queryId_, [this, sharedSelf]() {
               if (currentNode_) {
                 if (!currentNode_->hasNext()) {
                   (*sharedSelf)(nullptr);
                 } else {
                   currentNode_ = currentNode_->next();
                   (*sharedSelf)(currentNode_->getPayload());
                 }
               } else {
                 if (informationQueues_.contains(queryId_)) {
                   currentNode_ = informationQueues_.at(queryId_).getHead();
                   (*sharedSelf)(currentNode_->getPayload());
                 } else {
                   (*sharedSelf)(nullptr);
                 }
               }
             }});
      });
    }
  };
  return net::async_initiate<CompletionToken, void(payload_type)>(initiate,
                                                                  token);
}

net::awaitable<void> WebSocketManager::handleClientCommands(websocket& ws) {
  beast::flat_buffer buffer;

  while (ws.is_open()) {
    co_await ws.async_read(buffer, net::use_awaitable);
    // TODO<RobinTF> replace with cancellation process
    ws.text(ws.got_text());
    co_await ws.async_write(buffer.data(), net::use_awaitable);
    buffer.clear();
  }
}

net::awaitable<void> WebSocketManager::waitForServerEvents(
    websocket& ws, const QueryId& queryId) {
  UpdateFetcher updateFetcher{informationQueues_, wakeupCalls_, queryId,
                              registryStrand_.value()};
  while (ws.is_open()) {
    auto json = co_await updateFetcher.waitForEvent(net::use_awaitable);
    if (json == nullptr) {
      if (ws.is_open()) {
        co_await ws.async_close(beast::websocket::close_code::normal,
                                net::use_awaitable);
      }
      break;
    }
    ws.text(true);
    co_await ws.async_write(net::buffer(json->data(), json->length()),
                            net::use_awaitable);
  }
}

net::awaitable<void> WebSocketManager::connectionLifecycle(
    tcp::socket socket, http::request<http::string_body> request) {
  auto match = ctre::match<"/watch/([^/?]+)">(request.target());
  AD_CORRECTNESS_CHECK(match);
  QueryId queryId = QueryId::idFromString(match.get<1>().to_string());
  websocket ws{std::move(socket)};

  try {
    ws.set_option(beast::websocket::stream_base::timeout::suggested(
        beast::role_type::server));

    co_await ws.async_accept(request, boost::asio::use_awaitable);

    auto strand = net::make_strand(ws.get_executor());

    // experimental operators
    co_await (
        net::co_spawn(strand, waitForServerEvents(ws, queryId),
                      net::use_awaitable) &&
        net::co_spawn(strand, handleClientCommands(ws), net::use_awaitable));

  } catch (boost::system::system_error& error) {
    if (error.code() == beast::websocket::error::closed) {
      co_return;
    }
    // There was an unexpected error, rethrow
    throw;
  }

  // In case an unexpected exception is thrown, close the connection gracefully.
  if (ws.is_open()) {
    co_await ws.async_close(beast::websocket::close_code::internal_error,
                            net::use_awaitable);
  }
}

net::awaitable<void> WebSocketManager::manageConnection(
    tcp::socket socket, http::request<http::string_body> request) {
  auto executor = socket.get_executor();
  return net::co_spawn(
      executor, connectionLifecycle(std::move(socket), std::move(request)),
      net::use_awaitable);
}

std::optional<http::response<http::string_body>> checkPathIsValid(
    const http::request<http::string_body>& request) {
  auto path = request.target();

  if (ctre::match<"/watch/[^/?]+">(path)) {
    return std::nullopt;
  }
  return ad_utility::httpUtils::createNotFoundResponse(
      "No WebSocket on this path", request);
}
}  // namespace ad_utility::websocket
