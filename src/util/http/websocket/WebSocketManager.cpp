//  Copyright 2023, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#include "WebSocketManager.h"

#include <boost/asio/experimental/awaitable_operators.hpp>
#include <optional>

#include "Common.h"
#include "ctre/ctre.h"
#include "util/http/HttpUtils.h"

namespace ad_utility::websocket {
using namespace boost::asio::experimental::awaitable_operators;

std::shared_ptr<QueryToSocketDistributor> WebSocketManager::createDistributor(
    const QueryId& queryId) {
  auto future = net::post(
      registryStrand_.value(),
      std::packaged_task<std::shared_ptr<QueryToSocketDistributor>()>(
          [this, queryId]() {
            AD_CORRECTNESS_CHECK(!socketDistributors_.contains(queryId));
            auto distributor =
                std::make_shared<QueryToSocketDistributor>(queryId, ioContext_);
            socketDistributors_.emplace(queryId, distributor);
            waitingList_.signalQueryUpdate(queryId);
            return distributor;
          }));
  return future.get();
}

void WebSocketManager::invokeOnQueryStart(
    const QueryId& queryId,
    std::function<void(std::shared_ptr<QueryToSocketDistributor>)> callback) {
  net::post(registryStrand_.value(),
            [this, queryId, callback = std::move(callback)]() mutable {
              if (socketDistributors_.contains(queryId)) {
                callback(socketDistributors_.at(queryId));
              } else {
                waitingList_.callOnQueryUpdate(
                    queryId, [this, queryId, callback = std::move(callback)]() {
                      callback(socketDistributors_.at(queryId));
                    });
              }
            });
}

void WebSocketManager::releaseQuery(QueryId queryId) {
  net::post(registryStrand_.value(), [this, queryId = std::move(queryId)]() {
    auto distributor = socketDistributors_.at(queryId);
    socketDistributors_.erase(queryId);
    distributor->signalEnd();
    distributor->wakeUpWebSocketsForQuery();
  });
}

UpdateFetcher::payload_type UpdateFetcher::optionalFetchAndAdvance() {
  // FIXME access to data is not synchronized
  return nextIndex_ < distributor_->getData().size()
             ? distributor_->getData().at(nextIndex_++)
             : nullptr;
}

// Based on https://stackoverflow.com/a/69285751
template <typename CompletionToken>
auto UpdateFetcher::waitForEvent(CompletionToken&& token) {
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
            webSocketManager_.invokeOnQueryStart(
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
  UpdateFetcher updateFetcher{queryId, *this, registryStrand_.value()};
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

// Extracts the query id from a URL path. Returns the empty string when invalid.
std::string extractQueryId(std::string_view path) {
  auto match = ctre::match<"/watch/([^/?]+)">(path);
  if (match) {
    return match.get<1>().to_string();
  }
  return "";
}

net::awaitable<void> WebSocketManager::connectionLifecycle(
    tcp::socket socket, http::request<http::string_body> request) {
  auto queryIdString = extractQueryId(request.target());
  AD_CORRECTNESS_CHECK(!queryIdString.empty());
  QueryId queryId = QueryId::idFromString(queryIdString);
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

// TODO<C++23> use std::expected<void, ErrorResponse>
std::optional<http::response<http::string_body>> getErrorResponseIfPathIsValid(
    const http::request<http::string_body>& request) {
  auto path = request.target();

  if (extractQueryId(path).empty()) {
    return ad_utility::httpUtils::createNotFoundResponse(
        "No WebSocket on this path", request);
  }
  return std::nullopt;
}
}  // namespace ad_utility::websocket
