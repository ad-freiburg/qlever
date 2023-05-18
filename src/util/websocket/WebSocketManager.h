//  Copyright 2023, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#pragma once

#include <boost/beast/websocket.hpp>

#include "util/http/beast.h"
#include "util/websocket/Common.h"
#include "util/websocket/QueryState.h"

namespace ad_utility::websocket {
namespace net = boost::asio;
namespace beast = boost::beast;
namespace http = beast::http;
using boost::asio::ip::tcp;
using websocket::common::QueryId;
using websocket::common::SharedPayloadAndTimestamp;

net::awaitable<void> manageConnection(
    tcp::socket socket, http::request<http::string_body> request,
    ad_utility::query_state::QueryStateManager& queryStateManager);
// Returns true if there are other active connections that do no currently wait.
bool fireAllCallbacksForQuery(
    const QueryId& queryId,
    SharedPayloadAndTimestamp runtimeInformationSnapshot);

std::optional<http::response<http::string_body>> checkPathIsValid(
    const http::request<http::string_body>& request);
};  // namespace ad_utility::websocket
