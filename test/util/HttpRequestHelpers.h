// Copyright 2024, University of Freiburg
// Chair of Algorithms and Data Structures
// Authors: Julian Mundhahs <mundhahj@tf.uni-freiburg.de>

#pragma once

#include "util/HashMap.h"
#include "util/http/beast.h"

namespace ad_utility::testing {

namespace http = boost::beast::http;

// Construct a boost::beast request with the HTTP method, target path, headers
// and body.
inline auto makeRequest(
    const http::verb method = http::verb::get,
    const std::string_view target = "/",
    const ad_utility::HashMap<http::field, std::string>& headers = {},
    const std::optional<std::string>& body = std::nullopt) {
  // version 11 stands for HTTP/1.1
  auto req = http::request<http::string_body>{method, target, 11};
  for (const auto& [key, value] : headers) {
    req.set(key, value);
  }
  if (body.has_value()) {
    req.body() = body.value();
    req.prepare_payload();
  }
  return req;
}

// Constructs a boost::beast GET request with the target path.
inline auto makeGetRequest(const std::string& target) {
  return makeRequest(http::verb::get, target);
}

// Constructs a boost::beast POST request with the target path, body content
// type and body content.
inline auto makePostRequest(const std::string& target,
                            const std::string& contentType,
                            const std::string& body) {
  return makeRequest(http::verb::post, target,
                     {{http::field::content_type, contentType}}, body);
}

}  // namespace ad_utility::testing
