// Copyright 2024, University of Freiburg
// Chair of Algorithms and Data Structures
// Authors: Julian Mundhahs <mundhahj@tf.uni-freiburg.de>

#ifndef QLEVER_TEST_UTIL_HTTPREQUESTHELPERS_H
#define QLEVER_TEST_UTIL_HTTPREQUESTHELPERS_H

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
// Overload of `makeRequest` where the HTTP method is a string.
inline auto makeRequest(
    std::string_view method, const std::string_view target = "/",
    const ad_utility::HashMap<http::field, std::string>& headers = {},
    const std::optional<std::string>& body = std::nullopt) {
  auto request = makeRequest(http::verb::get, target, headers, body);
  request.method_string(method);
  return request;
}

// Constructs a boost::beast GET request with the target path.
inline auto makeGetRequest(std::string_view target) {
  return makeRequest(http::verb::get, target);
}

// Constructs a boost::beast POST request with the target path, body content
// type and body content.
inline auto makePostRequest(std::string_view target,
                            const std::string& contentType,
                            const std::string& body) {
  return makeRequest(http::verb::post, target,
                     {{http::field::content_type, contentType}}, body);
}

}  // namespace ad_utility::testing

#endif  // QLEVER_TEST_UTIL_HTTPREQUESTHELPERS_H
