// Copyright 2024, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Julian Mundhahs (mundhahj@tf.uni-freiburg.de)

#include <engine/Server.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <boost/beast/http.hpp>

#include "util/GTestHelpers.h"
#include "util/http/HttpUtils.h"
#include "util/http/UrlParser.h"

namespace {
auto ParsedRequest =
    [](const std::string& path,
       const ad_utility::HashMap<std::string, std::string>& parameters,
       const std::optional<std::string>& query)
    -> testing::Matcher<const ad_utility::url_parser::ParsedRequest> {
  return testing::AllOf(
      AD_FIELD(ad_utility::url_parser::ParsedRequest, path_, testing::Eq(path)),
      AD_FIELD(ad_utility::url_parser::ParsedRequest, parameters_,
               testing::ContainerEq(parameters)),
      AD_FIELD(ad_utility::url_parser::ParsedRequest, query_,
               testing::Eq(query)));
};
}  // namespace

TEST(ServerTest, parseHttpRequest) {
  namespace http = boost::beast::http;

  auto MakeBasicRequest = [](http::verb method, const std::string& target) {
    // version 11 stands for HTTP/1.1
    return http::request<http::string_body>{method, target, 11};
  };
  auto MakeGetRequest = [&MakeBasicRequest](const std::string& target) {
    return MakeBasicRequest(http::verb::get, target);
  };
  auto MakePostRequest = [&MakeBasicRequest](const std::string& target,
                                             const std::string& contentType,
                                             const std::string& body) {
    auto req = MakeBasicRequest(http::verb::post, target);
    req.set(http::field::content_type, contentType);
    req.body() = body;
    req.prepare_payload();
    return req;
  };
  auto parse = [](const ad_utility::httpUtils::HttpRequest auto& request) {
    return Server::parseHttpRequest(request);
  };
  const std::string URLENCODED =
      "application/x-www-form-urlencoded;charset=UTF-8";
  const std::string QUERY = "application/sparql-query";
  const std::string UPDATE = "application/sparql-update";
  EXPECT_THAT(parse(MakeGetRequest("/")), ParsedRequest("/", {}, std::nullopt));
  EXPECT_THAT(parse(MakeGetRequest("/ping")),
              ParsedRequest("/ping", {}, std::nullopt));
  EXPECT_THAT(parse(MakeGetRequest("/?cmd=stats")),
              ParsedRequest("/", {{"cmd", "stats"}}, std::nullopt));
  EXPECT_THAT(
      parse(MakeGetRequest(
          "/?query=SELECT%20%2A%20WHERE%20%7B%7D&action=csv_export")),
      ParsedRequest("/", {{"action", "csv_export"}}, "SELECT * WHERE {}"));
  EXPECT_THAT(
      parse(MakePostRequest("/", URLENCODED,
                            "query=SELECT%20%2A%20WHERE%20%7B%7D&send=100")),
      ParsedRequest("/", {{"send", "100"}}, "SELECT * WHERE {}"));
  EXPECT_THAT(
      parse(MakePostRequest("/", "application/x-www-form-urlencoded",
                            "query=SELECT%20%2A%20WHERE%20%7B%7D&send=100")),
      ParsedRequest("/", {{"send", "100"}}, "SELECT * WHERE {}"));
  EXPECT_THAT(parse(MakePostRequest("/", URLENCODED,
                                    "query=SELECT%20%2A%20WHERE%20%7B%7D")),
              ParsedRequest("/", {}, "SELECT * WHERE {}"));
  AD_EXPECT_THROW_WITH_MESSAGE(
      parse(MakePostRequest("/?send=100", URLENCODED,
                            "query=SELECT%20%2A%20WHERE%20%7B%7D")),
      testing::StrEq("URL-encoded POST requests must not contain query "
                     "parameters in the URL."));
  EXPECT_THAT(parse(MakePostRequest("/", URLENCODED, "cmd=clear-cache")),
              ParsedRequest("/", {{"cmd", "clear-cache"}}, std::nullopt));
  EXPECT_THAT(parse(MakePostRequest("/", QUERY, "SELECT * WHERE {}")),
              ParsedRequest("/", {}, "SELECT * WHERE {}"));
  EXPECT_THAT(parse(MakePostRequest("/?send=100", QUERY, "SELECT * WHERE {}")),
              ParsedRequest("/", {{"send", "100"}}, "SELECT * WHERE {}"));
  AD_EXPECT_THROW_WITH_MESSAGE(
      parse(MakeBasicRequest(http::verb::patch, "/")),
      testing::StrEq(
          "Request method \"PATCH\" not supported (has to be GET or POST)"));
  AD_EXPECT_THROW_WITH_MESSAGE(
      parse(MakePostRequest("/", UPDATE, "DELETE * WHERE {}")),
      testing::StrEq(
          "POST request with content type \"application/sparql-update\" not "
          "supported (must be \"application/x-www-form-urlencoded\" or "
          "\"application/sparql-query\")"));
}
