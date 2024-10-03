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

using namespace ad_utility::url_parser;

namespace {
auto ParsedRequestIs =
    [](const std::string& path,
       const ad_utility::HashMap<std::string, std::string>& parameters,
       const std::variant<QueryOp, UpdateOp, UndefinedOp>& operation)
    -> testing::Matcher<const ParsedRequest> {
  return testing::AllOf(
      AD_FIELD(ad_utility::url_parser::ParsedRequest, path_, testing::Eq(path)),
      AD_FIELD(ad_utility::url_parser::ParsedRequest, parameters_,
               testing::ContainerEq(parameters)),
      AD_FIELD(ad_utility::url_parser::ParsedRequest, operation_,
               testing::Eq(operation)));
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
  EXPECT_THAT(parse(MakeGetRequest("/")),
              ParsedRequestIs("/", {}, UndefinedOp{}));
  EXPECT_THAT(parse(MakeGetRequest("/ping")),
              ParsedRequestIs("/ping", {}, UndefinedOp{}));
  EXPECT_THAT(parse(MakeGetRequest("/?cmd=stats")),
              ParsedRequestIs("/", {{"cmd", "stats"}}, UndefinedOp{}));
  EXPECT_THAT(parse(MakeGetRequest(
                  "/?query=SELECT+%2A%20WHERE%20%7B%7D&action=csv_export")),
              ParsedRequestIs("/", {{"action", "csv_export"}},
                              QueryOp{"SELECT * WHERE {}"}));
  EXPECT_THAT(
      parse(MakePostRequest("/", URLENCODED,
                            "query=SELECT+%2A%20WHERE%20%7B%7D&send=100")),
      ParsedRequestIs("/", {{"send", "100"}}, QueryOp{"SELECT * WHERE {}"}));
  AD_EXPECT_THROW_WITH_MESSAGE(
      parse(MakePostRequest("/", URLENCODED,
                            "ääär y=SELECT+%2A%20WHERE%20%7B%7D&send=100")),
      ::testing::HasSubstr("Invalid URL-encoded POST request"));
  EXPECT_THAT(
      parse(MakePostRequest("/", "application/x-www-form-urlencoded",
                            "query=SELECT%20%2A%20WHERE%20%7B%7D&send=100")),
      ParsedRequestIs("/", {{"send", "100"}}, QueryOp{"SELECT * WHERE {}"}));
  EXPECT_THAT(parse(MakePostRequest("/", URLENCODED,
                                    "query=SELECT%20%2A%20WHERE%20%7B%7D")),
              ParsedRequestIs("/", {}, QueryOp{"SELECT * WHERE {}"}));
  AD_EXPECT_THROW_WITH_MESSAGE(
      parse(MakePostRequest("/?send=100", URLENCODED,
                            "query=SELECT%20%2A%20WHERE%20%7B%7D")),
      testing::StrEq("URL-encoded POST requests must not contain query "
                     "parameters in the URL."));
  EXPECT_THAT(parse(MakePostRequest("/", URLENCODED, "cmd=clear-cache")),
              ParsedRequestIs("/", {{"cmd", "clear-cache"}}, UndefinedOp{}));
  EXPECT_THAT(parse(MakePostRequest("/", QUERY, "SELECT * WHERE {}")),
              ParsedRequestIs("/", {}, QueryOp{"SELECT * WHERE {}"}));
  EXPECT_THAT(
      parse(MakePostRequest("/?send=100", QUERY, "SELECT * WHERE {}")),
      ParsedRequestIs("/", {{"send", "100"}}, QueryOp{"SELECT * WHERE {}"}));
  AD_EXPECT_THROW_WITH_MESSAGE(
      parse(MakeBasicRequest(http::verb::patch, "/")),
      testing::StrEq(
          "Request method \"PATCH\" not supported (has to be GET or POST)"));
  AD_EXPECT_THROW_WITH_MESSAGE(
      parse(MakeGetRequest("/?update=DELETE%20%2A%20WHERE%20%7B%7D")),
      testing::StrEq("SPARQL Update is not allowed as GET request."));
  EXPECT_THAT(parse(MakePostRequest("/", UPDATE, "DELETE * WHERE {}")),
              ParsedRequestIs("/", {}, UpdateOp{"DELETE * WHERE {}"}));
  EXPECT_THAT(parse(MakePostRequest("/", URLENCODED,
                                    "update=DELETE%20%2A%20WHERE%20%7B%7D")),
              ParsedRequestIs("/", {}, UpdateOp{"DELETE * WHERE {}"}));
  EXPECT_THAT(parse(MakePostRequest("/", URLENCODED,
                                    "update=DELETE+%2A+WHERE%20%7B%7D")),
              ParsedRequestIs("/", {}, UpdateOp{"DELETE * WHERE {}"}));
}
