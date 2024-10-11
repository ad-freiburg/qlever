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
using namespace ad_utility::url_parser::sparqlOperation;

namespace {
auto ParsedRequestIs = [](const std::string& path,
                          const ParamValueMap& parameters,
                          const std::variant<Query, Update, None>& operation)
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
  EXPECT_THAT(parse(MakeGetRequest("/")), ParsedRequestIs("/", {}, None{}));
  EXPECT_THAT(parse(MakeGetRequest("/ping")),
              ParsedRequestIs("/ping", {}, None{}));
  EXPECT_THAT(parse(MakeGetRequest("/?cmd=stats")),
              ParsedRequestIs("/", {{"cmd", {"stats"}}}, None{}));
  EXPECT_THAT(parse(MakeGetRequest(
                  "/?query=SELECT+%2A%20WHERE%20%7B%7D&action=csv_export")),
              ParsedRequestIs("/", {{"action", {"csv_export"}}},
                              Query{"SELECT * WHERE {}"}));
  EXPECT_THAT(
      parse(MakePostRequest("/", URLENCODED,
                            "query=SELECT+%2A%20WHERE%20%7B%7D&send=100")),
      ParsedRequestIs("/", {{"send", {"100"}}}, Query{"SELECT * WHERE {}"}));
  AD_EXPECT_THROW_WITH_MESSAGE(
      parse(MakePostRequest("/", URLENCODED,
                            "ääär y=SELECT+%2A%20WHERE%20%7B%7D&send=100")),
      ::testing::HasSubstr("Invalid URL-encoded POST request"));
  AD_EXPECT_THROW_WITH_MESSAGE(
      parse(MakeGetRequest("/?query=SELECT%20%2A%20WHERE%20%7B%7D&query=SELECT%"
                           "20%3Ffoo%20WHERE%20%7B%7D")),
      ::testing::StrEq(
          "Parameter \"query\" must be given exactly once. Is: 2"));
  AD_EXPECT_THROW_WITH_MESSAGE(
      parse(MakePostRequest("/", URLENCODED,
                            "query=SELECT%20%2A%20WHERE%20%7B%7D&update=DELETE%"
                            "20%7B%7D%20WHERE%20%7B%7D")),
      ::testing::HasSubstr(
          "Request must only contain one of \"query\" and \"update\"."));
  AD_EXPECT_THROW_WITH_MESSAGE(
      parse(MakePostRequest("/", URLENCODED,
                            "update=DELETE%20%7B%7D%20WHERE%20%7B%7D&update="
                            "DELETE%20%7B%7D%20WHERE%20%7B%7D")),
      ::testing::StrEq(
          "Parameter \"update\" must be given exactly once. Is: 2"));
  EXPECT_THAT(
      parse(MakePostRequest("/", "application/x-www-form-urlencoded",
                            "query=SELECT%20%2A%20WHERE%20%7B%7D&send=100")),
      ParsedRequestIs("/", {{"send", {"100"}}}, Query{"SELECT * WHERE {}"}));
  EXPECT_THAT(parse(MakePostRequest("/", URLENCODED,
                                    "query=SELECT%20%2A%20WHERE%20%7B%7D")),
              ParsedRequestIs("/", {}, Query{"SELECT * WHERE {}"}));
  EXPECT_THAT(
      parse(MakePostRequest(
          "/", URLENCODED,
          "query=SELECT%20%2A%20WHERE%20%7B%7D&default-graph-uri=https%3A%2F%"
          "2Fw3.org%2Fdefault&named-graph-uri=https%3A%2F%2Fw3.org%2F1&named-"
          "graph-uri=https%3A%2F%2Fw3.org%2F2")),
      ParsedRequestIs(
          "/",
          {{"default-graph-uri", {"https://w3.org/default"}},
           {"named-graph-uri", {"https://w3.org/1", "https://w3.org/2"}}},
          Query{"SELECT * WHERE {}"}));
  AD_EXPECT_THROW_WITH_MESSAGE(
      parse(MakePostRequest("/?send=100", URLENCODED,
                            "query=SELECT%20%2A%20WHERE%20%7B%7D")),
      testing::StrEq("URL-encoded POST requests must not contain query "
                     "parameters in the URL."));
  EXPECT_THAT(parse(MakePostRequest("/", URLENCODED, "cmd=clear-cache")),
              ParsedRequestIs("/", {{"cmd", {"clear-cache"}}}, None{}));
  EXPECT_THAT(parse(MakePostRequest("/", QUERY, "SELECT * WHERE {}")),
              ParsedRequestIs("/", {}, Query{"SELECT * WHERE {}"}));
  EXPECT_THAT(
      parse(MakePostRequest("/?send=100", QUERY, "SELECT * WHERE {}")),
      ParsedRequestIs("/", {{"send", {"100"}}}, Query{"SELECT * WHERE {}"}));
  AD_EXPECT_THROW_WITH_MESSAGE(
      parse(MakeBasicRequest(http::verb::patch, "/")),
      testing::StrEq(
          "Request method \"PATCH\" not supported (has to be GET or POST)"));
  AD_EXPECT_THROW_WITH_MESSAGE(
      parse(MakePostRequest("/", "invalid/content-type", "")),
      testing::StrEq(
          "POST request with content type \"invalid/content-type\" not "
          "supported (must be \"application/x-www-form-urlencoded\", "
          "\"application/sparql-query\" or \"application/sparql-update\")"));
  AD_EXPECT_THROW_WITH_MESSAGE(
      parse(MakeGetRequest("/?update=DELETE%20%2A%20WHERE%20%7B%7D")),
      testing::StrEq("SPARQL Update is not allowed as GET request."));
  EXPECT_THAT(parse(MakePostRequest("/", UPDATE, "DELETE * WHERE {}")),
              ParsedRequestIs("/", {}, Update{"DELETE * WHERE {}"}));
  EXPECT_THAT(parse(MakePostRequest("/", URLENCODED,
                                    "update=DELETE%20%2A%20WHERE%20%7B%7D")),
              ParsedRequestIs("/", {}, Update{"DELETE * WHERE {}"}));
  EXPECT_THAT(parse(MakePostRequest("/", URLENCODED,
                                    "update=DELETE+%2A+WHERE%20%7B%7D")),
              ParsedRequestIs("/", {}, Update{"DELETE * WHERE {}"}));
}

TEST(ServerTest, checkParameter) {
  const ParamValueMap exampleParams = {{"foo", {"bar"}},
                                       {"baz", {"qux", "quux"}}};

  EXPECT_THAT(Server::checkParameter(exampleParams, "doesNotExist", "", false),
              testing::Eq(std::nullopt));
  EXPECT_THAT(Server::checkParameter(exampleParams, "foo", "baz", false),
              testing::Eq(std::nullopt));
  AD_EXPECT_THROW_WITH_MESSAGE(
      Server::checkParameter(exampleParams, "foo", "bar", false),
      testing::StrEq("Access to \"foo=bar\" denied (requires a valid access "
                     "token), processing of request aborted"));
  EXPECT_THAT(Server::checkParameter(exampleParams, "foo", "bar", true),
              testing::Optional(testing::StrEq("bar")));
  AD_EXPECT_THROW_WITH_MESSAGE(
      Server::checkParameter(exampleParams, "baz", "qux", false),
      testing::StrEq("Parameter \"baz\" must be given exactly once. Is: 2"));
  EXPECT_THAT(Server::checkParameter(exampleParams, "foo", std::nullopt, true),
              testing::Optional(testing::StrEq("bar")));
  AD_EXPECT_THROW_WITH_MESSAGE(
      Server::checkParameter(exampleParams, "foo", std::nullopt, false),
      testing::StrEq("Access to \"foo=bar\" denied (requires a valid access "
                     "token), processing of request aborted"));
  AD_EXPECT_THROW_WITH_MESSAGE(
      Server::checkParameter(exampleParams, "baz", std::nullopt, false),
      testing::StrEq("Parameter \"baz\" must be given exactly once. Is: 2"));
  AD_EXPECT_THROW_WITH_MESSAGE(
      Server::checkParameter(exampleParams, "baz", std::nullopt, true),
      testing::StrEq("Parameter \"baz\" must be given exactly once. Is: 2"));
}
