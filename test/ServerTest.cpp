// Copyright 2024, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Julian Mundhahs (mundhahj@tf.uni-freiburg.de)

#include <engine/Server.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <boost/beast/http.hpp>

#include "util/GTestHelpers.h"
#include "util/HttpRequestHelpers.h"
#include "util/http/HttpUtils.h"
#include "util/http/UrlParser.h"

using namespace ad_utility::url_parser;
using namespace ad_utility::url_parser::sparqlOperation;
using namespace ad_utility::testing;

namespace {
auto ParsedRequestIs = [](const std::string& path,
                          const std::optional<std::string>& accessToken,
                          const ParamValueMap& parameters,
                          const std::variant<Query, Update, None>& operation)
    -> testing::Matcher<const ParsedRequest> {
  return testing::AllOf(
      AD_FIELD(ad_utility::url_parser::ParsedRequest, path_, testing::Eq(path)),
      AD_FIELD(ad_utility::url_parser::ParsedRequest, accessToken_,
               testing::Eq(accessToken)),
      AD_FIELD(ad_utility::url_parser::ParsedRequest, parameters_,
               testing::ContainerEq(parameters)),
      AD_FIELD(ad_utility::url_parser::ParsedRequest, operation_,
               testing::Eq(operation)));
};
}  // namespace

TEST(ServerTest, parseHttpRequest) {
  auto parse = [](const ad_utility::httpUtils::HttpRequest auto& request) {
    return Server::parseHttpRequest(request);
  };
  const std::string URLENCODED =
      "application/x-www-form-urlencoded;charset=UTF-8";
  const std::string QUERY = "application/sparql-query";
  const std::string UPDATE = "application/sparql-update";
  EXPECT_THAT(parse(makeGetRequest("/")),
              ParsedRequestIs("/", std::nullopt, {}, None{}));
  EXPECT_THAT(parse(makeGetRequest("/ping")),
              ParsedRequestIs("/ping", std::nullopt, {}, None{}));
  EXPECT_THAT(parse(makeGetRequest("/?cmd=stats")),
              ParsedRequestIs("/", std::nullopt, {{"cmd", {"stats"}}}, None{}));
  EXPECT_THAT(parse(makeGetRequest(
                  "/?query=SELECT+%2A%20WHERE%20%7B%7D&action=csv_export")),
              ParsedRequestIs("/", std::nullopt, {{"action", {"csv_export"}}},
                              Query{"SELECT * WHERE {}"}));
  EXPECT_THAT(
      parse(makePostRequest("/", URLENCODED,
                            "query=SELECT+%2A%20WHERE%20%7B%7D&send=100")),
      ParsedRequestIs("/", std::nullopt, {{"send", {"100"}}},
                      Query{"SELECT * WHERE {}"}));
  AD_EXPECT_THROW_WITH_MESSAGE(
      parse(makePostRequest("/", URLENCODED,
                            "ääär y=SELECT+%2A%20WHERE%20%7B%7D&send=100")),
      ::testing::HasSubstr("Invalid URL-encoded POST request"));
  AD_EXPECT_THROW_WITH_MESSAGE(
      parse(makeGetRequest("/?query=SELECT%20%2A%20WHERE%20%7B%7D&query=SELECT%"
                           "20%3Ffoo%20WHERE%20%7B%7D")),
      ::testing::StrEq(
          "Parameter \"query\" must be given exactly once. Is: 2"));
  AD_EXPECT_THROW_WITH_MESSAGE(
      parse(makePostRequest("/", URLENCODED,
                            "query=SELECT%20%2A%20WHERE%20%7B%7D&update=DELETE%"
                            "20%7B%7D%20WHERE%20%7B%7D")),
      ::testing::HasSubstr(
          "Request must only contain one of \"query\" and \"update\"."));
  AD_EXPECT_THROW_WITH_MESSAGE(
      parse(makePostRequest("/", URLENCODED,
                            "update=DELETE%20%7B%7D%20WHERE%20%7B%7D&update="
                            "DELETE%20%7B%7D%20WHERE%20%7B%7D")),
      ::testing::StrEq(
          "Parameter \"update\" must be given exactly once. Is: 2"));
  EXPECT_THAT(
      parse(makePostRequest("/", "application/x-www-form-urlencoded",
                            "query=SELECT%20%2A%20WHERE%20%7B%7D&send=100")),
      ParsedRequestIs("/", std::nullopt, {{"send", {"100"}}},
                      Query{"SELECT * WHERE {}"}));
  EXPECT_THAT(
      parse(makePostRequest("/", URLENCODED,
                            "query=SELECT%20%2A%20WHERE%20%7B%7D")),
      ParsedRequestIs("/", std::nullopt, {}, Query{"SELECT * WHERE {}"}));
  EXPECT_THAT(
      parse(makePostRequest(
          "/", URLENCODED,
          "query=SELECT%20%2A%20WHERE%20%7B%7D&default-graph-uri=https%3A%2F%"
          "2Fw3.org%2Fdefault&named-graph-uri=https%3A%2F%2Fw3.org%2F1&named-"
          "graph-uri=https%3A%2F%2Fw3.org%2F2")),
      ParsedRequestIs(
          "/", std::nullopt,
          {{"default-graph-uri", {"https://w3.org/default"}},
           {"named-graph-uri", {"https://w3.org/1", "https://w3.org/2"}}},
          Query{"SELECT * WHERE {}"}));
  AD_EXPECT_THROW_WITH_MESSAGE(
      parse(makePostRequest("/?send=100", URLENCODED,
                            "query=SELECT%20%2A%20WHERE%20%7B%7D")),
      testing::StrEq("URL-encoded POST requests must not contain query "
                     "parameters in the URL."));
  EXPECT_THAT(
      parse(makePostRequest("/", URLENCODED, "cmd=clear-cache")),
      ParsedRequestIs("/", std::nullopt, {{"cmd", {"clear-cache"}}}, None{}));
  EXPECT_THAT(
      parse(makePostRequest("/", QUERY, "SELECT * WHERE {}")),
      ParsedRequestIs("/", std::nullopt, {}, Query{"SELECT * WHERE {}"}));
  EXPECT_THAT(parse(makePostRequest("/?send=100", QUERY, "SELECT * WHERE {}")),
              ParsedRequestIs("/", std::nullopt, {{"send", {"100"}}},
                              Query{"SELECT * WHERE {}"}));
  AD_EXPECT_THROW_WITH_MESSAGE(
      parse(makeRequest(http::verb::patch, "/")),
      testing::StrEq(
          "Request method \"PATCH\" not supported (has to be GET or POST)"));
  AD_EXPECT_THROW_WITH_MESSAGE(
      parse(makePostRequest("/", "invalid/content-type", "")),
      testing::StrEq(
          "POST request with content type \"invalid/content-type\" not "
          "supported (must be \"application/x-www-form-urlencoded\", "
          "\"application/sparql-query\" or \"application/sparql-update\")"));
  AD_EXPECT_THROW_WITH_MESSAGE(
      parse(makeGetRequest("/?update=DELETE%20%2A%20WHERE%20%7B%7D")),
      testing::StrEq("SPARQL Update is not allowed as GET request."));
  EXPECT_THAT(
      parse(makePostRequest("/", UPDATE, "DELETE * WHERE {}")),
      ParsedRequestIs("/", std::nullopt, {}, Update{"DELETE * WHERE {}"}));
  EXPECT_THAT(
      parse(makePostRequest("/", URLENCODED,
                            "update=DELETE%20%2A%20WHERE%20%7B%7D")),
      ParsedRequestIs("/", std::nullopt, {}, Update{"DELETE * WHERE {}"}));
  EXPECT_THAT(
      parse(
          makePostRequest("/", URLENCODED, "update=DELETE+%2A+WHERE%20%7B%7D")),
      ParsedRequestIs("/", std::nullopt, {}, Update{"DELETE * WHERE {}"}));
  // TODO<qup42>: there could be some more here, but i'll wait until #1668
  EXPECT_THAT(
      parse(makeGetRequest("/?query=a&access-token=foo")),
      ParsedRequestIs("/", "foo", {{"access-token", {"foo"}}}, Query{"a"}));
  EXPECT_THAT(
      parse(makePostRequest("/", URLENCODED,
                            "update=DELETE%20WHERE%20%7B%7D&access-token=foo")),
      ParsedRequestIs("/", "foo", {{"access-token", {"foo"}}},
                      Update{"DELETE WHERE {}"}));
  EXPECT_THAT(parse(makePostRequest(
                  "/", URLENCODED,
                  "query=SELECT%20%2A%20WHERE%20%7B%7D&access-token=foo")),
              ParsedRequestIs("/", "foo", {{"access-token", {"foo"}}},
                              Query{"SELECT * WHERE {}"}));
  EXPECT_THAT(
      parse(makePostRequest("/?access-token=foo", UPDATE, "DELETE * WHERE {}")),
      ParsedRequestIs("/", "foo", {{"access-token", {"foo"}}},
                      Update{"DELETE * WHERE {}"}));
}

TEST(ServerTest, determineResultPinning) {
  EXPECT_THAT(Server::determineResultPinning(
                  {{"pinsubtrees", {"true"}}, {"pinresult", {"true"}}}),
              testing::Pair(true, true));
  EXPECT_THAT(Server::determineResultPinning({{"pinresult", {"true"}}}),
              testing::Pair(false, true));
  EXPECT_THAT(Server::determineResultPinning({{"pinsubtrees", {"otherValue"}}}),
              testing::Pair(false, false));
}

TEST(ServerTest, determineMediaType) {
  auto MakeRequest = [](const std::optional<std::string>& accept,
                        const http::verb method = http::verb::get,
                        const std::string& target = "/",
                        const std::string& body = "") {
    auto req = http::request<http::string_body>{method, target, 11};
    if (accept.has_value()) {
      req.set(http::field::accept, accept.value());
    }
    req.body() = body;
    req.prepare_payload();
    return req;
  };
  auto checkActionMediatype = [&](const std::string& actionName,
                                  ad_utility::MediaType expectedMediaType) {
    EXPECT_THAT(Server::determineMediaType({{"action", {actionName}}},
                                           MakeRequest(std::nullopt)),
                testing::Eq(expectedMediaType));
  };
  // The media type associated with the action overrides the `Accept` header.
  EXPECT_THAT(Server::determineMediaType(
                  {{"action", {"csv_export"}}},
                  MakeRequest("application/sparql-results+json")),
              testing::Eq(ad_utility::MediaType::csv));
  checkActionMediatype("csv_export", ad_utility::MediaType::csv);
  checkActionMediatype("tsv_export", ad_utility::MediaType::tsv);
  checkActionMediatype("qlever_json_export", ad_utility::MediaType::qleverJson);
  checkActionMediatype("sparql_json_export", ad_utility::MediaType::sparqlJson);
  checkActionMediatype("turtle_export", ad_utility::MediaType::turtle);
  checkActionMediatype("binary_export", ad_utility::MediaType::octetStream);
  EXPECT_THAT(Server::determineMediaType(
                  {}, MakeRequest("application/sparql-results+json")),
              testing::Eq(ad_utility::MediaType::sparqlJson));
  // No supported media type in the `Accept` header. (Contrary to it's docstring
  // and interface) `ad_utility::getMediaTypeFromAcceptHeader` throws an
  // exception if no supported media type is found.
  AD_EXPECT_THROW_WITH_MESSAGE(
      Server::determineMediaType({}, MakeRequest("text/css")),
      testing::HasSubstr("Not a single media type known to this parser was "
                         "detected in \"text/css\"."));
  // No `Accept` header means that any content type is allowed.
  EXPECT_THAT(Server::determineMediaType({}, MakeRequest(std::nullopt)),
              testing::Eq(ad_utility::MediaType::sparqlJson));
  // No `Accept` header and an empty `Accept` header are not distinguished.
  EXPECT_THAT(Server::determineMediaType({}, MakeRequest("")),
              testing::Eq(ad_utility::MediaType::sparqlJson));
}

TEST(ServerTest, getQueryId) {
  using namespace ad_utility::websocket;
  Server server{9999, 1, ad_utility::MemorySize::megabytes(1), "accessToken"};
  auto reqWithExplicitQueryId = makeGetRequest("/");
  reqWithExplicitQueryId.set("Query-Id", "100");
  const auto req = makeGetRequest("/");
  {
    // A request with a custom query id.
    auto queryId1 = server.getQueryId(reqWithExplicitQueryId,
                                      "SELECT * WHERE { ?a ?b ?c }");
    // Another request with the same custom query id. This throws an error,
    // because query id cannot be used for multiple queries at the same time.
    AD_EXPECT_THROW_WITH_MESSAGE(
        server.getQueryId(reqWithExplicitQueryId,
                          "SELECT * WHERE { ?a ?b ?c }"),
        testing::HasSubstr("Query id '100' is already in use!"));
  }
  // The custom query id can be reused, once the query is finished.
  auto queryId1 =
      server.getQueryId(reqWithExplicitQueryId, "SELECT * WHERE { ?a ?b ?c }");
  // Without custom query ids, unique ids are generated.
  auto queryId2 = server.getQueryId(req, "SELECT * WHERE { ?a ?b ?c }");
  auto queryId3 = server.getQueryId(req, "SELECT * WHERE { ?a ?b ?c }");
}

TEST(ServerTest, createMessageSender) {
  Server server{9999, 1, ad_utility::MemorySize::megabytes(1), "accessToken"};
  auto reqWithExplicitQueryId = makeGetRequest("/");
  std::string customQueryId = "100";
  reqWithExplicitQueryId.set("Query-Id", customQueryId);
  const auto req = makeGetRequest("/");
  // The query hub is only valid once, the server has been started.
  AD_EXPECT_THROW_WITH_MESSAGE(
      server.createMessageSender(server.queryHub_, req,
                                 "SELECT * WHERE { ?a ?b ?c }"),
      testing::HasSubstr("Assertion `queryHubLock` failed."));
  {
    // Set a dummy query hub.
    boost::asio::io_context io_context;
    auto queryHub =
        std::make_shared<ad_utility::websocket::QueryHub>(io_context);
    server.queryHub_ = queryHub;
    // MessageSenders are created normally.
    server.createMessageSender(server.queryHub_, req,
                               "SELECT * WHERE { ?a ?b ?c }");
    server.createMessageSender(server.queryHub_, req,
                               "INSERT DATA { <foo> <bar> <baz> }");
    EXPECT_THAT(
        server.createMessageSender(server.queryHub_, reqWithExplicitQueryId,
                                   "INSERT DATA { <foo> <bar> <baz> }"),
        AD_PROPERTY(ad_utility::websocket::MessageSender, getQueryId,
                    testing::Eq(ad_utility::websocket::QueryId::idFromString(
                        customQueryId))));
  }
  // Once the query hub expires (e.g. because the io context dies), message
  // senders can no longer be created.
  AD_EXPECT_THROW_WITH_MESSAGE(
      server.createMessageSender(server.queryHub_, req,
                                 "SELECT * WHERE { ?a ?b ?c }"),
      testing::HasSubstr("Assertion `queryHubLock` failed."));
}

TEST(ServerTest, extractAccessToken) {
  auto extract = [](const ad_utility::httpUtils::HttpRequest auto& request) {
    auto parsedUrl = parseRequestTarget(request.target());
    return Server::extractAccessToken(request, parsedUrl.parameters_);
  };
  // TODO<qup42>: replace once #1668 is merged
  auto makeRequest =
      [](const http::verb method = http::verb::get,
         const std::string& target = "/",
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
      };
  EXPECT_THAT(extract(makeGetRequest("/")), testing::Eq(std::nullopt));
  EXPECT_THAT(extract(makeGetRequest("/?access-token=foo")),
              testing::Optional(testing::Eq("foo")));
  EXPECT_THAT(
      extract(makeRequest(http::verb::get, "/",
                          {{http::field::authorization, "Bearer foo"}})),
      testing::Optional(testing::Eq("foo")));
  // The header takes precedence over the query parameter.
  EXPECT_THAT(
      extract(makeRequest(http::verb::get, "/?access-token=bar",
                          {{http::field::authorization, "Bearer foo"}})),
      testing::Optional(testing::Eq("foo")));
  AD_EXPECT_THROW_WITH_MESSAGE(
      extract(makeRequest(http::verb::get, "/",
                          {{http::field::authorization, "foo"}})),
      testing::HasSubstr(
          "Authorization header must start with \"Bearer \". Got: \"foo\"."));
  EXPECT_THAT(extract(makePostRequest("/", "text/turtle", "")),
              testing::Eq(std::nullopt));
  EXPECT_THAT(extract(makePostRequest("/?access-token=foo", "text/turtle", "")),
              testing::Optional(testing::Eq("foo")));
  EXPECT_THAT(
      extract(makeRequest(http::verb::post, "/?access-token=bar",
                          {{http::field::authorization, "Bearer foo"}})),
      testing::Optional(testing::Eq("foo")));
  AD_EXPECT_THROW_WITH_MESSAGE(
      extract(makeRequest(http::verb::post, "/?access-token=bar",
                          {{http::field::authorization, "foo"}})),
      testing::HasSubstr(
          "Authorization header must start with \"Bearer \". Got: \"foo\"."));
}
