// Copyright 2024-2025, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Julian Mundhahs (mundhahj@tf.uni-freiburg.de)
//
// Copyright 2025, Bayerische Motoren Werke Aktiengesellschaft (BMW AG)

#include <gmock/gmock.h>

#include "engine/ParsedRequestBuilder.h"
#include "util/GTestHelpers.h"
#include "util/HttpRequestHelpers.h"
#include "util/TypeIdentity.h"
#include "util/http/HttpUtils.h"
#include "util/http/UrlParser.h"

using namespace ad_utility::use_type_identity;
using namespace ad_utility::url_parser;
using namespace ad_utility::url_parser::sparqlOperation;
using namespace ad_utility::testing;

// _____________________________________________________________________________________________
TEST(ParsedRequestBuilderTest, Constructor) {
  auto expect = [](const auto& request, const std::string& path,
                   const ParamValueMap& params,
                   const ad_utility::source_location l =
                       ad_utility::source_location::current()) {
    auto t = generateLocationTrace(l);
    const auto builder = ParsedRequestBuilder(request);
    EXPECT_THAT(
        builder.parsedRequest_,
        AllOf(AD_FIELD(ParsedRequest, path_, testing::Eq(path)),
              AD_FIELD(ParsedRequest, accessToken_, testing::Eq(std::nullopt)),
              AD_FIELD(ParsedRequest, parameters_, testing::Eq(params)),
              AD_FIELD(ParsedRequest, operation_,
                       testing::VariantWith<None>(None{}))));
  };
  expect(makeGetRequest("/"), "/", {});
  expect(makeGetRequest("/default?graph=bar"), "/default",
         {{"graph", {"bar"}}});
  expect(makeGetRequest("/api/foo?graph=bar&query=foo&graph=baz"), "/api/foo",
         {{"graph", {"bar", "baz"}}, {"query", {"foo"}}});
}

// _____________________________________________________________________________________________
TEST(ParsedRequestBuilderTest, extractAccessToken) {
  auto expect = [](const auto& request, const std::optional<string>& expected,
                   const ad_utility::source_location l =
                       ad_utility::source_location::current()) {
    auto t = generateLocationTrace(l);
    auto builder = ParsedRequestBuilder(request);
    EXPECT_THAT(builder.parsedRequest_.accessToken_, testing::Eq(std::nullopt));
    builder.extractAccessToken(request);
    EXPECT_THAT(builder.parsedRequest_.accessToken_, testing::Eq(expected));
  };
  expect(makeGetRequest("/"), std::nullopt);
  expect(makeGetRequest("/?query=foo"), std::nullopt);
  expect(makeGetRequest("/?query=foo&access-token=bar"), "bar");
  expect(makePostRequest("/?access-token=bar",
                         "application/x-www-form-urlencoded", "query=foo"),
         "bar");
  expect(
      makePostRequest("/?access-token=bar", "application/sparql-update", "foo"),
      "bar");
  expect(makeRequest(http::verb::get, "/",
                     {{http::field::authorization, "Bearer bar"}}, ""),
         "bar");
  expect(makeRequest(http::verb::post, "/",
                     {{http::field::authorization, "Bearer bar"}}, ""),
         "bar");
}

// _____________________________________________________________________________________________
TEST(ParsedRequestBuilderTest, extractDatasetClause) {
  auto expect = [](const auto& request, auto ti,
                   const std::vector<DatasetClause>& expected,
                   const ad_utility::source_location l =
                       ad_utility::source_location::current()) {
    using T = typename decltype(ti)::type;
    auto t = generateLocationTrace(l);
    auto builder = ParsedRequestBuilder(request);
    // Initialize an empty operation with no dataset clauses set.
    builder.parsedRequest_.operation_ = T{"", {}};
    builder.extractDatasetClauses();
    EXPECT_THAT(builder.parsedRequest_.operation_,
                testing::VariantWith<T>(
                    AD_FIELD(T, datasetClauses_, testing::Eq(expected))));
  };
  auto Iri = ad_utility::triple_component::Iri::fromIriref;
  expect(makeGetRequest("/"), ti<Query>, {});
  expect(makeGetRequest("/?default-graph-uri=foo"), ti<Query>,
         {{Iri("<foo>"), false}});
  expect(makeGetRequest("/?named-graph-uri=bar"), ti<Query>,
         {{Iri("<bar>"), true}});
  expect(makeGetRequest("/?default-graph-uri=foo&named-graph-uri=bar&using-"
                        "graph-uri=baz&using-named-graph-uri=abc"),
         ti<Query>, {{Iri("<foo>"), false}, {Iri("<bar>"), true}});
  expect(makePostRequest("/?default-graph-uri=foo&named-graph-uri=bar&using-"
                         "graph-uri=baz&using-named-graph-uri=abc",
                         "", ""),
         ti<Update>, {{Iri("<baz>"), false}, {Iri("<abc>"), true}});
}

// _____________________________________________________________________________________________
TEST(ParsedRequestBuilderTest, extractOperationIfSpecified) {
  auto expect = [](const auto& request, auto ti, std::string_view paramName,
                   const Operation& expected,
                   const ad_utility::source_location l =
                       ad_utility::source_location::current()) {
    using T = typename decltype(ti)::type;
    auto t = generateLocationTrace(l);
    auto builder = ParsedRequestBuilder(request);
    EXPECT_THAT(builder.parsedRequest_.operation_,
                testing::VariantWith<None>(None{}));
    // Initialize an empty operation with no dataset clauses set.
    builder.extractOperationIfSpecified<T>(paramName);
    EXPECT_THAT(builder.parsedRequest_.operation_, testing::Eq(expected));
  };
  expect(makeGetRequest("/"), ti<Query>, "query", None{});
  expect(makeGetRequest("/?query=foo"), ti<Update>, "update", None{});
  expect(makeGetRequest("/?query=foo"), ti<Query>, "query", Query{"foo", {}});
  expect(makePostRequest("/", "", ""), ti<Update>, "update", None{});
  expect(makePostRequest("/?update=bar", "", ""), ti<Update>, "update",
         Update{"bar", {}});
}

// _____________________________________________________________________________________________
TEST(ParsedRequestBuilderTest, isGraphStoreOperation) {
  auto isGraphStoreOperation =
      CPP_template_lambda()(typename RequestT)(const RequestT& request)(
          requires ad_utility::httpUtils::HttpRequest<RequestT>) {
    const auto builder = ParsedRequestBuilder(request);
    return builder.isGraphStoreOperation();
  };
  EXPECT_THAT(isGraphStoreOperation(makeGetRequest("/")), testing::IsFalse());
  EXPECT_THAT(
      isGraphStoreOperation(makeGetRequest("/?query=foo&access-token=bar")),
      testing::IsFalse());
  EXPECT_THAT(isGraphStoreOperation(makeGetRequest("/?default")),
              testing::IsTrue());
  EXPECT_THAT(isGraphStoreOperation(makeGetRequest("/?graph=foo")),
              testing::IsTrue());
  EXPECT_THAT(isGraphStoreOperation(
                  makeGetRequest("/default?query=foo&access-token=bar")),
              testing::IsFalse());
}

// _____________________________________________________________________________________________
TEST(ParsedRequestBuilderTest, extractGraphStoreOperation) {
  auto Iri = ad_utility::triple_component::Iri::fromIriref;
  auto expect = [](const auto& request, const GraphOrDefault& graph,
                   const ad_utility::source_location l =
                       ad_utility::source_location::current()) {
    auto t = generateLocationTrace(l);
    auto builder = ParsedRequestBuilder(request);
    EXPECT_THAT(builder.parsedRequest_.operation_,
                testing::VariantWith<None>(None{}));
    builder.extractGraphStoreOperation();
    EXPECT_THAT(builder.parsedRequest_.operation_,
                testing::VariantWith<GraphStoreOperation>(
                    AD_FIELD(GraphStoreOperation, graph_, testing::Eq(graph))));
  };
  expect(makeGetRequest("/?default"), DEFAULT{});
  expect(makeGetRequest("/?graph=foo"), Iri("<foo>"));
  expect(makePostRequest("/?default", "", ""), DEFAULT{});
  expect(makePostRequest("/?graph=bar", "", ""), Iri("<bar>"));
  {
    auto builder = ParsedRequestBuilder(makeGetRequest("/?default&graph=foo"));
    AD_EXPECT_THROW_WITH_MESSAGE(
        builder.extractGraphStoreOperation(),
        testing::HasSubstr(
            R"(Parameters "graph" and "default" must not be set at the same time.)"));
  }
  {
    auto builder = ParsedRequestBuilder(makeGetRequest("/default"));
    builder.parsedRequest_.operation_ = Query{"foo", {}};
    AD_EXPECT_THROW_WITH_MESSAGE(builder.extractGraphStoreOperation(),
                                 testing::HasSubstr(""));
  }
}

// _____________________________________________________________________________________________
TEST(ParsedRequestBuilderTest, parametersContain) {
  auto builder =
      ParsedRequestBuilder(makeGetRequest("/?query=foo&access-token=bar&baz"));
  EXPECT_THAT(builder.parametersContain("query"), testing::IsTrue());
  EXPECT_THAT(builder.parametersContain("access-token"), testing::IsTrue());
  EXPECT_THAT(builder.parametersContain("baz"), testing::IsTrue());
  EXPECT_THAT(builder.parametersContain("default"), testing::IsFalse());
  EXPECT_THAT(builder.parametersContain("graph"), testing::IsFalse());
  builder.parsedRequest_.parameters_ = {{"graph", {"foo"}}};
  EXPECT_THAT(builder.parametersContain("query"), testing::IsFalse());
  EXPECT_THAT(builder.parametersContain("access-token"), testing::IsFalse());
  EXPECT_THAT(builder.parametersContain("baz"), testing::IsFalse());
  EXPECT_THAT(builder.parametersContain("default"), testing::IsFalse());
  EXPECT_THAT(builder.parametersContain("graph"), testing::IsTrue());
}

// _____________________________________________________________________________________________
TEST(ParsedRequestBuilderTest, reportUnsupportedContentTypeIfGraphStore) {
  auto builderGraphStore = ParsedRequestBuilder(makeGetRequest("/?default"));
  AD_EXPECT_THROW_WITH_MESSAGE(
      builderGraphStore.reportUnsupportedContentTypeIfGraphStore(
          "application/x-www-form-urlencoded"),
      testing::HasSubstr(""));
  auto builderQuery = ParsedRequestBuilder(makeGetRequest("/?query=foo"));
  EXPECT_NO_THROW(builderQuery.reportUnsupportedContentTypeIfGraphStore(
      "application/sparql-query"));
}

// _____________________________________________________________________________________________
TEST(ParsedRequestBuilderTest, parameterIsContainedExactlyOnce) {
  auto builder = ParsedRequestBuilder(
      makeGetRequest("/?query=foo&access-token=bar&baz&query=baz"));
  EXPECT_THAT(builder.parameterIsContainedExactlyOnce("does-not-exist"),
              testing::IsFalse());
  EXPECT_THAT(builder.parameterIsContainedExactlyOnce("access-token"),
              testing::IsTrue());
  AD_EXPECT_THROW_WITH_MESSAGE(
      builder.parameterIsContainedExactlyOnce("query"),
      testing::HasSubstr(
          "Parameter \"query\" must be given exactly once. Is: 2"));
}

// _____________________________________________________________________________________________
TEST(ParsedRequestBuilderTest, extractTargetGraph) {
  auto Iri = ad_utility::triple_component::Iri::fromIriref;
  const auto extractTargetGraph = ParsedRequestBuilder::extractTargetGraph;
  // Equivalent to `/?default`
  EXPECT_THAT(extractTargetGraph({{"default", {""}}}), DEFAULT{});
  // Equivalent to `/?graph=foo`
  EXPECT_THAT(extractTargetGraph({{"graph", {"foo"}}}), Iri("<foo>"));
  // Equivalent to `/?graph=foo&graph=bar`
  AD_EXPECT_THROW_WITH_MESSAGE(
      extractTargetGraph({{"graph", {"foo", "bar"}}}),
      testing::HasSubstr(
          "Parameter \"graph\" must be given exactly once. Is: 2"));
  const std::string eitherDefaultOrGraphErrorMsg =
      R"(Exactly one of the query parameters "default" or "graph" must be set to identify the graph for the graph store protocol request.)";
  // Equivalent to `/` or `/?`
  AD_EXPECT_THROW_WITH_MESSAGE(
      extractTargetGraph({}), testing::HasSubstr(eitherDefaultOrGraphErrorMsg));
  // Equivalent to `/?unrelated=a&unrelated=b`
  AD_EXPECT_THROW_WITH_MESSAGE(
      extractTargetGraph({{"unrelated", {"a", "b"}}}),
      testing::HasSubstr(eitherDefaultOrGraphErrorMsg));
  // Equivalent to `/?default&graph=foo`
  AD_EXPECT_THROW_WITH_MESSAGE(
      extractTargetGraph({{"default", {""}}, {"graph", {"foo"}}}),
      testing::HasSubstr(eitherDefaultOrGraphErrorMsg));
}

// _____________________________________________________________________________________________
TEST(ParsedRequestBuilderTest, determineAccessToken) {
  auto extract =
      CPP_template_lambda()(typename RequestT)(const RequestT& request)(
          requires ad_utility::httpUtils::HttpRequest<RequestT>) {
    auto parsedUrl = parseRequestTarget(request.target());
    return ParsedRequestBuilder::determineAccessToken(request,
                                                      parsedUrl.parameters_);
  };
  EXPECT_THAT(extract(makeGetRequest("/")), testing::Eq(std::nullopt));
  EXPECT_THAT(extract(makeGetRequest("/?access-token=foo")),
              testing::Optional(testing::Eq("foo")));
  EXPECT_THAT(
      extract(makeRequest(http::verb::get, "/",
                          {{http::field::authorization, "Bearer foo"}})),
      testing::Optional(testing::Eq("foo")));
  EXPECT_THAT(
      extract(makeRequest(http::verb::get, "/?access-token=foo",
                          {{http::field::authorization, "Bearer foo"}})),
      testing::Optional(testing::Eq("foo")));
  AD_EXPECT_THROW_WITH_MESSAGE(
      extract(makeRequest(http::verb::get, "/?access-token=bar",
                          {{http::field::authorization, "Bearer foo"}})),
      testing::HasSubstr(
          "Access token is specified both in the `Authorization` header and "
          "by the `access-token` parameter, but they are not the same"));
  AD_EXPECT_THROW_WITH_MESSAGE(
      extract(makeRequest(http::verb::get, "/",
                          {{http::field::authorization, "foo"}})),
      testing::HasSubstr(
          "Authorization header doesn't start with \"Bearer \"."));
  EXPECT_THAT(extract(makePostRequest("/", "text/turtle", "")),
              testing::Eq(std::nullopt));
  EXPECT_THAT(extract(makePostRequest("/?access-token=foo", "text/turtle", "")),
              testing::Optional(testing::Eq("foo")));
  AD_EXPECT_THROW_WITH_MESSAGE(
      extract(makeRequest(http::verb::post, "/?access-token=bar",
                          {{http::field::authorization, "Bearer foo"}})),
      testing::HasSubstr(
          "Access token is specified both in the `Authorization` header and "
          "by the `access-token` parameter, but they are not the same"));
  AD_EXPECT_THROW_WITH_MESSAGE(
      extract(makeRequest(http::verb::post, "/?access-token=bar",
                          {{http::field::authorization, "foo"}})),
      testing::HasSubstr(
          "Authorization header doesn't start with \"Bearer \"."));
}
