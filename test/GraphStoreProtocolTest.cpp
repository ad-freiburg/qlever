// Copyright 2024, University of Freiburg
// Chair of Algorithms and Data Structures
// Authors: Julian Mundhahs <mundhahj@tf.uni-freiburg.de>

#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <parser/SparqlParserHelpers.h>

#include "./util/GTestHelpers.h"
#include "./util/HttpRequestHelpers.h"
#include "./util/TripleComponentTestHelpers.h"
#include "SparqlAntlrParserTestHelpers.h"
#include "engine/GraphStoreProtocol.h"

namespace m = matchers;
using namespace ad_utility::testing;

using Var = Variable;
using TC = TripleComponent;

// _____________________________________________________________________________________________
TEST(GraphStoreProtocolTest, extractTargetGraph) {
  // Equivalent to `/?default`
  EXPECT_THAT(GraphStoreProtocol::extractTargetGraph({{"default", {""}}}),
              DEFAULT{});
  // Equivalent to `/?graph=foo`
  EXPECT_THAT(GraphStoreProtocol::extractTargetGraph({{"graph", {"foo"}}}),
              iri("<foo>"));
  // Equivalent to `/?graph=foo&graph=bar`
  AD_EXPECT_THROW_WITH_MESSAGE(
      GraphStoreProtocol::extractTargetGraph({{"graph", {"foo", "bar"}}}),
      testing::HasSubstr(
          "Parameter \"graph\" must be given exactly once. Is: 2"));
  const std::string eitherDefaultOrGraphErrorMsg =
      "Exactly one of the query parameters default or graph must be set to "
      "identify the graph for the graph store protocol request.";
  // Equivalent to `/` or `/?`
  AD_EXPECT_THROW_WITH_MESSAGE(
      GraphStoreProtocol::extractTargetGraph({}),
      testing::HasSubstr(eitherDefaultOrGraphErrorMsg));
  // Equivalent to `/?unrelated=a&unrelated=b`
  AD_EXPECT_THROW_WITH_MESSAGE(
      GraphStoreProtocol::extractTargetGraph({{"unrelated", {"a", "b"}}}),
      testing::HasSubstr(eitherDefaultOrGraphErrorMsg));
  // Equivalent to `/?default&graph=foo`
  AD_EXPECT_THROW_WITH_MESSAGE(
      GraphStoreProtocol::extractTargetGraph(
          {{"default", {""}}, {"graph", {"foo"}}}),
      testing::HasSubstr(eitherDefaultOrGraphErrorMsg));
}

// _____________________________________________________________________________________________
TEST(GraphStoreProtocolTest, transformPost) {
  auto expectTransformPost =
      [](const ad_utility::httpUtils::HttpRequest auto& request,
         const testing::Matcher<const ParsedQuery&>& matcher,
         ad_utility::source_location l =
             ad_utility::source_location::current()) {
        auto trace = generateLocationTrace(l);
        const ad_utility::url_parser::ParsedUrl parsedUrl =
            ad_utility::url_parser::parseRequestTarget(request.target());
        const GraphOrDefault graph =
            GraphStoreProtocol::extractTargetGraph(parsedUrl.parameters_);
        EXPECT_THAT(GraphStoreProtocol::transformPost(request, graph), matcher);
      };

  expectTransformPost(
      makePostRequest("/?default", "text/turtle", "<a> <b> <c> ."),
      m::UpdateClause(
          m::GraphUpdate(
              {}, {{iri("<a>"), iri("<b>"), iri("<c>"), std::monostate{}}},
              std::nullopt),
          m::GraphPattern()));
  expectTransformPost(
      makePostRequest("/?default", "application/n-triples", "<a> <b> <c> ."),
      m::UpdateClause(
          m::GraphUpdate(
              {}, {{iri("<a>"), iri("<b>"), iri("<c>"), std::monostate{}}},
              std::nullopt),
          m::GraphPattern()));
  expectTransformPost(
      makePostRequest("/?graph=bar", "application/n-triples", "<a> <b> <c> ."),
      m::UpdateClause(
          m::GraphUpdate({},
                         {{iri("<a>"), iri("<b>"), iri("<c>"), Iri("<bar>")}},
                         std::nullopt),
          m::GraphPattern()));
  AD_EXPECT_THROW_WITH_MESSAGE(
      GraphStoreProtocol::transformPost(
          ad_utility::testing::makePostRequest(
              "/?default", "application/sparql-results+xml", ""),
          DEFAULT{}),
      testing::HasSubstr(
          "Mediatype \"application/sparql-results+xml\" is not supported for "
          "SPARQL Graph Store HTTP Protocol in QLever."));
  AD_EXPECT_THROW_WITH_MESSAGE(
      GraphStoreProtocol::transformPost(
          ad_utility::testing::makePostRequest(
              "/?default", "application/n-quads", "<a> <b> <c> <d> ."),
          DEFAULT{}),
      testing::HasSubstr("Not a single media type known to this parser was "
                         "detected in \"application/n-quads\"."));
  AD_EXPECT_THROW_WITH_MESSAGE(
      GraphStoreProtocol::transformPost(
          ad_utility::testing::makePostRequest(
              "/?default", "application/unknown", "fantasy"),
          DEFAULT{}),
      testing::HasSubstr("Not a single media type known to this parser was "
                         "detected in \"application/unknown\"."));
}

// _____________________________________________________________________________________________
TEST(GraphStoreProtocolTest, transformGet) {
  auto expectTransformGet =
      [](const ad_utility::httpUtils::HttpRequest auto& request,
         const testing::Matcher<const ParsedQuery&>& matcher,
         ad_utility::source_location l =
             ad_utility::source_location::current()) {
        auto trace = generateLocationTrace(l);
        const ad_utility::url_parser::ParsedUrl parsedUrl =
            ad_utility::url_parser::parseRequestTarget(request.target());
        const GraphOrDefault graph =
            GraphStoreProtocol::extractTargetGraph(parsedUrl.parameters_);
        EXPECT_THAT(GraphStoreProtocol::transformGet(graph), matcher);
      };
  expectTransformGet(
      makeGetRequest("/?default"),
      m::ConstructQuery({{Var{"?s"}, Var{"?p"}, Var{"?o"}}},
                        m::GraphPattern(matchers::Triples({SparqlTriple(
                            TC(Var{"?s"}), "?p", TC(Var{"?o"}))}))));
  expectTransformGet(
      makeGetRequest("/?graph=foo"),
      m::ConstructQuery({{Var{"?s"}, Var{"?p"}, Var{"?o"}}},
                        m::GraphPattern(matchers::Triples({SparqlTriple(
                            TC(Var{"?s"}), "?p", TC(Var{"?o"}))})),
                        ScanSpecificationAsTripleComponent::Graphs{
                            {TripleComponent(iri("<foo>"))}}));
}

// _____________________________________________________________________________________________
TEST(GraphStoreProtocolTest, transformGraphStoreProtocol) {
  EXPECT_THAT(GraphStoreProtocol::transformGraphStoreProtocol(
                  ad_utility::testing::makeGetRequest("/?default")),
              m::ConstructQuery({{Var{"?s"}, Var{"?p"}, Var{"?o"}}},
                                m::GraphPattern(matchers::Triples({SparqlTriple(
                                    TC(Var{"?s"}), "?p", TC(Var{"?o"}))}))));
  EXPECT_THAT(
      GraphStoreProtocol::transformGraphStoreProtocol(
          ad_utility::testing::makePostRequest(
              "/?default", "application/n-triples", "<foo> <bar> <baz> .")),
      m::UpdateClause(m::GraphUpdate({},
                                     {{iri("<foo>"), iri("<bar>"), iri("<baz>"),
                                       std::monostate{}}},
                                     std::nullopt),
                      m::GraphPattern()));
  auto expectUnsupportedMethod =
      [](const http::verb method, ad_utility::source_location l =
                                      ad_utility::source_location::current()) {
        auto trace = generateLocationTrace(l);
        AD_EXPECT_THROW_WITH_MESSAGE(
            GraphStoreProtocol::transformGraphStoreProtocol(
                ad_utility::testing::makeRequest(method, "/?default")),
            testing::HasSubstr(
                absl::StrCat(std::string{boost::beast::http::to_string(method)},
                             " in the SPARQL Graph Store HTTP Protocol")));
      };
  expectUnsupportedMethod(http::verb::put);
  expectUnsupportedMethod(http::verb::delete_);
  expectUnsupportedMethod(http::verb::head);
  expectUnsupportedMethod(http::verb::patch);
  AD_EXPECT_THROW_WITH_MESSAGE(
      GraphStoreProtocol::transformGraphStoreProtocol(
          ad_utility::testing::makeRequest(boost::beast::http::verb::connect,
                                           "/?default")),
      testing::HasSubstr("Unsupported HTTP method \"CONNECT\""));
}

// _____________________________________________________________________________________________
TEST(GraphStoreProtocolTest, extractMediatype) {
  using enum http::field;
  auto makeRequest =
      [](const ad_utility::HashMap<http::field, std::string>& headers) {
        return ad_utility::testing::makeRequest(http::verb::get, "/", headers);
      };
  AD_EXPECT_THROW_WITH_MESSAGE(
      GraphStoreProtocol::extractMediatype(makeRequest({})),
      testing::HasSubstr("Mediatype empty or not set."));
  AD_EXPECT_THROW_WITH_MESSAGE(
      GraphStoreProtocol::extractMediatype(makeRequest({{content_type, ""}})),
      testing::HasSubstr("Mediatype empty or not set."));
  EXPECT_THAT(GraphStoreProtocol::extractMediatype(
                  makeRequest({{content_type, "text/csv"}})),
              testing::Eq(ad_utility::MediaType::csv));
  AD_EXPECT_THROW_WITH_MESSAGE(
      GraphStoreProtocol::extractMediatype(
          makeRequest({{content_type, "text/plain"}})),
      testing::HasSubstr("Mediatype \"text/plain\" is not supported for SPARQL "
                         "Graph Store HTTP Protocol in QLever."));
  EXPECT_THAT(GraphStoreProtocol::extractMediatype(
                  makeRequest({{content_type, "application/n-triples"}})),
              testing::Eq(ad_utility::MediaType::ntriples));
}

// _____________________________________________________________________________________________
TEST(GraphStoreProtocolTest, parseTriples) {
  AD_EXPECT_THROW_WITH_MESSAGE(
      GraphStoreProtocol::parseTriples("<a> <b> <c>",
                                       ad_utility::MediaType::json),
      testing::HasSubstr(
          "Mediatype \"application/json\" is not supported for SPARQL "
          "Graph Store HTTP Protocol in QLever."));
  const auto expectedTriples =
      std::vector<TurtleTriple>{{{iri("<a>")}, {iri("<b>")}, {iri("<c>")}}};
  EXPECT_THAT(GraphStoreProtocol::parseTriples("<a> <b> <c> .",
                                               ad_utility::MediaType::ntriples),
              testing::Eq(expectedTriples));
  EXPECT_THAT(GraphStoreProtocol::parseTriples("<a> <b> <c> .",
                                               ad_utility::MediaType::turtle),
              testing::Eq(expectedTriples));
  EXPECT_THAT(
      GraphStoreProtocol::parseTriples("", ad_utility::MediaType::ntriples),
      testing::Eq(std::vector<TurtleTriple>{}));
  AD_EXPECT_THROW_WITH_MESSAGE(
      GraphStoreProtocol::parseTriples("<a> <b>",
                                       ad_utility::MediaType::ntriples),
      testing::HasSubstr(" Parse error at byte position 7"));
}

// _____________________________________________________________________________________________
TEST(GraphStoreProtocolTest, convertTriples) {
  auto expectConvert =
      [](const GraphOrDefault& graph, std::vector<TurtleTriple> triples,
         const std::vector<SparqlTripleSimpleWithGraph>& expectedTriples,
         ad_utility::source_location l =
             ad_utility::source_location::current()) {
        auto trace = generateLocationTrace(l);
        EXPECT_THAT(
            GraphStoreProtocol::convertTriples(graph, std::move(triples)),
            testing::Eq(expectedTriples));
      };
  expectConvert(DEFAULT{}, {}, {});
  expectConvert(iri("<a>"), {}, {});
  expectConvert(DEFAULT{}, {{{iri("<a>")}, {iri("<b>")}, {iri("<c>")}}},
                {SparqlTripleSimpleWithGraph{iri("<a>"), iri("<b>"), iri("<c>"),
                                             std::monostate{}}});
  expectConvert(iri("<a>"), {}, {});
}
