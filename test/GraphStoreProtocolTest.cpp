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
namespace t = ad_utility::testing;

using Var = Variable;
using TC = TripleComponent;

TEST(GraphStoreProtocolTest, extractTargetGraph) {
  // Equivalent to `/?default`
  EXPECT_THAT(GraphStoreProtocol::extractTargetGraph({{"default", {""}}}),
              DEFAULT{});
  // Equivalent to `/?graph=foo`
  EXPECT_THAT(GraphStoreProtocol::extractTargetGraph({{"graph", {"foo"}}}),
              t::iri("<foo>"));
  // Equivalent to `/` or `/?`
  AD_EXPECT_THROW_WITH_MESSAGE(
      GraphStoreProtocol::extractTargetGraph({}),
      testing::HasSubstr("No graph IRI specified in the request."));
  // Equivalent to `/?unrelated=a&unrelated=b`
  AD_EXPECT_THROW_WITH_MESSAGE(
      GraphStoreProtocol::extractTargetGraph({{"unrelated", {"a", "b"}}}),
      testing::HasSubstr("No graph IRI specified in the request."));
  // Equivalent to `/?default&graph=foo`
  AD_EXPECT_THROW_WITH_MESSAGE(
      GraphStoreProtocol::extractTargetGraph(
          {{"default", {""}}, {"graph", {"foo"}}}),
      testing::HasSubstr("Only one of `default` and `graph` may be used for "
                         "graph identification."));
}

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
      ad_utility::testing::MakePostRequest("/?default", "text/turtle",
                                           "<a> <b> <c> ."),
      m::UpdateClause(m::GraphUpdate({},
                                     {{t::iri("<a>"), t::iri("<b>"),
                                       t::iri("<c>"), std::monostate{}}},
                                     std::nullopt),
                      m::GraphPattern()));
  expectTransformPost(
      ad_utility::testing::MakePostRequest("/?default", "application/n-triples",
                                           "<a> <b> <c> ."),
      m::UpdateClause(m::GraphUpdate({},
                                     {{t::iri("<a>"), t::iri("<b>"),
                                       t::iri("<c>"), std::monostate{}}},
                                     std::nullopt),
                      m::GraphPattern()));
  expectTransformPost(
      ad_utility::testing::MakePostRequest(
          "/?graph=bar", "application/n-triples", "<a> <b> <c> ."),
      m::UpdateClause(
          m::GraphUpdate(
              {}, {{t::iri("<a>"), t::iri("<b>"), t::iri("<c>"), Iri("<bar>")}},
              std::nullopt),
          m::GraphPattern()));
  AD_EXPECT_THROW_WITH_MESSAGE(
      GraphStoreProtocol::transformPost(
          ad_utility::testing::MakeRequest(boost::beast::http::verb::post,
                                           "/?default", {}, "<a> <b> <c>"),
          DEFAULT{}),
      testing::HasSubstr(
          "Mediatype \"application/sparql-results+json\" is not supported for "
          "SPARQL Graph Store HTTP Protocol in QLever."));
  AD_EXPECT_THROW_WITH_MESSAGE(
      GraphStoreProtocol::transformPost(
          ad_utility::testing::MakePostRequest(
              "/?default", "application/sparql-results+xml", ""),
          DEFAULT{}),
      testing::HasSubstr(
          "Mediatype \"application/sparql-results+xml\" is not supported for "
          "SPARQL Graph Store HTTP Protocol in QLever."));
  AD_EXPECT_THROW_WITH_MESSAGE(
      GraphStoreProtocol::transformPost(
          ad_utility::testing::MakePostRequest(
              "/?default", "application/n-quads", "<a> <b> <c> <d> ."),
          DEFAULT{}),
      testing::HasSubstr("Not a single media type known to this parser was "
                         "detected in \"application/n-quads\"."));
  AD_EXPECT_THROW_WITH_MESSAGE(
      GraphStoreProtocol::transformPost(
          ad_utility::testing::MakePostRequest(
              "/?default", "application/unknown", "fantasy"),
          DEFAULT{}),
      testing::HasSubstr("Not a single media type known to this parser was "
                         "detected in \"application/unknown\"."));
}

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
      ad_utility::testing::MakeGetRequest("/?default"),
      m::ConstructQuery({{Var{"?s"}, Var{"?p"}, Var{"?o"}}},
                        m::GraphPattern(matchers::Triples({SparqlTriple(
                            TC(Var{"?s"}), "?p", TC(Var{"?o"}))}))));
  expectTransformGet(
      ad_utility::testing::MakeGetRequest("/?graph=foo"),
      m::ConstructQuery({{Var{"?s"}, Var{"?p"}, Var{"?o"}}},
                        m::GraphPattern(m::GroupGraphPatternWithGraph(
                            {}, t::iri("<foo>"),
                            matchers::Triples({SparqlTriple(
                                TC(Var{"?s"}), "?p", TC(Var{"?o"}))})))));
}

TEST(GraphStoreProtocolTest, transformGraphStoreProtocol) {
  EXPECT_THAT(GraphStoreProtocol::transformGraphStoreProtocol(
                  ad_utility::testing::MakeGetRequest("/?default")),
              m::ConstructQuery({{Var{"?s"}, Var{"?p"}, Var{"?o"}}},
                                m::GraphPattern(matchers::Triples({SparqlTriple(
                                    TC(Var{"?s"}), "?p", TC(Var{"?o"}))}))));
  EXPECT_THAT(
      GraphStoreProtocol::transformGraphStoreProtocol(
          ad_utility::testing::MakePostRequest(
              "/?default", "application/n-triples", "<foo> <bar> <baz> .")),
      m::UpdateClause(m::GraphUpdate({},
                                     {{t::iri("<foo>"), t::iri("<bar>"),
                                       t::iri("<baz>"), std::monostate{}}},
                                     std::nullopt),
                      m::GraphPattern()));
  AD_EXPECT_THROW_WITH_MESSAGE(
      GraphStoreProtocol::transformGraphStoreProtocol(
          ad_utility::testing::MakeRequest(boost::beast::http::verb::put,
                                           "/?default")),
      testing::HasSubstr("PUT in the SPARQL Graph Store HTTP Protocol"));
  AD_EXPECT_THROW_WITH_MESSAGE(
      GraphStoreProtocol::transformGraphStoreProtocol(
          ad_utility::testing::MakeRequest(boost::beast::http::verb::delete_,
                                           "/?default")),
      testing::HasSubstr("DELETE in the SPARQL Graph Store HTTP Protocol"));
  AD_EXPECT_THROW_WITH_MESSAGE(
      GraphStoreProtocol::transformGraphStoreProtocol(
          ad_utility::testing::MakeRequest(boost::beast::http::verb::head,
                                           "/?default")),
      testing::HasSubstr("HEAD in the SPARQL Graph Store HTTP Protocol"));
  AD_EXPECT_THROW_WITH_MESSAGE(
      GraphStoreProtocol::transformGraphStoreProtocol(
          ad_utility::testing::MakeRequest(boost::beast::http::verb::patch,
                                           "/?default")),
      testing::HasSubstr("PATCH in the SPARQL Graph Store HTTP Protocol"));
  AD_EXPECT_THROW_WITH_MESSAGE(
      GraphStoreProtocol::transformGraphStoreProtocol(
          ad_utility::testing::MakeRequest(boost::beast::http::verb::connect,
                                           "/?default")),
      testing::HasSubstr("Unsupported HTTP method \"CONNECT\""));
}
