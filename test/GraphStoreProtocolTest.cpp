// Copyright 2024, University of Freiburg
// Chair of Algorithms and Data Structures
// Authors: Julian Mundhahs <mundhahj@tf.uni-freiburg.de>

#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <parser/SparqlParserHelpers.h>

#include "./util/GTestHelpers.h"
#include "./util/HttpRequestHelpers.h"
#include "SparqlAntlrParserTestHelpers.h"
#include "engine/GraphStoreProtocol.h"

namespace m = matchers;

TEST(GraphStoreProtocolTest, extractTargetGraph) {
  EXPECT_THAT(GraphStoreProtocol::extractTargetGraph({{"default", {""}}}),
              DEFAULT{});
  EXPECT_THAT(GraphStoreProtocol::extractTargetGraph({{"graph", {"foo"}}}),
              TripleComponent::Iri::fromIriref("<foo>"));
  AD_EXPECT_THROW_WITH_MESSAGE(
      GraphStoreProtocol::extractTargetGraph({}),
      testing::HasSubstr("No graph IRI specified in the request."));
  AD_EXPECT_THROW_WITH_MESSAGE(
      GraphStoreProtocol::extractTargetGraph({{"unrelated", {"a", "b"}}}),
      testing::HasSubstr("No graph IRI specified in the request."));
  AD_EXPECT_THROW_WITH_MESSAGE(
      GraphStoreProtocol::extractTargetGraph(
          {{"default", {""}}, {"graph", {"foo"}}}),
      testing::HasSubstr("Only one of `default` and `graph` may be used for "
                         "graph identification."));
}

TEST(GraphStoreProtocolTest, transformPost) {
  auto Iri = [](std::string_view stringWithBrackets) {
    return TripleComponent::Iri::fromIriref(stringWithBrackets);
  };
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
      m::UpdateClause(
          m::GraphUpdate(
              {}, {{Iri("<a>"), Iri("<b>"), Iri("<c>"), std::monostate{}}},
              std::nullopt),
          m::GraphPattern()));
  expectTransformPost(
      ad_utility::testing::MakePostRequest("/?default", "application/n-triples",
                                           "<a> <b> <c> ."),
      m::UpdateClause(
          m::GraphUpdate(
              {}, {{Iri("<a>"), Iri("<b>"), Iri("<c>"), std::monostate{}}},
              std::nullopt),
          m::GraphPattern()));
  expectTransformPost(
      ad_utility::testing::MakePostRequest(
          "/?graph=bar", "application/n-triples", "<a> <b> <c> ."),
      m::UpdateClause(
          m::GraphUpdate({},
                         {{Iri("<a>"), Iri("<b>"), Iri("<c>"), ::Iri("<bar>")}},
                         std::nullopt),
          m::GraphPattern()));
  expectTransformPost(
      ad_utility::testing::MakePostRequest("/?default", "application/n-quads",
                                           "<a> <b> <c> <d> ."),
      m::UpdateClause(
          m::GraphUpdate({},
                         {{Iri("<a>"), Iri("<b>"), Iri("<c>"), ::Iri("<d>")}},
                         std::nullopt),
          m::GraphPattern()));
  expectTransformPost(
      ad_utility::testing::MakePostRequest("/?graph=baz", "application/n-quads",
                                           "<a> <b> <c> <d> ."),
      m::UpdateClause(
          m::GraphUpdate({},
                         {{Iri("<a>"), Iri("<b>"), Iri("<c>"), ::Iri("<baz>")}},
                         std::nullopt),
          m::GraphPattern()));
  AD_EXPECT_THROW_WITH_MESSAGE(
      GraphStoreProtocol::transformPost(
          ad_utility::testing::MakePostRequest(
              "/?default", "application/unknown", "fantasy"),
          DEFAULT{}),
      testing::HasSubstr("Not a single media type known to this parser was "
                         "detected in \"application/unknown\"."));
}

TEST(GraphStoreProtocolTest, transformGet) {
  using Var = Variable;
  using TC = TripleComponent;
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
                            {}, TC::Iri::fromIriref("<foo>"),
                            matchers::Triples({SparqlTriple(
                                TC(Var{"?s"}), "?p", TC(Var{"?o"}))})))));
}

TEST(GraphStoreProtocolTest, transformGraphStoreProtocol) {
  // TODO: re-add some simple tests for POST and GET
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
}
