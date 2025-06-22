// Copyright 2024, University of Freiburg
// Chair of Algorithms and Data Structures
// Authors: Julian Mundhahs <mundhahj@tf.uni-freiburg.de>

#include <gmock/gmock.h>

#include "./util/GTestHelpers.h"
#include "./util/HttpRequestHelpers.h"
#include "./util/TripleComponentTestHelpers.h"
#include "SparqlAntlrParserTestHelpers.h"
#include "engine/GraphStoreProtocol.h"
#include "parser/SparqlParserHelpers.h"

namespace m = matchers;
using namespace ad_utility::testing;
using namespace ad_utility::url_parser::sparqlOperation;

using Var = Variable;
using TC = TripleComponent;

namespace {
// A matcher that matches a ParsedQuery that is an updated that deletes all
// triples from the given `graph`.
auto ClearGraph = [](SparqlTripleSimpleWithGraph::Graph graph)
    -> testing::Matcher<const ParsedQuery&> {
  return m::UpdateClause(
      m::GraphUpdate({{Var("?s"), Var("?p"), Var("?o"), graph}}, {}),
      m::GraphPattern(m::GroupGraphPatternWithGraph(
          graph, m::Triples({SparqlTriple(TC(Var{"?s"}), Var{"?p"},
                                          TC(Var{"?o"}))}))));
};
}  // namespace

// _____________________________________________________________________________________________
TEST(GraphStoreProtocolTest, transformPostAndTsop) {
  auto runTests = [](auto transform, bool isInsertion) {
    std::vector<SparqlTripleSimpleWithGraph> defaultGraph{
        {iri("<a>"), iri("<b>"), iri("<c>"), std::monostate{}}};
    std::vector<SparqlTripleSimpleWithGraph> graph{
        {iri("<a>"), iri("<b>"), iri("<c>"), iri("<bar>")}};
    std::vector<SparqlTripleSimpleWithGraph> empty{};
    EXPECT_THAT(
        transform(makePostRequest("/?default", "text/turtle", "<a> <b> <c> ."),
                  DEFAULT{}),
        m::UpdateClause(m::GraphUpdate(isInsertion ? empty : defaultGraph,
                                       isInsertion ? defaultGraph : empty),
                        m::GraphPattern()));
    EXPECT_THAT(
        transform(makePostRequest("/?default", "application/n-triples",
                                  "<a> <b> <c> ."),
                  DEFAULT{}),
        m::UpdateClause(m::GraphUpdate(isInsertion ? empty : defaultGraph,
                                       isInsertion ? defaultGraph : empty),
                        m::GraphPattern()));
    EXPECT_THAT(
        transform(makePostRequest("/?graph=bar", "application/n-triples",
                                  "<a> <b> <c> ."),
                  iri("<bar>")),
        m::UpdateClause(m::GraphUpdate(isInsertion ? empty : graph,
                                       isInsertion ? graph : empty),
                        m::GraphPattern()));
    AD_EXPECT_THROW_WITH_MESSAGE(
        transform(
            ad_utility::testing::makePostRequest(
                "/?default", "application/sparql-results+xml", "<foo></foo>"),
            DEFAULT{}),
        testing::HasSubstr(
            "Mediatype \"application/sparql-results+xml\" is not supported for "
            "SPARQL Graph Store HTTP Protocol in QLever."));

    AD_EXPECT_THROW_WITH_MESSAGE(transform(ad_utility::testing::makePostRequest(
                                               "/?default", "text/turtle", ""),
                                           DEFAULT{}),
                                 testing::HasSubstr("Request body is empty."));
    AD_EXPECT_THROW_WITH_MESSAGE(
        transform(ad_utility::testing::makePostRequest(
                      "/?default", "application/n-quads", "<a> <b> <c> <d> ."),
                  DEFAULT{}),
        testing::HasSubstr("Not a single media type known to this parser was "
                           "detected in \"application/n-quads\"."));
    AD_EXPECT_THROW_WITH_MESSAGE(
        transform(ad_utility::testing::makePostRequest(
                      "/?default", "application/unknown", "fantasy"),
                  DEFAULT{}),
        testing::HasSubstr("Not a single media type known to this parser was "
                           "detected in \"application/unknown\"."));
  };

  runTests(
      [](http::request<http::string_body> request, GraphOrDefault graph) {
        return GraphStoreProtocol::transformPost(request, graph);
      },
      true);
  runTests(
      [](http::request<http::string_body> request, GraphOrDefault graph) {
        return GraphStoreProtocol::transformTsop(request, graph);
      },
      false);
}

// _____________________________________________________________________________________________
TEST(GraphStoreProtocolTest, transformGet) {
  auto expectTransformGet =
      [](const GraphOrDefault& graph,
         const testing::Matcher<const ParsedQuery&>& matcher,
         ad_utility::source_location l =
             ad_utility::source_location::current()) {
        auto trace = generateLocationTrace(l);
        EXPECT_THAT(GraphStoreProtocol::transformGet(graph), matcher);
      };
  // expectTransformGet(
  //     DEFAULT{},
  //     m::ConstructQuery({{Var{"?s"}, Var{"?p"}, Var{"?o"}}},
  //                       m::GraphPattern(matchers::Triples({SparqlTriple(
  //                           TC(Var{"?s"}), Var{"?p"}, TC(Var{"?o"}))}))));
  expectTransformGet(
      iri("<foo>"),
      m::ConstructQuery(
          {{Var{"?s"}, Var{"?p"}, Var{"?o"}}},
          m::GraphPattern(m::GroupGraphPatternWithGraph(
              iri("<foo>"), m::Triples({SparqlTriple(TC(Var{"?s"}), Var{"?p"},
                                                     TC(Var{"?o"}))})))));
}

// _____________________________________________________________________________________________
TEST(GraphStoreProtocolTest, transformPut) {
  auto expectTransformPut = CPP_template_lambda()(typename RequestT)(
      const RequestT& request, const GraphOrDefault& graph,
      const testing::Matcher<std::vector<ParsedQuery>>& matcher,
      ad_utility::source_location l = ad_utility::source_location::current())(
      requires ad_utility::httpUtils::HttpRequest<RequestT>) {
    auto trace = generateLocationTrace(l);
    EXPECT_THAT(GraphStoreProtocol::transformPut(request, graph), matcher);
  };

  expectTransformPut(
      makePostRequest("/?default", "text/turtle", "<a> <b> <c> ."), DEFAULT{},
      testing::ElementsAre(
          ClearGraph(iri(DEFAULT_GRAPH_IRI)),
          m::UpdateClause(m::GraphUpdate({}, {{iri("<a>"), iri("<b>"),
                                               iri("<c>"), std::monostate{}}}),
                          m::GraphPattern())));
  expectTransformPut(
      makePostRequest("/?default", "application/n-triples", "<a> <b> <c> ."),
      DEFAULT{},
      testing::ElementsAre(
          ClearGraph(iri(DEFAULT_GRAPH_IRI)),
          m::UpdateClause(m::GraphUpdate({}, {{iri("<a>"), iri("<b>"),
                                               iri("<c>"), std::monostate{}}}),
                          m::GraphPattern())));
  expectTransformPut(
      makePostRequest("/?graph=bar", "application/n-triples", "<a> <b> <c> ."),
      iri("<bar>"),
      testing::ElementsAre(
          ClearGraph(iri("<bar>")),
          m::UpdateClause(m::GraphUpdate({}, {{iri("<a>"), iri("<b>"),
                                               iri("<c>"), iri("<bar>")}}),
                          m::GraphPattern())));
  AD_EXPECT_THROW_WITH_MESSAGE(
      GraphStoreProtocol::transformPut(
          ad_utility::testing::makeRequest(http::verb::put, "/?default"),
          DEFAULT{}),
      testing::HasSubstr("Mediatype empty or not set."));
  AD_EXPECT_THROW_WITH_MESSAGE(
      GraphStoreProtocol::transformPut(
          ad_utility::testing::makePostRequest(
              "/?default", "application/sparql-results+xml", ""),
          DEFAULT{}),
      testing::HasSubstr(
          "Mediatype \"application/sparql-results+xml\" is not supported for "
          "SPARQL Graph Store HTTP Protocol in QLever."));
  AD_EXPECT_THROW_WITH_MESSAGE(
      GraphStoreProtocol::transformPut(
          ad_utility::testing::makePostRequest(
              "/?default", "application/n-quads", "<a> <b> <c> <d> ."),
          DEFAULT{}),
      testing::HasSubstr("Not a single media type known to this parser was "
                         "detected in \"application/n-quads\"."));
  AD_EXPECT_THROW_WITH_MESSAGE(
      GraphStoreProtocol::transformPut(
          ad_utility::testing::makePostRequest(
              "/?default", "application/unknown", "fantasy"),
          DEFAULT{}),
      testing::HasSubstr("Not a single media type known to this parser was "
                         "detected in \"application/unknown\"."));
}

// _____________________________________________________________________________________________
TEST(GraphStoreProtocolTest, transformDelete) {
  auto expectTransformDelete =
      [](const GraphOrDefault& graph,
         const testing::Matcher<const ParsedQuery&>& matcher,
         ad_utility::source_location l =
             ad_utility::source_location::current()) {
        auto trace = generateLocationTrace(l);
        EXPECT_THAT(GraphStoreProtocol::transformDelete(graph), matcher);
      };
  expectTransformDelete(DEFAULT{}, ClearGraph(iri(DEFAULT_GRAPH_IRI)));
  expectTransformDelete(iri("<foo>"), ClearGraph(iri("<foo>")));
}

// _____________________________________________________________________________________________
TEST(GraphStoreProtocolTest, transformGraphStoreProtocol) {
  // EXPECT_THAT(GraphStoreProtocol::transformGraphStoreProtocol(
  //                 GraphStoreOperation{DEFAULT{}},
  //                 ad_utility::testing::makeGetRequest("/?default")),
  //             testing::ElementsAre(m::ConstructQuery(
  //                 {{Var{"?s"}, Var{"?p"}, Var{"?o"}}},
  //                 m::GraphPattern(matchers::Triples({SparqlTriple(
  //                     TC(Var{"?s"}), Var{"?p"}, TC(Var{"?o"}))})))));
  EXPECT_THAT(
      GraphStoreProtocol::transformGraphStoreProtocol(
          GraphStoreOperation{DEFAULT{}},
          ad_utility::testing::makePostRequest(
              "/?default", "application/n-triples", "<foo> <bar> <baz> .")),
      testing::ElementsAre(m::UpdateClause(
          m::GraphUpdate({}, {{iri("<foo>"), iri("<bar>"), iri("<baz>"),
                               std::monostate{}}}),
          m::GraphPattern())));
  EXPECT_THAT(GraphStoreProtocol::transformGraphStoreProtocol(
                  GraphStoreOperation{DEFAULT{}},
                  ad_utility::testing::makeRequest(
                      "TSOP", "/?default",
                      {{http::field::content_type, "application/n-triples"}},
                      "<foo> <bar> <baz> .")),
              testing::ElementsAre(m::UpdateClause(
                  m::GraphUpdate({{iri("<foo>"), iri("<bar>"), iri("<baz>"),
                                   std::monostate{}}},
                                 {}),
                  m::GraphPattern())));
  EXPECT_THAT(
      GraphStoreProtocol::transformGraphStoreProtocol(
          GraphStoreOperation{iri("<foo>")},
          ad_utility::testing::makeRequest(http::verb::delete_, "/?graph=foo")),
      testing::ElementsAre(ClearGraph(iri("<foo>"))));
  EXPECT_THAT(
      GraphStoreProtocol::transformGraphStoreProtocol(
          GraphStoreOperation{iri("<foo>")},
          ad_utility::testing::makeRequest(
              http::verb::put, "/?graph=foo",
              {{http::field::content_type, "text/turtle"}}, "<a> <b> <c>")),
      testing::ElementsAre(
          ClearGraph(iri("<foo>")),
          m::UpdateClause(m::GraphUpdate({}, {{iri("<a>"), iri("<b>"),
                                               iri("<c>"), iri("<foo>")}}),
                          m::GraphPattern())));
  auto expectUnsupportedMethod =
      [](const http::verb method, ad_utility::source_location l =
                                      ad_utility::source_location::current()) {
        auto trace = generateLocationTrace(l);
        AD_EXPECT_THROW_WITH_MESSAGE(
            GraphStoreProtocol::transformGraphStoreProtocol(
                GraphStoreOperation{DEFAULT{}},
                ad_utility::testing::makeRequest(method, "/?default")),
            testing::HasSubstr(
                absl::StrCat(std::string{boost::beast::http::to_string(method)},
                             " in the SPARQL Graph Store HTTP Protocol")));
      };
  expectUnsupportedMethod(http::verb::head);
  expectUnsupportedMethod(http::verb::patch);
  AD_EXPECT_THROW_WITH_MESSAGE(
      GraphStoreProtocol::transformGraphStoreProtocol(
          GraphStoreOperation{DEFAULT{}},
          ad_utility::testing::makeRequest("PUMPKIN", "/?default")),
      testing::HasSubstr("Unsupported HTTP method \"PUMPKIN\""));
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
