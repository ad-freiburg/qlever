// Copyright 2024, University of Freiburg
// Chair of Algorithms and Data Structures
// Authors: Julian Mundhahs <mundhahj@tf.uni-freiburg.de>

#include <gmock/gmock.h>

#include "./parser/SparqlAntlrParserTestHelpers.h"
#include "./util/GTestHelpers.h"
#include "./util/HttpRequestHelpers.h"
#include "./util/IndexTestHelpers.h"
#include "./util/TripleComponentTestHelpers.h"
#include "engine/GraphStoreProtocol.h"
#include "parser/SparqlParserHelpers.h"

namespace m = matchers;
using namespace ad_utility::testing;
using namespace ad_utility::url_parser::sparqlOperation;

using Var = Variable;
using TC = TripleComponent;

auto lit = ad_utility::testing::tripleComponentLiteral;

const EncodedIriManager* encodedIriManager() {
  static EncodedIriManager encodedIriManager_;
  return &encodedIriManager_;
}

// _____________________________________________________________________________________________
TEST(GraphStoreProtocolTest, transformPost) {
  auto index = ad_utility::testing::makeTestIndex("GraphStoreProtocolTest",
                                                  TestIndexConfig{});
  auto expectTransformPost = CPP_template_lambda(&index)(typename RequestT)(
      const RequestT& request, const GraphOrDefault& graph,
      const testing::Matcher<const ParsedQuery&>& matcher,
      ad_utility::source_location l = ad_utility::source_location::current())(
      requires ad_utility::httpUtils::HttpRequest<RequestT>) {
    auto trace = generateLocationTrace(l);
    EXPECT_THAT(GraphStoreProtocol::transformPost(request, graph, index),
                matcher);
  };

  expectTransformPost(
      makePostRequest("/?default", "text/turtle", "<a> <b> <c> ."), DEFAULT{},
      m::UpdateClause(m::GraphUpdate({}, {{iri("<a>"), iri("<b>"), iri("<c>"),
                                           std::monostate{}}}),
                      m::GraphPattern()));
  expectTransformPost(
      makePostRequest("/?default", "application/n-triples", "<a> <b> <c> ."),
      DEFAULT{},
      m::UpdateClause(m::GraphUpdate({}, {{iri("<a>"), iri("<b>"), iri("<c>"),
                                           std::monostate{}}}),
                      m::GraphPattern()));
  expectTransformPost(
      makePostRequest("/?graph=bar", "application/n-triples", "<a> <b> <c> ."),
      iri("<bar>"),
      m::UpdateClause(m::GraphUpdate({}, {{iri("<a>"), iri("<b>"), iri("<c>"),
                                           iri("<bar>")}}),
                      m::GraphPattern()));
  AD_EXPECT_THROW_WITH_MESSAGE(
      GraphStoreProtocol::transformPost(
          ad_utility::testing::makePostRequest(
              "/?default", "application/sparql-results+xml", "f"),
          DEFAULT{}, index),
      testing::HasSubstr(
          "Mediatype \"application/sparql-results+xml\" is not supported for "
          "SPARQL Graph Store HTTP Protocol in QLever."));
  AD_EXPECT_THROW_WITH_MESSAGE(
      GraphStoreProtocol::transformPost(
          ad_utility::testing::makePostRequest(
              "/?default", "application/sparql-results+xml", ""),
          DEFAULT{}, index),
      testing::HasSubstr("Request body is empty."));
  AD_EXPECT_THROW_WITH_MESSAGE(
      GraphStoreProtocol::transformPost(
          ad_utility::testing::makePostRequest(
              "/?default", "application/n-quads", "<a> <b> <c> <d> ."),
          DEFAULT{}, index),
      testing::HasSubstr("Not a single media type known to this parser was "
                         "detected in \"application/n-quads\"."));
  AD_EXPECT_THROW_WITH_MESSAGE(
      GraphStoreProtocol::transformPost(
          ad_utility::testing::makePostRequest(
              "/?default", "application/unknown", "fantasy"),
          DEFAULT{}, index),
      testing::HasSubstr("Not a single media type known to this parser was "
                         "detected in \"application/unknown\"."));
}

// _____________________________________________________________________________________________
TEST(GraphStoreProtocolTest, transformGet) {
  auto expectTransformGet =
      [](const GraphOrDefault& graph,
         const testing::Matcher<const ParsedQuery&>& matcher,
         ad_utility::source_location l =
             ad_utility::source_location::current()) {
        auto trace = generateLocationTrace(l);
        EXPECT_THAT(
            GraphStoreProtocol::transformGet(graph, encodedIriManager()),
            matcher);
      };
  expectTransformGet(
      DEFAULT{},
      m::ConstructQuery({{Var{"?s"}, Var{"?p"}, Var{"?o"}}},
                        m::GraphPattern(matchers::Triples({SparqlTriple(
                            TC(Var{"?s"}), Var{"?p"}, TC(Var{"?o"}))}))));
  expectTransformGet(
      iri("<foo>"),
      m::ConstructQuery(
          {{Var{"?s"}, Var{"?p"}, Var{"?o"}}},
          m::GraphPattern(m::GroupGraphPatternWithGraph(
              iri("<foo>"), m::Triples({SparqlTriple(TC(Var{"?s"}), Var{"?p"},
                                                     TC(Var{"?o"}))})))));
}

// _____________________________________________________________________________________________
TEST(GraphStoreProtocolTest, transformGraphStoreProtocol) {
  auto index = ad_utility::testing::makeTestIndex("GraphStoreProtocolTest",
                                                  TestIndexConfig{});
  EXPECT_THAT(GraphStoreProtocol::transformGraphStoreProtocol(
                  GraphStoreOperation{DEFAULT{}},
                  ad_utility::testing::makeGetRequest("/?default"), index),
              testing::ElementsAre(m::ConstructQuery(
                  {{Var{"?s"}, Var{"?p"}, Var{"?o"}}},
                  m::GraphPattern(matchers::Triples({SparqlTriple(
                      TC(Var{"?s"}), Var{"?p"}, TC(Var{"?o"}))})))));
  EXPECT_THAT(
      GraphStoreProtocol::transformGraphStoreProtocol(
          GraphStoreOperation{DEFAULT{}},
          ad_utility::testing::makePostRequest(
              "/?default", "application/n-triples", "<foo> <bar> <baz> ."),
          index),
      testing::ElementsAre(m::UpdateClause(
          m::GraphUpdate({}, {{iri("<foo>"), iri("<bar>"), iri("<baz>"),
                               std::monostate{}}}),
          m::GraphPattern())));
  auto expectUnsupportedMethod =
      [&index](const http::verb method,
               ad_utility::source_location l =
                   ad_utility::source_location::current()) {
        auto trace = generateLocationTrace(l);
        AD_EXPECT_THROW_WITH_MESSAGE(
            GraphStoreProtocol::transformGraphStoreProtocol(
                GraphStoreOperation{DEFAULT{}},
                ad_utility::testing::makeRequest(method, "/?default"), index),
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
          GraphStoreOperation{DEFAULT{}},
          ad_utility::testing::makeRequest(boost::beast::http::verb::connect,
                                           "/?default"),
          index),
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
// If the `TripleComponent` is a `ValueId` which is a `BlankNodeIndex` then
// `sub` must match on it.
MATCHER_P(IfBlankNode, sub, "") {
  if (arg.isId()) {
    auto id = arg.getId();
    if (id.getDatatype() == Datatype::BlankNodeIndex) {
      return testing::ExplainMatchResult(sub, id.getBlankNodeIndex(),
                                         result_listener);
    }
  }
  return true;
}

// _____________________________________________________________________________________________
TEST(GraphStoreProtocolTest, convertTriples) {
  auto index = ad_utility::testing::makeTestIndex("GraphStoreProtocolTest",
                                                  TestIndexConfig{});
  Quads::BlankNodeAdder bn{{}, {}, index.getBlankNodeManager()};
  auto expectConvert =
      [&bn](const GraphOrDefault& graph, std::vector<TurtleTriple>&& triples,
            const std::vector<SparqlTripleSimpleWithGraph>& expectedTriples,
            ad_utility::source_location l =
                ad_utility::source_location::current()) {
        auto trace = generateLocationTrace(l);
        auto convertedTriples =
            GraphStoreProtocol::convertTriples(graph, std::move(triples), bn);
        EXPECT_THAT(convertedTriples,
                    AD_FIELD(updateClause::UpdateTriples, triples_,
                             testing::Eq(expectedTriples)));
        auto AllComponents =
            [](const testing::Matcher<const TripleComponent&>& sub)
            -> testing::Matcher<const SparqlTripleSimpleWithGraph&> {
          return testing::AllOf(AD_FIELD(SparqlTripleSimpleWithGraph, s_, sub),
                                AD_FIELD(SparqlTripleSimpleWithGraph, p_, sub),
                                AD_FIELD(SparqlTripleSimpleWithGraph, o_, sub));
        };
        auto BlankNodeContained = [](const LocalVocab& lv)
            -> testing::Matcher<const BlankNodeIndex&> {
          return testing::ResultOf(
              [&lv](const BlankNodeIndex& i) {
                return lv.isBlankNodeIndexContained(i);
              },
              testing::IsTrue());
        };
        EXPECT_THAT(
            convertedTriples,
            AD_FIELD(updateClause::UpdateTriples, triples_,
                     testing::Each(AllComponents(IfBlankNode(
                         BlankNodeContained(convertedTriples.localVocab_))))));
      };
  expectConvert(DEFAULT{}, {}, {});
  expectConvert(iri("<a>"), {}, {});
  expectConvert(DEFAULT{}, {{{iri("<a>")}, {iri("<b>")}, {iri("<c>")}}},
                {SparqlTripleSimpleWithGraph{iri("<a>"), iri("<b>"), iri("<c>"),
                                             std::monostate{}}});
  expectConvert(iri("<a>"), {}, {});
  expectConvert(
      iri("<a>"), {{{iri("<a>")}, {iri("<b>")}, TC("_:a")}},
      {SparqlTripleSimpleWithGraph{iri("<a>"), iri("<b>"),
                                   bn.getBlankNodeIndex("_:a"), iri("<a>")}});

  expectConvert(
      iri("<a>"),
      {{TC("_:b"), {iri("<b>")}, iri("<c>")},
       {TC("_:b"), {iri("<d>")}, iri("<e>")},
       {TC("_:c"), {iri("<f>")}, iri("<g>")}},
      {SparqlTripleSimpleWithGraph{bn.getBlankNodeIndex("_:b"), iri("<b>"),
                                   iri("<c>"), iri("<a>")},
       SparqlTripleSimpleWithGraph{bn.getBlankNodeIndex("_:b"), iri("<d>"),
                                   iri("<e>"), iri("<a>")},
       SparqlTripleSimpleWithGraph{bn.getBlankNodeIndex("_:c"), iri("<f>"),
                                   iri("<g>"), iri("<a>")}});
}

// _____________________________________________________________________________________________
TEST(GraphStoreProtocolTest, EncodedIriManagerUsage) {
  // Create a simple index with default config for now
  auto index = ad_utility::testing::makeTestIndex("GraphStoreProtocolTest",
                                                  TestIndexConfig{});

  // Test transformPost with IRIs that would be encoded if the feature were
  // enabled
  auto expectTransformPost = CPP_template_lambda(&index)(typename RequestT)(
      const RequestT& request, const GraphOrDefault& graph,
      const testing::Matcher<const ParsedQuery&>& matcher,
      ad_utility::source_location l = ad_utility::source_location::current())(
      requires ad_utility::httpUtils::HttpRequest<RequestT>) {
    auto trace = generateLocationTrace(l);
    EXPECT_THAT(GraphStoreProtocol::transformPost(request, graph, index),
                matcher);
  };

  // Test with encodable IRIs - they should remain as IRIs since the index
  // doesn't have EncodedIriManager configured
  expectTransformPost(
      makePostRequest(
          "/?default", "text/turtle",
          "<http://example.org/123> <http://test.com/id/456> \"value\" ."),
      DEFAULT{},
      m::UpdateClause(
          m::GraphUpdate({}, {{iri("<http://example.org/123>"),
                               iri("<http://test.com/id/456>"),
                               lit("\"value\""), std::monostate{}}}),
          m::GraphPattern()));

  // Test with non-encodable IRIs
  expectTransformPost(
      makePostRequest(
          "/?default", "text/turtle",
          "<http://other.org/123> <http://different.com/456> \"value\" ."),
      DEFAULT{},
      m::UpdateClause(
          m::GraphUpdate({}, {{iri("<http://other.org/123>"),
                               iri("<http://different.com/456>"),
                               lit("\"value\""), std::monostate{}}}),
          m::GraphPattern()));

  // Test with multiple triples
  expectTransformPost(
      makePostRequest(
          "/?default", "text/turtle",
          "<http://example.org/111> <http://test.com/id/222> \"value1\" .\n"
          "<http://example.org/333> <http://test.com/id/444> \"value2\" ."),
      DEFAULT{},
      m::UpdateClause(
          m::GraphUpdate({}, {{iri("<http://example.org/111>"),
                               iri("<http://test.com/id/222>"),
                               lit("\"value1\""), std::monostate{}},
                              {iri("<http://example.org/333>"),
                               iri("<http://test.com/id/444>"),
                               lit("\"value2\""), std::monostate{}}}),
          m::GraphPattern()));

  // Test transformGet functionality
  auto getQuery =
      GraphStoreProtocol::transformGet(DEFAULT{}, encodedIriManager());
  EXPECT_EQ(getQuery._originalString,
            "CONSTRUCT { ?s ?p ?o } WHERE { ?s ?p ?o }");
  EXPECT_TRUE(
      std::holds_alternative<parsedQuery::ConstructClause>(getQuery._clause));

  // Test transformGet with specific graph IRI
  auto graphIri =
      ad_utility::triple_component::Iri::fromIriref("<http://example.org/123>");
  auto graphQuery =
      GraphStoreProtocol::transformGet(graphIri, encodedIriManager());
  EXPECT_THAT(
      graphQuery._originalString,
      testing::HasSubstr("GRAPH <http://example.org/123> { ?s ?p ?o }"));
}
