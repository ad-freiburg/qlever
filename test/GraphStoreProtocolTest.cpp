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

namespace {
// A matcher that matches a ParsedQuery that is an updated that deletes all
// triples from the given `graph`.
auto ClearGraph = [](ad_utility::triple_component::Iri graph)
    -> testing::Matcher<const ParsedQuery&> {
  return m::UpdateClause(
      m::GraphUpdate({{Var("?s"), Var("?p"), Var("?o"),
                       SparqlTripleSimpleWithGraph::Graph{graph}}},
                     {}),
      m::GraphPattern(m::GroupGraphPatternWithGraph(
          graph, m::Triples({SparqlTriple(TC(Var{"?s"}), Var{"?p"},
                                          TC(Var{"?o"}))}))));
};

auto lit = ad_utility::testing::tripleComponentLiteral;

const EncodedIriManager* encodedIriManager() {
  static EncodedIriManager encodedIriManager_;
  return &encodedIriManager_;
}
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
    // This results in an HTTP status 204 which must have an empty response
    // body.
    AD_EXPECT_THROW_WITH_MESSAGE(transform(ad_utility::testing::makePostRequest(
                                               "/?default", "text/turtle", ""),
                                           DEFAULT{}),
                                 testing::StrEq(""));
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

  auto index = ad_utility::testing::makeTestIndex("GraphStoreProtocolTest",
                                                  TestIndexConfig{});
  runTests(
      [&index](http::request<http::string_body> request, GraphOrDefault graph) {
        return GraphStoreProtocol::transformPost(request, graph, index);
      },
      true);
  runTests(
      [&index](http::request<http::string_body> request, GraphOrDefault graph) {
        return GraphStoreProtocol::transformTsop(request, graph, index);
      },
      false);
}

// _____________________________________________________________________________________________
TEST(GraphStoreProtocolTest, transformGet) {
  auto expectTransformGet =
      [](const GraphOrDefault& graph,
         const testing::Matcher<const ParsedQuery&>& matcher,
         ad_utility::source_location l = AD_CURRENT_SOURCE_LOC()) {
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
TEST(GraphStoreProtocolTest, transformPut) {
  auto index = ad_utility::testing::makeTestIndex("GraphStoreProtocolTest",
                                                  TestIndexConfig{});
  auto expectTransformPut = CPP_template_lambda(&index)(typename RequestT)(
      const RequestT& request, const GraphOrDefault& graph,
      const testing::Matcher<std::vector<ParsedQuery>>& matcher,
      ad_utility::source_location l = AD_CURRENT_SOURCE_LOC())(
      requires ad_utility::httpUtils::HttpRequest<RequestT>) {
    auto trace = generateLocationTrace(l);
    EXPECT_THAT(GraphStoreProtocol::transformPut(request, graph, index),
                matcher);
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
          DEFAULT{}, index),
      testing::HasSubstr("Mediatype empty or not set."));
  AD_EXPECT_THROW_WITH_MESSAGE(
      GraphStoreProtocol::transformPut(
          ad_utility::testing::makePostRequest(
              "/?default", "application/sparql-results+xml", ""),
          DEFAULT{}, index),
      testing::HasSubstr(
          "Mediatype \"application/sparql-results+xml\" is not supported for "
          "SPARQL Graph Store HTTP Protocol in QLever."));
  AD_EXPECT_THROW_WITH_MESSAGE(
      GraphStoreProtocol::transformPut(
          ad_utility::testing::makePostRequest(
              "/?default", "application/n-quads", "<a> <b> <c> <d> ."),
          DEFAULT{}, index),
      testing::HasSubstr("Not a single media type known to this parser was "
                         "detected in \"application/n-quads\"."));
  AD_EXPECT_THROW_WITH_MESSAGE(
      GraphStoreProtocol::transformPut(
          ad_utility::testing::makePostRequest(
              "/?default", "application/unknown", "fantasy"),
          DEFAULT{}, index),
      testing::HasSubstr("Not a single media type known to this parser was "
                         "detected in \"application/unknown\"."));
}

// _____________________________________________________________________________________________
TEST(GraphStoreProtocolTest, transformDelete) {
  auto index = ad_utility::testing::makeTestIndex("GraphStoreProtocolTest",
                                                  TestIndexConfig{});
  auto expectTransformDelete =
      [&index](const GraphOrDefault& graph,
               const testing::Matcher<const ParsedQuery&>& matcher,
               ad_utility::source_location l = AD_CURRENT_SOURCE_LOC()) {
        auto trace = generateLocationTrace(l);
        EXPECT_THAT(GraphStoreProtocol::transformDelete(graph, index), matcher);
      };
  expectTransformDelete(DEFAULT{}, ClearGraph(iri(DEFAULT_GRAPH_IRI)));
  expectTransformDelete(iri("<foo>"), ClearGraph(iri("<foo>")));
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
  EXPECT_THAT(GraphStoreProtocol::transformGraphStoreProtocol(
                  GraphStoreOperation{DEFAULT{}},
                  ad_utility::testing::makeRequest(
                      "TSOP", "/?default",
                      {{http::field::content_type, "application/n-triples"}},
                      "<foo> <bar> <baz> ."),
                  index),
              testing::ElementsAre(m::UpdateClause(
                  m::GraphUpdate({{iri("<foo>"), iri("<bar>"), iri("<baz>"),
                                   std::monostate{}}},
                                 {}),
                  m::GraphPattern())));
  EXPECT_THAT(
      GraphStoreProtocol::transformGraphStoreProtocol(
          GraphStoreOperation{iri("<foo>")},
          ad_utility::testing::makeRequest(http::verb::delete_, "/?graph=foo"),
          index),
      testing::ElementsAre(ClearGraph(iri("<foo>"))));
  EXPECT_THAT(
      GraphStoreProtocol::transformGraphStoreProtocol(
          GraphStoreOperation{iri("<foo>")},
          ad_utility::testing::makeRequest(
              http::verb::put, "/?graph=foo",
              {{http::field::content_type, "text/turtle"}}, "<a> <b> <c>"),
          index),
      testing::ElementsAre(
          ClearGraph(iri("<foo>")),
          m::UpdateClause(m::GraphUpdate({}, {{iri("<a>"), iri("<b>"),
                                               iri("<c>"), iri("<foo>")}}),
                          m::GraphPattern())));
  auto expectUnsupportedMethod = [&index](const http::verb method,
                                          ad_utility::source_location l =
                                              AD_CURRENT_SOURCE_LOC()) {
    auto trace = generateLocationTrace(l);
    AD_EXPECT_THROW_WITH_MESSAGE(
        GraphStoreProtocol::transformGraphStoreProtocol(
            GraphStoreOperation{DEFAULT{}},
            ad_utility::testing::makeRequest(method, "/?default"), index),
        testing::HasSubstr(
            absl::StrCat(std::string{boost::beast::http::to_string(method)},
                         " in the SPARQL Graph Store HTTP Protocol")));
  };
  expectUnsupportedMethod(http::verb::head);
  expectUnsupportedMethod(http::verb::patch);
  AD_EXPECT_THROW_WITH_MESSAGE(
      GraphStoreProtocol::transformGraphStoreProtocol(
          GraphStoreOperation{DEFAULT{}},
          ad_utility::testing::makeRequest(boost::beast::http::verb::connect,
                                           "/?default"),
          index),
      testing::HasSubstr("Unsupported HTTP method \"CONNECT\""));
  AD_EXPECT_THROW_WITH_MESSAGE(
      GraphStoreProtocol::transformGraphStoreProtocol(
          GraphStoreOperation{DEFAULT{}},
          ad_utility::testing::makeRequest("PUMPKIN", "/?default"), index),
      testing::HasSubstr("Unsupported HTTP method \"PUMPKIN\""));
}

// _____________________________________________________________________________________________
TEST(GraphStoreProtocolTest, extractMediatype) {
  [[maybe_unused]] static constexpr auto unknown =
      boost::beast::http::field::unknown;
  [[maybe_unused]] static constexpr auto a_im = boost::beast::http::field::a_im;
  [[maybe_unused]] static constexpr auto accept =
      boost::beast::http::field::accept;
  [[maybe_unused]] static constexpr auto accept_additions =
      boost::beast::http::field::accept_additions;
  [[maybe_unused]] static constexpr auto accept_charset =
      boost::beast::http::field::accept_charset;
  [[maybe_unused]] static constexpr auto accept_datetime =
      boost::beast::http::field::accept_datetime;
  [[maybe_unused]] static constexpr auto accept_encoding =
      boost::beast::http::field::accept_encoding;
  [[maybe_unused]] static constexpr auto accept_features =
      boost::beast::http::field::accept_features;
  [[maybe_unused]] static constexpr auto accept_language =
      boost::beast::http::field::accept_language;
  [[maybe_unused]] static constexpr auto accept_patch =
      boost::beast::http::field::accept_patch;
  [[maybe_unused]] static constexpr auto accept_post =
      boost::beast::http::field::accept_post;
  [[maybe_unused]] static constexpr auto accept_ranges =
      boost::beast::http::field::accept_ranges;
  [[maybe_unused]] static constexpr auto access_control =
      boost::beast::http::field::access_control;
  [[maybe_unused]] static constexpr auto access_control_allow_credentials =
      boost::beast::http::field::access_control_allow_credentials;
  [[maybe_unused]] static constexpr auto access_control_allow_headers =
      boost::beast::http::field::access_control_allow_headers;
  [[maybe_unused]] static constexpr auto access_control_allow_methods =
      boost::beast::http::field::access_control_allow_methods;
  [[maybe_unused]] static constexpr auto access_control_allow_origin =
      boost::beast::http::field::access_control_allow_origin;
  [[maybe_unused]] static constexpr auto access_control_expose_headers =
      boost::beast::http::field::access_control_expose_headers;
  [[maybe_unused]] static constexpr auto access_control_max_age =
      boost::beast::http::field::access_control_max_age;
  [[maybe_unused]] static constexpr auto access_control_request_headers =
      boost::beast::http::field::access_control_request_headers;
  [[maybe_unused]] static constexpr auto access_control_request_method =
      boost::beast::http::field::access_control_request_method;
  [[maybe_unused]] static constexpr auto age = boost::beast::http::field::age;
  [[maybe_unused]] static constexpr auto allow =
      boost::beast::http::field::allow;
  [[maybe_unused]] static constexpr auto alpn = boost::beast::http::field::alpn;
  [[maybe_unused]] static constexpr auto also_control =
      boost::beast::http::field::also_control;
  [[maybe_unused]] static constexpr auto alt_svc =
      boost::beast::http::field::alt_svc;
  [[maybe_unused]] static constexpr auto alt_used =
      boost::beast::http::field::alt_used;
  [[maybe_unused]] static constexpr auto alternate_recipient =
      boost::beast::http::field::alternate_recipient;
  [[maybe_unused]] static constexpr auto alternates =
      boost::beast::http::field::alternates;
  [[maybe_unused]] static constexpr auto apparently_to =
      boost::beast::http::field::apparently_to;
  [[maybe_unused]] static constexpr auto apply_to_redirect_ref =
      boost::beast::http::field::apply_to_redirect_ref;
  [[maybe_unused]] static constexpr auto approved =
      boost::beast::http::field::approved;
  [[maybe_unused]] static constexpr auto archive =
      boost::beast::http::field::archive;
  [[maybe_unused]] static constexpr auto archived_at =
      boost::beast::http::field::archived_at;
  [[maybe_unused]] static constexpr auto article_names =
      boost::beast::http::field::article_names;
  [[maybe_unused]] static constexpr auto article_updates =
      boost::beast::http::field::article_updates;
  [[maybe_unused]] static constexpr auto authentication_control =
      boost::beast::http::field::authentication_control;
  [[maybe_unused]] static constexpr auto authentication_info =
      boost::beast::http::field::authentication_info;
  [[maybe_unused]] static constexpr auto authentication_results =
      boost::beast::http::field::authentication_results;
  [[maybe_unused]] static constexpr auto authorization =
      boost::beast::http::field::authorization;
  [[maybe_unused]] static constexpr auto auto_submitted =
      boost::beast::http::field::auto_submitted;
  [[maybe_unused]] static constexpr auto autoforwarded =
      boost::beast::http::field::autoforwarded;
  [[maybe_unused]] static constexpr auto autosubmitted =
      boost::beast::http::field::autosubmitted;
  [[maybe_unused]] static constexpr auto base = boost::beast::http::field::base;
  [[maybe_unused]] static constexpr auto bcc = boost::beast::http::field::bcc;
  [[maybe_unused]] static constexpr auto body = boost::beast::http::field::body;
  [[maybe_unused]] static constexpr auto c_ext =
      boost::beast::http::field::c_ext;
  [[maybe_unused]] static constexpr auto c_man =
      boost::beast::http::field::c_man;
  [[maybe_unused]] static constexpr auto c_opt =
      boost::beast::http::field::c_opt;
  [[maybe_unused]] static constexpr auto c_pep =
      boost::beast::http::field::c_pep;
  [[maybe_unused]] static constexpr auto c_pep_info =
      boost::beast::http::field::c_pep_info;
  [[maybe_unused]] static constexpr auto cache_control =
      boost::beast::http::field::cache_control;
  [[maybe_unused]] static constexpr auto caldav_timezones =
      boost::beast::http::field::caldav_timezones;
  [[maybe_unused]] static constexpr auto cancel_key =
      boost::beast::http::field::cancel_key;
  [[maybe_unused]] static constexpr auto cancel_lock =
      boost::beast::http::field::cancel_lock;
  [[maybe_unused]] static constexpr auto cc = boost::beast::http::field::cc;
  [[maybe_unused]] static constexpr auto close =
      boost::beast::http::field::close;
  [[maybe_unused]] static constexpr auto comments =
      boost::beast::http::field::comments;
  [[maybe_unused]] static constexpr auto compliance =
      boost::beast::http::field::compliance;
  [[maybe_unused]] static constexpr auto connection =
      boost::beast::http::field::connection;
  [[maybe_unused]] static constexpr auto content_alternative =
      boost::beast::http::field::content_alternative;
  [[maybe_unused]] static constexpr auto content_base =
      boost::beast::http::field::content_base;
  [[maybe_unused]] static constexpr auto content_description =
      boost::beast::http::field::content_description;
  [[maybe_unused]] static constexpr auto content_disposition =
      boost::beast::http::field::content_disposition;
  [[maybe_unused]] static constexpr auto content_duration =
      boost::beast::http::field::content_duration;
  [[maybe_unused]] static constexpr auto content_encoding =
      boost::beast::http::field::content_encoding;
  [[maybe_unused]] static constexpr auto content_features =
      boost::beast::http::field::content_features;
  [[maybe_unused]] static constexpr auto content_id =
      boost::beast::http::field::content_id;
  [[maybe_unused]] static constexpr auto content_identifier =
      boost::beast::http::field::content_identifier;
  [[maybe_unused]] static constexpr auto content_language =
      boost::beast::http::field::content_language;
  [[maybe_unused]] static constexpr auto content_length =
      boost::beast::http::field::content_length;
  [[maybe_unused]] static constexpr auto content_location =
      boost::beast::http::field::content_location;
  [[maybe_unused]] static constexpr auto content_md5 =
      boost::beast::http::field::content_md5;
  [[maybe_unused]] static constexpr auto content_range =
      boost::beast::http::field::content_range;
  [[maybe_unused]] static constexpr auto content_return =
      boost::beast::http::field::content_return;
  [[maybe_unused]] static constexpr auto content_script_type =
      boost::beast::http::field::content_script_type;
  [[maybe_unused]] static constexpr auto content_style_type =
      boost::beast::http::field::content_style_type;
  [[maybe_unused]] static constexpr auto content_transfer_encoding =
      boost::beast::http::field::content_transfer_encoding;
  [[maybe_unused]] static constexpr auto content_type =
      boost::beast::http::field::content_type;
  [[maybe_unused]] static constexpr auto content_version =
      boost::beast::http::field::content_version;
  [[maybe_unused]] static constexpr auto control =
      boost::beast::http::field::control;
  [[maybe_unused]] static constexpr auto conversion =
      boost::beast::http::field::conversion;
  [[maybe_unused]] static constexpr auto conversion_with_loss =
      boost::beast::http::field::conversion_with_loss;
  [[maybe_unused]] static constexpr auto cookie =
      boost::beast::http::field::cookie;
  [[maybe_unused]] static constexpr auto cookie2 =
      boost::beast::http::field::cookie2;
  [[maybe_unused]] static constexpr auto cost = boost::beast::http::field::cost;
  [[maybe_unused]] static constexpr auto dasl = boost::beast::http::field::dasl;
  [[maybe_unused]] static constexpr auto date = boost::beast::http::field::date;
  [[maybe_unused]] static constexpr auto date_received =
      boost::beast::http::field::date_received;
  [[maybe_unused]] static constexpr auto dav = boost::beast::http::field::dav;
  [[maybe_unused]] static constexpr auto default_style =
      boost::beast::http::field::default_style;
  [[maybe_unused]] static constexpr auto deferred_delivery =
      boost::beast::http::field::deferred_delivery;
  [[maybe_unused]] static constexpr auto delivery_date =
      boost::beast::http::field::delivery_date;
  [[maybe_unused]] static constexpr auto delta_base =
      boost::beast::http::field::delta_base;
  [[maybe_unused]] static constexpr auto depth =
      boost::beast::http::field::depth;
  [[maybe_unused]] static constexpr auto derived_from =
      boost::beast::http::field::derived_from;
  [[maybe_unused]] static constexpr auto destination =
      boost::beast::http::field::destination;
  [[maybe_unused]] static constexpr auto differential_id =
      boost::beast::http::field::differential_id;
  [[maybe_unused]] static constexpr auto digest =
      boost::beast::http::field::digest;
  [[maybe_unused]] static constexpr auto discarded_x400_ipms_extensions =
      boost::beast::http::field::discarded_x400_ipms_extensions;
  [[maybe_unused]] static constexpr auto discarded_x400_mts_extensions =
      boost::beast::http::field::discarded_x400_mts_extensions;
  [[maybe_unused]] static constexpr auto disclose_recipients =
      boost::beast::http::field::disclose_recipients;
  [[maybe_unused]] static constexpr auto disposition_notification_options =
      boost::beast::http::field::disposition_notification_options;
  [[maybe_unused]] static constexpr auto disposition_notification_to =
      boost::beast::http::field::disposition_notification_to;
  [[maybe_unused]] static constexpr auto distribution =
      boost::beast::http::field::distribution;
  [[maybe_unused]] static constexpr auto dkim_signature =
      boost::beast::http::field::dkim_signature;
  [[maybe_unused]] static constexpr auto dl_expansion_history =
      boost::beast::http::field::dl_expansion_history;
  [[maybe_unused]] static constexpr auto downgraded_bcc =
      boost::beast::http::field::downgraded_bcc;
  [[maybe_unused]] static constexpr auto downgraded_cc =
      boost::beast::http::field::downgraded_cc;
  [[maybe_unused]] static constexpr auto
      downgraded_disposition_notification_to =
          boost::beast::http::field::downgraded_disposition_notification_to;
  [[maybe_unused]] static constexpr auto downgraded_final_recipient =
      boost::beast::http::field::downgraded_final_recipient;
  [[maybe_unused]] static constexpr auto downgraded_from =
      boost::beast::http::field::downgraded_from;
  [[maybe_unused]] static constexpr auto downgraded_in_reply_to =
      boost::beast::http::field::downgraded_in_reply_to;
  [[maybe_unused]] static constexpr auto downgraded_mail_from =
      boost::beast::http::field::downgraded_mail_from;
  [[maybe_unused]] static constexpr auto downgraded_message_id =
      boost::beast::http::field::downgraded_message_id;
  [[maybe_unused]] static constexpr auto downgraded_original_recipient =
      boost::beast::http::field::downgraded_original_recipient;
  [[maybe_unused]] static constexpr auto downgraded_rcpt_to =
      boost::beast::http::field::downgraded_rcpt_to;
  [[maybe_unused]] static constexpr auto downgraded_references =
      boost::beast::http::field::downgraded_references;
  [[maybe_unused]] static constexpr auto downgraded_reply_to =
      boost::beast::http::field::downgraded_reply_to;
  [[maybe_unused]] static constexpr auto downgraded_resent_bcc =
      boost::beast::http::field::downgraded_resent_bcc;
  [[maybe_unused]] static constexpr auto downgraded_resent_cc =
      boost::beast::http::field::downgraded_resent_cc;
  [[maybe_unused]] static constexpr auto downgraded_resent_from =
      boost::beast::http::field::downgraded_resent_from;
  [[maybe_unused]] static constexpr auto downgraded_resent_reply_to =
      boost::beast::http::field::downgraded_resent_reply_to;
  [[maybe_unused]] static constexpr auto downgraded_resent_sender =
      boost::beast::http::field::downgraded_resent_sender;
  [[maybe_unused]] static constexpr auto downgraded_resent_to =
      boost::beast::http::field::downgraded_resent_to;
  [[maybe_unused]] static constexpr auto downgraded_return_path =
      boost::beast::http::field::downgraded_return_path;
  [[maybe_unused]] static constexpr auto downgraded_sender =
      boost::beast::http::field::downgraded_sender;
  [[maybe_unused]] static constexpr auto downgraded_to =
      boost::beast::http::field::downgraded_to;
  [[maybe_unused]] static constexpr auto ediint_features =
      boost::beast::http::field::ediint_features;
  [[maybe_unused]] static constexpr auto eesst_version =
      boost::beast::http::field::eesst_version;
  [[maybe_unused]] static constexpr auto encoding =
      boost::beast::http::field::encoding;
  [[maybe_unused]] static constexpr auto encrypted =
      boost::beast::http::field::encrypted;
  [[maybe_unused]] static constexpr auto errors_to =
      boost::beast::http::field::errors_to;
  [[maybe_unused]] static constexpr auto etag = boost::beast::http::field::etag;
  [[maybe_unused]] static constexpr auto expect =
      boost::beast::http::field::expect;
  [[maybe_unused]] static constexpr auto expires =
      boost::beast::http::field::expires;
  [[maybe_unused]] static constexpr auto expiry_date =
      boost::beast::http::field::expiry_date;
  [[maybe_unused]] static constexpr auto ext = boost::beast::http::field::ext;
  [[maybe_unused]] static constexpr auto followup_to =
      boost::beast::http::field::followup_to;
  [[maybe_unused]] static constexpr auto forwarded =
      boost::beast::http::field::forwarded;
  [[maybe_unused]] static constexpr auto from = boost::beast::http::field::from;
  [[maybe_unused]] static constexpr auto generate_delivery_report =
      boost::beast::http::field::generate_delivery_report;
  [[maybe_unused]] static constexpr auto getprofile =
      boost::beast::http::field::getprofile;
  [[maybe_unused]] static constexpr auto hobareg =
      boost::beast::http::field::hobareg;
  [[maybe_unused]] static constexpr auto host = boost::beast::http::field::host;
  [[maybe_unused]] static constexpr auto http2_settings =
      boost::beast::http::field::http2_settings;
  [[maybe_unused]] static constexpr auto if_ = boost::beast::http::field::if_;
  [[maybe_unused]] static constexpr auto if_match =
      boost::beast::http::field::if_match;
  [[maybe_unused]] static constexpr auto if_modified_since =
      boost::beast::http::field::if_modified_since;
  [[maybe_unused]] static constexpr auto if_none_match =
      boost::beast::http::field::if_none_match;
  [[maybe_unused]] static constexpr auto if_range =
      boost::beast::http::field::if_range;
  [[maybe_unused]] static constexpr auto if_schedule_tag_match =
      boost::beast::http::field::if_schedule_tag_match;
  [[maybe_unused]] static constexpr auto if_unmodified_since =
      boost::beast::http::field::if_unmodified_since;
  [[maybe_unused]] static constexpr auto im = boost::beast::http::field::im;
  [[maybe_unused]] static constexpr auto importance =
      boost::beast::http::field::importance;
  [[maybe_unused]] static constexpr auto in_reply_to =
      boost::beast::http::field::in_reply_to;
  [[maybe_unused]] static constexpr auto incomplete_copy =
      boost::beast::http::field::incomplete_copy;
  [[maybe_unused]] static constexpr auto injection_date =
      boost::beast::http::field::injection_date;
  [[maybe_unused]] static constexpr auto injection_info =
      boost::beast::http::field::injection_info;
  [[maybe_unused]] static constexpr auto jabber_id =
      boost::beast::http::field::jabber_id;
  [[maybe_unused]] static constexpr auto keep_alive =
      boost::beast::http::field::keep_alive;
  [[maybe_unused]] static constexpr auto keywords =
      boost::beast::http::field::keywords;
  [[maybe_unused]] static constexpr auto label =
      boost::beast::http::field::label;
  [[maybe_unused]] static constexpr auto language =
      boost::beast::http::field::language;
  [[maybe_unused]] static constexpr auto last_modified =
      boost::beast::http::field::last_modified;
  [[maybe_unused]] static constexpr auto latest_delivery_time =
      boost::beast::http::field::latest_delivery_time;
  [[maybe_unused]] static constexpr auto lines =
      boost::beast::http::field::lines;
  [[maybe_unused]] static constexpr auto link = boost::beast::http::field::link;
  [[maybe_unused]] static constexpr auto list_archive =
      boost::beast::http::field::list_archive;
  [[maybe_unused]] static constexpr auto list_help =
      boost::beast::http::field::list_help;
  [[maybe_unused]] static constexpr auto list_id =
      boost::beast::http::field::list_id;
  [[maybe_unused]] static constexpr auto list_owner =
      boost::beast::http::field::list_owner;
  [[maybe_unused]] static constexpr auto list_post =
      boost::beast::http::field::list_post;
  [[maybe_unused]] static constexpr auto list_subscribe =
      boost::beast::http::field::list_subscribe;
  [[maybe_unused]] static constexpr auto list_unsubscribe =
      boost::beast::http::field::list_unsubscribe;
  [[maybe_unused]] static constexpr auto list_unsubscribe_post =
      boost::beast::http::field::list_unsubscribe_post;
  [[maybe_unused]] static constexpr auto location =
      boost::beast::http::field::location;
  [[maybe_unused]] static constexpr auto lock_token =
      boost::beast::http::field::lock_token;
  [[maybe_unused]] static constexpr auto man = boost::beast::http::field::man;
  [[maybe_unused]] static constexpr auto max_forwards =
      boost::beast::http::field::max_forwards;
  [[maybe_unused]] static constexpr auto memento_datetime =
      boost::beast::http::field::memento_datetime;
  [[maybe_unused]] static constexpr auto message_context =
      boost::beast::http::field::message_context;
  [[maybe_unused]] static constexpr auto message_id =
      boost::beast::http::field::message_id;
  [[maybe_unused]] static constexpr auto message_type =
      boost::beast::http::field::message_type;
  [[maybe_unused]] static constexpr auto meter =
      boost::beast::http::field::meter;
  [[maybe_unused]] static constexpr auto method_check =
      boost::beast::http::field::method_check;
  [[maybe_unused]] static constexpr auto method_check_expires =
      boost::beast::http::field::method_check_expires;
  [[maybe_unused]] static constexpr auto mime_version =
      boost::beast::http::field::mime_version;
  [[maybe_unused]] static constexpr auto mmhs_acp127_message_identifier =
      boost::beast::http::field::mmhs_acp127_message_identifier;
  [[maybe_unused]] static constexpr auto mmhs_authorizing_users =
      boost::beast::http::field::mmhs_authorizing_users;
  [[maybe_unused]] static constexpr auto mmhs_codress_message_indicator =
      boost::beast::http::field::mmhs_codress_message_indicator;
  [[maybe_unused]] static constexpr auto mmhs_copy_precedence =
      boost::beast::http::field::mmhs_copy_precedence;
  [[maybe_unused]] static constexpr auto mmhs_exempted_address =
      boost::beast::http::field::mmhs_exempted_address;
  [[maybe_unused]] static constexpr auto mmhs_extended_authorisation_info =
      boost::beast::http::field::mmhs_extended_authorisation_info;
  [[maybe_unused]] static constexpr auto mmhs_handling_instructions =
      boost::beast::http::field::mmhs_handling_instructions;
  [[maybe_unused]] static constexpr auto mmhs_message_instructions =
      boost::beast::http::field::mmhs_message_instructions;
  [[maybe_unused]] static constexpr auto mmhs_message_type =
      boost::beast::http::field::mmhs_message_type;
  [[maybe_unused]] static constexpr auto mmhs_originator_plad =
      boost::beast::http::field::mmhs_originator_plad;
  [[maybe_unused]] static constexpr auto mmhs_originator_reference =
      boost::beast::http::field::mmhs_originator_reference;
  [[maybe_unused]] static constexpr auto mmhs_other_recipients_indicator_cc =
      boost::beast::http::field::mmhs_other_recipients_indicator_cc;
  [[maybe_unused]] static constexpr auto mmhs_other_recipients_indicator_to =
      boost::beast::http::field::mmhs_other_recipients_indicator_to;
  [[maybe_unused]] static constexpr auto mmhs_primary_precedence =
      boost::beast::http::field::mmhs_primary_precedence;
  [[maybe_unused]] static constexpr auto mmhs_subject_indicator_codes =
      boost::beast::http::field::mmhs_subject_indicator_codes;
  [[maybe_unused]] static constexpr auto mt_priority =
      boost::beast::http::field::mt_priority;
  [[maybe_unused]] static constexpr auto negotiate =
      boost::beast::http::field::negotiate;
  [[maybe_unused]] static constexpr auto newsgroups =
      boost::beast::http::field::newsgroups;
  [[maybe_unused]] static constexpr auto nntp_posting_date =
      boost::beast::http::field::nntp_posting_date;
  [[maybe_unused]] static constexpr auto nntp_posting_host =
      boost::beast::http::field::nntp_posting_host;
  [[maybe_unused]] static constexpr auto non_compliance =
      boost::beast::http::field::non_compliance;
  [[maybe_unused]] static constexpr auto obsoletes =
      boost::beast::http::field::obsoletes;
  [[maybe_unused]] static constexpr auto opt = boost::beast::http::field::opt;
  [[maybe_unused]] static constexpr auto optional =
      boost::beast::http::field::optional;
  [[maybe_unused]] static constexpr auto optional_www_authenticate =
      boost::beast::http::field::optional_www_authenticate;
  [[maybe_unused]] static constexpr auto ordering_type =
      boost::beast::http::field::ordering_type;
  [[maybe_unused]] static constexpr auto organization =
      boost::beast::http::field::organization;
  [[maybe_unused]] static constexpr auto origin =
      boost::beast::http::field::origin;
  [[maybe_unused]] static constexpr auto original_encoded_information_types =
      boost::beast::http::field::original_encoded_information_types;
  [[maybe_unused]] static constexpr auto original_from =
      boost::beast::http::field::original_from;
  [[maybe_unused]] static constexpr auto original_message_id =
      boost::beast::http::field::original_message_id;
  [[maybe_unused]] static constexpr auto original_recipient =
      boost::beast::http::field::original_recipient;
  [[maybe_unused]] static constexpr auto original_sender =
      boost::beast::http::field::original_sender;
  [[maybe_unused]] static constexpr auto original_subject =
      boost::beast::http::field::original_subject;
  [[maybe_unused]] static constexpr auto originator_return_address =
      boost::beast::http::field::originator_return_address;
  [[maybe_unused]] static constexpr auto overwrite =
      boost::beast::http::field::overwrite;
  [[maybe_unused]] static constexpr auto p3p = boost::beast::http::field::p3p;
  [[maybe_unused]] static constexpr auto path = boost::beast::http::field::path;
  [[maybe_unused]] static constexpr auto pep = boost::beast::http::field::pep;
  [[maybe_unused]] static constexpr auto pep_info =
      boost::beast::http::field::pep_info;
  [[maybe_unused]] static constexpr auto pics_label =
      boost::beast::http::field::pics_label;
  [[maybe_unused]] static constexpr auto position =
      boost::beast::http::field::position;
  [[maybe_unused]] static constexpr auto posting_version =
      boost::beast::http::field::posting_version;
  [[maybe_unused]] static constexpr auto pragma =
      boost::beast::http::field::pragma;
  [[maybe_unused]] static constexpr auto prefer =
      boost::beast::http::field::prefer;
  [[maybe_unused]] static constexpr auto preference_applied =
      boost::beast::http::field::preference_applied;
  [[maybe_unused]] static constexpr auto prevent_nondelivery_report =
      boost::beast::http::field::prevent_nondelivery_report;
  [[maybe_unused]] static constexpr auto priority =
      boost::beast::http::field::priority;
  [[maybe_unused]] static constexpr auto privicon =
      boost::beast::http::field::privicon;
  [[maybe_unused]] static constexpr auto profileobject =
      boost::beast::http::field::profileobject;
  [[maybe_unused]] static constexpr auto protocol =
      boost::beast::http::field::protocol;
  [[maybe_unused]] static constexpr auto protocol_info =
      boost::beast::http::field::protocol_info;
  [[maybe_unused]] static constexpr auto protocol_query =
      boost::beast::http::field::protocol_query;
  [[maybe_unused]] static constexpr auto protocol_request =
      boost::beast::http::field::protocol_request;
  [[maybe_unused]] static constexpr auto proxy_authenticate =
      boost::beast::http::field::proxy_authenticate;
  [[maybe_unused]] static constexpr auto proxy_authentication_info =
      boost::beast::http::field::proxy_authentication_info;
  [[maybe_unused]] static constexpr auto proxy_authorization =
      boost::beast::http::field::proxy_authorization;
  [[maybe_unused]] static constexpr auto proxy_connection =
      boost::beast::http::field::proxy_connection;
  [[maybe_unused]] static constexpr auto proxy_features =
      boost::beast::http::field::proxy_features;
  [[maybe_unused]] static constexpr auto proxy_instruction =
      boost::beast::http::field::proxy_instruction;
  [[maybe_unused]] static constexpr auto public_ =
      boost::beast::http::field::public_;
  [[maybe_unused]] static constexpr auto public_key_pins =
      boost::beast::http::field::public_key_pins;
  [[maybe_unused]] static constexpr auto public_key_pins_report_only =
      boost::beast::http::field::public_key_pins_report_only;
  [[maybe_unused]] static constexpr auto range =
      boost::beast::http::field::range;
  [[maybe_unused]] static constexpr auto received =
      boost::beast::http::field::received;
  [[maybe_unused]] static constexpr auto received_spf =
      boost::beast::http::field::received_spf;
  [[maybe_unused]] static constexpr auto redirect_ref =
      boost::beast::http::field::redirect_ref;
  [[maybe_unused]] static constexpr auto references =
      boost::beast::http::field::references;
  [[maybe_unused]] static constexpr auto referer =
      boost::beast::http::field::referer;
  [[maybe_unused]] static constexpr auto referer_root =
      boost::beast::http::field::referer_root;
  [[maybe_unused]] static constexpr auto relay_version =
      boost::beast::http::field::relay_version;
  [[maybe_unused]] static constexpr auto reply_by =
      boost::beast::http::field::reply_by;
  [[maybe_unused]] static constexpr auto reply_to =
      boost::beast::http::field::reply_to;
  [[maybe_unused]] static constexpr auto require_recipient_valid_since =
      boost::beast::http::field::require_recipient_valid_since;
  [[maybe_unused]] static constexpr auto resent_bcc =
      boost::beast::http::field::resent_bcc;
  [[maybe_unused]] static constexpr auto resent_cc =
      boost::beast::http::field::resent_cc;
  [[maybe_unused]] static constexpr auto resent_date =
      boost::beast::http::field::resent_date;
  [[maybe_unused]] static constexpr auto resent_from =
      boost::beast::http::field::resent_from;
  [[maybe_unused]] static constexpr auto resent_message_id =
      boost::beast::http::field::resent_message_id;
  [[maybe_unused]] static constexpr auto resent_reply_to =
      boost::beast::http::field::resent_reply_to;
  [[maybe_unused]] static constexpr auto resent_sender =
      boost::beast::http::field::resent_sender;
  [[maybe_unused]] static constexpr auto resent_to =
      boost::beast::http::field::resent_to;
  [[maybe_unused]] static constexpr auto resolution_hint =
      boost::beast::http::field::resolution_hint;
  [[maybe_unused]] static constexpr auto resolver_location =
      boost::beast::http::field::resolver_location;
  [[maybe_unused]] static constexpr auto retry_after =
      boost::beast::http::field::retry_after;
  [[maybe_unused]] static constexpr auto return_path =
      boost::beast::http::field::return_path;
  [[maybe_unused]] static constexpr auto safe = boost::beast::http::field::safe;
  [[maybe_unused]] static constexpr auto schedule_reply =
      boost::beast::http::field::schedule_reply;
  [[maybe_unused]] static constexpr auto schedule_tag =
      boost::beast::http::field::schedule_tag;
  [[maybe_unused]] static constexpr auto sec_fetch_dest =
      boost::beast::http::field::sec_fetch_dest;
  [[maybe_unused]] static constexpr auto sec_fetch_mode =
      boost::beast::http::field::sec_fetch_mode;
  [[maybe_unused]] static constexpr auto sec_fetch_site =
      boost::beast::http::field::sec_fetch_site;
  [[maybe_unused]] static constexpr auto sec_fetch_user =
      boost::beast::http::field::sec_fetch_user;
  [[maybe_unused]] static constexpr auto sec_websocket_accept =
      boost::beast::http::field::sec_websocket_accept;
  [[maybe_unused]] static constexpr auto sec_websocket_extensions =
      boost::beast::http::field::sec_websocket_extensions;
  [[maybe_unused]] static constexpr auto sec_websocket_key =
      boost::beast::http::field::sec_websocket_key;
  [[maybe_unused]] static constexpr auto sec_websocket_protocol =
      boost::beast::http::field::sec_websocket_protocol;
  [[maybe_unused]] static constexpr auto sec_websocket_version =
      boost::beast::http::field::sec_websocket_version;
  [[maybe_unused]] static constexpr auto security_scheme =
      boost::beast::http::field::security_scheme;
  [[maybe_unused]] static constexpr auto see_also =
      boost::beast::http::field::see_also;
  [[maybe_unused]] static constexpr auto sender =
      boost::beast::http::field::sender;
  [[maybe_unused]] static constexpr auto sensitivity =
      boost::beast::http::field::sensitivity;
  [[maybe_unused]] static constexpr auto server =
      boost::beast::http::field::server;
  [[maybe_unused]] static constexpr auto set_cookie =
      boost::beast::http::field::set_cookie;
  [[maybe_unused]] static constexpr auto set_cookie2 =
      boost::beast::http::field::set_cookie2;
  [[maybe_unused]] static constexpr auto setprofile =
      boost::beast::http::field::setprofile;
  [[maybe_unused]] static constexpr auto sio_label =
      boost::beast::http::field::sio_label;
  [[maybe_unused]] static constexpr auto sio_label_history =
      boost::beast::http::field::sio_label_history;
  [[maybe_unused]] static constexpr auto slug = boost::beast::http::field::slug;
  [[maybe_unused]] static constexpr auto soapaction =
      boost::beast::http::field::soapaction;
  [[maybe_unused]] static constexpr auto solicitation =
      boost::beast::http::field::solicitation;
  [[maybe_unused]] static constexpr auto status_uri =
      boost::beast::http::field::status_uri;
  [[maybe_unused]] static constexpr auto strict_transport_security =
      boost::beast::http::field::strict_transport_security;
  [[maybe_unused]] static constexpr auto subject =
      boost::beast::http::field::subject;
  [[maybe_unused]] static constexpr auto subok =
      boost::beast::http::field::subok;
  [[maybe_unused]] static constexpr auto subst =
      boost::beast::http::field::subst;
  [[maybe_unused]] static constexpr auto summary =
      boost::beast::http::field::summary;
  [[maybe_unused]] static constexpr auto supersedes =
      boost::beast::http::field::supersedes;
  [[maybe_unused]] static constexpr auto surrogate_capability =
      boost::beast::http::field::surrogate_capability;
  [[maybe_unused]] static constexpr auto surrogate_control =
      boost::beast::http::field::surrogate_control;
  [[maybe_unused]] static constexpr auto tcn = boost::beast::http::field::tcn;
  [[maybe_unused]] static constexpr auto te = boost::beast::http::field::te;
  [[maybe_unused]] static constexpr auto timeout =
      boost::beast::http::field::timeout;
  [[maybe_unused]] static constexpr auto title =
      boost::beast::http::field::title;
  [[maybe_unused]] static constexpr auto to = boost::beast::http::field::to;
  [[maybe_unused]] static constexpr auto topic =
      boost::beast::http::field::topic;
  [[maybe_unused]] static constexpr auto trailer =
      boost::beast::http::field::trailer;
  [[maybe_unused]] static constexpr auto transfer_encoding =
      boost::beast::http::field::transfer_encoding;
  [[maybe_unused]] static constexpr auto ttl = boost::beast::http::field::ttl;
  [[maybe_unused]] static constexpr auto ua_color =
      boost::beast::http::field::ua_color;
  [[maybe_unused]] static constexpr auto ua_media =
      boost::beast::http::field::ua_media;
  [[maybe_unused]] static constexpr auto ua_pixels =
      boost::beast::http::field::ua_pixels;
  [[maybe_unused]] static constexpr auto ua_resolution =
      boost::beast::http::field::ua_resolution;
  [[maybe_unused]] static constexpr auto ua_windowpixels =
      boost::beast::http::field::ua_windowpixels;
  [[maybe_unused]] static constexpr auto upgrade =
      boost::beast::http::field::upgrade;
  [[maybe_unused]] static constexpr auto urgency =
      boost::beast::http::field::urgency;
  [[maybe_unused]] static constexpr auto uri = boost::beast::http::field::uri;
  [[maybe_unused]] static constexpr auto user_agent =
      boost::beast::http::field::user_agent;
  [[maybe_unused]] static constexpr auto variant_vary =
      boost::beast::http::field::variant_vary;
  [[maybe_unused]] static constexpr auto vary = boost::beast::http::field::vary;
  [[maybe_unused]] static constexpr auto vbr_info =
      boost::beast::http::field::vbr_info;
  [[maybe_unused]] static constexpr auto version =
      boost::beast::http::field::version;
  [[maybe_unused]] static constexpr auto via = boost::beast::http::field::via;
  [[maybe_unused]] static constexpr auto want_digest =
      boost::beast::http::field::want_digest;
  [[maybe_unused]] static constexpr auto warning =
      boost::beast::http::field::warning;
  [[maybe_unused]] static constexpr auto www_authenticate =
      boost::beast::http::field::www_authenticate;
  [[maybe_unused]] static constexpr auto x_archived_at =
      boost::beast::http::field::x_archived_at;
  [[maybe_unused]] static constexpr auto x_device_accept =
      boost::beast::http::field::x_device_accept;
  [[maybe_unused]] static constexpr auto x_device_accept_charset =
      boost::beast::http::field::x_device_accept_charset;
  [[maybe_unused]] static constexpr auto x_device_accept_encoding =
      boost::beast::http::field::x_device_accept_encoding;
  [[maybe_unused]] static constexpr auto x_device_accept_language =
      boost::beast::http::field::x_device_accept_language;
  [[maybe_unused]] static constexpr auto x_device_user_agent =
      boost::beast::http::field::x_device_user_agent;
  [[maybe_unused]] static constexpr auto x_frame_options =
      boost::beast::http::field::x_frame_options;
  [[maybe_unused]] static constexpr auto x_mittente =
      boost::beast::http::field::x_mittente;
  [[maybe_unused]] static constexpr auto x_pgp_sig =
      boost::beast::http::field::x_pgp_sig;
  [[maybe_unused]] static constexpr auto x_ricevuta =
      boost::beast::http::field::x_ricevuta;
  [[maybe_unused]] static constexpr auto x_riferimento_message_id =
      boost::beast::http::field::x_riferimento_message_id;
  [[maybe_unused]] static constexpr auto x_tiporicevuta =
      boost::beast::http::field::x_tiporicevuta;
  [[maybe_unused]] static constexpr auto x_trasporto =
      boost::beast::http::field::x_trasporto;
  [[maybe_unused]] static constexpr auto x_verificasicurezza =
      boost::beast::http::field::x_verificasicurezza;
  [[maybe_unused]] static constexpr auto x400_content_identifier =
      boost::beast::http::field::x400_content_identifier;
  [[maybe_unused]] static constexpr auto x400_content_return =
      boost::beast::http::field::x400_content_return;
  [[maybe_unused]] static constexpr auto x400_content_type =
      boost::beast::http::field::x400_content_type;
  [[maybe_unused]] static constexpr auto x400_mts_identifier =
      boost::beast::http::field::x400_mts_identifier;
  [[maybe_unused]] static constexpr auto x400_originator =
      boost::beast::http::field::x400_originator;
  [[maybe_unused]] static constexpr auto x400_received =
      boost::beast::http::field::x400_received;
  [[maybe_unused]] static constexpr auto x400_recipients =
      boost::beast::http::field::x400_recipients;
  [[maybe_unused]] static constexpr auto x400_trace =
      boost::beast::http::field::x400_trace;
  [[maybe_unused]] static constexpr auto xref = boost::beast::http::field::xref;
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
            ad_utility::source_location l = AD_CURRENT_SOURCE_LOC()) {
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
      ad_utility::source_location l = AD_CURRENT_SOURCE_LOC())(
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
