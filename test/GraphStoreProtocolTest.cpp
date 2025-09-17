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
  using boost::beast::http::field::a_im;
  using boost::beast::http::field::accept;
  using boost::beast::http::field::accept_additions;
  using boost::beast::http::field::accept_charset;
  using boost::beast::http::field::accept_datetime;
  using boost::beast::http::field::accept_encoding;
  using boost::beast::http::field::accept_features;
  using boost::beast::http::field::accept_language;
  using boost::beast::http::field::accept_patch;
  using boost::beast::http::field::accept_post;
  using boost::beast::http::field::accept_ranges;
  using boost::beast::http::field::access_control;
  using boost::beast::http::field::access_control_allow_credentials;
  using boost::beast::http::field::access_control_allow_headers;
  using boost::beast::http::field::access_control_allow_methods;
  using boost::beast::http::field::access_control_allow_origin;
  using boost::beast::http::field::access_control_expose_headers;
  using boost::beast::http::field::access_control_max_age;
  using boost::beast::http::field::access_control_request_headers;
  using boost::beast::http::field::access_control_request_method;
  using boost::beast::http::field::age;
  using boost::beast::http::field::allow;
  using boost::beast::http::field::alpn;
  using boost::beast::http::field::also_control;
  using boost::beast::http::field::alt_svc;
  using boost::beast::http::field::alt_used;
  using boost::beast::http::field::alternate_recipient;
  using boost::beast::http::field::alternates;
  using boost::beast::http::field::apparently_to;
  using boost::beast::http::field::apply_to_redirect_ref;
  using boost::beast::http::field::approved;
  using boost::beast::http::field::archive;
  using boost::beast::http::field::archived_at;
  using boost::beast::http::field::article_names;
  using boost::beast::http::field::article_updates;
  using boost::beast::http::field::authentication_control;
  using boost::beast::http::field::authentication_info;
  using boost::beast::http::field::authentication_results;
  using boost::beast::http::field::authorization;
  using boost::beast::http::field::auto_submitted;
  using boost::beast::http::field::autoforwarded;
  using boost::beast::http::field::autosubmitted;
  using boost::beast::http::field::base;
  using boost::beast::http::field::bcc;
  using boost::beast::http::field::body;
  using boost::beast::http::field::c_ext;
  using boost::beast::http::field::c_man;
  using boost::beast::http::field::c_opt;
  using boost::beast::http::field::c_pep;
  using boost::beast::http::field::c_pep_info;
  using boost::beast::http::field::cache_control;
  using boost::beast::http::field::caldav_timezones;
  using boost::beast::http::field::cancel_key;
  using boost::beast::http::field::cancel_lock;
  using boost::beast::http::field::cc;
  using boost::beast::http::field::close;
  using boost::beast::http::field::comments;
  using boost::beast::http::field::compliance;
  using boost::beast::http::field::connection;
  using boost::beast::http::field::content_alternative;
  using boost::beast::http::field::content_base;
  using boost::beast::http::field::content_description;
  using boost::beast::http::field::content_disposition;
  using boost::beast::http::field::content_duration;
  using boost::beast::http::field::content_encoding;
  using boost::beast::http::field::content_features;
  using boost::beast::http::field::content_id;
  using boost::beast::http::field::content_identifier;
  using boost::beast::http::field::content_language;
  using boost::beast::http::field::content_length;
  using boost::beast::http::field::content_location;
  using boost::beast::http::field::content_md5;
  using boost::beast::http::field::content_range;
  using boost::beast::http::field::content_return;
  using boost::beast::http::field::content_script_type;
  using boost::beast::http::field::content_style_type;
  using boost::beast::http::field::content_transfer_encoding;
  using boost::beast::http::field::content_type;
  using boost::beast::http::field::content_version;
  using boost::beast::http::field::control;
  using boost::beast::http::field::conversion;
  using boost::beast::http::field::conversion_with_loss;
  using boost::beast::http::field::cookie;
  using boost::beast::http::field::cookie2;
  using boost::beast::http::field::cost;
  using boost::beast::http::field::dasl;
  using boost::beast::http::field::date;
  using boost::beast::http::field::date_received;
  using boost::beast::http::field::dav;
  using boost::beast::http::field::default_style;
  using boost::beast::http::field::deferred_delivery;
  using boost::beast::http::field::delivery_date;
  using boost::beast::http::field::delta_base;
  using boost::beast::http::field::depth;
  using boost::beast::http::field::derived_from;
  using boost::beast::http::field::destination;
  using boost::beast::http::field::differential_id;
  using boost::beast::http::field::digest;
  using boost::beast::http::field::discarded_x400_ipms_extensions;
  using boost::beast::http::field::discarded_x400_mts_extensions;
  using boost::beast::http::field::disclose_recipients;
  using boost::beast::http::field::disposition_notification_options;
  using boost::beast::http::field::disposition_notification_to;
  using boost::beast::http::field::distribution;
  using boost::beast::http::field::dkim_signature;
  using boost::beast::http::field::dl_expansion_history;
  using boost::beast::http::field::downgraded_bcc;
  using boost::beast::http::field::downgraded_cc;
  using boost::beast::http::field::downgraded_disposition_notification_to;
  using boost::beast::http::field::downgraded_final_recipient;
  using boost::beast::http::field::downgraded_from;
  using boost::beast::http::field::downgraded_in_reply_to;
  using boost::beast::http::field::downgraded_mail_from;
  using boost::beast::http::field::downgraded_message_id;
  using boost::beast::http::field::downgraded_original_recipient;
  using boost::beast::http::field::downgraded_rcpt_to;
  using boost::beast::http::field::downgraded_references;
  using boost::beast::http::field::downgraded_reply_to;
  using boost::beast::http::field::downgraded_resent_bcc;
  using boost::beast::http::field::downgraded_resent_cc;
  using boost::beast::http::field::downgraded_resent_from;
  using boost::beast::http::field::downgraded_resent_reply_to;
  using boost::beast::http::field::downgraded_resent_sender;
  using boost::beast::http::field::downgraded_resent_to;
  using boost::beast::http::field::downgraded_return_path;
  using boost::beast::http::field::downgraded_sender;
  using boost::beast::http::field::downgraded_to;
  using boost::beast::http::field::ediint_features;
  using boost::beast::http::field::eesst_version;
  using boost::beast::http::field::encoding;
  using boost::beast::http::field::encrypted;
  using boost::beast::http::field::errors_to;
  using boost::beast::http::field::etag;
  using boost::beast::http::field::expect;
  using boost::beast::http::field::expires;
  using boost::beast::http::field::expiry_date;
  using boost::beast::http::field::ext;
  using boost::beast::http::field::followup_to;
  using boost::beast::http::field::forwarded;
  using boost::beast::http::field::from;
  using boost::beast::http::field::generate_delivery_report;
  using boost::beast::http::field::getprofile;
  using boost::beast::http::field::hobareg;
  using boost::beast::http::field::host;
  using boost::beast::http::field::http2_settings;
  using boost::beast::http::field::if_;
  using boost::beast::http::field::if_match;
  using boost::beast::http::field::if_modified_since;
  using boost::beast::http::field::if_none_match;
  using boost::beast::http::field::if_range;
  using boost::beast::http::field::if_schedule_tag_match;
  using boost::beast::http::field::if_unmodified_since;
  using boost::beast::http::field::im;
  using boost::beast::http::field::importance;
  using boost::beast::http::field::in_reply_to;
  using boost::beast::http::field::incomplete_copy;
  using boost::beast::http::field::injection_date;
  using boost::beast::http::field::injection_info;
  using boost::beast::http::field::jabber_id;
  using boost::beast::http::field::keep_alive;
  using boost::beast::http::field::keywords;
  using boost::beast::http::field::label;
  using boost::beast::http::field::language;
  using boost::beast::http::field::last_modified;
  using boost::beast::http::field::latest_delivery_time;
  using boost::beast::http::field::lines;
  using boost::beast::http::field::link;
  using boost::beast::http::field::list_archive;
  using boost::beast::http::field::list_help;
  using boost::beast::http::field::list_id;
  using boost::beast::http::field::list_owner;
  using boost::beast::http::field::list_post;
  using boost::beast::http::field::list_subscribe;
  using boost::beast::http::field::list_unsubscribe;
  using boost::beast::http::field::list_unsubscribe_post;
  using boost::beast::http::field::location;
  using boost::beast::http::field::lock_token;
  using boost::beast::http::field::man;
  using boost::beast::http::field::max_forwards;
  using boost::beast::http::field::memento_datetime;
  using boost::beast::http::field::message_context;
  using boost::beast::http::field::message_id;
  using boost::beast::http::field::message_type;
  using boost::beast::http::field::meter;
  using boost::beast::http::field::method_check;
  using boost::beast::http::field::method_check_expires;
  using boost::beast::http::field::mime_version;
  using boost::beast::http::field::mmhs_acp127_message_identifier;
  using boost::beast::http::field::mmhs_authorizing_users;
  using boost::beast::http::field::mmhs_codress_message_indicator;
  using boost::beast::http::field::mmhs_copy_precedence;
  using boost::beast::http::field::mmhs_exempted_address;
  using boost::beast::http::field::mmhs_extended_authorisation_info;
  using boost::beast::http::field::mmhs_handling_instructions;
  using boost::beast::http::field::mmhs_message_instructions;
  using boost::beast::http::field::mmhs_message_type;
  using boost::beast::http::field::mmhs_originator_plad;
  using boost::beast::http::field::mmhs_originator_reference;
  using boost::beast::http::field::mmhs_other_recipients_indicator_cc;
  using boost::beast::http::field::mmhs_other_recipients_indicator_to;
  using boost::beast::http::field::mmhs_primary_precedence;
  using boost::beast::http::field::mmhs_subject_indicator_codes;
  using boost::beast::http::field::mt_priority;
  using boost::beast::http::field::negotiate;
  using boost::beast::http::field::newsgroups;
  using boost::beast::http::field::nntp_posting_date;
  using boost::beast::http::field::nntp_posting_host;
  using boost::beast::http::field::non_compliance;
  using boost::beast::http::field::obsoletes;
  using boost::beast::http::field::opt;
  using boost::beast::http::field::optional;
  using boost::beast::http::field::optional_www_authenticate;
  using boost::beast::http::field::ordering_type;
  using boost::beast::http::field::organization;
  using boost::beast::http::field::origin;
  using boost::beast::http::field::original_encoded_information_types;
  using boost::beast::http::field::original_from;
  using boost::beast::http::field::original_message_id;
  using boost::beast::http::field::original_recipient;
  using boost::beast::http::field::original_sender;
  using boost::beast::http::field::original_subject;
  using boost::beast::http::field::originator_return_address;
  using boost::beast::http::field::overwrite;
  using boost::beast::http::field::p3p;
  using boost::beast::http::field::path;
  using boost::beast::http::field::pep;
  using boost::beast::http::field::pep_info;
  using boost::beast::http::field::pics_label;
  using boost::beast::http::field::position;
  using boost::beast::http::field::posting_version;
  using boost::beast::http::field::pragma;
  using boost::beast::http::field::prefer;
  using boost::beast::http::field::preference_applied;
  using boost::beast::http::field::prevent_nondelivery_report;
  using boost::beast::http::field::priority;
  using boost::beast::http::field::privicon;
  using boost::beast::http::field::profileobject;
  using boost::beast::http::field::protocol;
  using boost::beast::http::field::protocol_info;
  using boost::beast::http::field::protocol_query;
  using boost::beast::http::field::protocol_request;
  using boost::beast::http::field::proxy_authenticate;
  using boost::beast::http::field::proxy_authentication_info;
  using boost::beast::http::field::proxy_authorization;
  using boost::beast::http::field::proxy_connection;
  using boost::beast::http::field::proxy_features;
  using boost::beast::http::field::proxy_instruction;
  using boost::beast::http::field::public_;
  using boost::beast::http::field::public_key_pins;
  using boost::beast::http::field::public_key_pins_report_only;
  using boost::beast::http::field::range;
  using boost::beast::http::field::received;
  using boost::beast::http::field::received_spf;
  using boost::beast::http::field::redirect_ref;
  using boost::beast::http::field::references;
  using boost::beast::http::field::referer;
  using boost::beast::http::field::referer_root;
  using boost::beast::http::field::relay_version;
  using boost::beast::http::field::reply_by;
  using boost::beast::http::field::reply_to;
  using boost::beast::http::field::require_recipient_valid_since;
  using boost::beast::http::field::resent_bcc;
  using boost::beast::http::field::resent_cc;
  using boost::beast::http::field::resent_date;
  using boost::beast::http::field::resent_from;
  using boost::beast::http::field::resent_message_id;
  using boost::beast::http::field::resent_reply_to;
  using boost::beast::http::field::resent_sender;
  using boost::beast::http::field::resent_to;
  using boost::beast::http::field::resolution_hint;
  using boost::beast::http::field::resolver_location;
  using boost::beast::http::field::retry_after;
  using boost::beast::http::field::return_path;
  using boost::beast::http::field::safe;
  using boost::beast::http::field::schedule_reply;
  using boost::beast::http::field::schedule_tag;
  using boost::beast::http::field::sec_fetch_dest;
  using boost::beast::http::field::sec_fetch_mode;
  using boost::beast::http::field::sec_fetch_site;
  using boost::beast::http::field::sec_fetch_user;
  using boost::beast::http::field::sec_websocket_accept;
  using boost::beast::http::field::sec_websocket_extensions;
  using boost::beast::http::field::sec_websocket_key;
  using boost::beast::http::field::sec_websocket_protocol;
  using boost::beast::http::field::sec_websocket_version;
  using boost::beast::http::field::security_scheme;
  using boost::beast::http::field::see_also;
  using boost::beast::http::field::sender;
  using boost::beast::http::field::sensitivity;
  using boost::beast::http::field::server;
  using boost::beast::http::field::set_cookie;
  using boost::beast::http::field::set_cookie2;
  using boost::beast::http::field::setprofile;
  using boost::beast::http::field::sio_label;
  using boost::beast::http::field::sio_label_history;
  using boost::beast::http::field::slug;
  using boost::beast::http::field::soapaction;
  using boost::beast::http::field::solicitation;
  using boost::beast::http::field::status_uri;
  using boost::beast::http::field::strict_transport_security;
  using boost::beast::http::field::subject;
  using boost::beast::http::field::subok;
  using boost::beast::http::field::subst;
  using boost::beast::http::field::summary;
  using boost::beast::http::field::supersedes;
  using boost::beast::http::field::surrogate_capability;
  using boost::beast::http::field::surrogate_control;
  using boost::beast::http::field::tcn;
  using boost::beast::http::field::te;  // codespell-ignore
  using boost::beast::http::field::timeout;
  using boost::beast::http::field::title;
  using boost::beast::http::field::to;
  using boost::beast::http::field::topic;
  using boost::beast::http::field::trailer;
  using boost::beast::http::field::transfer_encoding;
  using boost::beast::http::field::ttl;
  using boost::beast::http::field::ua_color;
  using boost::beast::http::field::ua_media;
  using boost::beast::http::field::ua_pixels;
  using boost::beast::http::field::ua_resolution;
  using boost::beast::http::field::ua_windowpixels;
  using boost::beast::http::field::unknown;
  using boost::beast::http::field::upgrade;
  using boost::beast::http::field::urgency;
  using boost::beast::http::field::uri;
  using boost::beast::http::field::user_agent;
  using boost::beast::http::field::variant_vary;
  using boost::beast::http::field::vary;
  using boost::beast::http::field::vbr_info;
  using boost::beast::http::field::version;
  using boost::beast::http::field::via;
  using boost::beast::http::field::want_digest;
  using boost::beast::http::field::warning;
  using boost::beast::http::field::www_authenticate;
  using boost::beast::http::field::x400_content_identifier;
  using boost::beast::http::field::x400_content_return;
  using boost::beast::http::field::x400_content_type;
  using boost::beast::http::field::x400_mts_identifier;
  using boost::beast::http::field::x400_originator;
  using boost::beast::http::field::x400_received;
  using boost::beast::http::field::x400_recipients;
  using boost::beast::http::field::x400_trace;
  using boost::beast::http::field::x_archived_at;
  using boost::beast::http::field::x_device_accept;
  using boost::beast::http::field::x_device_accept_charset;
  using boost::beast::http::field::x_device_accept_encoding;
  using boost::beast::http::field::x_device_accept_language;
  using boost::beast::http::field::x_device_user_agent;
  using boost::beast::http::field::x_frame_options;
  using boost::beast::http::field::x_mittente;
  using boost::beast::http::field::x_pgp_sig;
  using boost::beast::http::field::x_ricevuta;
  using boost::beast::http::field::x_riferimento_message_id;
  using boost::beast::http::field::x_tiporicevuta;
  using boost::beast::http::field::x_trasporto;
  using boost::beast::http::field::x_verificasicurezza;
  using boost::beast::http::field::xref;
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
