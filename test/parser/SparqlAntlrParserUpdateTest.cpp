// Copyright 2025, University of Freiburg
// Chair of Algorithms and Data Structures
// Authors: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>
//          Julian Mundhahs <mundhahj@cs.uni-freiburg.de>
#include "../util/TripleComponentTestHelpers.h"
#include "./SparqlAntlrParserTestHelpers.h"
#include "parser/TripleComponent.h"
#include "rdfTypes/Iri.h"

using namespace sparqlParserTestHelpers;
namespace {
auto iri = ad_utility::testing::iri;

// _____________________________________________________________________________
TEST(SparqlParser, Update) {
  auto expectUpdate_ = ExpectCompleteParse<&Parser::update>{defaultPrefixMap};
  // Automatically test all updates for their `_originalString`.
  auto expectUpdate = [&expectUpdate_](
                          const std::string& query, auto&& expected,
                          ad_utility::source_location l =
                              ad_utility::source_location::current()) {
    expectUpdate_(query,
                  testing::ElementsAre(
                      testing::AllOf(expected, m::pq::OriginalString(query))),
                  l);
  };
  auto expectUpdateFails = ExpectParseFails<&Parser::update>{};
  auto Iri = [](std::string_view stringWithBrackets) {
    return TripleComponent::Iri::fromIriref(stringWithBrackets);
  };
  auto Literal = [](std::string s) {
    return TripleComponent::Literal::fromStringRepresentation(std::move(s));
  };
  auto noGraph = std::monostate{};

  // Test the parsing of the update clause in the ParsedQuery.
  expectUpdate(
      "INSERT DATA { <a> <b> <c> }",
      m::UpdateClause(
          m::GraphUpdate({}, {{Iri("<a>"), Iri("<b>"), Iri("<c>"), noGraph}}),
          m::GraphPattern()));
  expectUpdate(
      "INSERT DATA { <a> <b> \"foo:bar\" }",
      m::UpdateClause(m::GraphUpdate({}, {{Iri("<a>"), Iri("<b>"),
                                           Literal("\"foo:bar\""), noGraph}}),
                      m::GraphPattern()));
  expectUpdate(
      "DELETE DATA { <a> <b> <c> }",
      m::UpdateClause(
          m::GraphUpdate({{Iri("<a>"), Iri("<b>"), Iri("<c>"), noGraph}}, {}),
          m::GraphPattern()));
  expectUpdate(
      "DELETE { ?a <b> <c> } WHERE { <d> <e> ?a }",
      m::UpdateClause(
          m::GraphUpdate({{Var("?a"), Iri("<b>"), Iri("<c>"), noGraph}}, {}),
          m::GraphPattern(m::Triples({{Iri("<d>"), iri("<e>"), Var{"?a"}}}))));
  // Use variables that are not visible in the query body. Do this for all parts
  // of the quad for coverage reasons.
  expectUpdateFails("DELETE { ?a <b> <c> } WHERE { <a> ?b ?c }");
  expectUpdateFails("DELETE { <c> <d> <c> . <e> ?a <f> } WHERE { <a> ?b ?c }");
  expectUpdateFails(
      "DELETE { GRAPH <foo> { <c> <d> <c> . <e> <f> ?a } } WHERE { <a> ?b ?c "
      "}");
  expectUpdateFails("DELETE { GRAPH ?a { <c> <d> <c> } } WHERE { <a> ?b ?c }");
  expectUpdate(
      "DELETE { ?a <b> <c> } INSERT { <a> ?a <c> } WHERE { <d> <e> ?a }",
      m::UpdateClause(
          m::GraphUpdate({{Var("?a"), Iri("<b>"), Iri("<c>"), noGraph}},
                         {{Iri("<a>"), Var("?a"), Iri("<c>"), noGraph}}),
          m::GraphPattern(m::Triples({{Iri("<d>"), iri("<e>"), Var{"?a"}}}))));
  expectUpdate(
      "DELETE WHERE { ?a <foo> ?c }",
      m::UpdateClause(
          m::GraphUpdate({{Var("?a"), Iri("<foo>"), Var("?c"), noGraph}}, {}),
          m::GraphPattern(m::Triples({{Var{"?a"}, iri("<foo>"), Var{"?c"}}}))));
  expectUpdateFails("INSERT DATA { ?a ?b ?c }");  // Variables are not allowed
  // inside INSERT DATA.
  expectUpdate(
      "WITH <foo> DELETE { ?a ?b ?c } WHERE { ?a ?b ?c }",
      m::UpdateClause(
          m::GraphUpdate({{Var("?a"), Var("?b"), Var("?c"), Iri("<foo>")}}, {}),
          m::GraphPattern(m::Triples({{Var{"?a"}, Var{"?b"}, Var{"?c"}}})),
          m::datasetClausesMatcher(m::Graphs{TripleComponent(Iri("<foo>"))},
                                   std::nullopt)));
  expectUpdate(
      "DELETE { ?a ?b ?c } USING <foo> WHERE { ?a ?b ?c }",
      m::UpdateClause(
          m::GraphUpdate({{Var("?a"), Var("?b"), Var("?c"), noGraph}}, {}),
          m::GraphPattern(m::Triples({{Var{"?a"}, Var{"?b"}, Var{"?c"}}})),
          m::datasetClausesMatcher(m::Graphs{TripleComponent(Iri("<foo>"))},
                                   m::Graphs{})));
  expectUpdate("INSERT DATA { GRAPH <foo> { } }",
               m::UpdateClause(m::GraphUpdate({}, {}), m::GraphPattern()));
  expectUpdate("INSERT DATA { GRAPH <foo> { <a> <b> <c> } }",
               m::UpdateClause(m::GraphUpdate({}, {{Iri("<a>"), Iri("<b>"),
                                                    Iri("<c>"), Iri("<foo>")}}),
                               m::GraphPattern()));
  expectUpdateFails(
      "INSERT DATA { GRAPH ?f { } }",
      testing::HasSubstr(
          "Invalid SPARQL query: Variables (?f) are not allowed here."));
  expectUpdate(
      "DELETE { ?a <b> <c> } USING NAMED <foo> WHERE { <d> <e> ?a }",
      m::UpdateClause(
          m::GraphUpdate({{Var("?a"), Iri("<b>"), Iri("<c>"), noGraph}}, {}),
          m::GraphPattern(m::Triples({{Iri("<d>"), iri("<e>"), Var{"?a"}}})),
          m::datasetClausesMatcher(m::Graphs{},
                                   m::Graphs{TripleComponent(Iri("<foo>"))})));
  expectUpdate(
      "WITH <foo> DELETE { ?a <b> <c> } WHERE { <d> <e> ?a }",
      m::UpdateClause(
          m::GraphUpdate({{Var("?a"), Iri("<b>"), Iri("<c>"), Iri("<foo>")}},
                         {}),
          m::GraphPattern(m::Triples({{Iri("<d>"), iri("<e>"), Var{"?a"}}})),
          m::datasetClausesMatcher(m::Graphs{TripleComponent(Iri("<foo>"))},
                                   std::nullopt)));
  const auto insertMatcher = m::UpdateClause(
      m::GraphUpdate({}, {{Iri("<a>"), Iri("<b>"), Iri("<c>"), noGraph}}),
      m::GraphPattern());
  const auto fooInsertMatcher = m::UpdateClause(
      m::GraphUpdate(
          {}, {{Iri("<foo/a>"), Iri("<foo/b>"), Iri("<foo/c>"), noGraph}}),
      m::GraphPattern());
  const auto deleteWhereAllMatcher = m::UpdateClause(
      m::GraphUpdate({{Var("?s"), Var("?p"), Var("?o"), noGraph}}, {}),
      m::GraphPattern(m::Triples({{Var("?s"), Var{"?p"}, Var("?o")}})));
  expectUpdate("INSERT DATA { <a> <b> <c> }", insertMatcher);
  // Multiple Updates
  expectUpdate_(
      "INSERT DATA { <a> <b> <c> };",
      ElementsAre(AllOf(insertMatcher,
                        m::pq::OriginalString("INSERT DATA { <a> <b> <c> }"))));
  expectUpdate_(
      "INSERT DATA { <a> <b> <c> }; BASE <https://example.org> PREFIX foo: "
      "<foo>",
      ElementsAre(AllOf(insertMatcher,
                        m::pq::OriginalString("INSERT DATA { <a> <b> <c> }"))));
  expectUpdate_(
      "INSERT DATA { <a> <b> <c> }; DELETE WHERE { ?s ?p ?o }",
      ElementsAre(AllOf(insertMatcher,
                        m::pq::OriginalString("INSERT DATA { <a> <b> <c> }")),
                  AllOf(deleteWhereAllMatcher,
                        m::pq::OriginalString("DELETE WHERE { ?s ?p ?o }"))));
  expectUpdate_(
      "PREFIX foo: <foo/> INSERT DATA { <a> <b> <c> }; INSERT DATA { foo:a "
      "foo:b foo:c }",
      ElementsAre(
          AllOf(insertMatcher,
                m::pq::OriginalString(
                    "PREFIX foo: <foo/> INSERT DATA { <a> <b> <c> }")),
          AllOf(fooInsertMatcher,
                m::pq::OriginalString("INSERT DATA { foo:a foo:b foo:c }"))));
  expectUpdate_(
      "PREFIX foo: <bar/> INSERT DATA { <a> <b> <c> }; PREFIX foo: <foo/> "
      "INSERT DATA { foo:a foo:b foo:c }",
      ElementsAre(
          AllOf(insertMatcher,
                m::pq::OriginalString(
                    "PREFIX foo: <bar/> INSERT DATA { <a> <b> <c> }")),
          AllOf(fooInsertMatcher,
                m::pq::OriginalString(
                    "PREFIX foo: <foo/> INSERT DATA { foo:a foo:b foo:c }"))));
  expectUpdate_("", testing::IsEmpty());
  expectUpdate_(" ", testing::IsEmpty());
  expectUpdate_("PREFIX ex: <http://example.org>", testing::IsEmpty());
  expectUpdate_("INSERT DATA { <a> <b> <c> }; PREFIX ex: <http://example.org>",
                testing::ElementsAre(insertMatcher));
  expectUpdate_("### Some comment \n \n #someMoreComments", testing::IsEmpty());
  expectUpdate_(
      "INSERT DATA { <a> <b> <c> };### Some comment \n \n #someMoreComments",
      testing::ElementsAre(insertMatcher));
}

// _____________________________________________________________________________
TEST(SparqlParser, Create) {
  auto expectCreate = ExpectCompleteParse<&Parser::create>{defaultPrefixMap};
  auto expectCreateFails = ExpectParseFails<&Parser::create>{defaultPrefixMap};

  expectCreate("CREATE GRAPH <foo>", testing::IsEmpty());
  expectCreate("CREATE SILENT GRAPH <foo>", testing::IsEmpty());
  expectCreateFails("CREATE <foo>");
  expectCreateFails("CREATE ?foo");
}

// _____________________________________________________________________________
TEST(SparqlParser, Add) {
  auto expectAdd = ExpectCompleteParse<&Parser::add>{defaultPrefixMap};
  auto expectAddFails = ExpectParseFails<&Parser::add>{defaultPrefixMap};
  auto Iri = TripleComponent::Iri::fromIriref;

  auto addMatcher = ElementsAre(m::AddAll(Iri("<foo>"), Iri("<bar>")));
  expectAdd("ADD GRAPH <baz> to GRAPH <baz>", IsEmpty());
  expectAdd("ADD DEFAULT TO DEFAULT", IsEmpty());
  expectAdd("ADD GRAPH <foo> TO GRAPH <bar>", addMatcher);
  expectAdd("ADD SILENT GRAPH <foo> TO <bar>", addMatcher);
  expectAdd("ADD <foo> to DEFAULT",
            ElementsAre(m::AddAll(Iri("<foo>"), Iri(DEFAULT_GRAPH_IRI))));
  expectAdd("ADD GRAPH <foo> to GRAPH <foo>", testing::IsEmpty());
  expectAddFails("ADD ALL TO NAMED");
}

// _____________________________________________________________________________
TEST(SparqlParser, Clear) {
  auto expectClear = ExpectCompleteParse<&Parser::clear>{defaultPrefixMap};
  auto expectClearFails = ExpectParseFails<&Parser::clear>{defaultPrefixMap};
  auto Iri = TripleComponent::Iri::fromIriref;

  expectClear("CLEAR ALL", m::Clear(Variable("?g")));
  expectClear("CLEAR SILENT GRAPH <foo>", m::Clear(Iri("<foo>")));
  expectClear("CLEAR NAMED", m::Clear(Variable("?g"),
                                      "?g != "
                                      "<http://qlever.cs.uni-freiburg.de/"
                                      "builtin-functions/default-graph>"));
  expectClear("CLEAR DEFAULT", m::Clear(Iri(DEFAULT_GRAPH_IRI)));
}

// _____________________________________________________________________________
TEST(SparqlParser, Drop) {
  // TODO: deduplicate with clear which is the same in our case (implicit
  // graph existence)
  auto expectDrop = ExpectCompleteParse<&Parser::drop>{defaultPrefixMap};
  auto expectDropFails = ExpectParseFails<&Parser::drop>{defaultPrefixMap};
  auto Iri = TripleComponent::Iri::fromIriref;

  expectDrop("DROP ALL", m::Clear(Variable("?g")));
  expectDrop("DROP SILENT GRAPH <foo>", m::Clear(Iri("<foo>")));
  expectDrop("DROP NAMED", m::Clear(Variable("?g"),
                                    "?g != "
                                    "<http://qlever.cs.uni-freiburg.de/"
                                    "builtin-functions/default-graph>"));
  expectDrop("DROP DEFAULT", m::Clear(Iri(DEFAULT_GRAPH_IRI)));
}

// _____________________________________________________________________________
TEST(SparqlParser, Move) {
  auto expectMove = ExpectCompleteParse<&Parser::move>{defaultPrefixMap};
  auto expectMoveFails = ExpectParseFails<&Parser::move>{defaultPrefixMap};
  auto Iri = TripleComponent::Iri::fromIriref;

  // Moving a graph onto itself changes nothing
  expectMove("MOVE SILENT DEFAULT TO DEFAULT", testing::IsEmpty());
  expectMove("MOVE GRAPH <foo> TO <foo>", testing::IsEmpty());
  expectMove("MOVE GRAPH <foo> TO DEFAULT",
             ElementsAre(m::Clear(Iri(DEFAULT_GRAPH_IRI)),
                         m::AddAll(Iri("<foo>"), Iri(DEFAULT_GRAPH_IRI)),
                         m::Clear(Iri("<foo>"))));
}

// _____________________________________________________________________________
TEST(SparqlParser, Copy) {
  auto expectCopy = ExpectCompleteParse<&Parser::copy>{defaultPrefixMap};
  auto expectCopyFails = ExpectParseFails<&Parser::copy>{defaultPrefixMap};
  auto Iri = TripleComponent::Iri::fromIriref;

  // Copying a graph onto itself changes nothing
  expectCopy("COPY SILENT DEFAULT TO DEFAULT", testing::IsEmpty());
  expectCopy("COPY GRAPH <foo> TO <foo>", testing::IsEmpty());
  expectCopy("COPY DEFAULT TO GRAPH <foo>",
             ElementsAre(m::Clear(Iri("<foo>")),
                         m::AddAll(Iri(DEFAULT_GRAPH_IRI), Iri("<foo>"))));
}

// _____________________________________________________________________________
TEST(SparqlParser, Load) {
  auto expectLoad = ExpectCompleteParse<&Parser::load>{defaultPrefixMap};
  auto Iri = [](std::string_view stringWithBrackets) {
    return TripleComponent::Iri::fromIriref(stringWithBrackets);
  };
  auto noGraph = std::monostate{};

  expectLoad(
      "LOAD <https://example.com>",
      m::UpdateClause(
          m::GraphUpdate({}, {SparqlTripleSimpleWithGraph{Var("?s"), Var("?p"),
                                                          Var("?o"), noGraph}}),
          m::GraphPattern(m::Load(Iri("<https://example.com>"), false))));
  expectLoad("LOAD SILENT <http://example.com> into GRAPH <bar>",
             m::UpdateClause(
                 m::GraphUpdate(
                     {}, {SparqlTripleSimpleWithGraph{
                             Var("?s"), Var("?p"), Var("?o"), Iri("<bar>")}}),
                 m::GraphPattern(m::Load(Iri("<http://example.com>"), true))));
}

// The following section consists of matchers for the behavior of blank nodes
// in SPARQL update.

// _____________________________________________________________________________
MATCHER(SoEqual, "Assert that the subject and object of a triple are equal") {
  return arg.s_ == arg.o_;
}

// _____________________________________________________________________________
MATCHER(SpNotEqual,
        "Assert that the subject and object of a triple are NOT equal") {
  return arg.s_ != arg.p_;
}

// Assert that a triple component stores an ID with datatype `BlankNodeIndex`.
auto isBlank = []() -> testing::Matcher<const TripleComponent&> {
  using namespace testing;
  return AD_PROPERTY(TripleComponent, getVariant,
                     VariantWith<Id>(AD_PROPERTY(
                         Id, getDatatype, Eq(Datatype::BlankNodeIndex))));
};

// Assert that the subject and the object of a triple are blank nodes.
// Note: The SPARQL grammar forbids predicates that are blank.
auto bnodeTriple =
    []() -> testing::Matcher<const SparqlTripleSimpleWithGraph&> {
  using namespace testing;
  using Tr = SparqlTripleSimpleWithGraph;
  return AllOf(AD_FIELD(Tr, s_, isBlank()), AD_FIELD(Tr, o_, isBlank()),
               Not(AD_FIELD(Tr, p_, isBlank())));
};

// Assert that for all triples in a `vector<SparqlTripleSimpleWithGraph>` the
// subject is the same, and is an ID with datatype `BlankNodeIndex`.
MATCHER(allSubjectsTheSameAndBlank,
        "check that the subjects of the triples all are the same blank node") {
  for (const auto& triple : arg) {
    if (triple.s_ != arg.at(0).s_) {
      return false;
    }
    if (!triple.s_.isId() ||
        triple.s_.getId().getDatatype() != Datatype::BlankNodeIndex) {
      return false;
    }
  }
  return true;
}

// Assert that for all triples in a `vector<SparqlTripleSimpleWithGraph>` the
// subjects are pairwise disjoint but all are IDs with datatype
// `BlankNodeIndex`.
MATCHER(allSubjectsDifferentAndBlank,
        "check that the subjects of the triples all are different blank node") {
  ad_utility::HashSet<Id> ids;
  for (const auto& triple : arg) {
    if (!triple.s_.isId() ||
        triple.s_.getId().getDatatype() != Datatype::BlankNodeIndex) {
      return false;
    }
    ids.insert(triple.s_.getId());
  }
  return ids.size() == arg.size();
}

// Test the behavior of blank nodes in UPDATE requests.
TEST(SparqlParser, BlankNodesInUpdate) {
  auto expectUpdate = ExpectCompleteParse<&Parser::update>{defaultPrefixMap};

  // In the following tests we only check the triples, not the `LocalVocab` of
  // the `UpdateTriples` (This is tested in isolation in
  // `UpdateTriplesTest.cpp`).
  static constexpr auto getVec =
      [](const updateClause::UpdateTriples& tr) -> decltype(auto) {
    // Note: we have to use a lambda and can't use a function-to-member pointer
    // because of problems in GTest internals.
    return tr.triples_;
  };

  using namespace testing;
  // Match update triples with a single triple where the subject and object are
  // the same blank node.
  auto matchBpB = []() -> Matcher<const updateClause::UpdateTriples&> {
    return ResultOf(getVec,
                    ElementsAre(AllOf(SoEqual(), SpNotEqual(), bnodeTriple())));
  };
  // Match empty update triples.
  const auto empty = []() -> Matcher<const updateClause::UpdateTriples&> {
    return ResultOf(getVec, IsEmpty());
  }();

  // Simple check that the duplicate usage of blank node label `_:b` is
  // consistently mapped to the same ID.
  expectUpdate(
      "INSERT DATA { _:b <p> _:b}",
      ElementsAre(m::UpdateClause(m::MatchGraphUpdate(empty, matchBpB()),
                                  ::m::GraphPattern())));

  // Blank nodes in the pattern remain blank nodes, but in the where clause they
  // become internal variables.
  Var internalVar{"?_QLever_internal_variable_bn_b"};
  expectUpdate("INSERT { _:b <p> _:b} WHERE { _:b <p> _:b}",
               ElementsAre(m::UpdateClause(
                   m::MatchGraphUpdate(empty, matchBpB()),
                   m::GraphPattern(m::OrderedTriples(
                       {{internalVar, iri("<p>"), internalVar}})))));

  // Test that the blank node mapping is also consistent between different
  // `GRAPH` blocks.
  expectUpdate(
      "INSERT DATA { GRAPH <g1>  { _:b <p> <o>. _:b <p2> <o2> }"
      "GRAPH <g2>  { _:b <p> <o> } }",
      ElementsAre(m::UpdateClause(
          m::MatchGraphUpdate(empty,
                              ResultOf(getVec, allSubjectsTheSameAndBlank())),
          ::m::GraphPattern())));

  // Test that different blank node labels lead to different IDs.
  expectUpdate(
      "INSERT DATA { GRAPH <g1>  { _:b <p> <o>. _:c <p2> <o2> }"
      "GRAPH <g2>  { _:d <p> <o> } }",
      ElementsAre(m::UpdateClause(
          m::MatchGraphUpdate(empty,
                              ResultOf(getVec, allSubjectsDifferentAndBlank())),
          ::m::GraphPattern())));

  auto expectUpdateFails = ExpectParseFails<&Parser::update>{};

  // DELETE with blank nodes is forbidden by the SPARQL standard.
  expectUpdateFails("DELETE WHERE { _:b <p> _:b}");
  expectUpdateFails("DELETE DATA { _:b <p> _:b}");
}
}  // namespace
