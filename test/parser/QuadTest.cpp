// Copyright 2025, University of Freiburg
// Chair of Algorithms and Data Structures
// Author: Julian Mundhahs <mundhahj@tf.uni-freiburg.de>

#include <gtest/gtest.h>

#include "../util/GTestHelpers.h"
#include "./SparqlAntlrParserTestHelpers.h"
#include "parser/Quads.h"

namespace tc {
auto Iri = ad_utility::triple_component::Iri::fromIriref;
}

// _____________________________________________________________________________
TEST(QuadTest, getQuads) {
  auto expectGetQuads =
      [](ad_utility::sparql_types::Triples triples,
         std::vector<Quads::GraphBlock> graphs,
         const std::vector<SparqlTripleSimpleWithGraph>& expected,
         ad_utility::source_location l =
             ad_utility::source_location::current()) {
        auto t = generateLocationTrace(l);
        // For this test, there are no blank nodes. Below you find a dedicated
        // test with blank nodes.
        ad_utility::BlankNodeManager manager;
        Quads::BlankNodeAdder bn{{}, {}, &manager};
        const Quads quads{std::move(triples), std::move(graphs)};
        auto res = quads.toTriplesWithGraph(std::monostate{}, bn);
        EXPECT_THAT(res.triples_, testing::UnorderedElementsAreArray(expected));
        EXPECT_EQ(manager.numBlocksUsed(), 0);
      };
  auto TripleOf = [](const GraphTerm& t) -> std::array<GraphTerm, 3> {
    return {t, t, t};
  };
  auto QuadOf = [](const TripleComponent& c,
                   const SparqlTripleSimpleWithGraph::Graph& g) {
    return SparqlTripleSimpleWithGraph(c, c, c, g);
  };
  expectGetQuads({}, {}, {});
  expectGetQuads({TripleOf(Iri("<a>"))}, {},
                 {QuadOf(tc::Iri("<a>"), std::monostate{})});
  expectGetQuads({TripleOf(Iri("<a>"))},
                 {{tc::Iri("<b>"), {TripleOf(Iri("<a>"))}}},
                 {QuadOf(tc::Iri("<a>"), std::monostate{}),
                  QuadOf(tc::Iri("<a>"), tc::Iri("<b>"))});
  expectGetQuads(
      {TripleOf(Iri("<a>")), TripleOf(Iri("<d>"))},
      {{tc::Iri("<b>"), {TripleOf(Iri("<a>"))}},
       {tc::Iri("<b>"), {TripleOf(Iri("<b>")), TripleOf(Iri("<c>"))}}},
      {QuadOf(tc::Iri("<a>"), std::monostate{}),
       QuadOf(tc::Iri("<d>"), std::monostate{}),
       QuadOf(tc::Iri("<a>"), tc::Iri("<b>")),
       QuadOf(tc::Iri("<b>"), tc::Iri("<b>")),
       QuadOf(tc::Iri("<c>"), tc::Iri("<b>"))});
}

// _____________________________________________________________________________
TEST(QuadTest, getQuadsWithBlankNodes) {
  auto bn = [](std::string_view s) {
    return GraphTerm{BlankNode{false, std::string{s}}};
  };

  std::array tr{bn("a"), bn("b"), bn("a")};
  ad_utility::BlankNodeManager manager;
  Quads::BlankNodeAdder adder{{}, {}, &manager};
  const Quads quads{{tr}, {}};
  auto res = quads.toTriplesWithGraph(std::monostate{}, adder);
  EXPECT_EQ(res.triples_.size(), 1ul);
  const auto& triple = res.triples_.at(0);
  EXPECT_EQ(triple.s_, triple.o_);
  EXPECT_NE(triple.p_, triple.o_);
  EXPECT_EQ(triple.s_.getId().getDatatype(), Datatype::BlankNodeIndex);
  EXPECT_EQ(triple.p_.getId().getDatatype(), Datatype::BlankNodeIndex);
  EXPECT_EQ(triple.o_.getId().getDatatype(), Datatype::BlankNodeIndex);

  EXPECT_TRUE(res.localVocab_.isBlankNodeIndexContained(
      triple.s_.getId().getBlankNodeIndex()));
  EXPECT_TRUE(res.localVocab_.isBlankNodeIndexContained(
      triple.p_.getId().getBlankNodeIndex()));
  EXPECT_TRUE(res.localVocab_.isBlankNodeIndexContained(
      triple.o_.getId().getBlankNodeIndex()));
  EXPECT_GT(manager.numBlocksUsed(), 0);
}

TEST(QuadTest, getOperations) {
  auto expectGetQuads =
      [](ad_utility::sparql_types::Triples triples,
         std::vector<Quads::GraphBlock> graphs,
         const testing::Matcher<
             std::vector<parsedQuery::GraphPatternOperation>>& m,
         ad_utility::source_location l =
             ad_utility::source_location::current()) {
        auto t = generateLocationTrace(l);
        const Quads quads{std::move(triples), std::move(graphs)};
        EXPECT_THAT(quads.toGraphPatternOperations(), m);
      };
  auto TripleOf = [](const GraphTerm& t) -> std::array<GraphTerm, 3> {
    return {t, t, t};
  };
  auto SparqlTriple = [](const TripleComponent& t) -> ::SparqlTriple {
    return {t, t.getIri(), t};
  };
  auto GraphTriples =
      [](const std::vector<::SparqlTriple>& triples,
         const parsedQuery::GroupGraphPattern::GraphSpec& graph) {
        return matchers::GroupGraphPatternWithGraph({}, graph,
                                                    matchers::Triples(triples));
      };
  expectGetQuads({}, {}, ElementsAre(matchers::Triples({})));
  expectGetQuads(
      {TripleOf(Iri("<a>"))}, {},
      ElementsAre(matchers::Triples({SparqlTriple(tc::Iri("<a>"))})));
  expectGetQuads(
      {TripleOf(Iri("<a>"))}, {{tc::Iri("<b>"), {TripleOf(Iri("<a>"))}}},
      ElementsAre(
          matchers::Triples({SparqlTriple(tc::Iri("<a>"))}),
          GraphTriples({SparqlTriple(tc::Iri("<a>"))}, tc::Iri("<b>"))));
  expectGetQuads(
      {TripleOf(Iri("<a>")), TripleOf(Iri("<d>"))},
      {{tc::Iri("<b>"), {TripleOf(Iri("<a>"))}},
       {tc::Iri("<b>"), {TripleOf(Iri("<b>")), TripleOf(Iri("<c>"))}}},
      ElementsAre(matchers::Triples({SparqlTriple(tc::Iri("<a>")),
                                     SparqlTriple(tc::Iri("<d>"))}),
                  GraphTriples({SparqlTriple(tc::Iri("<a>"))}, tc::Iri("<b>")),
                  GraphTriples({SparqlTriple(tc::Iri("<b>")),
                                SparqlTriple(tc::Iri("<c>"))},
                               tc::Iri("<b>"))));
}

TEST(QuadTest, forAllVariables) {
  auto expectForAllVariables =
      [](Quads quads, const ad_utility::HashSet<Variable>& expectVariables,
         ad_utility::source_location l =
             ad_utility::source_location::current()) {
        auto t = generateLocationTrace(l);
        ad_utility::HashSet<Variable> calledVariables;
        quads.forAllVariables([&calledVariables](const Variable& var) {
          calledVariables.insert(var);
        });
        EXPECT_THAT(calledVariables, testing::Eq(expectVariables));
      };
  auto TCIri = ad_utility::triple_component::Iri::fromIriref;
  using Var = Variable;

  using Triple = std::array<GraphTerm, 3>;
  Triple noVars{GraphTerm(Iri("<a>")), GraphTerm(Iri("<b>")),
                GraphTerm(Iri("<c>"))};
  Triple differentVars{GraphTerm(Var("?a")), GraphTerm(Var("?b")),
                       GraphTerm(Var("?c"))};
  Triple sameVar{GraphTerm(Var("?a")), GraphTerm(Var("?a")),
                 GraphTerm(Var("?a"))};
  expectForAllVariables({}, {});
  expectForAllVariables({{noVars}, {}}, {});
  expectForAllVariables({{differentVars}, {}},
                        {Var("?a"), Var("?b"), Var("?c")});
  expectForAllVariables({{sameVar}, {}}, {Var("?a")});
  expectForAllVariables({{}, {{{TCIri("<a>"), {}}}}}, {});
  expectForAllVariables({{}, {{{TCIri("<a>"), {noVars}}}}}, {});
  expectForAllVariables({{}, {{{TCIri("<a>"), {differentVars}}}}},
                        {Var("?a"), Var("?b"), Var("?c")});
  expectForAllVariables({{}, {{{TCIri("<a>"), {sameVar}}}}}, {Var("?a")});
  // Even if the graph block is empty, the variable is still omitted.
  expectForAllVariables({{}, {{{Var("?d"), {}}}}}, {Var("?d")});
  expectForAllVariables(
      {{noVars, differentVars, sameVar}, {{{Var("?d"), {differentVars}}}}},
      {Var("?a"), Var("?b"), Var("?c"), Var("?d")});
}
