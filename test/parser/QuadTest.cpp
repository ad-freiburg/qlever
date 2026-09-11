// Copyright 2025, University of Freiburg
// Chair of Algorithms and Data Structures
// Author: Julian Mundhahs <mundhahj@tf.uni-freiburg.de>

#include <gtest/gtest.h>

#include "../util/GTestHelpers.h"
#include "./SparqlAntlrParserTestHelpers.h"
#include "parser/Quads.h"

using namespace ad_utility::testing;
// _____________________________________________________________________________
TEST(QuadTest, getQuads) {
  auto expectGetQuads =
      [](ad_utility::sparql_types::Triples triples,
         std::vector<Quads::GraphBlock> graphs,
         const std::vector<SparqlTripleSimpleWithGraph>& expected,
         ad_utility::source_location l = AD_CURRENT_SOURCE_LOC()) {
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
  expectGetQuads({TripleOf(iri("<a>"))}, {},
                 {QuadOf(iri("<a>"), std::monostate{})});
  expectGetQuads(
      {TripleOf(iri("<a>"))}, {{iri("<b>"), {TripleOf(iri("<a>"))}}},
      {QuadOf(iri("<a>"), std::monostate{}), QuadOf(iri("<a>"), iri("<b>"))});
  expectGetQuads(
      {TripleOf(iri("<a>")), TripleOf(iri("<d>"))},
      {{iri("<b>"), {TripleOf(iri("<a>"))}},
       {iri("<b>"), {TripleOf(iri("<b>")), TripleOf(iri("<c>"))}}},
      {QuadOf(iri("<a>"), std::monostate{}),
       QuadOf(iri("<d>"), std::monostate{}), QuadOf(iri("<a>"), iri("<b>")),
       QuadOf(iri("<b>"), iri("<b>")), QuadOf(iri("<c>"), iri("<b>"))});
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
         ad_utility::source_location l = AD_CURRENT_SOURCE_LOC()) {
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
        return matchers::GroupGraphPatternWithGraph(graph,
                                                    matchers::Triples(triples));
      };
  expectGetQuads({}, {}, ElementsAre(matchers::Triples({})));
  expectGetQuads({TripleOf(iri("<a>"))}, {},
                 ElementsAre(matchers::Triples({SparqlTriple(iri("<a>"))})));
  expectGetQuads(
      {TripleOf(iri("<a>"))}, {{iri("<b>"), {TripleOf(iri("<a>"))}}},
      ElementsAre(matchers::Triples({SparqlTriple(iri("<a>"))}),
                  GraphTriples({SparqlTriple(iri("<a>"))}, iri("<b>"))));
  expectGetQuads(
      {TripleOf(iri("<a>")), TripleOf(iri("<d>"))},
      {{iri("<b>"), {TripleOf(iri("<a>"))}},
       {iri("<b>"), {TripleOf(iri("<b>")), TripleOf(iri("<c>"))}}},
      ElementsAre(
          matchers::Triples(
              {SparqlTriple(iri("<a>")), SparqlTriple(iri("<d>"))}),
          GraphTriples({SparqlTriple(iri("<a>"))}, iri("<b>")),
          GraphTriples({SparqlTriple(iri("<b>")), SparqlTriple(iri("<c>"))},
                       iri("<b>"))));
}

TEST(QuadTest, forAllVariables) {
  auto expectForAllVariables =
      [](Quads quads, const ad_utility::HashSet<Variable>& expectVariables,
         ad_utility::source_location l = AD_CURRENT_SOURCE_LOC()) {
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
  Triple noVars{GraphTerm(iri("<a>")), GraphTerm(iri("<b>")),
                GraphTerm(iri("<c>"))};
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

// _____________________________________________________________________________
// Two `SparqlTripleSimpleWithGraph` are only equal if all their members are
// equal, including those of the base class `SparqlTripleSimple`.
TEST(QuadTest, equalityOfSparqlTripleSimpleWithGraph) {
  using Graph = SparqlTripleSimpleWithGraph::Graph;
  using Triple = SparqlTripleSimpleWithGraph;
  auto makeTriple =
      [](std::string_view s, std::string_view p, std::string_view o,
         const Graph& g,
         Triple::AdditionalScanColumns additionalScanColumns = {}) {
        return Triple{iri(s), iri(p), iri(o), g,
                      std::move(additionalScanColumns)};
      };
  const Graph graph{iri("<d>")};
  const Triple triple = makeTriple("<a>", "<b>", "<c>", graph);

  EXPECT_EQ(triple, makeTriple("<a>", "<b>", "<c>", graph));
  // Differences in the subject, predicate, object, and additional scan columns
  // (which are all stored in the base class) must not be ignored.
  EXPECT_NE(triple, makeTriple("<x>", "<b>", "<c>", graph));
  EXPECT_NE(triple, makeTriple("<a>", "<x>", "<c>", graph));
  EXPECT_NE(triple, makeTriple("<a>", "<b>", "<x>", graph));
  EXPECT_NE(triple,
            makeTriple("<a>", "<b>", "<c>", graph, {{0, Variable{"?x"}}}));
  // A difference in the graph of course also makes them unequal.
  EXPECT_NE(triple, makeTriple("<a>", "<b>", "<c>", Graph{std::monostate{}}));
  EXPECT_NE(triple, makeTriple("<a>", "<b>", "<c>", Graph{iri("<x>")}));
}
