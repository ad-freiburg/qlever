// Copyright 2025, University of Freiburg
// Chair of Algorithms and Data Structures
// Author: Julian Mundhahs <mundhahj@tf.uni-freiburg.de>

#include <gtest/gtest.h>

#include "../SparqlAntlrParserTestHelpers.h"
#include "../util/GTestHelpers.h"
#include "parser/Quads.h"

namespace tc {
auto Iri = ad_utility::triple_component::Iri::fromIriref;
}

TEST(QuadTest, getQuads) {
  auto expectGetQuads =
      [](ad_utility::sparql_types::Triples triples,
         std::vector<Quads::GraphBlock> graphs,
         const std::vector<SparqlTripleSimpleWithGraph>& expected,
         ad_utility::source_location l =
             ad_utility::source_location::current()) {
        auto t = generateLocationTrace(l);
        const Quads quads{std::move(triples), std::move(graphs)};
        EXPECT_THAT(quads.toTriplesWithGraph(),
                    testing::UnorderedElementsAreArray(expected));
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
    return {t, t.toString(), t};
  };
  auto GraphTriples =
      [](const vector<::SparqlTriple>& triples,
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
