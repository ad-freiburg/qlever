// Copyright 2024, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Julian Mundhahs (mundhahj@tf.uni-freiburg.de)

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "DeltaTriplesTestHelpers.h"
#include "QueryPlannerTestHelpers.h"
#include "engine/ExecuteUpdate.h"
#include "index/IndexImpl.h"
#include "parser/sparqlParser/SparqlQleverVisitor.h"
#include "util/GTestHelpers.h"
#include "util/IdTableHelpers.h"
#include "util/IndexTestHelpers.h"

using namespace deltaTriplesTestHelpers;

auto V = [](const uint64_t index) {
  return Id::makeFromVocabIndex(VocabIndex::make(index));
};

// `ExecuteUpdate::IdOrVariableIndex` extended by `LiteralOrIri` which denotes
// an entry from the local vocab.
using TripleComponentT =
    std::variant<Id, ColumnIndex, ad_utility::triple_component::LiteralOrIri>;

// A matcher that never matches and outputs the given message.
MATCHER_P(AlwaysFalse, msg, "") {
  (void)arg;  // avoid compiler warning for unused value.
  *result_listener << msg;
  return false;
}

// _____________________________________________________________________________
TEST(ExecuteUpdate, transformTriplesTemplate) {
  // Create an index for testing.
  const auto qec = ad_utility::testing::getQec("<bar> <bar> \"foo\"");
  const Index& index = qec->getIndex();
  // We need a non-const vocab for the test.
  auto& vocab = const_cast<Index::Vocab&>(index.getVocab());

  // Helpers
  const auto Id = ad_utility::testing::makeGetId(index);
  using Graph = SparqlTripleSimpleWithGraph::Graph;
  using LocalVocab = ad_utility::triple_component::LiteralOrIri;
  auto defaultGraphId = Id(std::string{DEFAULT_GRAPH_IRI});
  auto Iri = [](const std::string& iri) {
    return ad_utility::triple_component::Iri::fromIriref(iri);
  };
  auto Literal = [](const std::string& literal) {
    return ad_utility::triple_component::Literal::fromStringRepresentation(
        literal);
  };
  // Matchers
  using MatcherType = testing::Matcher<const ExecuteUpdate::IdOrVariableIndex&>;
  auto TripleComponentMatcher = [](const ::LocalVocab& localVocab,
                                   TripleComponentT component) -> MatcherType {
    return std::visit(
        ad_utility::OverloadCallOperator{
            [](const ::Id& id) -> MatcherType {
              return testing::VariantWith<::Id>(testing::Eq(id));
            },
            [](const ColumnIndex& index) -> MatcherType {
              return testing::VariantWith<ColumnIndex>(testing::Eq(index));
            },
            [&localVocab](
                const ad_utility::triple_component::LiteralOrIri& literalOrIri)
                -> MatcherType {
              const auto lviOpt = localVocab.getIndexOrNullopt(literalOrIri);
              if (!lviOpt) {
                return AlwaysFalse(
                    absl::StrCat(literalOrIri.toStringRepresentation(),
                                 " not in local vocab"));
              }
              const auto id = Id::makeFromLocalVocabIndex(lviOpt.value());
              return testing::VariantWith<::Id>(
                  AD_PROPERTY(Id, getBits, testing::Eq(id.getBits())));
            }},
        component);
  };
  auto expectTransformTriplesTemplate =
      [&vocab, &TripleComponentMatcher](
          const VariableToColumnMap& variableColumns,
          std::vector<SparqlTripleSimpleWithGraph>&& triples,
          const std::vector<std::array<TripleComponentT, 4>>&
              expectedTransformedTriples) {
        auto [transformedTriples, localVocab] =
            ExecuteUpdate::transformTriplesTemplate(vocab, variableColumns,
                                                    std::move(triples));
        const auto transformedTriplesMatchers = ad_utility::transform(
            expectedTransformedTriples,
            [&localVocab, &TripleComponentMatcher](const auto& expectedTriple) {
              return ElementsAre(
                  TripleComponentMatcher(localVocab, expectedTriple.at(0)),
                  TripleComponentMatcher(localVocab, expectedTriple.at(1)),
                  TripleComponentMatcher(localVocab, expectedTriple.at(2)),
                  TripleComponentMatcher(localVocab, expectedTriple.at(3)));
            });
        EXPECT_THAT(transformedTriples,
                    testing::ElementsAreArray(transformedTriplesMatchers));
      };
  auto expectTransformTriplesTemplateFails =
      [&vocab](const VariableToColumnMap& variableColumns,
               std::vector<SparqlTripleSimpleWithGraph>&& triples,
               const testing::Matcher<const std::string&>& messageMatcher) {
        AD_EXPECT_THROW_WITH_MESSAGE(
            ExecuteUpdate::transformTriplesTemplate(vocab, variableColumns,
                                                    std::move(triples)),
            messageMatcher);
      };
  // Transforming an empty vector of template results in no `TransformedTriple`s
  // and leaves the `LocalVocab` empty.
  expectTransformTriplesTemplate({}, {}, {});
  // Resolve a `SparqlTripleSimpleWithGraph` without variables.
  expectTransformTriplesTemplate(
      {},
      {SparqlTripleSimpleWithGraph{Literal("\"foo\""), Iri("<bar>"),
                                   Literal("\"foo\""), Graph{}}},
      {{Id("\"foo\""), Id("<bar>"), Id("\"foo\""), defaultGraphId}});
  // Literals in the template that are not in the index are added to the
  // `LocalVocab`.
  expectTransformTriplesTemplate(
      {},
      {SparqlTripleSimpleWithGraph{Literal("\"foo\""), Iri("<bar>"),
                                   Literal("\"foo\""), Graph{::Iri("<baz>")}}},
      {{Id("\"foo\""), Id("<bar>"), Id("\"foo\""), LocalVocab(Iri("<baz>"))}});
  // A variable in the template (`?f`) is not mapped in the
  // `VariableToColumnMap`.
  expectTransformTriplesTemplateFails(
      {},
      {SparqlTripleSimpleWithGraph{Literal("\"foo\""), Iri("<bar>"),
                                   Variable("?f"), Graph{}}},
      testing::HasSubstr(
          "Assertion `variableColumns.contains(component.getVariable())` "
          "failed."));
  expectTransformTriplesTemplateFails(
      {},
      {SparqlTripleSimpleWithGraph{Literal("\"foo\""), Iri("<bar>"),
                                   Literal("\"foo\""), Graph{Variable("?f")}}},
      testing::HasSubstr("Assertion `variableColumns.contains(var)` failed."));
  // Variables in the template are mapped to their column index.
  expectTransformTriplesTemplate(
      {{Variable("?f"), {0, ColumnIndexAndTypeInfo::PossiblyUndefined}}},
      {SparqlTripleSimpleWithGraph{Literal("\"foo\""), Iri("<bar>"),
                                   Variable("?f"), Graph{}}},
      {{Id("\"foo\""), Id("<bar>"), 0UL, defaultGraphId}});
  expectTransformTriplesTemplate(
      {{Variable("?f"), {0, ColumnIndexAndTypeInfo::PossiblyUndefined}}},
      {SparqlTripleSimpleWithGraph{Literal("\"foo\""), Iri("<bar>"),
                                   Literal("\"foo\""), Graph{Variable("?f")}}},
      {{Id("\"foo\""), Id("<bar>"), Id("\"foo\""), 0UL}});
}

// _____________________________________________________________________________
TEST(ExecuteUpdate, resolveVariable) {
  const auto idTable =
      makeIdTableFromVector({{V(0), V(1), V(2)},
                             {V(3), V(4), V(5)},
                             {V(6), Id::makeUndefined(), V(8)}});
  auto resolveVariable =
      std::bind_front(&ExecuteUpdate::resolveVariable, std::cref(idTable));
  EXPECT_THAT(resolveVariable(0, V(10)), testing::Eq(V(10)));
  EXPECT_THAT(resolveVariable(0, 1UL), testing::Eq(V(1)));
  EXPECT_THAT(resolveVariable(1, 1UL), testing::Eq(V(4)));
  EXPECT_THAT(resolveVariable(2, 1UL), testing::Eq(std::nullopt));
  EXPECT_THAT(resolveVariable(2, Id::makeUndefined()),
              testing::Eq(std::nullopt));
}

// _____________________________________________________________________________
TEST(ExecuteUpdate, computeAndAddQuadsForResultRow) {
  const auto idTable =
      makeIdTableFromVector({{V(0), V(1), V(2)},
                             {V(3), V(4), V(5)},
                             {V(6), Id::makeUndefined(), V(8)}});
  auto expectComputeQuads =
      [](const std::vector<ExecuteUpdate::TransformedTriple>& templates,
         const IdTable& idTable, uint64_t rowIdx,
         const testing::Matcher<const std::vector<IdTriple<>>&>&
             expectedQuads) {
        std::vector<IdTriple<>> result;
        ExecuteUpdate::computeAndAddQuadsForResultRow(templates, result,
                                                      idTable, rowIdx);
        EXPECT_THAT(result, expectedQuads);
      };
  // Compute the quads for an empty template set yields no quads.
  expectComputeQuads({}, idTable, 0, testing::IsEmpty());
  // Compute the quads for template without variables yields the templates
  // unmodified.
  expectComputeQuads(
      {{V(0), V(1), V(2), V(3)}}, idTable, 0,
      testing::ElementsAreArray({IdTriple{{V(0), V(1), V(2), V(3)}}}));
  expectComputeQuads(
      {{V(0), V(1), V(2), V(3)}}, idTable, 1,
      testing::ElementsAreArray({IdTriple{{V(0), V(1), V(2), V(3)}}}));
  // The variables in templates are resolved to the value of the variable in the
  // specified row of the result.
  expectComputeQuads(
      {{0UL, V(1), 1UL, V(3)}}, idTable, 0,
      testing::ElementsAreArray({IdTriple{{V(0), V(1), V(1), V(3)}}}));
  expectComputeQuads(
      {{0UL, V(1), 1UL, V(3)}}, idTable, 1,
      testing::ElementsAreArray({IdTriple{{V(3), V(1), V(4), V(3)}}}));
  // Quads with undefined IDs cannot be stored and are not returned.
  expectComputeQuads({{0UL, V(1), 1UL, V(3)}}, idTable, 2, testing::IsEmpty());
  expectComputeQuads({{V(0), V(1), Id::makeUndefined(), V(3)}}, idTable, 0,
                     testing::IsEmpty());
  // Some extra cases to cover all branches.
  expectComputeQuads({{Id::makeUndefined(), V(1), V(2), V(3)}}, idTable, 0,
                     testing::IsEmpty());
  expectComputeQuads({{V(0), Id::makeUndefined(), V(2), V(3)}}, idTable, 0,
                     testing::IsEmpty());
  expectComputeQuads({{V(0), V(1), V(2), Id::makeUndefined()}}, idTable, 0,
                     testing::IsEmpty());
  // All the templates are evaluated for the specified row of the result.
  expectComputeQuads(
      {{0UL, V(1), 1UL, V(3)}, {V(0), 1UL, 2UL, V(3)}}, idTable, 0,
      testing::ElementsAreArray({IdTriple{{V(0), V(1), V(1), V(3)}},
                                 IdTriple{{V(0), V(1), V(2), V(3)}}}));
}
