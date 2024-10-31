// Copyright 2024, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Julian Mundhahs (mundhahj@tf.uni-freiburg.de)

#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <index/IndexImpl.h>

#include "QueryPlannerTestHelpers.h"
#include "engine/ExecuteUpdate.h"
#include "util/GTestHelpers.h"
#include "util/IdTableHelpers.h"
#include "util/IndexTestHelpers.h"

TEST(ExecuteUpdate, transformTriplesTemplate) {
  // Create an index for testing.
  Index index = ad_utility::testing::makeIndexWithTestSettings();
  index.getImpl().setGlobalIndexAndComparatorOnlyForTesting();
  auto& vocab = const_cast<Index::Vocab&>(index.getVocab());
  vocab.createFromSet({"\"foo\"", "<bar>", std::string{DEFAULT_GRAPH_IRI}},
                      "vocTest1.dat");
  // Helpers
  auto expectTransformTriplesTemplate =
      [&vocab](const VariableToColumnMap& variableColumns,
               std::vector<SparqlTripleSimpleWithGraph>&& triples,
               const testing::Matcher<
                   const std::vector<ExecuteUpdate::TransformedTriple>>&
                   expectedTransformedTriples,
               const testing::Matcher<const LocalVocab&>& expectedLocalVocab) {
        auto [transformedTriples, localVocab] =
            ExecuteUpdate::transformTriplesTemplate(vocab, variableColumns,
                                                    std::move(triples));
        EXPECT_THAT(transformedTriples, expectedTransformedTriples);
        EXPECT_THAT(localVocab, expectedLocalVocab);
      };
  auto Iri = [](const std::string& iri) {
    return ad_utility::triple_component::Iri::fromIriref(iri);
  };
  auto Literal = [](const std::string& literal) {
    return ad_utility::triple_component::Literal::fromStringRepresentation(
        literal);
  };
  auto VocabIndex = [](const uint64_t index) {
    return Id::makeFromVocabIndex(VocabIndex::make(index));
  };
  auto LocalVocabIndex = [](const LocalVocabEntry* entry) {
    return Id::makeFromLocalVocabIndex(entry);
  };
  // Transforming an empty vector of template results in no `TransformedTriple`s
  // and leaves the `LocalVocab` empty.
  expectTransformTriplesTemplate({}, {}, testing::IsEmpty(),
                                 testing::IsEmpty());
  // Resolve a `SparqlTripleSimpleWithGraph` without variables.
  expectTransformTriplesTemplate(
      {},
      {SparqlTripleSimpleWithGraph{Literal("\"foo\""), Iri("<bar>"),
                                   Literal("\"foo\""),
                                   SparqlTripleSimpleWithGraph::Graph{}}},
      testing::ElementsAre(ExecuteUpdate::TransformedTriple{
          VocabIndex(0), VocabIndex(1), VocabIndex(0), VocabIndex(2)}),
      testing::IsEmpty());
  const auto entryNotInIndex = LocalVocabEntry(Iri("<baz>"));
  // Literals in the template that are not in the index are added to the
  // `LocalVocab`.
  expectTransformTriplesTemplate(
      {},
      {SparqlTripleSimpleWithGraph{
          Literal("\"foo\""), Iri("<bar>"), Literal("\"foo\""),
          SparqlTripleSimpleWithGraph::Graph{::Iri("<baz>")}}},
      testing::ElementsAre(
          testing::ElementsAre(VocabIndex(0), VocabIndex(1), VocabIndex(0),
                               LocalVocabIndex(&entryNotInIndex))),
      testing::SizeIs(1));
  // A variable in the template (`?f`) is not mapped in the
  // `VariableToColumnMap`.
  AD_EXPECT_THROW_WITH_MESSAGE(
      expectTransformTriplesTemplate(
          {},
          {SparqlTripleSimpleWithGraph{Literal("\"foo\""), Iri("<bar>"),
                                       Variable("?f"),
                                       SparqlTripleSimpleWithGraph::Graph{}}},
          testing::ElementsAre(testing::ElementsAre(
              VocabIndex(0), VocabIndex(1), 0UL, VocabIndex(2))),
          testing::IsEmpty()),
      testing::HasSubstr(
          "Assertion `variableColumns.contains(component.getVariable())` "
          "failed."));
  AD_EXPECT_THROW_WITH_MESSAGE(
      expectTransformTriplesTemplate(
          {},
          {SparqlTripleSimpleWithGraph{
              Literal("\"foo\""), Iri("<bar>"), Literal("\"foo\""),
              SparqlTripleSimpleWithGraph::Graph{Variable("?f")}}},
          testing::ElementsAre(testing::ElementsAre(
              VocabIndex(0), VocabIndex(1), 0UL, VocabIndex(2))),
          testing::IsEmpty()),
      testing::HasSubstr("Assertion `variableColumns.contains(var)` failed."));
  // Variables in the template are mapped to their column index.
  expectTransformTriplesTemplate(
      {{Variable("?f"), {0, ColumnIndexAndTypeInfo::PossiblyUndefined}}},
      {SparqlTripleSimpleWithGraph{Literal("\"foo\""), Iri("<bar>"),
                                   Variable("?f"),
                                   SparqlTripleSimpleWithGraph::Graph{}}},
      testing::ElementsAre(testing::ElementsAre(VocabIndex(0), VocabIndex(1),
                                                0UL, VocabIndex(2))),
      testing::IsEmpty());
  expectTransformTriplesTemplate(
      {{Variable("?f"), {0, ColumnIndexAndTypeInfo::PossiblyUndefined}}},
      {SparqlTripleSimpleWithGraph{
          Literal("\"foo\""), Iri("<bar>"), Literal("\"foo\""),
          SparqlTripleSimpleWithGraph::Graph{Variable("?f")}}},
      testing::ElementsAre(testing::ElementsAre(VocabIndex(0), VocabIndex(1),
                                                VocabIndex(0), 0UL)),
      testing::IsEmpty());
}

TEST(ExecuteUpdate, resolveVariable) {
  auto VocabIndex = [](const uint64_t index) {
    return Id::makeFromVocabIndex(VocabIndex::make(index));
  };
  const auto idTable = makeIdTableFromVector(
      {{VocabIndex(0), VocabIndex(1), VocabIndex(2)},
       {VocabIndex(3), VocabIndex(4), VocabIndex(5)},
       {VocabIndex(6), Id::makeUndefined(), VocabIndex(8)}});
  EXPECT_THAT(ExecuteUpdate::resolveVariable(idTable, 0, VocabIndex(10)),
              testing::Eq(VocabIndex(10)));
  EXPECT_THAT(ExecuteUpdate::resolveVariable(idTable, 0, 1UL),
              testing::Eq(VocabIndex(1)));
  EXPECT_THAT(ExecuteUpdate::resolveVariable(idTable, 1, 1UL),
              testing::Eq(VocabIndex(4)));
  EXPECT_THAT(ExecuteUpdate::resolveVariable(idTable, 2, 1UL),
              testing::Eq(std::nullopt));
  EXPECT_THAT(ExecuteUpdate::resolveVariable(idTable, 2, Id::makeUndefined()),
              testing::Eq(std::nullopt));
}

TEST(ExecuteUpdate, computeAndAddQuadsForResultRow) {
  auto VocabIndex = [](const uint64_t index) {
    return Id::makeFromVocabIndex(VocabIndex::make(index));
  };
  auto idTable = makeIdTableFromVector(
      {{VocabIndex(0), VocabIndex(1), VocabIndex(2)},
       {VocabIndex(3), VocabIndex(4), VocabIndex(5)},
       {VocabIndex(6), Id::makeUndefined(), VocabIndex(8)}});
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
      {{VocabIndex(0), VocabIndex(1), VocabIndex(2), VocabIndex(3)}}, idTable,
      0,
      testing::ElementsAreArray({IdTriple{
          {VocabIndex(0), VocabIndex(1), VocabIndex(2), VocabIndex(3)}}}));
  expectComputeQuads(
      {{VocabIndex(0), VocabIndex(1), VocabIndex(2), VocabIndex(3)}}, idTable,
      1,
      testing::ElementsAreArray({IdTriple{
          {VocabIndex(0), VocabIndex(1), VocabIndex(2), VocabIndex(3)}}}));
  // The variables in templates are resolved to the value of the variable in the
  // specified row of the result.
  expectComputeQuads(
      {{0UL, VocabIndex(1), 1UL, VocabIndex(3)}}, idTable, 0,
      testing::ElementsAreArray({IdTriple{
          {VocabIndex(0), VocabIndex(1), VocabIndex(1), VocabIndex(3)}}}));
  expectComputeQuads(
      {{0UL, VocabIndex(1), 1UL, VocabIndex(3)}}, idTable, 1,
      testing::ElementsAreArray({IdTriple{
          {VocabIndex(3), VocabIndex(1), VocabIndex(4), VocabIndex(3)}}}));
  // Quads with undefined IDs cannot be stored and are not returned.
  expectComputeQuads({{0UL, VocabIndex(1), 1UL, VocabIndex(3)}}, idTable, 2,
                     testing::IsEmpty());
  expectComputeQuads(
      {{VocabIndex(0), VocabIndex(1), Id::makeUndefined(), VocabIndex(3)}},
      idTable, 0, testing::IsEmpty());
  // Some extra cases to cover all branches.
  expectComputeQuads(
      {{Id::makeUndefined(), VocabIndex(1), VocabIndex(2), VocabIndex(3)}},
      idTable, 0, testing::IsEmpty());
  expectComputeQuads(
      {{VocabIndex(0), Id::makeUndefined(), VocabIndex(2), VocabIndex(3)}},
      idTable, 0, testing::IsEmpty());
  expectComputeQuads(
      {{VocabIndex(0), VocabIndex(1), VocabIndex(2), Id::makeUndefined()}},
      idTable, 0, testing::IsEmpty());
  // All the templates are evaluated for the specified row of the result.
  expectComputeQuads(
      {{0UL, VocabIndex(1), 1UL, VocabIndex(3)},
       {VocabIndex(0), 1UL, 2UL, VocabIndex(3)}},
      idTable, 0,
      testing::ElementsAreArray({IdTriple{{VocabIndex(0), VocabIndex(1),
                                           VocabIndex(1), VocabIndex(3)}},
                                 IdTriple{{VocabIndex(0), VocabIndex(1),
                                           VocabIndex(2), VocabIndex(3)}}}));
}
