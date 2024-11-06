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

// _____________________________________________________________________________
TEST(ExecuteUpdate, computeGraphUpdateQuads) {
  // These tests run on the default dataset defined in
  // `IndexTestHelpers::makeTestIndex`.
  QueryExecutionContext* qec = ad_utility::testing::getQec(std::nullopt);
  const Index& index = qec->getIndex();
  const auto Id = ad_utility::testing::makeGetId(index);
  auto defaultGraphId = Id(std::string{DEFAULT_GRAPH_IRI});

  ad_utility::HashMap<std::string, std::unique_ptr<LocalVocabEntry>>
      localVocabEntries;
  auto LVI = [&localVocabEntries](const std::string& iri) {
    if (!localVocabEntries.contains(iri)) {
      localVocabEntries.try_emplace(
          iri, std::make_unique<LocalVocabEntry>(
                   ad_utility::triple_component::Iri::fromIriref(iri)));
    }
    return Id::makeFromLocalVocabIndex(localVocabEntries.at(iri).get());
  };

  auto IdTriple = [defaultGraphId](const ::Id s, const ::Id p, const ::Id o,
                                   const std::optional<::Id> graph =
                                       std::nullopt) -> ::IdTriple<> {
    return ::IdTriple({s, p, o, graph.value_or(defaultGraphId)});
  };

  auto executeComputeGraphUpdateQuads = [&qec,
                                         &index](const std::string& update) {
    const auto sharedHandle =
        std::make_shared<ad_utility::CancellationHandle<>>();
    const std::vector<DatasetClause> datasets = {};
    auto pq = SparqlParser::parseQuery(update);
    QueryPlanner qp{qec, sharedHandle};
    const auto qet = qp.createExecutionTree(pq);
    return ExecuteUpdate::computeGraphUpdateQuads(index, pq, qet, sharedHandle);
  };
  auto expectComputeGraphUpdateQuads =
      [&executeComputeGraphUpdateQuads](
          const std::string& update,
          const testing::Matcher<const std::vector<::IdTriple<>>&>&
              toInsertMatcher,
          const testing::Matcher<const std::vector<::IdTriple<>>&>&
              toDeleteMatcher) {
        EXPECT_THAT(
            executeComputeGraphUpdateQuads(update),
            testing::Pair(AD_FIELD(ExecuteUpdate::IdTriplesAndLocalVocab,
                                   idTriples, toInsertMatcher),
                          AD_FIELD(ExecuteUpdate::IdTriplesAndLocalVocab,
                                   idTriples, toDeleteMatcher)));
      };
  auto expectComputeGraphUpdateQuadsFails =
      [&executeComputeGraphUpdateQuads](
          const std::string& update,
          const testing::Matcher<const std::string&>& messageMatcher) {
        AD_EXPECT_THROW_WITH_MESSAGE(executeComputeGraphUpdateQuads(update),
                                     messageMatcher);
      };

  expectComputeGraphUpdateQuads(
      "INSERT DATA { <s> <p> <o> . }",
      testing::ElementsAreArray({IdTriple(LVI("<s>"), LVI("<p>"), LVI("<o>"))}),
      testing::IsEmpty());
  expectComputeGraphUpdateQuads(
      "DELETE DATA { <z> <label> \"zz\"@en }", testing::IsEmpty(),
      testing::ElementsAreArray(
          {IdTriple(Id("<z>"), Id("<label>"), Id("\"zz\"@en"))}));
  expectComputeGraphUpdateQuads(
      "DELETE { ?s <is-a> ?o } INSERT { <s> <p> <o> } WHERE { ?s <is-a> ?o }",
      testing::ElementsAreArray({IdTriple(LVI("<s>"), LVI("<p>"), LVI("<o>")),
                                 // TODO: what to do with this duplicate entry?
                                 IdTriple(LVI("<s>"), LVI("<p>"), LVI("<o>"))}),
      testing::ElementsAreArray(
          {IdTriple(Id("<x>"), Id("<is-a>"), Id("<y>")),
           IdTriple(Id("<y>"), Id("<is-a>"), Id("<x>"))}));
  expectComputeGraphUpdateQuads(
      "DELETE { <s> <p> <o> } INSERT { <s> <p> <o> } WHERE { ?s <is-a> ?o }",
      testing::ElementsAreArray({IdTriple(LVI("<s>"), LVI("<p>"), LVI("<o>")),
                                 IdTriple(LVI("<s>"), LVI("<p>"), LVI("<o>"))}),
      testing::ElementsAreArray(
          {IdTriple(LVI("<s>"), LVI("<p>"), LVI("<o>")),
           IdTriple(LVI("<s>"), LVI("<p>"), LVI("<o>"))}));
  expectComputeGraphUpdateQuads(
      "DELETE { ?s <is-a> ?o } INSERT { ?s <is-a> ?o } WHERE { ?s <is-a> ?o }",
      testing::ElementsAreArray({IdTriple(Id("<x>"), Id("<is-a>"), Id("<y>")),
                                 IdTriple(Id("<y>"), Id("<is-a>"), Id("<x>"))}),
      testing::ElementsAreArray(
          {IdTriple(Id("<x>"), Id("<is-a>"), Id("<y>")),
           IdTriple(Id("<y>"), Id("<is-a>"), Id("<x>"))}));
  expectComputeGraphUpdateQuads(
      "DELETE WHERE { ?s ?p ?o }", testing::IsEmpty(),
      testing::UnorderedElementsAreArray(
          {IdTriple(Id("<x>"), Id("<label>"), Id("\"alpha\"")),
           IdTriple(Id("<x>"), Id("<label>"), Id("\"älpha\"")),
           IdTriple(Id("<x>"), Id("<label>"), Id("\"A\"")),
           IdTriple(Id("<x>"), Id("<label>"), Id("\"Beta\"")),
           IdTriple(Id("<x>"), Id("<is-a>"), Id("<y>")),
           IdTriple(Id("<y>"), Id("<is-a>"), Id("<x>")),
           IdTriple(Id("<z>"), Id("<label>"), Id("\"zz\"@en")),
           IdTriple(Id("<zz>"), Id("<label>"), Id("<zz>"))}));
  expectComputeGraphUpdateQuadsFails(
      "SELECT * WHERE { ?s ?p ?o }",
      testing::HasSubstr("Assertion `query.hasUpdateClause()` failed."));
  expectComputeGraphUpdateQuadsFails(
      "CLEAR DEFAULT",
      testing::HasSubstr(
          "Only INSERT/DELETE update operations are currently supported."));
}

// _____________________________________________________________________________
TEST(ExecuteUpdate, transformTriplesTemplate) {
  // Create an index for testing.
  const auto qec = ad_utility::testing::getQec("<bar> <bar> \"foo\"");
  const Index& index = qec->getIndex();
  // We need a non-const vocab for the test.
  auto& vocab = const_cast<Index::Vocab&>(index.getVocab());

  const auto Id = ad_utility::testing::makeGetId(index);
  // Helpers
  auto expectTransformTriplesTemplate =
      [&vocab](const VariableToColumnMap& variableColumns,
               std::vector<SparqlTripleSimpleWithGraph>&& triples,
               const testing::Matcher<
                   const std::vector<ExecuteUpdate::TransformedTriple>>&
                   expectedTransformedTriples,
               const testing::Matcher<const LocalVocab&>& expectedLocalVocab =
                   testing::IsEmpty()) {
        auto [transformedTriples, localVocab] =
            ExecuteUpdate::transformTriplesTemplate(vocab, variableColumns,
                                                    std::move(triples));
        EXPECT_THAT(transformedTriples, expectedTransformedTriples);
        EXPECT_THAT(localVocab, expectedLocalVocab);
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
  auto Iri = [](const std::string& iri) {
    return ad_utility::triple_component::Iri::fromIriref(iri);
  };
  auto Literal = [](const std::string& literal) {
    return ad_utility::triple_component::Literal::fromStringRepresentation(
        literal);
  };
  auto LocalVocabIndex = [](const LocalVocabEntry* entry) {
    return Id::makeFromLocalVocabIndex(entry);
  };
  auto localVocabContains = [](const LocalVocabEntry& word) {
    return ResultOf(
        absl::StrCat(".getIndexOrNullopt(", word.toStringRepresentation(), ")"),
        [&word](const LocalVocab& lv) { return lv.getIndexOrNullopt(word); },
        AllOf(testing::Not(testing::Eq(std::nullopt)),
              AD_PROPERTY(std::optional<::LocalVocabIndex>, value,
                          testing::Pointee(testing::Eq(word)))));
  };
  using Graph = SparqlTripleSimpleWithGraph::Graph;
  using TransformedTriple = ExecuteUpdate::TransformedTriple;
  auto defaultGraphId = Id(std::string{DEFAULT_GRAPH_IRI});
  // Transforming an empty vector of template results in no `TransformedTriple`s
  // and leaves the `LocalVocab` empty.
  expectTransformTriplesTemplate({}, {}, testing::IsEmpty());
  // Resolve a `SparqlTripleSimpleWithGraph` without variables.
  expectTransformTriplesTemplate(
      {},
      {SparqlTripleSimpleWithGraph{Literal("\"foo\""), Iri("<bar>"),
                                   Literal("\"foo\""), Graph{}}},
      testing::ElementsAre(TransformedTriple{Id("\"foo\""), Id("<bar>"),
                                             Id("\"foo\""), defaultGraphId}));
  const auto entryNotInIndex = LocalVocabEntry(Iri("<baz>"));
  // Literals in the template that are not in the index are added to the
  // `LocalVocab`.
  expectTransformTriplesTemplate(
      {},
      {SparqlTripleSimpleWithGraph{Literal("\"foo\""), Iri("<bar>"),
                                   Literal("\"foo\""), Graph{::Iri("<baz>")}}},
      testing::ElementsAre(
          TransformedTriple{Id("\"foo\""), Id("<bar>"), Id("\"foo\""),
                            LocalVocabIndex(&entryNotInIndex)}),
      localVocabContains(LocalVocabEntry(Iri("<baz>"))));
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
      testing::ElementsAre(
          TransformedTriple{Id("\"foo\""), Id("<bar>"), 0UL, defaultGraphId}));
  expectTransformTriplesTemplate(
      {{Variable("?f"), {0, ColumnIndexAndTypeInfo::PossiblyUndefined}}},
      {SparqlTripleSimpleWithGraph{Literal("\"foo\""), Iri("<bar>"),
                                   Literal("\"foo\""), Graph{Variable("?f")}}},
      testing::ElementsAre(
          TransformedTriple{Id("\"foo\""), Id("<bar>"), Id("\"foo\""), 0UL}));
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
