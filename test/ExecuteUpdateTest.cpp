// Copyright 2024, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Julian Mundhahs (mundhahj@tf.uni-freiburg.de)

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "DeltaTriplesTestHelpers.h"
#include "QueryPlannerTestHelpers.h"
#include "engine/ExecuteUpdate.h"
#include "engine/MaterializedViews.h"
#include "engine/NamedResultCache.h"
#include "index/IndexImpl.h"
#include "parser/sparqlParser/SparqlQleverVisitor.h"
#include "util/GTestHelpers.h"
#include "util/IdTableHelpers.h"
#include "util/IndexTestHelpers.h"

namespace {
using namespace deltaTriplesTestHelpers;

auto V = [](const uint64_t index) {
  return Id::makeFromVocabIndex(VocabIndex::make(index));
};

const EncodedIriManager* encodedIriManager() {
  static EncodedIriManager encodedIriManager_;
  return &encodedIriManager_;
}

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
}  // namespace

// Test the `ExecuteUpdate::executeUpdate` method. These tests run on the
// default dataset defined in `IndexTestHelpers::makeTestIndex`.
TEST(ExecuteUpdate, executeUpdate) {
  // Perform the given `update` and store result in given `deltaTriples`.
  auto expectExecuteUpdateHelper =
      [](const std::string& update, QueryExecutionContext& qec, Index& index) {
        const auto sharedHandle =
            std::make_shared<ad_utility::CancellationHandle<>>();
        const std::vector<DatasetClause> datasets = {};
        ad_utility::BlankNodeManager bnm;
        auto pqs = SparqlParser::parseUpdate(&bnm, encodedIriManager(), update);
        for (auto& pq : pqs) {
          QueryPlanner qp{&qec, sharedHandle};
          const auto qet = qp.createExecutionTree(pq);
          index.deltaTriplesManager().modify<void>(
              [&index, &pq, &qet, &sharedHandle](DeltaTriples& deltaTriples) {
                ExecuteUpdate::executeUpdate(index, pq, qet, deltaTriples,
                                             sharedHandle);
              });
          qec.updateLocatedTriplesSnapshot();
        }
      };
  ad_utility::testing::TestIndexConfig indexConfig{};
  // Execute the given `update` and check that the delta triples are correct.
  auto expectExecuteUpdate =
      [&expectExecuteUpdateHelper, &indexConfig](
          const std::string& update,
          const testing::Matcher<const DeltaTriples&>& deltaTriplesMatcher,
          source_location sourceLocation = AD_CURRENT_SOURCE_LOC()) {
        auto l = generateLocationTrace(sourceLocation);
        Index index = ad_utility::testing::makeTestIndex(
            "ExecuteUpdate_executeUpdate", indexConfig);
        QueryResultCache cache = QueryResultCache();
        NamedResultCache namedResultCache;
        MaterializedViewsManager materializedViewsManager;
        QueryExecutionContext qec(index, &cache,
                                  ad_utility::testing::makeAllocator(
                                      ad_utility::MemorySize::megabytes(100)),
                                  SortPerformanceEstimator{}, &namedResultCache,
                                  &materializedViewsManager);
        expectExecuteUpdateHelper(update, qec, index);
        index.deltaTriplesManager().modify<void>(
            [&deltaTriplesMatcher](DeltaTriples& deltaTriples) {
              EXPECT_THAT(deltaTriples, deltaTriplesMatcher);
            });
      };
  // Execute the given `update` and check that it fails with the given message.
  auto expectExecuteUpdateFails_ =
      [&expectExecuteUpdateHelper](
          Index& index, const std::string& update,
          const testing::Matcher<const std::string&>& messageMatcher,
          source_location sourceLocation = AD_CURRENT_SOURCE_LOC()) {
        auto l = generateLocationTrace(sourceLocation);
        QueryResultCache cache = QueryResultCache();
        NamedResultCache namedResultCache;
        MaterializedViewsManager materializedViewsManager;
        QueryExecutionContext qec(index, &cache,
                                  ad_utility::testing::makeAllocator(
                                      ad_utility::MemorySize::megabytes(100)),
                                  SortPerformanceEstimator{}, &namedResultCache,
                                  &materializedViewsManager);
        AD_EXPECT_THROW_WITH_MESSAGE(
            expectExecuteUpdateHelper(update, qec, index), messageMatcher);
      };
  {
    auto expectExecuteUpdateFails =
        [&expectExecuteUpdateFails_](
            const std::string& update,
            const testing::Matcher<const std::string&>& messageMatcher,
            source_location sourceLocation = AD_CURRENT_SOURCE_LOC()) {
          Index index = ad_utility::testing::makeTestIndex(
              "ExecuteUpdate_executeUpdate",
              ad_utility::testing::TestIndexConfig());
          expectExecuteUpdateFails_(index, update, messageMatcher,
                                    sourceLocation);
        };
    // Now the actual tests.
    expectExecuteUpdate("INSERT DATA { <s> <p> <o> . }", NumTriples(1, 0, 1));
    expectExecuteUpdate("DELETE DATA { <z> <label> \"zz\"@en }",
                        NumTriples(0, 1, 1, 0, 1));
    expectExecuteUpdate(
        "DELETE { ?s <is-a> ?o } INSERT { <a> <b> <c> } WHERE { ?s <is-a> ?o }",
        NumTriples(1, 2, 3));
    expectExecuteUpdate(
        "DELETE { <a> <b> <c> } INSERT { <a> <b> <c> } WHERE { ?s <is-a> ?o }",
        NumTriples(1, 0, 1));
    expectExecuteUpdate(
        "DELETE { ?s <is-a> ?o } INSERT { ?s <is-a> ?o } WHERE { ?s <is-a> ?o "
        "}",
        NumTriples(2, 0, 2));
    expectExecuteUpdate("DELETE WHERE { ?s ?p ?o }", NumTriples(0, 8, 8, 0, 1));
    expectExecuteUpdateFails(
        "SELECT * WHERE { ?s ?p ?o }",
        testing::HasSubstr(
            R"(Invalid SPARQL query: Token "SELECT": mismatched input 'SELECT')"));
    expectExecuteUpdate(
        "INSERT DATA { <a> <b> <c> }; INSERT DATA { <d> <e> <f> }",
        NumTriples(2, 0, 2));
    expectExecuteUpdate(
        "INSERT DATA { <a> <b> <c> }; INSERT DATA { <a> <b> <c> }",
        NumTriples(1, 0, 1));
    expectExecuteUpdate(
        "INSERT DATA { <a> <b> <c> }; DELETE DATA { <a> <b> <c> }",
        NumTriples(0, 1, 1));
    expectExecuteUpdate(
        "INSERT DATA { <a> <b> <c> }; DELETE WHERE { ?s ?p ?o }",
        NumTriples(0, 9, 9, 0, 1));
    expectExecuteUpdate("CLEAR SILENT GRAPH <x>", NumTriples(0, 0, 0));
    expectExecuteUpdate("CLEAR DEFAULT", NumTriples(0, 8, 8, 0, 1));
    expectExecuteUpdate("CLEAR SILENT NAMED", NumTriples(0, 0, 0));
    expectExecuteUpdate("CLEAR ALL", NumTriples(0, 8, 8, 0, 1));
    expectExecuteUpdate("DROP GRAPH <x>", NumTriples(0, 0, 0));
    expectExecuteUpdate("DROP SILENT DEFAULT", NumTriples(0, 8, 8, 0, 1));
    expectExecuteUpdate("DROP NAMED", NumTriples(0, 0, 0));
    expectExecuteUpdate("DROP SILENT ALL", NumTriples(0, 8, 8, 0, 1));
    expectExecuteUpdate("ADD <x> TO <x>", NumTriples(0, 0, 0));
    expectExecuteUpdate("ADD SILENT <x> TO DEFAULT", NumTriples(0, 0, 0));
    expectExecuteUpdate("ADD DEFAULT TO <x>", NumTriples(8, 0, 8, 1, 0));
    expectExecuteUpdate("ADD SILENT DEFAULT TO DEFAULT", NumTriples(0, 0, 0));
    expectExecuteUpdate("MOVE SILENT DEFAULT TO DEFAULT", NumTriples(0, 0, 0));
    expectExecuteUpdate("MOVE GRAPH <x> TO <x>", NumTriples(0, 0, 0));
    expectExecuteUpdate("MOVE <x> TO DEFAULT", NumTriples(0, 8, 8, 0, 1));
    expectExecuteUpdate("MOVE DEFAULT TO GRAPH <x>",
                        NumTriples(8, 8, 16, 1, 1));
    expectExecuteUpdate("COPY DEFAULT TO <x>", NumTriples(8, 0, 8, 1, 0));
    expectExecuteUpdate("COPY DEFAULT TO DEFAULT", NumTriples(0, 0, 0));
    expectExecuteUpdate("COPY <x> TO DEFAULT", NumTriples(0, 8, 8, 0, 1));
    expectExecuteUpdate("CREATE SILENT GRAPH <x>", NumTriples(0, 0, 0));
    expectExecuteUpdate("CREATE GRAPH <y>", NumTriples(0, 0, 0));
  }
  {
    indexConfig.turtleInput =
        "<x> <is-a> <y> . "
        "<v> <is-a> <y>  <q>. "
        "<y> <label> \"foo\"@en  <q>. "
        "<y> <label> \"bar\"@de  <q>. "
        "<u> <is-a> <a> <s> ."
        "<u> <label> \"baz\"@en <s> ."
        "<u> <blub> <blah> <s> .";
    indexConfig.indexType = qlever::Filetype::NQuad;
    // That the DEFAULT graph is the union graph again causes some problems.
    expectExecuteUpdate("CLEAR SILENT GRAPH <q>", NumTriples(0, 3, 3, 0, 2));
    expectExecuteUpdate("CLEAR GRAPH <a>", NumTriples(0, 0, 0));
    expectExecuteUpdate("CLEAR DEFAULT", NumTriples(0, 1, 1));
    expectExecuteUpdate("CLEAR SILENT NAMED", NumTriples(0, 6, 6, 0, 3));
    expectExecuteUpdate("CLEAR ALL", NumTriples(0, 7, 7, 0, 3));
    expectExecuteUpdate("DROP GRAPH <q>", NumTriples(0, 3, 3, 0, 2));
    expectExecuteUpdate("DROP SILENT GRAPH <a>", NumTriples(0, 0, 0));
    expectExecuteUpdate("DROP SILENT DEFAULT", NumTriples(0, 1, 1));
    expectExecuteUpdate("DROP NAMED", NumTriples(0, 6, 6, 0, 3));
    expectExecuteUpdate("DROP SILENT ALL", NumTriples(0, 7, 7, 0, 3));
    expectExecuteUpdate("ADD <q> TO <q>", NumTriples(0, 0, 0));
    expectExecuteUpdate("ADD <a> TO <q>", NumTriples(0, 0, 0));
    expectExecuteUpdate("ADD SILENT <q> TO DEFAULT", NumTriples(3, 0, 3, 2, 0));
    expectExecuteUpdate("ADD DEFAULT TO <q>", NumTriples(1, 0, 1));
    expectExecuteUpdate("ADD SILENT DEFAULT TO DEFAULT", NumTriples(0, 0, 0));
    expectExecuteUpdate("MOVE SILENT DEFAULT TO DEFAULT", NumTriples(0, 0, 0));
    expectExecuteUpdate("MOVE GRAPH <q> TO <t>", NumTriples(3, 3, 6, 2, 2));
    expectExecuteUpdate("MOVE <q> TO DEFAULT", NumTriples(3, 4, 7, 2, 2));
    expectExecuteUpdate("MOVE DEFAULT TO GRAPH <t>", NumTriples(1, 1, 2));
    expectExecuteUpdate("MOVE DEFAULT TO GRAPH <q>", NumTriples(1, 4, 5, 0, 2));
    expectExecuteUpdate("COPY DEFAULT TO <q>", NumTriples(1, 3, 4, 0, 2));
    expectExecuteUpdate("COPY DEFAULT TO DEFAULT", NumTriples(0, 0, 0));
    expectExecuteUpdate("COPY <q> TO DEFAULT", NumTriples(3, 1, 4, 2, 0));
    expectExecuteUpdate("CREATE SILENT GRAPH <x>", NumTriples(0, 0, 0));
    expectExecuteUpdate("CREATE GRAPH <y>", NumTriples(0, 0, 0));
  }
}

// _____________________________________________________________________________
TEST(ExecuteUpdate, computeGraphUpdateQuads) {
  // For each test suite the `qec` and the `defaultGraphId` have to be set
  // according to the current index. They must be set before any test can be
  // run.
  QueryExecutionContext* qec = nullptr;
  Id defaultGraphId = Id::makeUndefined();

  using namespace ::testing;
  auto IdTriple = [&defaultGraphId](const Id s, const Id p, const Id o,
                                    const std::optional<Id> graph =
                                        std::nullopt) -> ::IdTriple<> {
    return ::IdTriple({s, p, o, graph.value_or(defaultGraphId)});
  };

  auto executeComputeGraphUpdateQuads = [&qec](const std::string& update) {
    const auto sharedHandle =
        std::make_shared<ad_utility::CancellationHandle<>>();
    const std::vector<DatasetClause> datasets = {};
    auto& index = qec->getIndex();
    DeltaTriples deltaTriples{index};
    ad_utility::BlankNodeManager bnm;
    auto pqs = SparqlParser::parseUpdate(&bnm, encodedIriManager(), update);
    std::vector<std::pair<ExecuteUpdate::IdTriplesAndLocalVocab,
                          ExecuteUpdate::IdTriplesAndLocalVocab>>
        results;
    for (auto& pq : pqs) {
      QueryPlanner qp{qec, sharedHandle};
      const auto qet = qp.createExecutionTree(pq);
      UpdateMetadata metadata;
      auto result = qet.getResult(false);
      results.push_back(ExecuteUpdate::computeGraphUpdateQuads(
          index, pq, *result, qet.getVariableColumns(), sharedHandle,
          metadata));
      ExecuteUpdate::executeUpdate(index, pq, qet, deltaTriples, sharedHandle);
    }
    return results;
  };

  auto expectComputeGraphUpdateQuads =
      [&executeComputeGraphUpdateQuads](
          const std::string& update,
          std::vector<Matcher<const std::vector<::IdTriple<>>&>>
              toInsertMatchers,
          std::vector<Matcher<const std::vector<::IdTriple<>>&>>
              toDeleteMatchers,
          source_location sourceLocation = AD_CURRENT_SOURCE_LOC()) {
        auto l = generateLocationTrace(sourceLocation);
        ASSERT_THAT(toInsertMatchers, testing::SizeIs(toDeleteMatchers.size()));
        auto graphUpdateQuads = executeComputeGraphUpdateQuads(update);
        ASSERT_THAT(graphUpdateQuads, testing::SizeIs(toInsertMatchers.size()));
        std::vector<Matcher<std::pair<ExecuteUpdate::IdTriplesAndLocalVocab,
                                      ExecuteUpdate::IdTriplesAndLocalVocab>>>
            transformedMatchers;
        ql::ranges::transform(
            toInsertMatchers, toDeleteMatchers,
            std::back_inserter(transformedMatchers),
            [](auto insertMatcher, auto deleteMatcher) {
              return testing::Pair(
                  AD_FIELD(ExecuteUpdate::IdTriplesAndLocalVocab, idTriples_,
                           insertMatcher),
                  AD_FIELD(ExecuteUpdate::IdTriplesAndLocalVocab, idTriples_,
                           deleteMatcher));
            });
        EXPECT_THAT(graphUpdateQuads,
                    testing::ElementsAreArray(transformedMatchers));
      };

  auto expectComputeGraphUpdateQuadsFails =
      [&executeComputeGraphUpdateQuads](
          const std::string& update,
          const Matcher<const std::string&>& messageMatcher,
          source_location sourceLocation = AD_CURRENT_SOURCE_LOC()) {
        auto l = generateLocationTrace(sourceLocation);
        AD_EXPECT_THROW_WITH_MESSAGE(executeComputeGraphUpdateQuads(update),
                                     messageMatcher);
      };
  {
    // These tests run on the default dataset defined in
    // `IndexTestHelpers::makeTestIndex`.
    qec = ad_utility::testing::getQec(std::nullopt);
    auto Id = ad_utility::testing::makeGetId(qec->getIndex());
    defaultGraphId = Id(std::string{DEFAULT_GRAPH_IRI});

    LocalVocab localVocab;
    auto LVI = [&localVocab](const std::string& iri) {
      return Id::makeFromLocalVocabIndex(
          localVocab.getIndexAndAddIfNotContained(LocalVocabEntry(
              ad_utility::triple_component::Iri::fromIriref(iri))));
    };

    expectComputeGraphUpdateQuads(
        "INSERT DATA { <s> <p> <o> . }",
        {ElementsAreArray({IdTriple(LVI("<s>"), LVI("<p>"), LVI("<o>"))})},
        {IsEmpty()});
    expectComputeGraphUpdateQuads(
        "DELETE DATA { <z> <label> \"zz\"@en }", {IsEmpty()},
        {ElementsAreArray(
            {IdTriple(Id("<z>"), Id("<label>"), Id("\"zz\"@en"))})});
    expectComputeGraphUpdateQuads(
        "DELETE { ?s <is-a> ?o } INSERT { <s> <p> <o> } WHERE { ?s <is-a> ?o }",
        {ElementsAreArray({IdTriple(LVI("<s>"), LVI("<p>"), LVI("<o>"))})},
        {ElementsAreArray({IdTriple(Id("<x>"), Id("<is-a>"), Id("<y>")),
                           IdTriple(Id("<y>"), Id("<is-a>"), Id("<x>"))})});
    expectComputeGraphUpdateQuads(
        "DELETE { <s> <p> <o> } INSERT { <s> <p> <o> } WHERE { ?s <is-a> ?o }",
        {ElementsAreArray({IdTriple(LVI("<s>"), LVI("<p>"), LVI("<o>"))})},
        {IsEmpty()});
    expectComputeGraphUpdateQuads(
        "DELETE { ?s <is-a> ?o } INSERT { ?s <is-a> ?o } WHERE { ?s <is-a> ?o "
        "}",
        {ElementsAreArray({IdTriple(Id("<x>"), Id("<is-a>"), Id("<y>")),
                           IdTriple(Id("<y>"), Id("<is-a>"), Id("<x>"))})},
        {IsEmpty()});
    auto allTriplesWith = [&Id,
                           &IdTriple](std::optional<::Id> g = std::nullopt) {
      return std::vector{IdTriple(Id("<x>"), Id("<label>"), Id("\"alpha\""), g),
                         IdTriple(Id("<x>"), Id("<label>"), Id("\"älpha\""), g),
                         IdTriple(Id("<x>"), Id("<label>"), Id("\"A\""), g),
                         IdTriple(Id("<x>"), Id("<label>"), Id("\"Beta\""), g),
                         IdTriple(Id("<x>"), Id("<is-a>"), Id("<y>"), g),
                         IdTriple(Id("<y>"), Id("<is-a>"), Id("<x>"), g),
                         IdTriple(Id("<z>"), Id("<label>"), Id("\"zz\"@en"), g),
                         IdTriple(Id("<zz>"), Id("<label>"), Id("<zz>"), g)};
    };
    auto allTriples = UnorderedElementsAreArray(allTriplesWith(std::nullopt));
    expectComputeGraphUpdateQuads("DELETE WHERE { ?s ?p ?o }", {IsEmpty()},
                                  {allTriples});
    expectComputeGraphUpdateQuadsFails(
        "SELECT * WHERE { ?s ?p ?o }",
        HasSubstr(
            R"(Invalid SPARQL query: Token "SELECT": mismatched input 'SELECT')"));
    expectComputeGraphUpdateQuads("CLEAR DEFAULT", {IsEmpty()}, {allTriples});
    expectComputeGraphUpdateQuads("CLEAR GRAPH <x>", {IsEmpty()}, {IsEmpty()});
    expectComputeGraphUpdateQuads("CLEAR NAMED", {IsEmpty()}, {IsEmpty()});
    expectComputeGraphUpdateQuads("CLEAR ALL", {IsEmpty()}, {allTriples});
    expectComputeGraphUpdateQuads("DROP DEFAULT", {IsEmpty()}, {allTriples});
    expectComputeGraphUpdateQuads("DROP GRAPH <x>", {IsEmpty()}, {IsEmpty()});
    expectComputeGraphUpdateQuads("DROP NAMED", {IsEmpty()}, {IsEmpty()});
    expectComputeGraphUpdateQuads("DROP ALL", {IsEmpty()}, {allTriples});
    expectComputeGraphUpdateQuads(
        "ADD DEFAULT TO GRAPH <x>",
        {UnorderedElementsAreArray(allTriplesWith(Id("<x>")))}, {IsEmpty()});
    expectComputeGraphUpdateQuads("ADD <x> TO DEFAULT", {IsEmpty()},
                                  {IsEmpty()});
    expectComputeGraphUpdateQuads("MOVE DEFAULT TO DEFAULT", {}, {});
    expectComputeGraphUpdateQuads("MOVE <x> TO GRAPH <x>", {}, {});
    expectComputeGraphUpdateQuads(
        "MOVE DEFAULT TO <y>",
        {IsEmpty(), UnorderedElementsAreArray(allTriplesWith(Id("<y>"))),
         IsEmpty()},
        {IsEmpty(), IsEmpty(), allTriples});
    expectComputeGraphUpdateQuads("MOVE GRAPH <y> TO DEFAULT",
                                  {IsEmpty(), IsEmpty(), IsEmpty()},
                                  {allTriples, IsEmpty(), IsEmpty()});
    expectComputeGraphUpdateQuads(
        "COPY DEFAULT TO GRAPH <x>",
        {IsEmpty(), UnorderedElementsAreArray(allTriplesWith(Id("<x>")))},
        {IsEmpty(), IsEmpty()});
    expectComputeGraphUpdateQuads("CREATE GRAPH <x>", {}, {});
    expectComputeGraphUpdateQuads("CREATE GRAPH <foo>", {}, {});
  }
  {
    // An Index with Quads/triples that are not in the default graph.
    ad_utility::testing::TestIndexConfig config{
        "<a> <a> <a> <a> . <b> <b> <b> <b> . <c> <c> <c> <c> . <d> <d> <d> ."};
    config.indexType = qlever::Filetype::NQuad;
    qec = ad_utility::testing::getQec(std::move(config));
    auto Id = ad_utility::testing::makeGetId(qec->getIndex());
    auto QuadFrom = [&IdTriple](const ::Id& id) {
      return IdTriple(id, id, id, id);
    };
    defaultGraphId = Id(std::string{DEFAULT_GRAPH_IRI});

    expectComputeGraphUpdateQuads("DELETE WHERE { GRAPH <a> { ?s ?p ?o } }",
                                  {IsEmpty()},
                                  {ElementsAreArray({QuadFrom(Id("<a>"))})});
    expectComputeGraphUpdateQuads("DELETE WHERE { GRAPH ?g { <a> <a> <a> } }",
                                  {IsEmpty()},
                                  {ElementsAreArray({QuadFrom(Id("<a>"))})});
    expectComputeGraphUpdateQuads(
        "DELETE WHERE { GRAPH ?g { ?s ?p ?o } }", {IsEmpty()},
        {ElementsAreArray(
            {QuadFrom(Id("<a>")), QuadFrom(Id("<b>")), QuadFrom(Id("<c>")),
             IdTriple(Id("<d>"), Id("<d>"), Id("<d>"), defaultGraphId)})});
    // TODO<qup42>: the second triple is technically not correct. the funky
    // behaviour is caused by the default query graph being the union graph.
    expectComputeGraphUpdateQuads(
        "DELETE WHERE { GRAPH <a> { ?s ?p ?o } . ?s ?p ?o }", {IsEmpty()},
        {ElementsAreArray(
            {QuadFrom(Id("<a>")), IdTriple(Id("<a>"), Id("<a>"), Id("<a>"))})});
  }
}

// _____________________________________________________________________________
TEST(ExecuteUpdate, transformTriplesTemplate) {
  // Create an index for testing.
  EncodedIriManager encodedIriManager({"http://example.org/"});
  // <http://example.org/123> is an encoded IRI
  ad_utility::testing::TestIndexConfig indexConfig{
      "<bar> <bar> \"foo\" . <http://example.org/123> <http://qlever.dev/1> "
      "\"baz\" ."};
  indexConfig.encodedIriManager = encodedIriManager;
  Index index = ad_utility::testing::makeTestIndex(
      "_ExecuteUppdateTest_transformTriplesTemplate", indexConfig);
  // We need a non-const vocab for the test.
  auto& vocab = const_cast<Index::Vocab&>(index.getVocab());

  // Helpers
  using namespace ::testing;
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
  using MatcherType = Matcher<const ExecuteUpdate::IdOrVariableIndex&>;
  auto TripleComponentMatcher = [](const ::LocalVocab& localVocab,
                                   TripleComponentT component) -> MatcherType {
    return std::visit(
        ad_utility::OverloadCallOperator{
            [](const ::Id& id) -> MatcherType {
              return VariantWith<::Id>(Eq(id));
            },
            [](const ColumnIndex& index) -> MatcherType {
              return VariantWith<ColumnIndex>(Eq(index));
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
              return VariantWith<::Id>(
                  AD_PROPERTY(Id, getBits, Eq(id.getBits())));
            }},
        component);
  };
  auto expectTransformTriplesTemplate =
      [&vocab, &TripleComponentMatcher, &encodedIriManager](
          const VariableToColumnMap& variableColumns,
          std::vector<SparqlTripleSimpleWithGraph>&& triples,
          const std::vector<std::array<TripleComponentT, 4>>&
              expectedTransformedTriples,
          ad_utility::source_location sourceLocation =
              AD_CURRENT_SOURCE_LOC()) {
        auto loc = generateLocationTrace(sourceLocation);
        auto [transformedTriples, localVocab] =
            ExecuteUpdate::transformTriplesTemplate(encodedIriManager, vocab,
                                                    variableColumns, triples);
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
                    ElementsAreArray(transformedTriplesMatchers));
      };
  auto expectTransformTriplesTemplateFails =
      [&vocab, &encodedIriManager](
          const VariableToColumnMap& variableColumns,
          std::vector<SparqlTripleSimpleWithGraph>&& triples,
          const Matcher<const std::string&>& messageMatcher,
          ad_utility::source_location sourceLocation =
              AD_CURRENT_SOURCE_LOC()) {
        auto loc = generateLocationTrace(sourceLocation);
        AD_EXPECT_THROW_WITH_MESSAGE(
            ExecuteUpdate::transformTriplesTemplate(
                encodedIriManager, vocab, variableColumns, std::move(triples)),
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
                                   Literal("\"foo\""), Graph{Iri("<baz>")}}},
      {{Id("\"foo\""), Id("<bar>"), Id("\"foo\""), LocalVocab(Iri("<baz>"))}});
  // A variable in the template (`?f`) is not mapped in the
  // `VariableToColumnMap`.
  expectTransformTriplesTemplateFails(
      {},
      {SparqlTripleSimpleWithGraph{Literal("\"foo\""), Iri("<bar>"),
                                   Variable("?f"), Graph{}}},
      HasSubstr("Assertion `variableColumns.contains(tc.getVariable())` "
                "failed."));
  expectTransformTriplesTemplateFails(
      {},
      {SparqlTripleSimpleWithGraph{Literal("\"foo\""), Iri("<bar>"),
                                   Literal("\"foo\""), Graph{Variable("?f")}}},
      HasSubstr("Assertion `variableColumns.contains(var)` failed."));
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
  expectTransformTriplesTemplate(
      {},
      {SparqlTripleSimpleWithGraph{Iri("<http://example.org/123>"),
                                   Iri("<http://qlever.dev/1>"),
                                   Literal("\"baz\""), Graph{}}},
      {{encodedIriManager.encode("<http://example.org/123>").value(),
        Id("<http://qlever.dev/1>"), Id("\"baz\""), defaultGraphId}});
}

// _____________________________________________________________________________
TEST(ExecuteUpdate, resolveVariable) {
  using namespace ::testing;
  const auto idTable =
      makeIdTableFromVector({{V(0), V(1), V(2)},
                             {V(3), V(4), V(5)},
                             {V(6), Id::makeUndefined(), V(8)}});
  auto resolveVariable =
      std::bind_front(&ExecuteUpdate::resolveVariable, std::cref(idTable));
  EXPECT_THAT(resolveVariable(0, V(10)), Eq(V(10)));
  EXPECT_THAT(resolveVariable(0, 1UL), Eq(V(1)));
  EXPECT_THAT(resolveVariable(1, 1UL), Eq(V(4)));
  EXPECT_THAT(resolveVariable(2, 1UL), Eq(std::nullopt));
  EXPECT_THAT(resolveVariable(2, Id::makeUndefined()), Eq(std::nullopt));
}

// _____________________________________________________________________________
TEST(ExecuteUpdate, computeAndAddQuadsForResultRow) {
  using namespace ::testing;
  const auto idTable =
      makeIdTableFromVector({{V(0), V(1), V(2)},
                             {V(3), V(4), V(5)},
                             {V(6), Id::makeUndefined(), V(8)}});
  auto expectComputeQuads =
      [](const std::vector<ExecuteUpdate::TransformedTriple>& templates,
         const IdTable& idTable, uint64_t rowIdx,
         const Matcher<const std::vector<IdTriple<>>&>& expectedQuads) {
        std::vector<IdTriple<>> result;
        ExecuteUpdate::computeAndAddQuadsForResultRow(templates, result,
                                                      idTable, rowIdx);
        EXPECT_THAT(result, expectedQuads);
      };
  // Compute the quads for an empty template set yields no quads.
  expectComputeQuads({}, idTable, 0, IsEmpty());
  // Compute the quads for template without variables yields the templates
  // unmodified.
  expectComputeQuads({{V(0), V(1), V(2), V(3)}}, idTable, 0,
                     ElementsAreArray({IdTriple{{V(0), V(1), V(2), V(3)}}}));
  expectComputeQuads({{V(0), V(1), V(2), V(3)}}, idTable, 1,
                     ElementsAreArray({IdTriple{{V(0), V(1), V(2), V(3)}}}));
  // The variables in templates are resolved to the value of the variable in the
  // specified row of the result.
  expectComputeQuads({{0UL, V(1), 1UL, V(3)}}, idTable, 0,
                     ElementsAreArray({IdTriple{{V(0), V(1), V(1), V(3)}}}));
  expectComputeQuads({{0UL, V(1), 1UL, V(3)}}, idTable, 1,
                     ElementsAreArray({IdTriple{{V(3), V(1), V(4), V(3)}}}));
  // Quads with undefined IDs cannot be stored and are not returned.
  expectComputeQuads({{0UL, V(1), 1UL, V(3)}}, idTable, 2, IsEmpty());
  expectComputeQuads({{V(0), V(1), Id::makeUndefined(), V(3)}}, idTable, 0,
                     IsEmpty());
  // Some extra cases to cover all branches.
  expectComputeQuads({{Id::makeUndefined(), V(1), V(2), V(3)}}, idTable, 0,
                     IsEmpty());
  expectComputeQuads({{V(0), Id::makeUndefined(), V(2), V(3)}}, idTable, 0,
                     IsEmpty());
  expectComputeQuads({{V(0), V(1), V(2), Id::makeUndefined()}}, idTable, 0,
                     IsEmpty());
  // All the templates are evaluated for the specified row of the result.
  expectComputeQuads({{0UL, V(1), 1UL, V(3)}, {V(0), 1UL, 2UL, V(3)}}, idTable,
                     0,
                     ElementsAreArray({IdTriple{{V(0), V(1), V(1), V(3)}},
                                       IdTriple{{V(0), V(1), V(2), V(3)}}}));
}

TEST(ExecuteUpdate, sortAndRemoveDuplicates) {
  auto expect = [](std::vector<IdTriple<>> input,
                   const std::vector<IdTriple<>>& expected,
                   source_location l = AD_CURRENT_SOURCE_LOC()) {
    auto trace = generateLocationTrace(l);
    ExecuteUpdate::sortAndRemoveDuplicates(input);
    EXPECT_THAT(input, testing::ElementsAreArray(expected));
  };
  auto IdTriple = [&](uint64_t s, uint64_t p, uint64_t o, uint64_t g = 0) {
    return ::IdTriple({V(s), V(p), V(o), V(g)});
  };
  expect({}, {});
  expect({IdTriple(1, 1, 1)}, {IdTriple(1, 1, 1)});
  expect({IdTriple(1, 1, 1), IdTriple(2, 2, 2)},
         {IdTriple(1, 1, 1), IdTriple(2, 2, 2)});
  expect({IdTriple(2, 2, 2), IdTriple(1, 1, 1)},
         {IdTriple(1, 1, 1), IdTriple(2, 2, 2)});
  expect({IdTriple(1, 1, 1), IdTriple(1, 1, 1)}, {IdTriple(1, 1, 1)});
  expect({IdTriple(2, 2, 2), IdTriple(3, 3, 3), IdTriple(3, 3, 3),
          IdTriple(2, 2, 2), IdTriple(1, 1, 1)},
         {IdTriple(1, 1, 1), IdTriple(2, 2, 2), IdTriple(3, 3, 3)});
}
TEST(ExecuteUpdate, setMinus) {
  auto expect = [](std::vector<IdTriple<>> a, std::vector<IdTriple<>> b,
                   const std::vector<IdTriple<>>& expected,
                   source_location l = AD_CURRENT_SOURCE_LOC()) {
    auto trace = generateLocationTrace(l);
    EXPECT_THAT(ExecuteUpdate::setMinus(a, b),
                testing::ElementsAreArray(expected));
  };
  auto IdTriple = [&](uint64_t s, uint64_t p, uint64_t o, uint64_t g = 0) {
    return ::IdTriple({V(s), V(p), V(o), V(g)});
  };
  expect({}, {}, {});
  expect({IdTriple(1, 2, 3), IdTriple(4, 5, 6)}, {},
         {IdTriple(1, 2, 3), IdTriple(4, 5, 6)});
  expect({IdTriple(1, 2, 3), IdTriple(4, 5, 6), IdTriple(7, 8, 9)},
         {IdTriple(4, 5, 6), IdTriple(7, 8, 9)}, {IdTriple(1, 2, 3)});
  expect({IdTriple(1, 2, 3)},
         {IdTriple(1, 2, 3), IdTriple(4, 5, 6), IdTriple(7, 8, 9)}, {});
}
