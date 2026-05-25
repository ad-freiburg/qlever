// Copyright 2026, University of Freiburg,
// Chair of Algorithms and Data Structures.

#include <absl/cleanup/cleanup.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "engine/MaterializedViews.h"
#include "engine/NamedResultCache.h"
#include "engine/QueryExecutionContext.h"
#include "engine/QueryPlanner.h"
#include "engine/Reasoner.h"
#include "engine/ReasonerRules.h"
#include "engine/Server.h"
#include "global/RuntimeParameters.h"
#include "index/DeltaTriples.h"
#include "index/Index.h"
#include "util/IndexTestHelpers.h"

using namespace testing;

namespace {

// Turtle input with a small OWL/RDFS ontology for testing materialisation.
//   • Two-hop subclass hierarchy: Cat → Animal → LivingThing
//   • Instance: felix type Cat
//   • Symmetric property: knows, with one base assertion felix knows garfield
//   • InverseOf: friendOf inverseOf hasFriend, with felix friendOf garfield
static const std::string kOwlTurtle = R"(
<http://ex.org/Cat>      <http://www.w3.org/2000/01/rdf-schema#subClassOf>
                         <http://ex.org/Animal> .
<http://ex.org/Animal>   <http://www.w3.org/2000/01/rdf-schema#subClassOf>
                         <http://ex.org/LivingThing> .
<http://ex.org/felix>    <http://www.w3.org/1999/02/22-rdf-syntax-ns#type>
                         <http://ex.org/Cat> .
<http://ex.org/knows>    <http://www.w3.org/1999/02/22-rdf-syntax-ns#type>
                         <http://www.w3.org/2002/07/owl#SymmetricProperty> .
<http://ex.org/felix>    <http://ex.org/knows>
                         <http://ex.org/garfield> .
<http://ex.org/friendOf> <http://www.w3.org/2002/07/owl#inverseOf>
                         <http://ex.org/hasFriend> .
<http://ex.org/felix>    <http://ex.org/friendOf>
                         <http://ex.org/garfield> .
)";

// Helper: run Reasoner::materialize on the given Index and return the result.
Reasoner::MaterializationResult runMaterialize(Index& index) {
  const auto handle = std::make_shared<ad_utility::CancellationHandle<>>();
  const auto targetGraph =
      ad_utility::triple_component::Iri::fromIriref(QLEVER_INFERRED_GRAPH_IRI);

  QueryResultCache cache;
  NamedResultCache namedResultCache;
  auto materializedViewsManager = std::make_shared<MaterializedViewsManager>();
  auto indexPtr = std::shared_ptr<Index>(&index, [](Index*) {});
  QueryExecutionContext qec(indexPtr, &cache,
                            ad_utility::testing::makeAllocator(
                                ad_utility::MemorySize::megabytes(100)),
                            SortPerformanceEstimator{}, &namedResultCache,
                            materializedViewsManager);

  Reasoner::MaterializationResult result;
  index.deltaTriplesManager().modify<void>([&](DeltaTriples& deltaTriples) {
    qec.setLocatedTriplesForEvaluation(
        deltaTriples.getLocatedTriplesSharedStateReference());
    result =
        Reasoner::materialize(index, deltaTriples, qec, targetGraph, handle);
  });
  return result;
}

}  // namespace

// ─────────────────────────────────────────────────────────────────────────────
// Rule list sanity checks
// ─────────────────────────────────────────────────────────────────────────────
TEST(ReasonerRules, getAllReasonerRulesNonEmpty) {
  const auto& rules = getAllReasonerRules();
  EXPECT_FALSE(rules.empty());
  // Every rule must have a non-empty name, query, input and output predicates.
  for (const auto& rule : rules) {
    EXPECT_FALSE(rule.name.empty()) << "Rule name is empty";
    EXPECT_FALSE(rule.constructQuery.empty()) << "Rule " << rule.name;
    EXPECT_FALSE(rule.inputPredicates.empty()) << "Rule " << rule.name;
    EXPECT_FALSE(rule.outputPredicates.empty()) << "Rule " << rule.name;
  }
}

TEST(ReasonerRules, ruleNamesAreUnique) {
  const auto& rules = getAllReasonerRules();
  std::set<std::string> names;
  for (const auto& r : rules) {
    EXPECT_TRUE(names.insert(r.name).second)
        << "Duplicate rule name: " << r.name;
  }
}

TEST(ReasonerRules, allQueriesContainConstruct) {
  for (const auto& rule : getAllReasonerRules()) {
    EXPECT_NE(rule.constructQuery.find("CONSTRUCT"), std::string::npos)
        << "Rule " << rule.name << " query does not contain CONSTRUCT";
    EXPECT_NE(rule.constructQuery.find("WHERE"), std::string::npos)
        << "Rule " << rule.name << " query does not contain WHERE";
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// Core materialisation tests
// ─────────────────────────────────────────────────────────────────────────────

// The small OWL ontology defined in kOwlTurtle should produce at least the
// five canonical inferences:
//   1. Cat  rdfs:subClassOf  LivingThing          (scm-sco transitivity)
//   2. felix rdf:type        Animal               (cax-sco)
//   3. felix rdf:type        LivingThing          (cax-sco)
//   4. garfield ex:knows     felix                (prp-symp)
//   5. garfield ex:hasFriend felix                (prp-inv1)
//
// The engine additionally derives two "redundant" triples into the inferred
// graph for triples that already exist in the base index (different graph →
// still a new delta-triple):
//   6. felix ex:friendOf     garfield  (prp-inv2, base triple → inferred graph)
//   7. felix ex:knows        garfield  (prp-symp round 2, base → inferred)
//
// So numInsertedAfter - numInsertedBefore == 7.
TEST(Reasoner, materializeOwlOntology) {
  Index index = ad_utility::testing::makeTestIndex(
      "Reasoner_owlOntology", ad_utility::testing::TestIndexConfig(kOwlTurtle));

  auto result = runMaterialize(index);

  // At least the 5 truly new inferences (+ 2 redundant = 7 total).
  EXPECT_GE(result.totalNewTriples, 5u);
  EXPECT_GE(result.numInsertedAfter - result.numInsertedBefore, 5u);

  // Fixpoint must be reached in ≤ 5 rounds for this small ontology.
  EXPECT_LE(result.numRounds, 5u);
  EXPECT_GE(result.numRounds, 1u);

  // At least scm-sco, cax-sco, prp-inv1, prp-symp fired.
  EXPECT_GE(result.numRulesActivated, 4u);

  // Per-rule stats must cover all rules.
  EXPECT_EQ(result.triplesPerRule.size(), getAllReasonerRules().size());

  // scm-sco must have produced at least 1 new triple (Cat → LivingThing).
  auto it =
      std::find_if(result.triplesPerRule.begin(), result.triplesPerRule.end(),
                   [](const auto& p) { return p.first == "scm-sco"; });
  ASSERT_NE(it, result.triplesPerRule.end());
  EXPECT_GE(it->second, 1u)
      << "scm-sco should have inferred Cat subClassOf LivingThing";

  // cax-sco must have produced at least 2 new triples (type Animal +
  // LivingThing).
  auto it2 =
      std::find_if(result.triplesPerRule.begin(), result.triplesPerRule.end(),
                   [](const auto& p) { return p.first == "cax-sco"; });
  ASSERT_NE(it2, result.triplesPerRule.end());
  EXPECT_GE(it2->second, 2u)
      << "cax-sco should have inferred felix type Animal and LivingThing";
}

// An index with NO OWL/RDFS predicates at all should produce zero inferences
// and reach fixpoint in round 1 (the single pass over all rules yields 0 new
// triples, so we break immediately after round 1).
TEST(Reasoner, emptyOntologyReachesFixpointImmediately) {
  // Only plain, non-ontological triples.
  const std::string plainTurtle = R"(
<http://ex.org/a> <http://ex.org/likes> <http://ex.org/b> .
<http://ex.org/c> <http://ex.org/likes> <http://ex.org/d> .
)";

  Index index = ad_utility::testing::makeTestIndex(
      "Reasoner_emptyOntology",
      ad_utility::testing::TestIndexConfig(plainTurtle));

  auto result = runMaterialize(index);

  EXPECT_EQ(result.totalNewTriples, 0u);
  EXPECT_EQ(result.numInsertedAfter - result.numInsertedBefore, 0u);
  // The engine should have run exactly 1 round (and stopped because 0 new).
  EXPECT_EQ(result.numRounds, 1u);
  EXPECT_EQ(result.numRulesActivated, 0u);
}

// Calling materialize twice on the same index is idempotent with respect to
// the total number of delta insertions.  After the first call the delta
// contains all inferences; the second call should produce 0 additional
// triples (all are already in the delta).
TEST(Reasoner, materializeIsIdempotent) {
  Index index = ad_utility::testing::makeTestIndex(
      "Reasoner_idempotent", ad_utility::testing::TestIndexConfig(kOwlTurtle));

  auto r1 = runMaterialize(index);
  EXPECT_GT(r1.totalNewTriples, 0u);

  auto r2 = runMaterialize(index);
  // Second run: all inferences already in delta → 0 new triples.
  EXPECT_EQ(r2.totalNewTriples, 0u);
  EXPECT_EQ(r2.numInsertedAfter - r2.numInsertedBefore, 0u);
  EXPECT_EQ(r2.numRulesActivated, 0u);
}

// If reasoner-max-rounds is set to 1, the engine runs exactly one round and
// then stops even if more triples could be inferred.
TEST(Reasoner, maxRoundsLimitStopsEarly) {
  Index index = ad_utility::testing::makeTestIndex(
      "Reasoner_maxRounds", ad_utility::testing::TestIndexConfig(kOwlTurtle));

  // Set max-rounds to 1 so only the first round executes.
  globalRuntimeParameters.wlock()->setFromString("reasoner-max-rounds", "1");
  // Restore the original value when this scope exits.
  absl::Cleanup restoreMaxRounds{[] {
    globalRuntimeParameters.wlock()->setFromString("reasoner-max-rounds",
                                                   "100");
  }};

  auto result = runMaterialize(index);

  // The engine runs exactly 1 round (the first round always runs all rules).
  EXPECT_EQ(result.numRounds, 1u);
  // With only 1 round, all 14 rules execute so schema-level rules (scm-sco,
  // prp-symp, prp-inv1/2) fire and produce new triples.
  EXPECT_GT(result.totalNewTriples, 0u);
}

// Passing an already-cancelled handle should cause materialize to throw
// immediately (no rules execute, cancellation is detected in round 0).
TEST(Reasoner, cancelledHandleThrows) {
  Index index = ad_utility::testing::makeTestIndex(
      "Reasoner_cancelled", ad_utility::testing::TestIndexConfig(kOwlTurtle));

  const auto handle = std::make_shared<ad_utility::CancellationHandle<>>();
  const auto targetGraph =
      ad_utility::triple_component::Iri::fromIriref(QLEVER_INFERRED_GRAPH_IRI);

  // Cancel the handle before materialisation starts.
  handle->cancel(ad_utility::CancellationState::MANUAL);

  QueryResultCache cache;
  NamedResultCache namedResultCache;
  auto materializedViewsManager2 = std::make_shared<MaterializedViewsManager>();
  auto indexPtr2 = std::shared_ptr<Index>(&index, [](Index*) {});
  QueryExecutionContext qec(indexPtr2, &cache,
                            ad_utility::testing::makeAllocator(
                                ad_utility::MemorySize::megabytes(100)),
                            SortPerformanceEstimator{}, &namedResultCache,
                            materializedViewsManager2);

  EXPECT_THROW(
      index.deltaTriplesManager().modify<void>([&](DeltaTriples& deltaTriples) {
        qec.setLocatedTriplesForEvaluation(
            deltaTriples.getLocatedTriplesSharedStateReference());
        Reasoner::materialize(index, deltaTriples, qec, targetGraph, handle);
      }),
      ad_utility::CancellationException);
}

// Verify that the MaterializationResult JSON built by
// Server::createResponseMetadataForMaterialize contains the expected keys.
TEST(Reasoner, responseMetadataContainsExpectedKeys) {
  Reasoner::MaterializationResult result;
  result.totalNewTriples = 7;
  result.numRounds = 3;
  result.numRulesActivated = 5;
  result.triplesPerRule = {{"scm-sco", 1}, {"cax-sco", 2}};
  result.numInsertedBefore = 0;
  result.numInsertedAfter = 7;

  ad_utility::Timer timer(ad_utility::Timer::Started);
  auto json = Server::createResponseMetadataForMaterialize(result, timer);

  EXPECT_EQ(json["status"], "OK");
  EXPECT_TRUE(json.contains("materialize"));
  EXPECT_EQ(json["materialize"]["total-new-triples"], 7);
  EXPECT_EQ(json["materialize"]["num-rounds"], 3);
  EXPECT_EQ(json["materialize"]["rules-fired"], 5);
  EXPECT_TRUE(json["materialize"]["rules"].is_array());
  EXPECT_EQ(json["materialize"]["rules"].size(), 2u);
  EXPECT_EQ(json["delta-triples"]["new"], 7);
}
