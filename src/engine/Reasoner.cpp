// Copyright 2026, University of Freiburg,
// Chair of Algorithms and Data Structures.

#include "engine/Reasoner.h"

#include <absl/strings/str_cat.h>

#include <string_view>

#include "engine/ExecuteUpdate.h"
#include "engine/QueryPlanner.h"
#include "engine/ReasonerRules.h"
#include "global/RuntimeParameters.h"
#include "parser/ParsedQuery.h"
#include "parser/SparqlParser.h"
#include "util/Algorithm.h"
#include "util/Log.h"

// ─────────────────────────────────────────────────────────────────────────────
// Internal helpers
// ─────────────────────────────────────────────────────────────────────────────
namespace {

// Tracks which output predicates became newly active in the last round, used
// to decide which rules should fire in the next round (semi-naive evaluation).
struct SemiNaiveTracker {
  // Views borrow from the static kRules vector — no string copies needed.
  // With ≤9 distinct values a flat vector + linear search is faster than
  // hashing and avoids all per-element heap allocations.
  std::vector<std::string_view> newPredicates;
  // True when a rule with a variable-predicate CONSTRUCT head fired, meaning
  // that any rule whose input includes WILDCARD should be re-activated.
  bool wildcardActive = false;

  // Record the output predicates of a rule that produced new triples.
  void recordOutputs(const ReasonerRule& rule) {
    for (const auto& p : rule.outputPredicates) {
      if (p == reasoner_iris::WILDCARD) {
        wildcardActive = true;
      } else {
        // Deduplicate in-place (set semantics without hashing).
        std::string_view sv{p};
        if (!ad_utility::contains(newPredicates, sv)) {
          newPredicates.push_back(sv);
        }
      }
    }
  }

  // True if the given rule should run in the next round.
  [[nodiscard]] bool isActive(const ReasonerRule& rule) const {
    return std::ranges::any_of(rule.inputPredicates, [this](const auto& p) {
      if (p == reasoner_iris::WILDCARD) {
        return wildcardActive;
      }
      return ad_utility::contains(newPredicates, std::string_view{p});
    });
  }

  void reset() {
    newPredicates.clear();
    wildcardActive = false;
  }
};

// A rule that has been pre-parsed and is ready to be re-planned each round.
struct PreparedRule {
  const ReasonerRule& rule;
  ParsedQuery parsedQuery;
  size_t cumulativeNewTriples = 0;
};

}  // namespace

// ─────────────────────────────────────────────────────────────────────────────
// Reasoner::extractPredicatesFromUpdate
// ─────────────────────────────────────────────────────────────────────────────
std::vector<std::string> Reasoner::extractPredicatesFromUpdate(
    const ParsedQuery& update) {
  AD_CONTRACT_CHECK(update.hasUpdateClause());

  std::vector<std::string> result;

  // Iterate the triple templates and collect predicate IRIs (or WILDCARD for
  // variable predicates).  Duplicates are suppressed with a linear scan —
  // the number of distinct predicates in a single UPDATE is small.
  auto addFromTriples =
      [&result](const std::vector<SparqlTripleSimpleWithGraph>& triples) {
        for (const auto& triple : triples) {
          if (triple.p_.isIri()) {
            // angle-bracket form matches the constants in ReasonerRules.h.
            std::string iri = triple.p_.getIri().toStringRepresentation();
            if (!ad_utility::contains(result, iri)) {
              result.push_back(std::move(iri));
            }
          } else if (triple.p_.isVariable()) {
            // Unknown predicate at parse time: use the wildcard sentinel so
            // that all rules with variable-predicate WHERE clauses are
            // re-activated.
            std::string wc{reasoner_iris::WILDCARD};
            if (!ad_utility::contains(result, wc)) {
              result.push_back(std::move(wc));
            }
          }
        }
      };

  const auto& op = update.updateClause().op_;
  addFromTriples(op.toInsert_.triples_);
  addFromTriples(op.toDelete_.triples_);
  return result;
}

// ─────────────────────────────────────────────────────────────────────────────
// Reasoner::materialize
// ─────────────────────────────────────────────────────────────────────────────
Reasoner::MaterializationResult Reasoner::materialize(
    const Index& index, DeltaTriples& deltaTriples,
    QueryExecutionContext& qec,
    const ad_utility::triple_component::Iri& targetGraph,
    const CancellationHandle& handle, std::vector<std::string> seedPredicates) {
  MaterializationResult result;
  result.numInsertedBefore = deltaTriples.numInserted();

  const size_t maxRounds =
      getRuntimeParameter<&RuntimeParameters::reasonerMaxRounds_>();
  // maxRounds == 0 means unlimited (run until fixpoint or until cancelled).
  const bool unlimitedRounds = (maxRounds == 0);

  const auto& rules = getAllReasonerRules();

  // ── Pre-parse all rule queries once (caches ANTLR parsing overhead). ──────
  // Query *planning* is intentionally deferred to each round so that the
  // planner sees current index statistics after previous-round insertions.
  std::vector<PreparedRule> prepared;
  prepared.reserve(rules.size());
  for (auto& rule : rules) {
    ParsedQuery pq = SparqlParser::parseQuery(&index.encodedIriManager(),
                                              rule.constructQuery);
    prepared.emplace_back(rule, std::move(pq), 0);
  }

  const size_t maxTriples =
      getRuntimeParameter<&RuntimeParameters::constructInsertMaxTriples_>();

  // ── Seed the semi-naive tracker from caller-supplied predicates ───────────
  // When seeds are non-empty, semi-naive evaluation applies from round 0:
  // only rules whose inputPredicates overlap with the seeds will fire in the
  // first round (incremental mode). Empty seeds give the default "full"
  // behaviour where all rules fire in round 0.
  //
  // The string_views added here borrow from `seedPredicates`, which is owned
  // by the caller and outlives this function.  After round 0 the tracker is
  // replaced by `nextTracker` (whose views borrow from the static kRules),
  // so there are no dangling references.
  SemiNaiveTracker tracker;
  const bool hasSeeds = !seedPredicates.empty();
  if (hasSeeds) {
    for (const auto& p : seedPredicates) {
      if (p == reasoner_iris::WILDCARD) {
        tracker.wildcardActive = true;
      } else {
        std::string_view sv{p};
        if (!ad_utility::contains(tracker.newPredicates, sv)) {
          tracker.newPredicates.push_back(sv);
        }
      }
    }
    AD_LOG_INFO << "[Reasoner] Incremental materialisation seeded with "
                << tracker.newPredicates.size() << " predicate(s)"
                << (tracker.wildcardActive ? " + wildcard" : "") << "."
                << std::endl;
  }

  AD_LOG_INFO << "[Reasoner] Starting " << (hasSeeds ? "incremental " : "")
              << "materialisation with " << rules.size() << " rules, max "
              << maxRounds << " rounds." << std::endl;

  for (size_t round = 0; unlimitedRounds || round < maxRounds; ++round) {
    handle->throwIfCancelled();

    // Update augmented metadata so the query planner sees new delta triples
    // that were inserted in earlier rounds.
    if (round > 0) {
      deltaTriples.updateAugmentedMetadata();
    }

    size_t newTriplesThisRound = 0;
    SemiNaiveTracker nextTracker;

    // In the first round without seeds all rules run unconditionally (classic
    // semi-naive round 0).  With seeds, semi-naive already applies in round 0.
    const bool firstRound = !hasSeeds && (round == 0);

    for (auto& pr : prepared) {
      handle->throwIfCancelled();

      // Semi-naive: in rounds > 0 (or round 0 with seeds) skip rules whose
      // input predicates have not gained new triples in the previous round.
      if (!firstRound && !tracker.isActive(pr.rule)) {
        continue;
      }

      // Deep-copy the ParsedQuery before planning:
      // QueryPlanner::createExecutionTree takes ParsedQuery by mutable
      // reference and QueryPlanner::optimize() receives a non-const pointer to
      // the root graph pattern, so the query may be mutated during planning. We
      // keep `pr.parsedQuery` pristine for subsequent rounds.
      ParsedQuery pqForPlanning = pr.parsedQuery;

      // Re-plan the rule each round: a fresh QueryExecutionTree is produced so
      // the planner can incorporate cardinality changes from previous-round
      // insertions.
      QueryPlanner planner{&qec, handle};
      auto qet = planner.createExecutionTree(pqForPlanning);

      const int64_t before = deltaTriples.numInserted();
      ExecuteUpdate::executeConstructInsert(index, pqForPlanning, qet,
                                            deltaTriples, targetGraph, handle);
      const size_t newFromRule =
          static_cast<size_t>(deltaTriples.numInserted() - before);

      if (newFromRule > 0) {
        pr.cumulativeNewTriples += newFromRule;
        newTriplesThisRound += newFromRule;
        nextTracker.recordOutputs(pr.rule);

        AD_LOG_DEBUG << "[Reasoner] Round " << round << ", rule ["
                     << pr.rule.name << "]: +" << newFromRule << " triples"
                     << std::endl;
      }

      // Guard against runaway materialisation (0 = unlimited).
      if (maxTriples > 0 &&
          result.totalNewTriples + newTriplesThisRound > maxTriples) {
        throw std::runtime_error(absl::StrCat(
            "[Reasoner] construct-insert-max-triples limit (", maxTriples,
            ") exceeded during materialisation. Increase the limit via the "
            "'construct-insert-max-triples' runtime parameter if "
            "intentional."));
      }
    }

    result.numRounds++;
    result.totalNewTriples += newTriplesThisRound;

    AD_LOG_INFO << "[Reasoner] Round " << round << ": +" << newTriplesThisRound
                << " new triples (total so far: " << result.totalNewTriples
                << ")" << std::endl;

    if (newTriplesThisRound == 0) {
      AD_LOG_INFO << "[Reasoner] Fixpoint reached after " << result.numRounds
                  << " round(s). Total new triples: " << result.totalNewTriples
                  << std::endl;
      break;
    }

    tracker = std::move(nextTracker);
  }

  // ── Assemble per-rule statistics ──────────────────────────────────────────
  // Count rules that produced at least one triple (replaces the hot-loop set).
  result.triplesPerRule.reserve(prepared.size());
  for (const auto& pr : prepared) {
    result.triplesPerRule.emplace_back(pr.rule.name, pr.cumulativeNewTriples);
    if (pr.cumulativeNewTriples > 0) {
      ++result.numRulesActivated;
    }
  }

  result.numInsertedAfter = deltaTriples.numInserted();
  return result;
}
