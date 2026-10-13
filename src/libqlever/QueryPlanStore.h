// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_LIBQLEVER_QUERYPLANSTORE_H
#define QLEVER_SRC_LIBQLEVER_QUERYPLANSTORE_H

#include <initializer_list>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

#include "engine/ExternalValues.h"
#include "libqlever/QleverTypes.h"
#include "parser/GraphPatternOperation.h"
#include "util/Exception.h"

namespace qlever {

// A store for query templates that were planned once and are then executed
// repeatedly with varying values, keyed by name.
//
// The intended use is the following: parse and plan a query once (via
// `Qlever::parseQuery` and `Qlever::planQuery`), `store` the resulting
// `PlannedQuery` under a name, and then `prepare` it with concrete values as
// often as desired. This skips the parsing and the planning entirely. The
// values are supplied through the `ExternalValues` operation, so each stored
// query has to contain exactly one `ExternalValues` placeholder.
//
// NOTE: A `PlannedQuery` is tied to the `Qlever` instance it was planned
// against, so there has to be one store per instance.
class QueryPlanStore {
 private:
  // The stored templates.
  //
  // NOTE: The keys are `string_view`s, so the strings they refer to have to be
  // owned by the caller and outlive the stored plans.
  std::unordered_map<std::string_view, PlannedQuery> queryPlans_;

 public:
  // Store `plan` under `key`, replacing a plan that is already stored there.
  void store(std::string_view key, PlannedQuery plan) {
    // NOTE: `PlannedQuery` is not default-constructible, so `operator[]` cannot
    // be used here.
    queryPlans_.insert_or_assign(key, std::move(plan));
  }

  // Return the template stored under `key`. Throw if there is none; use
  // `contains` to check first.
  const PlannedQuery& get(std::string_view key) const {
    auto it = queryPlans_.find(key);
    AD_CONTRACT_CHECK(it != queryPlans_.end(),
                      "No query plan stored for the given key");
    return it->second;
  }

  // Return true if a template is stored under `key`.
  bool contains(std::string_view key) const {
    return queryPlans_.count(key) > 0;
  }

  // Remove all stored templates.
  void clear() { queryPlans_.clear(); }

  // Instantiate the template stored under `key`: inject `ids` (mapped to
  // `TripleComponent`s via `valueForId`) into its single `ExternalValues`
  // placeholder under the variables `varNames`, and return the ready-to-run
  // plan. The stored template is left unchanged, so it can be instantiated
  // again.
  //
  // NOTE: `ExternalValues` does not support cache keys, so the `Qlever`
  // instance that the template was planned against has to be configured with
  // `EngineConfig::disableCaching_` set to `DisableCaching::True`; otherwise
  // executing the returned plan throws.
  template <typename IdContainer, typename ValueForIdFunc>
  PlannedQuery prepare(std::string_view key, const IdContainer& ids,
                       ValueForIdFunc&& valueForId,
                       std::initializer_list<std::string> varNames) const;
};

// ____________________________________________________________________________
template <typename IdContainer, typename ValueForIdFunc>
PlannedQuery QueryPlanStore::prepare(
    std::string_view key, const IdContainer& ids, ValueForIdFunc&& valueForId,
    std::initializer_list<std::string> varNames) const {
  // Copy the template plan and give the copy its own query execution tree, so
  // that the stored template is not mutated by the injection below.
  PlannedQuery plan = get(key);
  plan.cloneQetInPlace();

  // Find the single `ExternalValues` placeholder.
  std::vector<ExternalValues*> values;
  plan.queryExecutionTree().getRootOperation()->getExternalValues(values);
  AD_CORRECTNESS_CHECK(values.size() == 1,
                       "Expected exactly one external values, but got ",
                       values.size());

  // Build the `SparqlValues`: variable list + one row per `id`.
  parsedQuery::SparqlValues v;
  for (auto const& name : varNames) {
    v._variables.emplace_back(Variable{name});
  }
  for (auto const& id : ids) {
    // `valueForId(id)` must return something convertible to
    // `std::vector<TripleComponent>`.
    auto comps = std::forward<ValueForIdFunc>(valueForId)(id);
    v._values.emplace_back(std::move(comps));
  }

  values.at(0)->updateValues(std::move(v));
  return plan;
}
}  // namespace qlever

#endif  // QLEVER_SRC_LIBQLEVER_QUERYPLANSTORE_H
