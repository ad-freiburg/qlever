// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR

// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "engine/ExternalValues.h"

#include "absl/strings/str_cat.h"
#include "util/HashSet.h"

// ____________________________________________________________________________
ExternalValues::ExternalValues(QueryExecutionContext* qec,
                               parsedQuery::SparqlValues parsedValues,
                               std::string name)
    : Operation(qec),
      Values(qec, std::move(parsedValues)),
      name_(std::move(name)) {}

// ____________________________________________________________________________
ExternalValues::ExternalValues(QueryExecutionContext* qec,
                               const parsedQuery::ExternalValuesQuery& query)
    : ExternalValues(
          qec,
          [&query]() {
            // Check that all variables are unique.
            ad_utility::HashSet<Variable> uniqueVars(query.variables_.begin(),
                                                     query.variables_.end());
            AD_CONTRACT_CHECK(
                uniqueVars.size() == query.variables_.size(),
                "Variables in external values query must be unique");
            parsedQuery::SparqlValues values;
            values._variables = query.variables_;
            return values;
          }(),
          query.name_) {}

// ____________________________________________________________________________
std::string ExternalValues::getCacheKeyImpl() const {
  // ExternalValues must only be used with caching disabled.
  throw std::runtime_error(
      "ExternalValues does not support cache keys. "
      "Caching must be disabled when using externally specified values.");
}

// ____________________________________________________________________________
Result ExternalValues::computeResult(bool requestLaziness) {
  AD_CONTRACT_CHECK(getExecutionContext()->disableCaching(),
                    "ExternalValues can only be used when caching is disabled. "
                    "Set the runtime parameter `disable-caching` to true.");
  return Values::computeResult(requestLaziness);
}

// ____________________________________________________________________________
std::string ExternalValues::getDescriptor() const {
  return absl::StrCat("EXTERNAL VALUES '", name_, "'");
}

// ____________________________________________________________________________
void ExternalValues::updateValues(parsedQuery::SparqlValues newValues) {
  AD_CONTRACT_CHECK(
      newValues._variables == parsedValues()._variables,
      absl::StrCat(
          "Variables in updateValues must match the existing variables. "
          "Expected: ",
          parsedValues().variablesToString(),
          ", got: ", newValues.variablesToString()));
  parsedValues() = std::move(newValues);
}

// ____________________________________________________________________________
std::unique_ptr<Operation> ExternalValues::cloneImpl() const {
  return std::make_unique<ExternalValues>(*this);
}
