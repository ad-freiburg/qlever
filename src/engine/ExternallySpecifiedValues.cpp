// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR

// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "engine/ExternallySpecifiedValues.h"

#include "absl/strings/str_cat.h"
#include "util/HashSet.h"

// ____________________________________________________________________________
ExternallySpecifiedValues::ExternallySpecifiedValues(
    QueryExecutionContext* qec, parsedQuery::SparqlValues parsedValues,
    std::string identifier)
    : Operation(qec),
      Values(qec, std::move(parsedValues)),
      identifier_(std::move(identifier)) {}

// ____________________________________________________________________________
ExternallySpecifiedValues::ExternallySpecifiedValues(
    QueryExecutionContext* qec, const parsedQuery::ExternalValuesQuery& query)
    : ExternallySpecifiedValues(
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
          query.identifier_) {}

// ____________________________________________________________________________
std::string ExternallySpecifiedValues::getCacheKeyImpl() const {
  // auto gen = ad_utility::FastRandomIntGenerator<size_t>{};
  return absl::StrCat("EXTERNAL VALUES #" /*,gen()*/, identifier_, "# (",
                      parsedValues_.variablesToString(), ") { ",
                      parsedValues_.valuesToString(), " }");
}

// ____________________________________________________________________________
std::string ExternallySpecifiedValues::getDescriptor() const {
  return absl::StrCat("External values with identifier '", identifier_,
                      "' and variables ", parsedValues_.variablesToString());
}

// ____________________________________________________________________________
void ExternallySpecifiedValues::updateValues(
    parsedQuery::SparqlValues newValues) {
  AD_CONTRACT_CHECK(
      newValues._variables == parsedValues_._variables,
      absl::StrCat(
          "Variables in updateValues must match the existing variables. "
          "Expected: ",
          parsedValues_.variablesToString(),
          ", got: ", newValues.variablesToString()));
  parsedValues_ = std::move(newValues);
}

// ____________________________________________________________________________
std::unique_ptr<Operation> ExternallySpecifiedValues::cloneImpl() const {
  return std::make_unique<ExternallySpecifiedValues>(*this);
}
