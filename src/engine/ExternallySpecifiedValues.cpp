// Copyright 2025, University of Freiburg
// Chair of Algorithms and Data Structures
// Author: Generated with Claude Code

#include "engine/ExternallySpecifiedValues.h"

#include <absl/strings/str_cat.h>

// ____________________________________________________________________________
ExternallySpecifiedValues::ExternallySpecifiedValues(
    QueryExecutionContext* qec, parsedQuery::SparqlValues parsedValues,
    std::string identifier)
    : Values(qec, std::move(parsedValues)),
      identifier_(std::move(identifier)) {}

// ____________________________________________________________________________
std::string ExternallySpecifiedValues::getCacheKeyImpl() const {
  return absl::StrCat("EXTERNAL VALUES #", identifier_, "# (",
                      parsedValues_.variablesToString(), ") { ",
                      parsedValues_.valuesToString(), " }");
}

// ____________________________________________________________________________
std::string ExternallySpecifiedValues::getDescriptor() const {
  return absl::StrCat("External values with identifier '", identifier_,
                      "' and variables ", parsedValues_.variablesToString());
}

// ____________________________________________________________________________
std::unique_ptr<Operation> ExternallySpecifiedValues::cloneImpl() const {
  return std::make_unique<ExternallySpecifiedValues>(*this);
}
