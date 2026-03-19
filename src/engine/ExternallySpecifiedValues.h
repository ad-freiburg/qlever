// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR

// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_ENGINE_EXTERNALLYSPECIFIEDVALUES_H
#define QLEVER_SRC_ENGINE_EXTERNALLYSPECIFIEDVALUES_H

#include "engine/Values.h"

namespace parsedQuery {
struct ExternalValuesQuery;
}

class ExternallySpecifiedValues : private Values, virtual public Operation {
 private:
  std::string identifier_;

 public:
  // Inherit public member functions from `Values` that are not overridden.
  using Values::computeVariableToColumnMap;
  using Values::getChildren;
  using Values::getCostEstimate;
  using Values::getMultiplicity;
  using Values::getResultWidth;
  using Values::resultSortedOn;

  // Create operation from parsed values and identifier.
  ExternallySpecifiedValues(QueryExecutionContext* qec,
                            parsedQuery::SparqlValues parsedValues,
                            std::string identifier);

  // Create operation from an `ExternalValuesQuery`. The variables are taken
  // from the query, and the values start empty.
  ExternallySpecifiedValues(QueryExecutionContext* qec,
                            const parsedQuery::ExternalValuesQuery& query);

  // Get the identifier of this external values operation.
  const std::string& getIdentifier() const { return identifier_; }

  // Update the values stored in this operation. Asserts that the variables
  // in the new values match the existing variables.
  void updateValues(parsedQuery::SparqlValues newValues);

  // Override to ensure external values are never cached.
  bool canResultBeCachedImpl() const override { return false; }

  // Override to ensure external values are never considered empty.
  bool knownEmptyResult() override { return false; }

  std::string getDescriptor() const override;
  Result computeResult(bool requestLaziness) override;
  std::string getCacheKeyImpl() const override;

  // ___________________________________________________________________________
  void getExternallySpecifiedValues(
      std::vector<ExternallySpecifiedValues*>& externalValues) {
    externalValues.push_back(this);
  }

 private:
  std::unique_ptr<Operation> cloneImpl() const override;
};

#endif  // QLEVER_SRC_ENGINE_EXTERNALLYSPECIFIEDVALUES_H
