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

// `ExternallySpecifiedValues` is similar to `Values` (implemented using private
// inheritance) but allows to modify the contents of the VALUES clause after the
// creation of the operation. It is created via the `ExternalValuesQuery` magic
// SERVICE. It can be used via `libqlever` to implement repeated queries that
// only differ in the contents of VALUES clauses without having to repeat the
// query parsing and planning. For an example usage of this feature end-to-end
// see `QLeverTest.cpp` Note: `ExternallySpecifiedValues` can currently only be
// used if caching is disabled (else an exception will be thrown from the
// `getCacheKey` and `computeResult` member function).
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

  // Get the identifier of this external values operation. It is useful in case
  // there are multiple `ExternallySpecifiedValues` in the same
  // `QueryExecutionTree`.
  const std::string& getIdentifier() const { return identifier_; }

  // Update the values stored in this operation. Assert that the variables
  // in the new values match the existing variables.
  void updateValues(parsedQuery::SparqlValues newValues);

  // Override to ensure external values are never cached.
  bool canResultBeCachedImpl() const override { return false; }

  // Override to ensure external values are never considered empty.
  bool knownEmptyResult() override { return false; }

  std::string getDescriptor() const override;
  Result computeResult(bool requestLaziness) override;
  std::string getCacheKeyImpl() const override;

  // Override the method that is used by the `Operation` base class to collect
  // all `ExternallySpecifiedValues` from a `Query
  void getExternallySpecifiedValues(
      std::vector<ExternallySpecifiedValues*>& externalValues) override {
    externalValues.push_back(this);
  }

 private:
  std::unique_ptr<Operation> cloneImpl() const override;
};

#endif  // QLEVER_SRC_ENGINE_EXTERNALLYSPECIFIEDVALUES_H
