// Copyright 2025, University of Freiburg
// Chair of Algorithms and Data Structures
// Author: Generated with Claude Code

#ifndef QLEVER_SRC_ENGINE_EXTERNALLYSPECIFIEDVALUES_H
#define QLEVER_SRC_ENGINE_EXTERNALLYSPECIFIEDVALUES_H

#include "engine/Values.h"

class ExternallySpecifiedValues : public Values {
 private:
  std::string identifier_;

 public:
  // Create operation from parsed values and identifier
  ExternallySpecifiedValues(QueryExecutionContext* qec,
                            parsedQuery::SparqlValues parsedValues,
                            std::string identifier);

  // Get the identifier of this external values operation
  const std::string& getIdentifier() const { return identifier_; }

  // Update the values stored in this operation. Asserts that the variables
  // in the new values match the existing variables.
  void updateValues(parsedQuery::SparqlValues newValues);

  // Override to ensure external values are never cached
  bool canResultBeCachedImpl() const override { return false; }

  // Override to ensure external values are never considered empty
  bool knownEmptyResult() override { return false; }

  std::string getCacheKeyImpl() const override;
  std::string getDescriptor() const override;

 private:
  std::unique_ptr<Operation> cloneImpl() const override;
};

#endif  // QLEVER_SRC_ENGINE_EXTERNALLYSPECIFIEDVALUES_H
