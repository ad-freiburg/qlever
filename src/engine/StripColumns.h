//
// Created by kalmbacj on 7/22/25.
//

#include "engine/Operation.h"

#ifndef QLEVER_STRIPCOLUMNS_H
#define QLEVER_STRIPCOLUMNS_H

class StripColumns : public Operation {
 private:
  std::shared_ptr<QueryExecutionTree> child_;
  std::vector<ColumnIndex> subset_;
  VariableToColumnMap varToCol_;

 public:
  StripColumns(QueryExecutionContext* ctx,
               std::shared_ptr<QueryExecutionTree> child,
               ad_utility::HashSet<Variable> keepVariables);

  // The constructor needed for cloning.
  StripColumns(QueryExecutionContext* ctx,
               std::shared_ptr<QueryExecutionTree> child,
               std::vector<ColumnIndex> subset, VariableToColumnMap varToCol);

  /// get non-owning pointers to all the held subtrees to actually use the
  /// Execution Trees as trees
  std::vector<QueryExecutionTree*> getChildren();
  // The individual implementation of `getCacheKey` (see above) that has to
  // be customized by every child class.
  std::string getCacheKeyImpl() const;
  // Gets a very short (one line without line ending) descriptor string for
  // this Operation.  This string is used in the RuntimeInformation
  std::string getDescriptor() const;
  size_t getResultWidth() const;

  size_t getCostEstimate();

 private:
  uint64_t getSizeEstimateBeforeLimit();

 public:
  float getMultiplicity(size_t col);
  bool knownEmptyResult();

 private:
  std::unique_ptr<Operation> cloneImpl() const;
  [[nodiscard]] std::vector<ColumnIndex> resultSortedOn() const;
  Result computeResult(bool requestLaziness);
  VariableToColumnMap computeVariableToColumnMap() const;
};

#endif  // QLEVER_STRIPCOLUMNS_H
