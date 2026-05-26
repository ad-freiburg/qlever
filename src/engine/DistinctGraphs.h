#ifndef QLEVER_DISTINCTGRAPHS_H
#define QLEVER_DISTINCTGRAPHS_H

#include "engine/Operation.h"
#include "engine/QueryExecutionTree.h"
#include "rdfTypes/Variable.h"

class DistinctGraphs : public Operation {
 public:
  explicit DistinctGraphs(QueryExecutionContext* qec, Variable graphVariable);

  std::vector<QueryExecutionTree*> getChildren() override;

  [[nodiscard]] std::string getDescriptor() const override;

  [[nodiscard]] size_t getResultWidth() const override;

  size_t getCostEstimate() override;

  float getMultiplicity(size_t col) override;

  bool knownEmptyResult() override;

  std::unique_ptr<Operation> cloneImpl() const override;

 protected:
  [[nodiscard]] std::vector<ColumnIndex> resultSortedOn() const override;

 private:
  [[nodiscard]] std::string getCacheKeyImpl() const override;

  uint64_t getSizeEstimateBeforeLimit() override;

  Result computeResult(bool requestLaziness) override;

  [[nodiscard]] VariableToColumnMap computeVariableToColumnMap() const override;

  Variable graphVariable_;
};

#endif
