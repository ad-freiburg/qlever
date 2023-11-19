#pragma once

#include <string>

#include "./Operation.h"

// This operation retrieves all text records and their corresponding
// entities from the fulltext index that contain a certain word or prefix.
// The entities are saved to the entityVar_. If the operation is called on a
// fixed entity instead, it only returns entries that contain this entity.
class EntityIndexScanForWord : public Operation {
 private:
  const QueryExecutionContext* qec_;
  const Variable textRecordVar_;
  const std::optional<Variable> entityVar_;
  const string word_;
  const std::optional<string> fixedEntity_;
  const bool hasFixedEntity_ = false;

 public:
  EntityIndexScanForWord(QueryExecutionContext* qec, Variable textRecordVar,
                         std::optional<Variable> entityVar, string word,
                         std::optional<string> fixedEntity = std::nullopt);

  virtual ~EntityIndexScanForWord() = default;

  vector<QueryExecutionTree*> getChildren() override { return {}; }

  string asStringImpl(size_t indent) const override;

  string getDescriptor() const override;

  size_t getResultWidth() const override;

  void setTextLimit(size_t) override {}

  size_t getCostEstimate() override { return 5; }

  uint64_t getSizeEstimateBeforeLimit() override { return 5; }

  float getMultiplicity(size_t col) override { return 0; }

  bool knownEmptyResult() override { return false; }

  vector<ColumnIndex> resultSortedOn() const override;

  ResultTable computeResult() override;

  VariableToColumnMap computeVariableToColumnMap() const override;
};
