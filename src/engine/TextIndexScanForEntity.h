#pragma once

#include <string>

#include "./Operation.h"

// This operation retrieves all text records and their corresponding
// entities from the fulltext index that contain a certain word or prefix.
// The entities are saved to the entityVar_. If the operation is called on a
// fixed entity instead, it only returns entries that contain this entity.
class TextIndexScanForEntity : public Operation {
 private:
  const Variable textRecordVar_;
  const std::variant<Variable, std::string> entity_;
  const string word_;
  const bool hasFixedEntity_ = false;

 public:
  TextIndexScanForEntity(QueryExecutionContext* qec, Variable textRecordVar,
                         std::variant<Variable, std::string> entity_,
                         string word);

  virtual ~TextIndexScanForEntity() = default;

  vector<QueryExecutionTree*> getChildren() override { return {}; }

  string asStringImpl(size_t indent) const override;

  string getDescriptor() const override;

  size_t getResultWidth() const override;

  void setTextLimit(size_t) override {}

  size_t getCostEstimate() override { return getSizeEstimateBeforeLimit(); }

  uint64_t getSizeEstimateBeforeLimit() override;

  float getMultiplicity(size_t col) override {
    (void)col;
    return 1;
  }

  bool knownEmptyResult() override;

  vector<ColumnIndex> resultSortedOn() const override;

  ResultTable computeResult() override;

  VariableToColumnMap computeVariableToColumnMap() const override;
};
