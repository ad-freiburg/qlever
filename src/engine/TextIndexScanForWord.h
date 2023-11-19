#pragma once

#include <string>

#include "./Operation.h"

// This operation retrieves all text records from the fulltext index that
// contain a certain word or prefix.
class TextIndexScanForWord : public Operation {
 private:
  const Variable textRecordVar_;
  const string word_;
  bool isPrefix_ = false;

 public:
  TextIndexScanForWord(QueryExecutionContext* qec, Variable textRecordVar,
                       string word);

  virtual ~TextIndexScanForWord() = default;

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

  // Returns a ResultTable containing an IdTable with the columns being
  // the text-varibale and the completed word (if it was prefixed)
  ResultTable computeResult() override;

  VariableToColumnMap computeVariableToColumnMap() const override;
};
