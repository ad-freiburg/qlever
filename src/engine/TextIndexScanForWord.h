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

  Variable textRecordVar() const { return textRecordVar_; }

  std::string word() const { return word_; }

  virtual ~TextIndexScanForWord() override = default;

  string getCacheKeyImpl() const override;

  string getDescriptor() const override;

  size_t getResultWidth() const override;

  void setTextLimit(size_t) override {
    // TODO: implement textLimit
  }

  size_t getCostEstimate() override;

  uint64_t getSizeEstimateBeforeLimit() override;

  float getMultiplicity(size_t col) override {
    (void)col;
    return 1;
  }

  bool knownEmptyResult() override {
    return getExecutionContext()->getIndex().getWordSizeEstimate(word_) == 0;
  }

  vector<ColumnIndex> resultSortedOn() const override;

  VariableToColumnMap computeVariableToColumnMap() const override;

 private:
  // Returns a ResultTable containing an IdTable with the columns being
  // the text-variable and the completed word (if it was prefixed)
  ResultTable computeResult() override;

  vector<QueryExecutionTree*> getChildren() override { return {}; }
};
