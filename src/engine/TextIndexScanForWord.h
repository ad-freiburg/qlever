//  Copyright 2023, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Nick Göckel <nick.goeckel@students.uni-freiburg.de>

#pragma once

#include <string>

#include "./Operation.h"
#include "parser/MagicServiceQuery.h"

struct TextIndexScanForWordConfiguration {
  Variable varToBindText_;
  string word;
  std::optional<Variable> varToBindMatch_;
  std::optional<Variable> varToBindScore_;
};

// This operation retrieves all text records from the fulltext index that
// contain a certain word or prefix.
class TextIndexScanForWord : public Operation {
 private:
  TextIndexScanForWordConfiguration config_;
  VariableToColumnMap variableColumns_;
  Variable textRecordVar_;
  string word_;
  bool isPrefix_ = false;

 public:
  TextIndexScanForWord(QueryExecutionContext* qec,
                       TextIndexScanForWordConfiguration config);

  TextIndexScanForWord(QueryExecutionContext* qec, Variable textRecordVar,
                       string word);

  ~TextIndexScanForWord() override = default;

  const Variable& textRecordVar() const { return textRecordVar_; }

  const std::string& word() const { return word_; }

  string getCacheKeyImpl() const override;

  string getDescriptor() const override;

  size_t getResultWidth() const override;

  size_t getCostEstimate() override;

  uint64_t getSizeEstimateBeforeLimit() override;

  float getMultiplicity(size_t col) override {
    (void)col;
    return 1;
  }

  bool knownEmptyResult() override { return getSizeEstimateBeforeLimit() == 0; }

  vector<ColumnIndex> resultSortedOn() const override;

  void setVariableToColumnMap();

  VariableToColumnMap computeVariableToColumnMap() const override;

 private:
  // Returns a Result containing an IdTable with the columns being
  // the text variable and the completed word (if it was prefixed)
  ProtoResult computeResult([[maybe_unused]] bool requestLaziness) override;

  vector<QueryExecutionTree*> getChildren() override { return {}; }
};
