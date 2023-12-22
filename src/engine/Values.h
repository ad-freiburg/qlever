// Copyright 2019 - 2022, University of Freiburg
// Chair of Algorithms and Data Structures
// Authors: Florian Kramer <kramerf@cs.uni-freiburg.de>
//          Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>
//          Hannah Bast <bast@cs.uni-freiburg.de>

#pragma once

#include "../parser/ParsedQuery.h"
#include "Operation.h"

class Values : public Operation {
  using SparqlValues = parsedQuery::SparqlValues;

 private:
  std::vector<float> multiplicities_;

  SparqlValues parsedValues_;

 public:
  // Create operation from parsed values. This calls `sanitizeValues`.
  // and values.
  Values(QueryExecutionContext* qec, SparqlValues parsedValues);

 protected:
  string getCacheKeyImpl() const override;

 public:
  virtual string getDescriptor() const override;

  virtual size_t getResultWidth() const override;

  virtual vector<ColumnIndex> resultSortedOn() const override;

  virtual void setTextLimit(size_t limit) override { (void)limit; }

  virtual bool knownEmptyResult() override {
    return parsedValues_._variables.empty() || parsedValues_._values.empty();
  }

  virtual float getMultiplicity(size_t col) override;

 private:
  uint64_t getSizeEstimateBeforeLimit() override;

 public:
  virtual size_t getCostEstimate() override;

  vector<QueryExecutionTree*> getChildren() override { return {}; }

 public:
  // These two are also used by class `Service`, hence public.
  virtual ResultTable computeResult() override;

  VariableToColumnMap computeVariableToColumnMap() const override;

 private:
  // Compute the per-column multiplicity of the parsed values.
  void computeMultiplicities();

  // Write `parsedValues_` to the given result object.
  //
  // NOTE: this moves the values out of `parsedValues_` (to save a string copy
  // for those values that end up in the local vocabulary).
  template <size_t I>
  void writeValues(IdTable* idTablePtr, LocalVocab* localVocab);
};
