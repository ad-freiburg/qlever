// Copyright 2019, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Florian Kramer (florian.kramer@netpun.uni-freiburg.de)

#pragma once

#include "../parser/ParsedQuery.h"
#include "Operation.h"

class Values : public Operation {
  using SparqlValues = parsedQuery::SparqlValues;

 private:
  std::vector<size_t> _multiplicities;

  SparqlValues _values;

 public:
  /// constructor sanitizes the input by removing completely undefined variables
  /// and values.
  Values(QueryExecutionContext* qec, SparqlValues values);

 protected:
  virtual string asStringImpl(size_t indent = 0) const override;

 public:
  virtual string getDescriptor() const override;

  virtual size_t getResultWidth() const override;

  virtual vector<size_t> resultSortedOn() const override;

  virtual void setTextLimit(size_t limit) override { (void)limit; }

  virtual bool knownEmptyResult() override {
    return _values._variables.empty() || _values._values.empty();
  }

  virtual float getMultiplicity(size_t col) override;

  virtual size_t getSizeEstimate() override;

  virtual size_t getCostEstimate() override;

  vector<QueryExecutionTree*> getChildren() override { return {}; }

 private:
  virtual void computeResult(ResultTable* result) override;

  Operation::VariableToColumnMap computeVariableToColumnMap() const override;

  template <size_t I>
  void writeValues(IdTable* res, const Index& index,
                   const SparqlValues& values);

  /// remove all completely undefined values and variables
  /// throw if nothing remains
  SparqlValues sanitizeValues(SparqlValues&& values);

  void computeMultiplicities();
};
