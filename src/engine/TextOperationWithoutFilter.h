// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)
#pragma once

#include <absl/strings/str_split.h>

#include <set>
#include <utility>
#include <vector>

#include "./Operation.h"
#include "./QueryExecutionTree.h"

using std::pair;
using std::vector;

class TextOperationWithoutFilter : public Operation {
 public:
  using SetOfVariables = ad_utility::HashSet<Variable>;

 private:
  string _words;
  const SetOfVariables _variables;
  const Variable _cvar;

  size_t _textLimit;

  size_t _sizeEstimate;
  vector<float> _multiplicities;

 public:
  TextOperationWithoutFilter(QueryExecutionContext* qec,
                             const std::vector<std::string>& words,
                             SetOfVariables variables, Variable cvar,
                             size_t textLimit = 1);

 protected:
  virtual string getCacheKeyImpl() const override;

 public:
  virtual string getDescriptor() const override;

  virtual size_t getResultWidth() const override;

  virtual vector<ColumnIndex> resultSortedOn() const override {
    // unsorted, obtained from iterating over a hash map.
    return {};
  }

  virtual void setTextLimit(size_t limit) override {
    _textLimit = limit;
    _multiplicities.clear();
    _sizeEstimate = std::numeric_limits<size_t>::max();
  }

 private:
  uint64_t getSizeEstimateBeforeLimit() override;

 public:
  virtual size_t getCostEstimate() override;

  virtual float getMultiplicity(size_t col) override;

  const string& getWordPart() const { return _words; }

  size_t getNofVars() const {
    // -1 because _variables also contains the context var
    return _variables.size() - 1;
  }

  size_t getNofPrefixedTerms() const;

  const SetOfVariables& getVars() const { return _variables; }

  const Variable& getCVar() const { return _cvar; }

  virtual bool knownEmptyResult() override {
    return _executionContext &&
           _executionContext->getIndex().getSizeEstimate(_words) == 0;
  }

  vector<QueryExecutionTree*> getChildren() override { return {}; }

 private:
  void computeMultiplicities();

  ResultTable computeResult() override;

  void computeResultNoVar(IdTable* idTable) const;

  void computeResultOneVar(IdTable* idTable) const;

  void computeResultMultVars(IdTable* idTable) const;

  VariableToColumnMap computeVariableToColumnMap() const override;
};
