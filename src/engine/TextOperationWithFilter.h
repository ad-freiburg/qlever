// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)
#pragma once

#include <set>
#include <utility>
#include <vector>

#include "./Operation.h"
#include "./QueryExecutionTree.h"

using std::list;

using std::pair;
using std::vector;

class TextOperationWithFilter : public Operation {
 public:
  TextOperationWithFilter(QueryExecutionContext* qec, const string& words,
                          const std::set<string>& variables, const string& cvar,
                          std::shared_ptr<QueryExecutionTree> filterResult,
                          size_t filterColumn, size_t textLimit = 1);

  virtual string asString(size_t indent) const override;

  virtual string getDescriptor() const override;

  virtual size_t getResultWidth() const override;

  virtual vector<size_t> resultSortedOn() const override {
    // unsorted, obtained from iterating over a hash map.
    return {};
  }

  virtual void setTextLimit(size_t limit) override {
    _textLimit = limit;
    _filterResult->setTextLimit(limit);
    _sizeEstimate = std::numeric_limits<size_t>::max();
    _multiplicities.clear();
  }

  virtual size_t getSizeEstimate() override;
  virtual size_t getCostEstimate() override;

  const string& getWordPart() const { return _words; }

  size_t getNofVars() const {
    // -1 because _variables includes the cvar
    return _variables.size() - 1;
  }

  const std::set<string>& getVars() const { return _variables; }

  const string getCVar() const { return _cvar; }

  virtual bool knownEmptyResult() override {
    return _filterResult->knownEmptyResult() ||
           (_executionContext &&
            _executionContext->getIndex().getSizeEstimate(_words) == 0);
  }

  virtual float getMultiplicity(size_t col) override;

  virtual ad_utility::HashMap<string, size_t> getVariableColumns()
      const override;

 private:
  const string _words;
  const std::set<string> _variables;
  const string _cvar;
  size_t _textLimit;

  std::shared_ptr<QueryExecutionTree> _filterResult;
  size_t _filterColumn;

  size_t _sizeEstimate;
  vector<float> _multiplicities;

  void computeMultiplicities();

  virtual void computeResult(ResultTable* result) override;
};
