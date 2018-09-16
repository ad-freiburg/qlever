// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)
#pragma once

#include <list>
#include <utility>
#include <vector>
#include "../parser/ParsedQuery.h"
#include "./Operation.h"
#include "./QueryExecutionTree.h"

using std::list;
using std::pair;
using std::vector;

class Filter : public Operation {
 public:
  virtual size_t getResultWidth() const;

 public:
  Filter(QueryExecutionContext* qec,
         std::shared_ptr<QueryExecutionTree> subtree,
         SparqlFilter::FilterType type, size_t var1Column, size_t var2Column,
         Id rhsId = std::numeric_limits<Id>::max());

  virtual string asString(size_t indent = 0) const override;

  virtual vector<size_t> resultSortedOn() const override {
    return _subtree->resultSortedOn();
  }

  virtual void setTextLimit(size_t limit) override {
    _subtree->setTextLimit(limit);
  }

  virtual size_t getSizeEstimate() override {
    if (_type == SparqlFilter::FilterType::REGEX) {
      // TODO(jbuerklin): return a better estimate
      return std::numeric_limits<Id>::max();
    }
    // TODO(schnelle): return a better estimate
    if (_rhsId == std::numeric_limits<Id>::max()) {
      if (_type == SparqlFilter::FilterType::EQ) {
        return _subtree->getSizeEstimate() / 1000;
      }
      if (_type == SparqlFilter::FilterType::NE) {
        return _subtree->getSizeEstimate() / 4;
      } else {
        return _subtree->getSizeEstimate() / 2;
      }
    } else {
      if (_type == SparqlFilter::FilterType::EQ) {
        return _subtree->getSizeEstimate() / 1000;
      }
      if (_type == SparqlFilter::FilterType::NE) {
        return _subtree->getSizeEstimate();
      } else {
        return _subtree->getSizeEstimate() / 50;
      }
    }
  }

  virtual size_t getCostEstimate() override {
    if (_type == SparqlFilter::FilterType::REGEX) {
      return std::numeric_limits<Id>::max();
    }
    return getSizeEstimate() + _subtree->getSizeEstimate() +
           _subtree->getCostEstimate();
  }

  void setRightHandSideString(std::string s) { _rhsString = s; }

  void setRegexIgnoreCase(bool i) { _regexIgnoreCase = i; }

  std::shared_ptr<QueryExecutionTree> getSubtree() const { return _subtree; };

  virtual bool knownEmptyResult() override {
    return _subtree->knownEmptyResult();
  }

  virtual float getMultiplicity(size_t col) override {
    return _subtree->getMultiplicity(col);
  }

 private:
  std::shared_ptr<QueryExecutionTree> _subtree;
  SparqlFilter::FilterType _type;
  size_t _lhsInd;
  size_t _rhsInd;
  Id _rhsId;
  std::string _rhsString;
  bool _regexIgnoreCase;

  template <class RT>
  vector<RT>* computeFilter(vector<RT>* res, size_t l, size_t r,
                            shared_ptr<const ResultTable> subRes) const;
  virtual void computeResult(ResultTable* result) const override;

  template <class RT>
  vector<RT>* computeFilterFixedValue(
      vector<RT>* res, size_t l, Id r,
      shared_ptr<const ResultTable> subRes) const;

  void computeResultFixedValue(ResultTable* result) const;
};
