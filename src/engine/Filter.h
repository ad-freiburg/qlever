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
#include "engine/sparqlExpressions/SparqlExpressionPimpl.h"

using std::list;
using std::pair;
using std::vector;

class Filter : public Operation {
 private:
  std::shared_ptr<QueryExecutionTree> _subtree;
  sparqlExpression::SparqlExpressionPimpl _expression;

 public:
  virtual size_t getResultWidth() const override;

 public:
  Filter(QueryExecutionContext* qec,
         std::shared_ptr<QueryExecutionTree> subtree,
         sparqlExpression::SparqlExpressionPimpl expression);

 private:
  virtual string asStringImpl(size_t indent = 0) const override;

 public:
  virtual string getDescriptor() const override;

  virtual vector<size_t> resultSortedOn() const override {
    return _subtree->resultSortedOn();
  }

  virtual void setTextLimit(size_t limit) override {
    _subtree->setTextLimit(limit);
  }

  virtual size_t getSizeEstimate() override {
    return _subtree->getSizeEstimate();
    // TODO<joka921> integrate the size estimates into the expressions.
    /*
    if (_type == SparqlFilter::FilterType::REGEX) {
      // TODO(jbuerklin): return a better estimate
      return std::numeric_limits<size_t>::max();
    }
    // TODO(schnelle): return a better estimate
    if (_rhs[0] == '?') {
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
      } else if (_type == SparqlFilter::FilterType::PREFIX) {
        // Assume that only 10^-k entries remain, where k is the length of the
        // prefix. The reason for the -2 is that at this point, _rhs always
        // starts with ^"
        double reductionFactor =
            std::pow(10, std::max(0, static_cast<int>(_rhs.size()) - 2));
        // Cap to reasonable minimal and maximal values to prevent numerical
        // stability problems.
        reductionFactor = std::min(100000000.0, reductionFactor);
        reductionFactor = std::max(1.0, reductionFactor);
        return _subtree->getSizeEstimate() /
               static_cast<size_t>(reductionFactor);
      } else {
        return _subtree->getSizeEstimate() / 50;
      }
    }
     */
  }

  virtual size_t getCostEstimate() override {
    return _subtree->getCostEstimate();
    // TODO<joka921> include the cost estimates into the expressions.
    /*
    if (_type == SparqlFilter::FilterType::REGEX) {
      // We assume that checking a REGEX for an element is 10 times more
      // expensive than an "ordinary" filter check.
      return getSizeEstimate() + 10 * _subtree->getSizeEstimate() +
             _subtree->getCostEstimate();
      // return std::numeric_limits<size_t>::max();
    }
    if (isLhsSorted()) {
      // we can apply the very cheap binary sort filter
      return getSizeEstimate() + _subtree->getCostEstimate();
    }
    // we have to look at each element of the result
    return getSizeEstimate() + _subtree->getSizeEstimate() +
           _subtree->getCostEstimate();
           */
  }

  std::shared_ptr<QueryExecutionTree> getSubtree() const { return _subtree; };
  vector<QueryExecutionTree*> getChildren() override {
    return {_subtree.get()};
  }

  virtual bool knownEmptyResult() override {
    return _subtree->knownEmptyResult();
  }

  virtual float getMultiplicity(size_t col) override {
    return _subtree->getMultiplicity(col);
  }

 private:
  virtual VariableToColumnMap computeVariableToColumnMap() const override {
    return _subtree->getVariableColumns();
  }

  virtual void computeResult(ResultTable* result) override;

  template <int WIDTH>
  void computeFilterImpl(ResultTable* outputResultTable,
                         const ResultTable& inputResultTable);
};
