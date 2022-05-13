// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)
#pragma once

#include <list>
#include <utility>
#include <vector>

#include "../global/ValueIdComparators.h"
#include "../parser/ParsedQuery.h"
#include "./Operation.h"
#include "./QueryExecutionTree.h"

using std::list;
using std::pair;
using std::vector;

class Filter : public Operation {
 public:
  virtual size_t getResultWidth() const override;

 public:
  Filter(QueryExecutionContext* qec,
         std::shared_ptr<QueryExecutionTree> subtree,
         SparqlFilter::FilterType type, string lhs, string rhs,
         vector<string> additionalLhs, vector<string> additionalPrefixes);

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
  }

  virtual size_t getCostEstimate() override {
    if (_type == SparqlFilter::FilterType::REGEX) {
      return std::numeric_limits<size_t>::max();
    }
    if (isLhsSorted()) {
      // we can apply the very cheap binary sort filter
      return getSizeEstimate() + _subtree->getCostEstimate();
    }
    // we have to look at each element of the result
    return getSizeEstimate() + _subtree->getSizeEstimate() +
           _subtree->getCostEstimate();
  }

  void setRegexIgnoreCase(bool i) { _regexIgnoreCase = i; }
  void setLhsAsString(bool i) { _lhsAsString = i; }

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

  virtual ad_utility::HashMap<string, size_t> getVariableColumns()
      const override {
    return _subtree->getVariableColumns();
  }

 private:
  std::shared_ptr<QueryExecutionTree> _subtree;
  SparqlFilter::FilterType _type;
  string _lhs;
  string _rhs;

  std::vector<string> _additionalLhs;
  std::vector<string> _additionalPrefixRegexes;
  bool _regexIgnoreCase;
  bool _lhsAsString;

  [[nodiscard]] bool isLhsSorted() const {
    const auto& subresSortedOn = _subtree->resultSortedOn();
    size_t lhsInd = _subtree->getVariableColumn(_lhs);
    return !subresSortedOn.empty() && subresSortedOn[0] == lhsInd;
  }

  template <int WIDTH>
  void computeResultDynamicValue(IdTable* dynResult, size_t lhsInd,
                                 size_t rhsInd, const IdTable& dynInput);

  /**
   * @brief Uses the result type and the filter type (_type) to apply the filter
   * to subRes and store it in res.
   * @return The pointer res.
   */
  template <int WIDTH>
  void computeFilterFixedValue(IdTableStatic<WIDTH>* res, size_t lhs, Id rhs,
                               const IdTableView<WIDTH>& input,
                               shared_ptr<const ResultTable> subRes) const;
  /**
   * @brief Uses the result type and applies a range filter
   * ( input[lhs] >= rhs_lower && input[rhs] < rhs_upper)
   * to subRes and store it in res.
   *
   */
  template <int WIDTH>
  void computeFilterRange(IdTableStatic<WIDTH>* res, size_t lhs, Id rhs_lower,
                          Id rhs_upper, const IdTableView<WIDTH>& input,
                          shared_ptr<const ResultTable> subRes) const;

  template <int WIDTH>
  void computeResultFixedValue(
      ResultTable* result,
      const std::shared_ptr<const ResultTable> subRes) const;

  virtual void computeResult(ResultTable* result) override;

  // Convert a FilterType to the corresponding `Comparison`. Throws if no
  // corresponding `Comparison` exists (supported are LE, LT, EQ, NE, GE, GT).
  // TODO<joka921> move to cpp file.
  static valueIdComparators::Comparison toComparison(
      SparqlFilter::FilterType filterType) {
    switch (filterType) {
      case SparqlFilter::LT:
        return valueIdComparators::Comparison::LT;
      case SparqlFilter::LE:
        return valueIdComparators::Comparison::LE;
      case SparqlFilter::EQ:
        return valueIdComparators::Comparison::EQ;
      case SparqlFilter::NE:
        return valueIdComparators::Comparison::NE;
      case SparqlFilter::GT:
        return valueIdComparators::Comparison::GT;
      case SparqlFilter::GE:
        return valueIdComparators::Comparison::GE;
      default:
        AD_CHECK(false);
    }
  }
};
