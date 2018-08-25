// Copyright 2018, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Julian Bürklin (buerklij@informatik.uni-freiburg.de)

#pragma once

#include <limits>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "../global/Pattern.h"
#include "../parser/ParsedQuery.h"
#include "./Operation.h"
#include "./QueryExecutionTree.h"

using std::string;
using std::vector;

class StatScan : public Operation {
 public:
  enum ScanType {
    POS_BOUND_O = 0,
    PSO_BOUND_S = 1,
    PSO_FREE_S = 2,
    POS_FREE_O = 3
  };

  StatScan(QueryExecutionContext* qec, Id statId, ScanType type);

  void setSubject(const string& subject) { _subject = subject; }
  void setObject(const string& object) { _object = object; }

  virtual string asString(size_t indent = 0) const;

  virtual size_t getResultWidth() const;

  virtual size_t resultSortedOn() const { return 0; }

  void determineMultiplicities();

  virtual float getMultiplicity(size_t col) {
    if (_multiplicity.size() == 0) {
      determineMultiplicities();
    }
    assert(col < _multiplicity.size());
    return _multiplicity[col];
  }

  virtual void setTextLimit(size_t) {
    // Do nothing.
  }

  virtual size_t getSizeEstimate() {
    if (_sizeEstimate == std::numeric_limits<size_t>::max()) {
      _sizeEstimate = computeSizeEstimate();
    }
    return _sizeEstimate;
  }

  virtual size_t getCostEstimate() { return getSizeEstimate(); }

  virtual bool knownEmptyResult() { return getSizeEstimate() == 0; }

 protected:
  size_t _sizeEstimate;
  vector<float> _multiplicity;

  size_t computeSizeEstimate() const;

 private:
  virtual void computeResult(ResultTable* result) const;
  virtual void computePSOfreeS(ResultTable* result) const;
  virtual void computePOSfreeO(ResultTable* result) const;
  virtual void computePOSboundO(ResultTable* result) const;
  virtual void computePSOboundS(ResultTable* result) const;
  Id _statId;
  ScanType _type;
  string _subject;
  string _object;
};
