// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)
#pragma once

#include <string>

#include "./Operation.h"

using std::string;

class SparqlTriple;

class IndexScan : public Operation {
 public:
  enum ScanType {
    PSO_BOUND_S = 0,
    POS_BOUND_O = 1,
    PSO_FREE_S = 2,
    POS_FREE_O = 3,
    SPO_FREE_P = 4,
    SOP_BOUND_O = 5,
    SOP_FREE_O = 6,
    OPS_FREE_P = 7,
    OSP_FREE_S = 8,
    FULL_INDEX_SCAN_SPO = 9,
    FULL_INDEX_SCAN_SOP = 10,
    FULL_INDEX_SCAN_PSO = 11,
    FULL_INDEX_SCAN_POS = 12,
    FULL_INDEX_SCAN_OSP = 13,
    FULL_INDEX_SCAN_OPS = 14
  };

 private:
  virtual string asStringImpl(size_t indent = 0) const override;

 public:
  virtual string getDescriptor() const override;

  IndexScan(QueryExecutionContext* qec, ScanType type,
            const SparqlTriple& triple);

  virtual ~IndexScan() = default;

  const string& getPredicate() const { return _predicate; }

  virtual size_t getResultWidth() const override;

  virtual vector<size_t> resultSortedOn() const override;

  virtual void setTextLimit(size_t) override {
    // Do nothing.
  }

  virtual size_t getSizeEstimate() override {
    if (_sizeEstimate == std::numeric_limits<size_t>::max()) {
      _sizeEstimate = computeSizeEstimate();
    }
    return _sizeEstimate;
  }

  virtual size_t getCostEstimate() override { return getSizeEstimate(); }

  void determineMultiplicities();

  virtual float getMultiplicity(size_t col) override {
    if (_multiplicity.size() == 0) {
      determineMultiplicities();
    }
    assert(col < _multiplicity.size());
    return _multiplicity[col];
  }

  void precomputeSizeEstimate() { _sizeEstimate = computeSizeEstimate(); }

  virtual bool knownEmptyResult() override { return getSizeEstimate() == 0; }

  virtual ad_utility::HashMap<string, size_t> getVariableColumns()
      const override;

  // Currently only the full scans support a limit clause.
  [[nodiscard]] bool supportsLimit() const override {
    switch (_type) {
      case FULL_INDEX_SCAN_SPO:
      case FULL_INDEX_SCAN_SOP:
      case FULL_INDEX_SCAN_PSO:
      case FULL_INDEX_SCAN_POS:
      case FULL_INDEX_SCAN_OSP:
      case FULL_INDEX_SCAN_OPS:
        return true;
      default:
        return false;
    }
  }

  ScanType getType() const { return _type; }

 protected:
  ScanType _type;
  TripleComponent _subject;
  string _predicate;
  TripleComponent _object;
  size_t _sizeEstimate;
  vector<float> _multiplicity;

  virtual void computeResult(ResultTable* result) override;

  vector<QueryExecutionTree*> getChildren() override { return {}; }

  void computePSOboundS(ResultTable* result) const;

  void computePSOfreeS(ResultTable* result) const;

  void computePOSboundO(ResultTable* result) const;

  void computePOSfreeO(ResultTable* result) const;

  void computeSPOfreeP(ResultTable* result) const;

  void computeSOPboundO(ResultTable* result) const;

  void computeSOPfreeO(ResultTable* result) const;

  void computeOPSfreeP(ResultTable* result) const;

  void computeOSPfreeS(ResultTable* result) const;

  void computeFullScan(ResultTable* result,
                       const Index::Permutation permutation) const;

  size_t computeSizeEstimate();
};
