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
  ScanType _type;
  TripleComponent _subject;
  string _predicate;
  TripleComponent _object;
  size_t _sizeEstimate;
  vector<float> _multiplicity;

  std::optional<std::shared_ptr<const ResultTable>> _precomputedResult =
      std::nullopt;

 public:
  string getDescriptor() const override;

  IndexScan(QueryExecutionContext* qec, ScanType type,
            const SparqlTriple& triple);

  virtual ~IndexScan() = default;

  const string& getPredicate() const { return _predicate; }
  const TripleComponent& getSubject() const { return _subject; }
  const TripleComponent& getObject() const { return _object; }

  size_t getResultWidth() const override;

  vector<ColumnIndex> resultSortedOn() const override;

  void setTextLimit(size_t) override {
    // Do nothing.
  }

  // Return the exact result size of the index scan. This is always known as it
  // can be read from the Metadata.
  size_t getExactSize() const { return _sizeEstimate; }

 private:
  // TODO<joka921> Make the `getSizeEstimateBeforeLimit()` function `const` for
  // ALL the `Operations`.
  uint64_t getSizeEstimateBeforeLimit() override { return getExactSize(); }

 public:
  size_t getCostEstimate() override;

  void determineMultiplicities();

  float getMultiplicity(size_t col) override {
    if (_multiplicity.empty()) {
      determineMultiplicities();
    }
    assert(col < _multiplicity.size());
    return _multiplicity[col];
  }

  void precomputeSizeEstimate() { _sizeEstimate = computeSizeEstimate(); }

  bool knownEmptyResult() override { return getSizeEstimateBeforeLimit() == 0; }

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

 private:
  ResultTable computeResult() override;

  vector<QueryExecutionTree*> getChildren() override { return {}; }

  void computePSOboundS(IdTable* result) const;

  void computePSOfreeS(IdTable* result) const;

  void computePOSboundO(IdTable* result) const;

  void computePOSfreeO(IdTable* result) const;

  void computeSPOfreeP(IdTable* result) const;

  void computeSOPboundO(IdTable* result) const;

  void computeSOPfreeO(IdTable* result) const;

  void computeOPSfreeP(IdTable* result) const;

  void computeOSPfreeS(IdTable* result) const;

  void computeFullScan(IdTable* result, Permutation::Enum permutation) const;

  size_t computeSizeEstimate();

  string asStringImpl(size_t indent) const override;

  VariableToColumnMap computeVariableToColumnMap() const override;

  std::optional<std::shared_ptr<const ResultTable>>
  getPrecomputedResultFromQueryPlanning() override {
    return _precomputedResult;
  }
};
