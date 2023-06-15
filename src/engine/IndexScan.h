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

  static size_t scanTypeToNumVariables(ScanType scanType) {
    switch (scanType) {
      case PSO_BOUND_S:
      case POS_BOUND_O:
      case SOP_BOUND_O:
        return 1;
      case PSO_FREE_S:
      case POS_FREE_O:
      case SOP_FREE_O:
      case SPO_FREE_P:
      case OSP_FREE_S:
      case OPS_FREE_P:
        return 2;
      case FULL_INDEX_SCAN_SPO:
      case FULL_INDEX_SCAN_SOP:
      case FULL_INDEX_SCAN_PSO:
      case FULL_INDEX_SCAN_POS:
      case FULL_INDEX_SCAN_OSP:
      case FULL_INDEX_SCAN_OPS:
        return 3;
    }
  }

  static Permutation::Enum scanTypeToPermutation(ScanType scanType) {
    using enum Permutation::Enum;
    switch (scanType) {
      case PSO_BOUND_S:
      case PSO_FREE_S:
      case FULL_INDEX_SCAN_PSO:
        return PSO;
      case POS_FREE_O:
      case POS_BOUND_O:
      case FULL_INDEX_SCAN_POS:
        return POS;
      case SPO_FREE_P:
      case FULL_INDEX_SCAN_SPO:
        return SPO;
      case SOP_FREE_O:
      case SOP_BOUND_O:
      case FULL_INDEX_SCAN_SOP:
        return SOP;
      case OSP_FREE_S:
      case FULL_INDEX_SCAN_OSP:
        return OSP;
      case OPS_FREE_P:
      case FULL_INDEX_SCAN_OPS:
        return OPS;
    }
  }

  static std::array<size_t, 3> permutationToKeyOrder(
      Permutation::Enum permutation) {
    using enum Permutation::Enum;
    switch (permutation) {
      case POS:
        return {1, 2, 0};
      case PSO:
        return {1, 0, 2};
      case SOP:
        return {0, 2, 1};
      case SPO:
        return {0, 1, 2};
      case OPS:
        return {2, 1, 0};
      case OSP:
        return {2, 0, 1};
    }
  }

  static std::string_view permutationToString(Permutation::Enum permutation) {
    using enum Permutation::Enum;
    switch (permutation) {
      case POS:
        return "POS";
      case PSO:
        return "PSO";
      case SOP:
        return "SOP";
      case SPO:
        return "SPO";
      case OPS:
        return "OPS";
      case OSP:
        return "OSP";
    }
  }

 private:
  Permutation::Enum _permutation;
  size_t _numVariables;
  TripleComponent _subject;
  TripleComponent _predicate;
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

  const TripleComponent& getPredicate() const { return _predicate; }
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

  static std::array<cppcoro::generator<IdTable>, 2> lazyScanForJoinOfTwoScans(
      const IndexScan& s1, const IndexScan& s2);
  static cppcoro::generator<IdTable> lazyScanForJoinOfColumnWithScan(
      std::span<const Id> joinColumn, const IndexScan& s);

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
    return getResultWidth() == 3;
  }

  Permutation::Enum permutation() const { return _permutation; }

 private:
  ResultTable computeResult() override;

  vector<QueryExecutionTree*> getChildren() override { return {}; }

  void computeFullScan(IdTable* result, Permutation::Enum permutation) const;

  size_t computeSizeEstimate();

  string asStringImpl(size_t indent) const override;

  VariableToColumnMap computeVariableToColumnMap() const override;

  std::optional<std::shared_ptr<const ResultTable>>
  getPrecomputedResultFromQueryPlanning() override {
    return _precomputedResult;
  }

  std::array<const TripleComponent* const, 3> getPermutedTriple() const {
    using Arr = std::array<const TripleComponent* const, 3>;
    Arr inp{&_subject, &_predicate, &_object};
    auto permutation = permutationToKeyOrder(_permutation);
    return {inp[permutation[0]], inp[permutation[1]], inp[permutation[2]]};
  }
};
