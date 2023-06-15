// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)
#pragma once

#include <string>

#include "./Operation.h"

using std::string;

class SparqlTriple;

class IndexScan : public Operation {
 private:
  Permutation::Enum _permutation;
  TripleComponent _subject;
  TripleComponent _predicate;
  TripleComponent _object;
  size_t _numVariables;
  size_t _sizeEstimate;
  vector<float> _multiplicity;

  std::optional<std::shared_ptr<const ResultTable>> _precomputedResult =
      std::nullopt;

 public:
  string getDescriptor() const override;

  IndexScan(QueryExecutionContext* qec, Permutation::Enum permutation,
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

  // Return the stored triple in the order that corresponds to the
  // `_permutation`. For example if `_permutation == PSO` then the result is
  // {&_predicate, &_subject, &_object}
  std::array<const TripleComponent* const, 3> getPermutedTriple() const;
};
