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
  Permutation::Enum permutation_;
  TripleComponent subject_;
  TripleComponent predicate_;
  TripleComponent object_;
  size_t numVariables_;
  size_t sizeEstimate_;
  vector<float> multiplicity_;

  std::optional<std::shared_ptr<const ResultTable>> precomputedResult_ =
      std::nullopt;

 public:
  string getDescriptor() const override;

  IndexScan(QueryExecutionContext* qec, Permutation::Enum permutation,
            const SparqlTriple& triple);

  virtual ~IndexScan() = default;

  const TripleComponent& getPredicate() const { return predicate_; }
  const TripleComponent& getSubject() const { return subject_; }
  const TripleComponent& getObject() const { return object_; }

  size_t getResultWidth() const override;

  vector<ColumnIndex> resultSortedOn() const override;

  void setTextLimit(size_t) override {
    // Do nothing.
  }

  // Return the exact result size of the index scan. This is always known as it
  // can be read from the Metadata.
  size_t getExactSize() const { return sizeEstimate_; }

 private:
  // TODO<joka921> Make the `getSizeEstimateBeforeLimit()` function `const` for
  // ALL the `Operations`.
  uint64_t getSizeEstimateBeforeLimit() override { return getExactSize(); }

 public:
  size_t getCostEstimate() override;

  void determineMultiplicities();

  float getMultiplicity(size_t col) override {
    if (multiplicity_.empty()) {
      determineMultiplicities();
    }
    assert(col < multiplicity_.size());
    return multiplicity_[col];
  }

  void precomputeSizeEstimate() { sizeEstimate_ = computeSizeEstimate(); }

  bool knownEmptyResult() override { return getSizeEstimateBeforeLimit() == 0; }

  // Currently only the full scans support a limit clause.
  [[nodiscard]] bool supportsLimit() const override {
    return getResultWidth() == 3;
  }

  Permutation::Enum permutation() const { return permutation_; }

 private:
  ResultTable computeResult() override;

  vector<QueryExecutionTree*> getChildren() override { return {}; }

  void computeFullScan(IdTable* result, Permutation::Enum permutation) const;

  size_t computeSizeEstimate();

  string asStringImpl(size_t indent) const override;

  VariableToColumnMap computeVariableToColumnMap() const override;

  std::optional<std::shared_ptr<const ResultTable>>
  getPrecomputedResultFromQueryPlanning() override {
    return precomputedResult_;
  }

  // Return the stored triple in the order that corresponds to the
  // `permutation_`. For example if `permutation_ == PSO` then the result is
  // {&predicate_, &subject_, &object_}
  std::array<const TripleComponent* const, 3> getPermutedTriple() const;
};
