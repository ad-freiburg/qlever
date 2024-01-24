// Copyright 2018, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Florian Kramer (florian.kramer@mail.uni-freiburg.de)
#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "../global/Pattern.h"
#include "../parser/ParsedQuery.h"
#include "./Operation.h"
#include "./QueryExecutionTree.h"

class HasPredicateScan : public Operation {
 public:
  enum class ScanType {
    // Given a constant predicate, return all subjects
    FREE_S,
    // Given a constant subject, return all predicates
    FREE_O,
    // For all subjects return their predicates
    FULL_SCAN,
    // For a given subset of subjects return their predicates
    SUBQUERY_S
  };

  struct SubtreeAndColumnIndex {
    std::shared_ptr<QueryExecutionTree> subtree_;
    size_t subtreeJoinColumn_;
  };

 private:
  ScanType type_;
  std::optional<SubtreeAndColumnIndex> subtree_;

  QueryExecutionTree& subtree() {
    auto* ptr = subtree_.value().subtree_.get();
    AD_CORRECTNESS_CHECK(ptr != nullptr);
    return *ptr;
  }

  const QueryExecutionTree& subtree() const {
    return const_cast<HasPredicateScan&>(*this).subtree();
  }

  size_t subtreeColIdx() const { return subtree_.value().subtreeJoinColumn_; }

  TripleComponent subject_;
  TripleComponent object_;

 public:
  HasPredicateScan() = delete;

  // TODO: The last argument should be of type `Variable`.
  HasPredicateScan(QueryExecutionContext* qec,
                   std::shared_ptr<QueryExecutionTree> subtree,
                   size_t subtreeJoinColumn, Variable objectVariable);

  HasPredicateScan(QueryExecutionContext* qec, SparqlTriple triple);

 private:
  [[nodiscard]] string getCacheKeyImpl() const override;

 public:
  [[nodiscard]] string getDescriptor() const override;

  [[nodiscard]] size_t getResultWidth() const override;

  [[nodiscard]] vector<ColumnIndex> resultSortedOn() const override;

  void setTextLimit(size_t limit) override;

  bool knownEmptyResult() override;

  float getMultiplicity(size_t col) override;

 private:
  uint64_t getSizeEstimateBeforeLimit() override;

 public:
  size_t getCostEstimate() override;

 public:
  [[nodiscard]] ScanType getType() const;

  [[nodiscard]] const TripleComponent& getObject() const;

  vector<QueryExecutionTree*> getChildren() override {
    if (subtree_) {
      return {std::addressof(subtree())};
    } else {
      return {};
    }
  }

  // These are made static and public mainly for easier testing
  static void computeFreeS(IdTable* resultTable, Id objectId, auto& hasPattern,
                           const CompactVectorOfStrings<Id>& patterns);

  void computeFreeO(IdTable* resultTable, Id subjectAsId,
                    const CompactVectorOfStrings<Id>& patterns) const;

  static void computeFullScan(IdTable* resultTable, auto& hasPattern,
                              const CompactVectorOfStrings<Id>& patterns,
                              size_t resultSize);

  template <int WIDTH>
  ResultTable computeSubqueryS(IdTable* result,
                               const CompactVectorOfStrings<Id>& patterns);

 private:
  ResultTable computeResult() override;

  [[nodiscard]] VariableToColumnMap computeVariableToColumnMap() const override;
};
