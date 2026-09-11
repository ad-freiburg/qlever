// Copyright 2018, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Florian Kramer (florian.kramer@mail.uni-freiburg.de)

#ifndef QLEVER_SRC_ENGINE_HASPREDICATESCAN_H
#define QLEVER_SRC_ENGINE_HASPREDICATESCAN_H

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "engine/Operation.h"
#include "engine/QueryExecutionTree.h"
#include "parser/ParsedQuery.h"
#include "util/CompactStringVector.h"

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

  // Return a non-owning pointer to the subtree. Note that this is `const` but
  // hands out a non-const pointer, for the same reason as
  // `Operation::getChildrenImpl`.
  QueryExecutionTree* subtreePtr() const {
    auto* ptr = subtree_.value().subtree_.get();
    AD_CORRECTNESS_CHECK(ptr != nullptr);
    return ptr;
  }

  QueryExecutionTree& subtree() { return *subtreePtr(); }

  const QueryExecutionTree& subtree() const { return *subtreePtr(); }

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
  [[nodiscard]] std::string getCacheKeyImpl() const override;

 public:
  [[nodiscard]] std::string getDescriptor() const override;

  [[nodiscard]] size_t getResultWidth() const override;

  [[nodiscard]] std::vector<ColumnIndex> resultSortedOn() const override;

  bool knownEmptyResult() override;

  float getMultiplicity(size_t col) override;

 private:
  uint64_t getSizeEstimateBeforeLimit() override;

 public:
  size_t getCostEstimate() override;

 public:
  [[nodiscard]] ScanType getType() const;

  [[nodiscard]] const TripleComponent& getObject() const;

 private:
  std::vector<QueryExecutionTree*> getChildrenImpl() const override {
    if (subtree_) {
      return {subtreePtr()};
    } else {
      return {};
    }
  }

 public:
  // These are made static and public mainly for easier testing
  template <typename HasPattern>
  void computeFreeS(IdTable* resultTable, Id objectId, HasPattern& hasPattern,
                    const CompactVectorOfStrings<Id>& patterns);

  void computeFreeO(IdTable* resultTable, TripleComponent subject,
                    const CompactVectorOfStrings<Id>& patterns) const;

  template <typename HasPattern>
  void computeFullScan(IdTable* resultTable, HasPattern& hasPattern,
                       const CompactVectorOfStrings<Id>& patterns,
                       size_t resultSize);

  template <int WIDTH>
  Result computeSubqueryS(IdTable* result,
                          const CompactVectorOfStrings<Id>& patterns);

 private:
  [[nodiscard]] bool isDeterministicImpl() const override { return true; }

  std::unique_ptr<Operation> cloneImpl() const override;

  Result computeResult([[maybe_unused]] bool requestLaziness) override;

  [[nodiscard]] VariableToColumnMap computeVariableToColumnMap() const override;

 public:
  // Create an `IndexScan` for the internal `ql:has-pattern` predicate, using
  // the internal `PSO` permutation. The parameters `subject` and `object` are
  // the placeholders for the `IndexScan` triple.
  static std::shared_ptr<QueryExecutionTree> makePatternScan(
      QueryExecutionContext* qec, TripleComponent subject, Variable object);
};

#endif  // QLEVER_SRC_ENGINE_HASPREDICATESCAN_H
