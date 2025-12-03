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
#include "global/Pattern.h"
#include "parser/ParsedQuery.h"

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

  std::vector<QueryExecutionTree*> getChildren() override {
    if (subtree_) {
      return {std::addressof(subtree())};
    } else {
      return {};
    }
  }

  // These are made static and public mainly for easier testing
  template <typename HasPattern>
  void computeFreeS(IdTable* resultTable, Id objectId, HasPattern& hasPattern,
                    const CompactVectorOfStrings<Id>& patterns);

  void computeFreeO(IdTable* resultTable, Id subjectAsId,
                    const CompactVectorOfStrings<Id>& patterns) const;

  template <typename HasPattern>
  void computeFullScan(IdTable* resultTable, HasPattern& hasPattern,
                       const CompactVectorOfStrings<Id>& patterns,
                       size_t resultSize);

  template <int WIDTH>
  Result computeSubqueryS(IdTable* result,
                          const CompactVectorOfStrings<Id>& patterns);

 private:
  std::unique_ptr<Operation> cloneImpl() const override;

  Result computeResult([[maybe_unused]] bool requestLaziness) override;

  [[nodiscard]] VariableToColumnMap computeVariableToColumnMap() const override;
};

#endif  // QLEVER_SRC_ENGINE_HASPREDICATESCAN_H
