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

 private:
  ScanType _type;
  std::shared_ptr<QueryExecutionTree> _subtree;
  size_t _subtreeJoinColumn;

  std::string _subject;
  std::string _object;

 public:
  HasPredicateScan() = delete;

  // TODO: The last argument should be of type `Variable`.
  HasPredicateScan(QueryExecutionContext* qec,
                   std::shared_ptr<QueryExecutionTree> subtree,
                   size_t subtreeJoinColumn, std::string objectVariable);

  HasPredicateScan(QueryExecutionContext* qec, SparqlTriple triple);

 private:
  [[nodiscard]] string getCacheKeyImpl() const override;

  void setSubject(const TripleComponent& subject);

  void setObject(const TripleComponent& object);

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

  [[nodiscard]] const std::string& getObject() const;

  vector<QueryExecutionTree*> getChildren() override {
    if (_subtree) {
      return {_subtree.get()};
    } else {
      return {};
    }
  }

  // These are made static and public mainly for easier testing
  static void computeFreeS(IdTable* resultTable, Id objectId, auto&& hasPattern,
                           const CompactVectorOfStrings<Id>& patterns);

  void computeFreeO(IdTable* resultTable, Id subjectAsId,
                    const CompactVectorOfStrings<Id>& patterns);

  static void computeFullScan(IdTable* resultTable, auto&& hasPattern,
                              const CompactVectorOfStrings<Id>& patterns,
                              size_t resultSize);

  template <int IN_WIDTH, int OUT_WIDTH>
  void computeSubqueryS(IdTable* result, const IdTable& _subtree,
                        size_t subtreeColIndex,
                        const CompactVectorOfStrings<Id>& patterns);

 private:
  ResultTable computeResult() override;

  [[nodiscard]] VariableToColumnMap computeVariableToColumnMap() const override;
};
