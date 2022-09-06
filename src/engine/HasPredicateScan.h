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

  HasPredicateScan(QueryExecutionContext* qec,
                   std::shared_ptr<QueryExecutionTree> subtree,
                   size_t subtreeJoinColumn, std::string objectVariable);

  HasPredicateScan(QueryExecutionContext* qec, SparqlTriple triple);

 private:
  [[nodiscard]] string asStringImpl(size_t indent) const override;

  void setSubject(const TripleComponent& subject);

  void setObject(const TripleComponent& object);

 public:
  [[nodiscard]] string getDescriptor() const override;

  [[nodiscard]] size_t getResultWidth() const override;

  [[nodiscard]] vector<size_t> resultSortedOn() const override;

  [[nodiscard]] ad_utility::HashMap<string, size_t> getVariableColumns()
      const override;

  void setTextLimit(size_t limit) override;

  bool knownEmptyResult() override;

  float getMultiplicity(size_t col) override;

  size_t getSizeEstimate() override;

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
  static void computeFreeS(ResultTable* result, Id objectId,
                           const std::vector<PatternID>& hasPattern,
                           const CompactVectorOfStrings<Id>& hasPredicate,
                           const CompactVectorOfStrings<Id>& patterns);

  static void computeFreeO(ResultTable* result, Id subjectId,
                           const std::vector<PatternID>& hasPattern,
                           const CompactVectorOfStrings<Id>& hasPredicate,
                           const CompactVectorOfStrings<Id>& patterns);

  static void computeFullScan(ResultTable* result,
                              const std::vector<PatternID>& hasPattern,
                              const CompactVectorOfStrings<Id>& hasPredicate,
                              const CompactVectorOfStrings<Id>& patterns,
                              size_t resultSize);

  template <int IN_WIDTH, int OUT_WIDTH>
  static void computeSubqueryS(IdTable* result, const IdTable& _subtree,
                               size_t subtreeColIndex,
                               const std::vector<PatternID>& hasPattern,
                               const CompactVectorOfStrings<Id>& hasPredicate,
                               const CompactVectorOfStrings<Id>& patterns);

 private:
  ScanType _type;
  std::shared_ptr<QueryExecutionTree> _subtree;
  size_t _subtreeJoinColumn = -1;

  std::string _subject;
  std::string _object;

  void computeResult(ResultTable* result) override;
};
