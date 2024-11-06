// Copyright 2024, University of Freiburg,
// Chair of Algorithms and Data Structures
// Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include "engine/Describe.h"
#include "engine/Join.h"
#include "engine/IndexScan.h"

Describe::Describe(QueryExecutionContext* qec,
                   std::shared_ptr<QueryExecutionTree> subtree,
                   ColumnIndex subtreeColumn)
    : Operation{qec},
      subtree_{std::move(subtree)},
      subtreeColumn_{subtreeColumn} {
  AD_CORRECTNESS_CHECK(subtree_ != nullptr);
}

std::vector<QueryExecutionTree*> Describe::getChildren() {
  return {subtree_.get()};
}

string Describe::getCacheKeyImpl() const {
  return absl::StrCat("DESCRIBE ", subtree_->getCacheKey());
}

string Describe::getDescriptor() const { return "DESCRIBE"; }

size_t Describe::getResultWidth() const { return 3; }

// As DESCRIBE is never part of the query planning, we can return dummy values
// for the following functions.

size_t Describe::getCostEstimate() { return 2 * subtree_->getCostEstimate(); }

uint64_t Describe::getSizeEstimateBeforeLimit() {
  return subtree_->getSizeEstimate() * 2;
}

float Describe::getMultiplicity(size_t col) { return 1.0f; }

bool Describe::knownEmptyResult() { return subtree_->knownEmptyResult(); }

vector<ColumnIndex> Describe::resultSortedOn() const { return {}; }

VariableToColumnMap Describe::computeVariableToColumnMap() const {
  using V = Variable;
  auto col = makeAlwaysDefinedColumn;
  return {{V("?subject"), col(0)},
          {V("?predicate"), col(1)},
          {V("?object"), col(2)}};
}

ProtoResult Describe::computeResult(bool requestLaziness) {

  using V = Variable;
  SparqlTripleSimple triple{V{"?subject"}, V{"?predicate"}, V{"?object"}};
  auto indexScan = ad_utility::makeExecutionTree<IndexScan>(getExecutionContext(), Permutation::SPO, triple);
  AD_CORRECTNESS_CHECK(subtree_->getVariableColumn(V{"?subject"}) == subtreeColumn_);

  // TODO<joka921> It might be, that the Column index is definitely not 0, we have to store this separately.
  auto join = ad_utility::makeExecutionTree<Join>(getExecutionContext(), subtree_, std::move(indexScan), subtreeColumn_, 0);

  auto result = join->getResult();
  auto resultTable = result->idTable().clone();

  auto s = join->getVariableColumn(V{"?subject"});
  auto p = join->getVariableColumn(V{"?predicate"});
  auto o = join->getVariableColumn(V{"?object"});

  resultTable.setColumnSubset(std::vector{s, p, o});
  return {std::move(resultTable), resultSortedOn(), result->getSharedLocalVocab()};
}
