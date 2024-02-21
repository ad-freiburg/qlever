// Copyright 2019, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Florian Kramer (florian.kramer@neptun.uni-freiburg.de)

#include "TransitivePathBase.h"

#include <limits>
#include <memory>
#include <optional>
#include <utility>

#include "engine/ExportQueryExecutionTrees.h"
#include "engine/IndexScan.h"
#include "engine/TransitivePathFallback.h"
#include "engine/TransitivePathGraphblas.h"
#include "util/Exception.h"

// _____________________________________________________________________________
TransitivePathBase::TransitivePathBase(
    QueryExecutionContext* qec, std::shared_ptr<QueryExecutionTree> child,
    TransitivePathSide leftSide, TransitivePathSide rightSide, size_t minDist,
    size_t maxDist)
    : Operation(qec),
      subtree_(child
                   ? QueryExecutionTree::createSortedTree(std::move(child), {0})
                   : nullptr),
      lhs_(std::move(leftSide)),
      rhs_(std::move(rightSide)),
      minDist_(minDist),
      maxDist_(maxDist) {
  AD_CORRECTNESS_CHECK(qec != nullptr);
  if (lhs_.isVariable()) {
    variableColumns_[std::get<Variable>(lhs_.value_)] =
        makeAlwaysDefinedColumn(0);
  }
  if (rhs_.isVariable()) {
    variableColumns_[std::get<Variable>(rhs_.value_)] =
        makeAlwaysDefinedColumn(1);
  }

  lhs_.outputCol_ = 0;
  rhs_.outputCol_ = 1;
}

// _____________________________________________________________________________
std::string TransitivePathBase::getCacheKeyImpl() const {
  std::ostringstream os;
  os << " minDist " << minDist_ << " maxDist " << maxDist_ << "\n";

  os << "Left side:\n";
  os << lhs_.getCacheKey();

  os << "Right side:\n";
  os << rhs_.getCacheKey();

  AD_CORRECTNESS_CHECK(subtree_);
  os << "Subtree:\n" << subtree_->getCacheKey() << '\n';

  return std::move(os).str();
}

// _____________________________________________________________________________
std::string TransitivePathBase::getDescriptor() const {
  std::ostringstream os;
  os << "TransitivePath ";
  // If not full transitive hull, show interval as [min, max].
  if (minDist_ > 1 || maxDist_ < std::numeric_limits<size_t>::max()) {
    os << "[" << minDist_ << ", " << maxDist_ << "] ";
  }
  auto getName = [this](ValueId id) {
    auto optStringAndType =
        ExportQueryExecutionTrees::idToStringAndType(getIndex(), id, {});
    if (optStringAndType.has_value()) {
      return optStringAndType.value().first;
    } else {
      return absl::StrCat("#", id.getBits());
    }
  };
  // Left variable or entity name.
  if (lhs_.isVariable()) {
    os << std::get<Variable>(lhs_.value_).name();
  } else {
    os << getName(std::get<Id>(lhs_.value_));
  }
  // The predicate.
  auto scanOperation =
      std::dynamic_pointer_cast<IndexScan>(subtree_->getRootOperation());
  if (scanOperation != nullptr) {
    os << " " << scanOperation->getPredicate() << " ";
  } else {
    // Escaped the question marks to avoid a warning about ignored trigraphs.
    os << R"( <???> )";
  }
  // Right variable or entity name.
  if (rhs_.isVariable()) {
    os << std::get<Variable>(rhs_.value_).name();
  } else {
    os << getName(std::get<Id>(rhs_.value_));
  }
  return std::move(os).str();
}

// _____________________________________________________________________________
size_t TransitivePathBase::getResultWidth() const { return resultWidth_; }

// _____________________________________________________________________________
vector<ColumnIndex> TransitivePathBase::resultSortedOn() const {
  if (lhs_.isSortedOnInputCol()) {
    return {0};
  }
  if (rhs_.isSortedOnInputCol()) {
    return {1};
  }

  return {};
}

// _____________________________________________________________________________
VariableToColumnMap TransitivePathBase::computeVariableToColumnMap() const {
  return variableColumns_;
}

// _____________________________________________________________________________
void TransitivePathBase::setTextLimit(size_t limit) {
  for (auto child : getChildren()) {
    child->setTextLimit(limit);
  }
}

// _____________________________________________________________________________
bool TransitivePathBase::knownEmptyResult() {
  return subtree_->knownEmptyResult();
}

// _____________________________________________________________________________
float TransitivePathBase::getMultiplicity(size_t col) {
  (void)col;
  // The multiplicities are not known.
  return 1;
}

// _____________________________________________________________________________
uint64_t TransitivePathBase::getSizeEstimateBeforeLimit() {
  if (std::holds_alternative<Id>(lhs_.value_) ||
      std::holds_alternative<Id>(rhs_.value_)) {
    // If the subject or object is fixed, assume that the number of matching
    // triples is 1000. This will usually be an overestimate, but it will do the
    // job of avoiding query plans that first generate large intermediate
    // results and only then merge them with a triple such as this. In the
    // lhs_.isVar && rhs_.isVar case below, we assume a worst-case blowup of
    // 10000; see the comment there.
    return 1000;
  }
  if (lhs_.treeAndCol_.has_value()) {
    return lhs_.treeAndCol_.value().first->getSizeEstimate();
  }
  if (rhs_.treeAndCol_.has_value()) {
    return rhs_.treeAndCol_.value().first->getSizeEstimate();
  }
  // Set costs to something very large, so that we never compute the complete
  // transitive hull (unless the variables on both sides are not bound in any
  // other way, so that the only possible query plan is to compute the complete
  // transitive hull).
  //
  // NOTE: _subtree->getSizeEstimateBeforeLimit() is the number of triples of
  // the predicate, for which the transitive hull operator (+) is specified. On
  // Wikidata, the predicate with the largest blowup when taking the
  // transitive hull is wdt:P2789 (connects with). The blowup is then from 90K
  // (without +) to 110M (with +), so about 1000 times larger.
  AD_CORRECTNESS_CHECK(lhs_.isVariable() && rhs_.isVariable());
  return subtree_->getSizeEstimate() * 10000;
}

// _____________________________________________________________________________
size_t TransitivePathBase::getCostEstimate() {
  // We assume that the cost of computing the transitive path is proportional to
  // the result size.
  auto costEstimate = getSizeEstimateBeforeLimit();
  // Add the cost for the index scan of the predicate involved.
  for (auto* ptr : getChildren()) {
    if (ptr) {
      costEstimate += ptr->getCostEstimate();
    }
  }
  return costEstimate;
}

// _____________________________________________________________________________
std::shared_ptr<TransitivePathBase> TransitivePathBase::makeTransitivePath(
    QueryExecutionContext* qec, std::shared_ptr<QueryExecutionTree> child,
    const TransitivePathSide& leftSide, const TransitivePathSide& rightSide,
    size_t minDist, size_t maxDist) {
  bool useGraphblas = !RuntimeParameters().get<"use-graphblas">();
  return makeTransitivePath(qec, child, leftSide, rightSide, minDist, maxDist,
                            useGraphblas);
}

// _____________________________________________________________________________
std::shared_ptr<TransitivePathBase> TransitivePathBase::makeTransitivePath(
    QueryExecutionContext* qec, std::shared_ptr<QueryExecutionTree> child,
    const TransitivePathSide& leftSide, const TransitivePathSide& rightSide,
    size_t minDist, size_t maxDist, bool useGraphblas) {
  if (useGraphblas) {
    return std::make_shared<TransitivePathGraphblas>(
        qec, child, leftSide, rightSide, minDist, maxDist);
  } else {
    return std::make_shared<TransitivePathFallback>(
        qec, child, leftSide, rightSide, minDist, maxDist);
  }
}

// _____________________________________________________________________________
std::shared_ptr<TransitivePathBase> TransitivePathBase::bindLeftSide(
    std::shared_ptr<QueryExecutionTree> leftop, size_t inputCol) const {
  return bindLeftOrRightSide(std::move(leftop), inputCol, true);
}

// _____________________________________________________________________________
std::shared_ptr<TransitivePathBase> TransitivePathBase::bindRightSide(
    std::shared_ptr<QueryExecutionTree> rightop, size_t inputCol) const {
  return bindLeftOrRightSide(std::move(rightop), inputCol, false);
}

// _____________________________________________________________________________
std::shared_ptr<TransitivePathBase> TransitivePathBase::bindLeftOrRightSide(
    std::shared_ptr<QueryExecutionTree> leftOrRightOp, size_t inputCol,
    bool isLeft) const {
  // Enforce required sorting of `leftOrRightOp`.
  leftOrRightOp = QueryExecutionTree::createSortedTree(std::move(leftOrRightOp),
                                                       {inputCol});
  // Create a copy of this.
  //
  // NOTE: The RHS used to be `std::make_shared<TransitivePath>()`, which is
  // wrong because it first calls the copy constructor of the base class
  // `Operation`, which  would then ignore the changes in `variableColumnMap_`
  // made below (see `Operation::getInternallyVisibleVariableColumns` and
  // `Operation::getExternallyVariableColumns`).
  std::shared_ptr<TransitivePathBase> p =
      TransitivePathBase::makeTransitivePath(getExecutionContext(), subtree_,
                                             lhs_, rhs_, minDist_, maxDist_);
  if (isLeft) {
    p->lhs_.treeAndCol_ = {leftOrRightOp, inputCol};
  } else {
    p->rhs_.treeAndCol_ = {leftOrRightOp, inputCol};
  }

  // Note: The `variable` in the following structured binding is `const`, even
  // if we bind by value. We deliberately make one unnecessary copy of the
  // `variable` to keep the code simpler.
  for (auto [variable, columnIndexWithType] :
       leftOrRightOp->getVariableColumns()) {
    ColumnIndex columnIndex = columnIndexWithType.columnIndex_;
    if (columnIndex == inputCol) {
      continue;
    }

    columnIndexWithType.columnIndex_ += columnIndex > inputCol ? 1 : 2;

    p->variableColumns_[variable] = columnIndexWithType;
  }
  return p;
}

// _____________________________________________________________________________
bool TransitivePathBase::isBoundOrId() const {
  return lhs_.isBoundVariable() || rhs_.isBoundVariable() ||
         !lhs_.isVariable() || !rhs_.isVariable();
}
