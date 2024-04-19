// Copyright 2019, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Florian Kramer (florian.kramer@neptun.uni-freiburg.de)
//         Johannes Herrmann (johannes.r.herrmann(at)gmail.com)

#include "TransitivePathBase.h"

#include <limits>
#include <memory>
#include <optional>
#include <utility>

#include "engine/CallFixedSize.h"
#include "engine/ExportQueryExecutionTrees.h"
#include "engine/IndexScan.h"
#include "engine/TransitivePathBinSearch.h"
#include "engine/TransitivePathHashMap.h"
#include "global/RuntimeParameters.h"
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
TransitivePathBase::~TransitivePathBase() = default;

// _____________________________________________________________________________
std::pair<TransitivePathSide&, TransitivePathSide&>
TransitivePathBase::decideDirection() {
  if (lhs_.isBoundVariable()) {
    LOG(DEBUG) << "Computing TransitivePath left to right" << std::endl;
    return {lhs_, rhs_};
  } else if (rhs_.isBoundVariable() || !rhs_.isVariable()) {
    LOG(DEBUG) << "Computing TransitivePath right to left" << std::endl;
    return {rhs_, lhs_};
  }
  LOG(DEBUG) << "Computing TransitivePath left to right" << std::endl;
  return {lhs_, rhs_};
}

// _____________________________________________________________________________
void TransitivePathBase::fillTableWithHull(IdTable& table, const Map& hull,
                                           std::vector<Id>& nodes,
                                           size_t startSideCol,
                                           size_t targetSideCol,
                                           const IdTable& startSideTable,
                                           size_t skipCol) const {
  CALL_FIXED_SIZE((std::array{table.numColumns(), startSideTable.numColumns()}),
                  &TransitivePathBase::fillTableWithHullImpl, this, table, hull,
                  nodes, startSideCol, targetSideCol, startSideTable, skipCol);
}

// _____________________________________________________________________________
template <size_t WIDTH, size_t START_WIDTH>
void TransitivePathBase::fillTableWithHullImpl(
    IdTable& tableDyn, const Map& hull, std::vector<Id>& nodes,
    size_t startSideCol, size_t targetSideCol, const IdTable& startSideTable,
    size_t skipCol) const {
  IdTableStatic<WIDTH> table = std::move(tableDyn).toStatic<WIDTH>();
  IdTableView<START_WIDTH> startView =
      startSideTable.asStaticView<START_WIDTH>();

  size_t rowIndex = 0;
  for (size_t i = 0; i < nodes.size(); i++) {
    Id node = nodes[i];
    auto it = hull.find(node);
    if (it == hull.end()) {
      continue;
    }

    for (Id otherNode : it->second) {
      table.emplace_back();
      table(rowIndex, startSideCol) = node;
      table(rowIndex, targetSideCol) = otherNode;

      copyColumns<START_WIDTH, WIDTH>(startView, table, i, rowIndex, skipCol);

      rowIndex++;
    }
  }

  tableDyn = std::move(table).toDynamic();
}

// _____________________________________________________________________________
void TransitivePathBase::fillTableWithHull(IdTable& table, const Map& hull,
                                           size_t startSideCol,
                                           size_t targetSideCol) const {
  CALL_FIXED_SIZE((std::array{table.numColumns()}),
                  &TransitivePathBase::fillTableWithHullImpl, this, table, hull,
                  startSideCol, targetSideCol);
}

// _____________________________________________________________________________
template <size_t WIDTH>
void TransitivePathBase::fillTableWithHullImpl(IdTable& tableDyn,
                                               const Map& hull,
                                               size_t startSideCol,
                                               size_t targetSideCol) const {
  IdTableStatic<WIDTH> table = std::move(tableDyn).toStatic<WIDTH>();
  size_t rowIndex = 0;
  for (auto const& [node, linkedNodes] : hull) {
    for (Id linkedNode : linkedNodes) {
      table.emplace_back();
      table(rowIndex, startSideCol) = node;
      table(rowIndex, targetSideCol) = linkedNode;

      rowIndex++;
    }
  }
  tableDyn = std::move(table).toDynamic();
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
    TransitivePathSide leftSide, TransitivePathSide rightSide, size_t minDist,
    size_t maxDist) {
  bool useBinSearch =
      RuntimeParameters().get<"use-binsearch-transitive-path">();
  return makeTransitivePath(qec, std::move(child), std::move(leftSide),
                            std::move(rightSide), minDist, maxDist,
                            useBinSearch);
}

// _____________________________________________________________________________
std::shared_ptr<TransitivePathBase> TransitivePathBase::makeTransitivePath(
    QueryExecutionContext* qec, std::shared_ptr<QueryExecutionTree> child,
    TransitivePathSide leftSide, TransitivePathSide rightSide, size_t minDist,
    size_t maxDist, bool useBinSearch) {
  if (useBinSearch) {
    return std::make_shared<TransitivePathBinSearch>(
        qec, std::move(child), std::move(leftSide), std::move(rightSide),
        minDist, maxDist);
  } else {
    return std::make_shared<TransitivePathHashMap>(
        qec, std::move(child), std::move(leftSide), std::move(rightSide),
        minDist, maxDist);
  }
}

// _____________________________________________________________________________
vector<QueryExecutionTree*> TransitivePathBase::getChildren() {
  std::vector<QueryExecutionTree*> res;
  auto addChildren = [](std::vector<QueryExecutionTree*>& res,
                        TransitivePathSide side) {
    if (side.treeAndCol_.has_value()) {
      res.push_back(side.treeAndCol_.value().first.get());
    }
  };
  addChildren(res, lhs_);
  addChildren(res, rhs_);
  res.push_back(subtree_.get());
  return res;
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
  auto lhs = lhs_;
  auto rhs = rhs_;
  if (isLeft) {
    lhs.treeAndCol_ = {leftOrRightOp, inputCol};
  } else {
    rhs.treeAndCol_ = {leftOrRightOp, inputCol};
  }

  // We use the cheapest tree that can be created using any of the alternative
  // subtrees. This has the effect that the `TransitivePathBinSearch` will
  // never re-sort an index scan (which should not happen because we can just
  // take the appropriate index scan in the first place).
  std::vector<std::shared_ptr<TransitivePathBase>> candidates;
  candidates.push_back(TransitivePathBase::makeTransitivePath(
      getExecutionContext(), subtree_, lhs, rhs, minDist_, maxDist_));
  for (const auto& alternativeSubtree : alternativeSubtrees()) {
    candidates.push_back(TransitivePathBase::makeTransitivePath(
        getExecutionContext(), alternativeSubtree, lhs, rhs, minDist_,
        maxDist_));
  }

  auto& p = *std::ranges::min_element(
      candidates, {}, [](const auto& tree) { return tree->getCostEstimate(); });

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

    AD_CORRECTNESS_CHECK(!p->variableColumns_.contains(variable));
    p->variableColumns_[variable] = columnIndexWithType;
    p->resultWidth_++;
  }
  return std::move(p);
}

// _____________________________________________________________________________
bool TransitivePathBase::isBoundOrId() const {
  return lhs_.isBoundVariable() || rhs_.isBoundVariable() ||
         !lhs_.isVariable() || !rhs_.isVariable();
}

// _____________________________________________________________________________
template <size_t INPUT_WIDTH, size_t OUTPUT_WIDTH>
void TransitivePathBase::copyColumns(const IdTableView<INPUT_WIDTH>& inputTable,
                                     IdTableStatic<OUTPUT_WIDTH>& outputTable,
                                     size_t inputRow, size_t outputRow,
                                     size_t skipCol) const {
  size_t inCol = 0;
  size_t outCol = 2;
  while (inCol < inputTable.numColumns() && outCol < outputTable.numColumns()) {
    if (skipCol == inCol) {
      inCol++;
      continue;
    }

    outputTable(outputRow, outCol) = inputTable(inputRow, inCol);
    inCol++;
    outCol++;
  }
}

// _____________________________________________________________________________
void TransitivePathBase::insertIntoMap(Map& map, Id key, Id value) const {
  auto [it, success] = map.try_emplace(key, allocator());
  it->second.insert(value);
}
