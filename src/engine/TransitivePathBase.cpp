// Copyright 2019, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Florian Kramer (florian.kramer@neptun.uni-freiburg.de)
//         Johannes Herrmann (johannes.r.herrmann(at)gmail.com)

#ifndef QLEVER_REDUCED_FEATURE_SET_FOR_CPP17

#include "engine/TransitivePathBase.h"

#include <absl/strings/str_cat.h>

#include <limits>
#include <memory>
#include <optional>
#include <utility>

#include "engine/CallFixedSize.h"
#include "engine/CartesianProductJoin.h"
#include "engine/DistinctGraphs.h"
#include "engine/EmptyPath.h"
#include "engine/IndexScan.h"
#include "engine/TransitivePathBinSearch.h"
#include "engine/TransitivePathHashMap.h"
#include "engine/Values.h"
#include "global/RuntimeParameters.h"
#include "util/ChunkedForLoop.h"
#include "util/Exception.h"
#include "util/Log.h"

namespace {
auto makeInternalVariable(std::string_view string) {
  return Variable{absl::StrCat("?internal_property_path_variable_", string)};
}
}  // namespace

// _____________________________________________________________________________
TransitivePathBase::TransitivePathBase(
    QueryExecutionContext* qec, std::shared_ptr<QueryExecutionTree> child,
    TransitivePathSide leftSide, TransitivePathSide rightSide, size_t minDist,
    size_t maxDist, Graphs activeGraphs,
    const std::optional<Variable>& graphVariable)
    : Operation(qec),
      subtree_(std::move(child)),
      lhs_(std::move(leftSide)),
      rhs_(std::move(rightSide)),
      minDist_(minDist),
      maxDist_(maxDist),
      activeGraphs_{std::move(activeGraphs)},
      graphVariable_{graphVariable} {
  AD_CORRECTNESS_CHECK(qec != nullptr);
  AD_CORRECTNESS_CHECK(subtree_);
  if (lhs_.isVariable()) {
    variableColumns_[lhs_.value_.getVariable()] = makeAlwaysDefinedColumn(0);
  }
  if (rhs_.isVariable()) {
    variableColumns_[rhs_.value_.getVariable()] = makeAlwaysDefinedColumn(1);
  }
  if (minDist_ == 0) {
    auto& startingSide = decideDirection().first;
    // If we have hardcoded differing values left and right, we can increase the
    // minimum distance to 1. Example: The triple pattern `<x> <p>* <y>` cannot
    // possibly match with length zero because <x> != <y>. Instead we compute
    // `<x> <p>+ <y>` which avoids the performance pessimisation of having to
    // match the iri or literal against the knowledge graph.
    if (!lhs_.isVariable() && !rhs_.isVariable() &&
        lhs_.value_ != rhs_.value_) {
      minDist_ = 1;
    } else if (lhs_.isUnboundVariable() && rhs_.isUnboundVariable()) {
      // Both sides are unbound, so the starting point for the empty path is the
      // set of all entities of the knowledge graph. This can be re-bound to
      // something cheaper later on (see `bindLeftOrRightSide`).
      boundVariableIsForEmptyPath_ = true;
      lhs_.treeAndCol_.emplace(
          ad_utility::makeExecutionTree<EmptyPath>(
              qec, makeInternalVariable("x"), activeGraphs_, graphVariable_),
          0);
    } else if (!startingSide.isVariable()) {
      // According to the SPARQL standard a hardcoded value is matched once in
      // every graph, even if it doesn't occur in the dataset at all, so we
      // simply feed the value itself into the transitive path.
      auto tree = makeStartingPoint(qec, activeGraphs_, graphVariable_,
                                    startingSide.value_);
      auto column = tree->getVariableColumn(makeInternalVariable("x"));
      startingSide.treeAndCol_.emplace(std::move(tree), column);
    }
  }

  lhs_.outputCol_ = 0;
  rhs_.outputCol_ = 1;

  // Add graph variable to output if present
  if (graphVariable_.has_value()) {
    // Don't overwrite entry if the graph variable has the same name as one of
    // the columns.
    if (!variableColumns_.contains(graphVariable_.value())) {
      variableColumns_[graphVariable_.value()] =
          makeAlwaysDefinedColumn(resultWidth_);
    }
    resultWidth_ += 1;
  }
}

// _____________________________________________________________________________
std::shared_ptr<QueryExecutionTree> TransitivePathBase::makeStartingPoint(
    QueryExecutionContext* qec, const Graphs& activeGraphs,
    const std::optional<Variable>& graphVariable,
    const TripleComponent& tripleComponent) {
  auto tree =
      makeValuesForSingleValue(qec, makeInternalVariable("x"), tripleComponent);
  if (graphVariable.has_value()) {
    // The transitive path implementations require a graph id for every starting
    // point, so the single value has to be combined with every graph it is
    // matched in.
    tree = ad_utility::makeExecutionTree<CartesianProductJoin>(
        qec,
        std::vector<std::shared_ptr<QueryExecutionTree>>{
            std::move(tree), DistinctGraphs::makeAllGraphs(
                                 qec, graphVariable.value(), activeGraphs)});
  }
  return tree;
}

// _____________________________________________________________________________
TransitivePathBase::~TransitivePathBase() = default;

// _____________________________________________________________________________
std::pair<TransitivePathSide&, TransitivePathSide&>
TransitivePathBase::decideDirection() {
  if (lhs_.isBoundVariable()) {
    AD_LOG_DEBUG << "Computing TransitivePath left to right" << std::endl;
    return {lhs_, rhs_};
  } else if (rhs_.isBoundVariable() || !rhs_.isVariable()) {
    AD_LOG_DEBUG << "Computing TransitivePath right to left" << std::endl;
    return {rhs_, lhs_};
  }
  AD_LOG_DEBUG << "Computing TransitivePath left to right" << std::endl;
  return {lhs_, rhs_};
}

// _____________________________________________________________________________
Result::Generator TransitivePathBase::fillTableWithHull(
    NodeGenerator hull, size_t startSideCol, size_t targetSideCol,
    bool yieldOnce, size_t inputWidth) const {
  return ad_utility::callFixedSizeVi(
      std::array{inputWidth, getResultWidth()},
      [&](auto INPUT_WIDTH, auto OUTPUT_WIDTH) {
        return fillTableWithHullImpl<INPUT_WIDTH, OUTPUT_WIDTH>(
            std::move(hull), startSideCol, targetSideCol, yieldOnce);
      });
}

// _____________________________________________________________________________
template <size_t INPUT_WIDTH, size_t OUTPUT_WIDTH>
Result::Generator TransitivePathBase::fillTableWithHullImpl(
    NodeGenerator hull, size_t startSideCol, size_t targetSideCol,
    bool yieldOnce) const {
  ad_utility::Timer timer{ad_utility::Timer::Stopped};
  size_t outputRow = 0;
  IdTableStatic<OUTPUT_WIDTH> table{getResultWidth(), allocator()};
  LocalVocab mergedVocab{};
  for (auto& [node, graph, linkedNodes, localVocab, idTable, inputRow] : hull) {
    timer.cont();
    // As an optimization nodes without any linked nodes should not get yielded
    // in the first place.
    AD_CONTRACT_CHECK(!linkedNodes.empty());
    if (!yieldOnce) {
      table.reserve(linkedNodes.size());
    }
    std::optional<IdTableView<INPUT_WIDTH>> inputView = std::nullopt;
    if (idTable.has_value()) {
      inputView = idTable->template asStaticView<INPUT_WIDTH>();
    }
    // Add one row per linked node, with a cancellation check per chunk (a
    // single node can be linked to millions of nodes).
    ad_utility::chunkedForEach(
        linkedNodes,
        [this, &table, &outputRow, &inputView, startSideCol, targetSideCol,
         node = node, graph = graph, inputRow = inputRow](Id linkedNode) {
          table.emplace_back();
          table(outputRow, startSideCol) = node;
          table(outputRow, targetSideCol) = linkedNode;

          if (inputView.has_value()) {
            copyColumns<INPUT_WIDTH, OUTPUT_WIDTH>(inputView.value(), table,
                                                   inputRow, outputRow);
          }
          if (graphVariable_.has_value()) {
            table(outputRow, table.numColumns() - 1) = graph;
          }

          outputRow++;
        },
        cancellationChecker());

    if (yieldOnce) {
      mergedVocab.mergeWith(localVocab);
    } else {
      timer.stop();
      runtimeInfo().addDetail("IdTable fill time", timer.msecs());
      co_yield {std::move(table).toDynamic(), std::move(localVocab)};
      table = IdTableStatic<OUTPUT_WIDTH>{getResultWidth(), allocator()};
      outputRow = 0;
    }
    timer.stop();
  }
  if (yieldOnce) {
    timer.start();
    runtimeInfo().addDetail("IdTable fill time", timer.msecs());
    co_yield {std::move(table).toDynamic(), std::move(mergedVocab)};
  }
}

// _____________________________________________________________________________
std::string TransitivePathBase::getCacheKeyImpl() const {
  std::ostringstream os;
  os << "TRANSITIVE PATH ";
  if (graphVariable_.has_value()) {
    os << "with graph " << graphVariable_.value().name() << ' ';
  }
  if (lhs_.isVariable() && lhs_.value_ == rhs_.value_) {
    // Use a different cache key if the same variable is used left and right,
    // because that changes the behaviour of this operation and variable names
    // are not found in the children's cache keys.
    os << "symmetric ";
  }
  os << "minDist " << minDist_ << " maxDist " << maxDist_ << "\n";

  os << "Left side:\n";
  os << lhs_.getCacheKey();

  os << "Right side:\n";
  os << rhs_.getCacheKey();

  os << "Subtree:\n" << subtree_->getCacheKey() << '\n';

  return std::move(os).str();
}

// _____________________________________________________________________________
std::optional<ColumnIndex> TransitivePathBase::getActualGraphColumnIndex(
    const std::shared_ptr<QueryExecutionTree>& tree) const {
  if (graphVariable_.has_value()) {
    auto helperVar = tree->getVariableColumnOrNullopt(internalGraphHelper_);
    if (helperVar.has_value()) {
      return helperVar;
    }
    return tree->getVariableColumnOrNullopt(graphVariable_.value());
  }
  return std::nullopt;
}

// _____________________________________________________________________________
size_t TransitivePathBase::numJoinColumnsWith(
    const std::shared_ptr<QueryExecutionTree>& tree, ColumnIndex joinColumn,
    std::optional<ColumnIndex> otherJoinColumn) const {
  return tree->getResultWidth() -
         computePayloadColumns(tree, joinColumn, otherJoinColumn).size();
}

// _____________________________________________________________________________
std::string TransitivePathBase::getDescriptor() const {
  std::ostringstream os;
  os << "TransitivePath ";
  // If not full transitive hull, show interval as [min, max].
  if (minDist_ > 1 || maxDist_ < std::numeric_limits<size_t>::max()) {
    os << "[" << minDist_ << ", " << maxDist_ << "] ";
  }
  // Left variable or entity name.
  os << lhs_.value_;
  // The predicate.
  auto scanOperation =
      std::dynamic_pointer_cast<IndexScan>(subtree_->getRootOperation());
  if (scanOperation != nullptr) {
    os << " " << scanOperation->predicate() << " ";
  } else {
    // Escaped the question marks to avoid a warning about ignored trigraphs.
    os << R"( <???> )";
  }
  // Right variable or entity name.
  os << rhs_.value_;
  return std::move(os).str();
}

// _____________________________________________________________________________
size_t TransitivePathBase::getResultWidth() const { return resultWidth_; }

// _____________________________________________________________________________
std::vector<ColumnIndex> TransitivePathBase::resultSortedOn() const {
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
bool TransitivePathBase::knownEmptyResult() {
  auto sideHasKnownEmptyResult = [this]() {
    auto tree = decideDirection().first.treeAndCol_;
    return tree.has_value() && tree.value().first->knownEmptyResult();
  };
  return (subtree_->knownEmptyResult() && minDist_ > 0) ||
         sideHasKnownEmptyResult();
}

// _____________________________________________________________________________
float TransitivePathBase::getMultiplicity(size_t col) {
  (void)col;
  // The multiplicities are not known.
  return 1;
}

// _____________________________________________________________________________
uint64_t TransitivePathBase::getSizeEstimateBeforeLimit() {
  if (!lhs_.isVariable() || !rhs_.isVariable()) {
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
    size_t maxDist, Graphs activeGraphs,
    const std::optional<Variable>& graphVariable) {
  bool useBinSearch =
      getRuntimeParameter<&RuntimeParameters::useBinsearchTransitivePath_>();
  return makeTransitivePath(
      qec, std::move(child), std::move(leftSide), std::move(rightSide), minDist,
      maxDist, useBinSearch, std::move(activeGraphs), graphVariable);
}

// _____________________________________________________________________________
std::shared_ptr<TransitivePathBase> TransitivePathBase::makeTransitivePath(
    QueryExecutionContext* qec, std::shared_ptr<QueryExecutionTree> child,
    TransitivePathSide leftSide, TransitivePathSide rightSide, size_t minDist,
    size_t maxDist, bool useBinSearch, Graphs activeGraphs,
    const std::optional<Variable>& graphVariable) {
  if (useBinSearch) {
    return qec->makeShared<TransitivePathBinSearch>(
        qec, std::move(child), std::move(leftSide), std::move(rightSide),
        minDist, maxDist, std::move(activeGraphs), graphVariable);
  } else {
    return qec->makeShared<TransitivePathHashMap>(
        qec, std::move(child), std::move(leftSide), std::move(rightSide),
        minDist, maxDist, std::move(activeGraphs), graphVariable);
  }
}

// _____________________________________________________________________________
qlm::vector<QueryExecutionTree*> TransitivePathBase::getChildrenImpl() const {
  qlm::vector<QueryExecutionTree*> res{allocator()};
  for (auto& side : {&lhs_, &rhs_}) {
    if (!side->treeAndCol_.has_value()) {
      continue;
    }
    auto* tree = side->treeAndCol_->first.get();
    if (ql::ranges::find(res, tree) == res.end()) {
      res.push_back(tree);
    }
  }
  res.push_back(subtree_.get());
  return res;
}

// _____________________________________________________________________________
std::shared_ptr<QueryExecutionTree> TransitivePathBase::matchWithKnowledgeGraph(
    size_t& inputCol, std::shared_ptr<QueryExecutionTree> leftOrRightOp) const {
  // If we don't include the empty path, then inputs which don't originate in
  // the graph will be automatically filtered out because they cannot appear in
  // the `subtree_`.
  if (minDist_ > 0) {
    return leftOrRightOp;
  }

  auto originalVar =
      leftOrRightOp->getVariableAndInfoByColumnIndex(inputCol).first;
  // If we join on the graph variable itself, we cannot reuse it for the graph
  // column, so we use an internal helper variable instead (which takes
  // precedence in `getActualGraphColumnIndex`). Note that the values of the
  // join column are not necessarily valid graph names, so this case always
  // requires matching against the knowledge graph.
  bool graphIsJoinColumn = originalVar == graphVariable_;
  std::optional<Variable> graphVariable =
      graphIsJoinColumn ? std::optional{internalGraphHelper_} : graphVariable_;

  // If the graph column is missing we have to add it, even if the values of the
  // join column are already guaranteed to be part of the knowledge graph.
  bool graphColumnIsMissing =
      graphVariable.has_value() &&
      !leftOrRightOp->getVariableColumnOrNullopt(graphVariable.value())
           .has_value();
  if (!graphColumnIsMissing &&
      leftOrRightOp->getRootOperation()->columnOriginatesFromGraphOrUndef(
          originalVar)) {
    return leftOrRightOp;
  }

  leftOrRightOp = ad_utility::makeExecutionTree<EmptyPath>(
      getExecutionContext(), originalVar, activeGraphs_,
      std::move(graphVariable),
      EmptyPath::CheckedChild{std::move(leftOrRightOp), inputCol});
  inputCol = leftOrRightOp->getVariableColumn(originalVar);
  return leftOrRightOp;
}

// _____________________________________________________________________________
std::shared_ptr<TransitivePathBase> TransitivePathBase::bindSides(
    std::shared_ptr<QueryExecutionTree> op, std::optional<size_t> leftCol,
    std::optional<size_t> rightCol) const {
  // Ensure at least one side is given.
  AD_CORRECTNESS_CHECK(leftCol.has_value() || rightCol.has_value());

  // Create a copy of this.
  //
  // NOTE: The RHS used to be `std::make_shared<TransitivePath>()`, which is
  // wrong because it first calls the copy constructor of the base class
  // `Operation`, which  would then ignore the changes in `variableColumnMap_`
  // made below (see `Operation::getInternallyVisibleVariableColumns` and
  // `Operation::getExternallyVariableColumns`).
  auto lhs = lhs_;
  auto rhs = rhs_;

  // Set `op` and the corresponding binding column to each side's
  // `treeAndCol_` member.
  auto assignTreeAndColToSide = [this, &op, &leftCol, &rightCol, &lhs, &rhs](
                                    auto& col, auto& side,
                                    bool resetPlaceholder) {
    // Obtain the variable of a column.
    auto getVar = [&op](const std::optional<size_t>& col) {
      return col.has_value()
                 ? std::optional<Variable>(
                       op->getVariableAndInfoByColumnIndex(col.value()).first)
                 : std::nullopt;
    };
    // Set a new column for a side (since they might get mixed up by
    // `matchWithKnowledgeGraph`).
    auto updateCol = [&op](auto& col, const auto& var, auto& side) {
      if (col.has_value()) {
        col = op->getVariableColumn(var.value());
        side.treeAndCol_ = {op, col.value()};
      }
    };
    if (col.has_value()) {
      auto leftVar = getVar(leftCol);
      auto rightVar = getVar(rightCol);
      op = matchWithKnowledgeGraph(col.value(), op);
      // Update the side columns indices which might get messed up by
      // `EmptyPath` operations added by `matchWithKnowedgeGraph`.
      updateCol(leftCol, leftVar, lhs);
      updateCol(rightCol, rightVar, rhs);
    }
    // Remove placeholder tree if binding actual tree.
    else if (resetPlaceholder || !side.isVariable()) {
      side.treeAndCol_ = std::nullopt;
    }
  };
  assignTreeAndColToSide(leftCol, lhs, boundVariableIsForEmptyPath_);
  assignTreeAndColToSide(rightCol, rhs, false);

  bool useBinSearch = dynamic_cast<const TransitivePathBinSearch*>(this);
  std::vector<std::shared_ptr<TransitivePathBase>> candidates;
  candidates.push_back(makeTransitivePath(getExecutionContext(), subtree_, lhs,
                                          rhs, minDist_, maxDist_, useBinSearch,
                                          activeGraphs_, graphVariable_));
  for (const auto& alternativeSubtree : alternativeSubtrees()) {
    candidates.push_back(makeTransitivePath(
        getExecutionContext(), alternativeSubtree, lhs, rhs, minDist_, maxDist_,
        useBinSearch, activeGraphs_, graphVariable_));
  }

  auto& plan = *ql::ranges::min_element(
      candidates, {}, [](const auto& tree) { return tree->getCostEstimate(); });

  computePayloadColumnOffsets(op, plan, leftCol, rightCol);
  // Since we also put the side column(s) and graph variables in the result,
  // we only have to add the amount of new (payload) columns to the resulting
  // output table's width.
  plan->resultWidth_ +=
      op->getResultWidth() -
      numJoinColumnsWith(
          op, leftCol.has_value() ? leftCol.value() : rightCol.value(),
          leftCol.has_value() ? rightCol : leftCol);

  // Make sure mapping actually points to the last column if it's not one of
  // the regular variables.
  if (graphVariable_.has_value()) {
    auto& graphIndex =
        plan->variableColumns_[graphVariable_.value()].columnIndex_;
    if (graphIndex == 2) {
      graphIndex = plan->resultWidth_ - 1;
    }
  }
  return std::move(plan);
}

// _____________________________________________________________________________
void TransitivePathBase::computePayloadColumnOffsets(
    const std::shared_ptr<QueryExecutionTree>& op,
    const std::shared_ptr<TransitivePathBase>& plan,
    std::optional<size_t> leftCol, std::optional<size_t> rightCol) const {
  auto payloadCols = computePayloadColumns(op, leftCol, rightCol);

  // Note: The `variable` in the following structured binding is `const`, even
  // if we bind by value. We deliberately make one unnecessary copy of the
  // `variable` to keep the code simpler.
  for (auto [variable, columnIndexWithType] : op->getVariableColumns()) {
    auto& colIndex = columnIndexWithType.columnIndex_;
    auto position = ql::ranges::find(payloadCols, colIndex);
    // If we couldn't find the column index in the payload columns, it is of a
    // side or the graph column.
    if (position == payloadCols.end()) {
      continue;
    }

    // The two side columns always come first in the output. Graph column comes
    // at the very end.
    colIndex = 2 + ql::ranges::distance(payloadCols.begin(), position);
    AD_CORRECTNESS_CHECK(!plan->variableColumns_.contains(variable));
    plan->variableColumns_[variable] = columnIndexWithType;
  }
}

// _____________________________________________________________________________
std::vector<ColumnIndex> TransitivePathBase::computePayloadColumns(
    const std::shared_ptr<QueryExecutionTree>& op,
    const std::optional<ColumnIndex>& leftCol,
    const std::optional<ColumnIndex>& rightCol) const {
  auto graphCol = getActualGraphColumnIndex(op);
  auto isNotPayload = [&graphCol, &leftCol, &rightCol](const auto& col) {
    return graphCol == col || leftCol == col || rightCol == col;
  };
  std::vector<ColumnIndex> payloadIndices;
  for (ColumnIndex col = 0; col < op->getResultWidth(); col++) {
    if (isNotPayload(col)) {
      continue;
    }
    payloadIndices.emplace_back(col);
  }
  return payloadIndices;
}

// _____________________________________________________________________________
bool TransitivePathBase::isBoundOrId() const {
  // Don't make the execution tree for the empty path count as "bound".
  return !boundVariableIsForEmptyPath_ &&
         (!lhs_.isUnboundVariable() || !rhs_.isUnboundVariable());
}

// _____________________________________________________________________________
template <size_t INPUT_WIDTH, size_t OUTPUT_WIDTH>
void TransitivePathBase::copyColumns(const IdTableView<INPUT_WIDTH>& inputTable,
                                     IdTableStatic<OUTPUT_WIDTH>& outputTable,
                                     size_t inputRow, size_t outputRow) const {
  size_t inCol = 0;
  // The first two columns are both sides of the transitive path, then they
  // are followed by the payload columns (if present) and then the (optional)
  // graph column follows (but it is not written in this function).
  size_t outCol = 2;
  AD_CORRECTNESS_CHECK(inputTable.numColumns() +
                           (graphVariable_.has_value() ? 3 : 2) ==
                       outputTable.numColumns());
  while (inCol < inputTable.numColumns()) {
    AD_CORRECTNESS_CHECK(outCol < outputTable.numColumns());
    outputTable.at(outputRow, outCol) = inputTable.at(inputRow, inCol);
    inCol++;
    outCol++;
  }
}

// _____________________________________________________________________________
bool TransitivePathBase::columnOriginatesFromGraphOrUndef(
    const Variable& variable) const {
  AD_CONTRACT_CHECK(getExternallyVisibleVariableColumns().contains(variable));
  if (variable != lhs_.value_ && variable != rhs_.value_) {
    // The payload columns of the bound side are copied over verbatim, so they
    // inherit the guarantee of that operation. The graph column ends up here
    // too (unless it is one of the two sides), but `subtree_` always provides
    // it and reports `false` for it, because in RDF only subjects and objects
    // are nodes (see `IndexScan::columnOriginatesFromGraphOrUndef`), so the
    // answer stays `false` for graphs as it has to.
    return Operation::columnOriginatesFromGraphOrUndef(variable);
  }
  // The empty path copies a hardcoded value over to the other side without
  // checking it against the knowledge graph (see the constructor), so in that
  // case the remaining variable may be bound to a value that is not part of the
  // knowledge graph.
  return minDist_ != 0 || (lhs_.isVariable() && rhs_.isVariable());
}

// _____________________________________________________________________________
void TransitivePathBase::setTreeAndChildrenToClone(
    TransitivePathBase& copy) const {
  copy.subtree_ = subtree_->clone();
  copy.lhs_ = lhs_.clone();

  // Don't duplicate trees if both sides are bound at once, instead use the same
  // for both sides.
  if (lhs_.isBoundVariable() && rhs_.isBoundVariable()) {
    copy.rhs_ = rhs_.clone(copy.lhs_.treeAndCol_->first);
  } else {
    copy.rhs_ = rhs_.clone();
  }
}

// _____________________________________________________________________________
// Don't check the name because this leads to segfaults during static
// initialization.
const Variable TransitivePathBase::internalGraphHelper_{
    "?_Qlever_internal_transitive_path_graph", false};

#endif
