// Copyright 2019, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Florian Kramer (florian.kramer@neptun.uni-freiburg.de)
//         Johannes Herrmann (johannes.r.herrmann(at)gmail.com)

#ifndef QLEVER_REDUCED_FEATURE_SET_FOR_CPP17

#include "TransitivePathBase.h"

#include <absl/strings/str_cat.h>

#include <limits>
#include <memory>
#include <optional>
#include <utility>

#include "engine/CallFixedSize.h"
#include "engine/Distinct.h"
#include "engine/Filter.h"
#include "engine/IndexScan.h"
#include "engine/Join.h"
#include "engine/MultiColumnJoin.h"
#include "engine/TransitivePathBinSearch.h"
#include "engine/TransitivePathHashMap.h"
#include "engine/Union.h"
#include "engine/Values.h"
#include "engine/sparqlExpressions/LiteralExpression.h"
#include "engine/sparqlExpressions/NaryExpression.h"
#include "global/RuntimeParameters.h"
#include "util/Exception.h"

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
      boundVariableIsForEmptyPath_ = true;
      lhs_.treeAndCol_.emplace(
          makeEmptyPathSide(qec, activeGraphs_, graphVariable_), 0);
    } else if (!startingSide.isVariable()) {
      startingSide.treeAndCol_.emplace(
          joinWithIndexScan(qec, activeGraphs_, graphVariable_,
                            startingSide.value_),
          0);
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

namespace {
auto makeInternalVariable(std::string_view string) {
  return Variable{absl::StrCat("?internal_property_path_variable_", string)};
}

// Helper function to make a sorted tree distinct on all columns.
auto makeDistinct(std::shared_ptr<QueryExecutionTree> executionTree) {
  auto* qec = executionTree->getRootOperation()->getExecutionContext();
  std::vector<ColumnIndex> distinctColumns;
  distinctColumns.reserve(executionTree->getResultWidth());
  ql::ranges::copy(ad_utility::integerRange(executionTree->getResultWidth()),
                   std::back_inserter(distinctColumns));
  return ad_utility::makeExecutionTree<Distinct>(qec, std::move(executionTree),
                                                 std::move(distinctColumns));
}
}  // namespace

// _____________________________________________________________________________
std::array<std::shared_ptr<QueryExecutionTree>, 2>
TransitivePathBase::makeIndexScanPair(
    QueryExecutionContext* qec, Graphs activeGraphs, const Variable& variable,
    const std::optional<Variable>& graphVariable) {
  // Dummy variables to get a full scan of the index.
  auto a = makeInternalVariable("a");
  auto b = makeInternalVariable("b");
  auto c = makeInternalVariable("c");
  auto d = makeInternalVariable("d");
  std::set variables{variable};
  SparqlTripleSimple::AdditionalScanColumns additionalColumns;
  if (graphVariable.has_value()) {
    additionalColumns.emplace_back(ADDITIONAL_COLUMN_GRAPH_ID,
                                   graphVariable.value());
    variables.emplace(graphVariable.value());
  }
  auto stripColumns =
      [&variables](std::shared_ptr<QueryExecutionTree> executionTree) {
        return QueryExecutionTree::makeTreeWithStrippedColumns(
            std::move(executionTree), variables);
      };

  return {
      stripColumns(ad_utility::makeExecutionTree<IndexScan>(
          qec, Permutation::Enum::SPO,
          SparqlTripleSimple{TripleComponent{variable}, std::move(a),
                             TripleComponent{std::move(b)}, additionalColumns},
          activeGraphs)),
      stripColumns(ad_utility::makeExecutionTree<IndexScan>(
          qec, Permutation::Enum::OPS,
          SparqlTripleSimple{TripleComponent{std::move(c)}, std::move(d),
                             TripleComponent{variable}, additionalColumns},
          activeGraphs))};
}

// _____________________________________________________________________________
std::shared_ptr<QueryExecutionTree> TransitivePathBase::joinWithIndexScan(
    QueryExecutionContext* qec, Graphs activeGraphs,
    const std::optional<Variable>& graphVariable,
    const TripleComponent& tripleComponent) {
  // TODO<RobinTF> Once prefiltering is propagated to nested index scans, we can
  // simplify this by calling `makeEmptyPathSide` and merging this tree instead.

  auto x = makeInternalVariable("x");

  auto joinWithValues = [qec, &tripleComponent, &x](
                            std::shared_ptr<QueryExecutionTree> executionTree) {
    auto valuesClause = ad_utility::makeExecutionTree<Values>(
        qec, parsedQuery::SparqlValues{{x}, {{tripleComponent}}});
    return ad_utility::makeExecutionTree<Join>(qec, std::move(executionTree),
                                               std::move(valuesClause), 0, 0);
  };
  auto [leftScan, rightScan] =
      makeIndexScanPair(qec, std::move(activeGraphs), x, graphVariable);
  return makeDistinct(ad_utility::makeExecutionTree<Union>(
      qec, joinWithValues(std::move(leftScan)),
      joinWithValues(std::move(rightScan))));
}

// _____________________________________________________________________________
std::shared_ptr<QueryExecutionTree> TransitivePathBase::makeEmptyPathSide(
    QueryExecutionContext* qec, Graphs activeGraphs,
    const std::optional<Variable>& graphVariable,
    std::optional<Variable> variable) {
  auto [leftScan, rightScan] = makeIndexScanPair(
      qec, std::move(activeGraphs),
      std::move(variable).value_or(makeInternalVariable("x")), graphVariable);
  return makeDistinct(ad_utility::makeExecutionTree<Union>(
      qec, std::move(leftScan), std::move(rightScan)));
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
    for (Id linkedNode : linkedNodes) {
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
    }

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
    const std::shared_ptr<QueryExecutionTree>& tree,
    ColumnIndex joinColumn) const {
  auto graphCol = getActualGraphColumnIndex(tree);
  if (!graphCol.has_value() || graphCol.value() == joinColumn) {
    return 1;
  } else {
    return 2;
  }
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
    return std::make_shared<TransitivePathBinSearch>(
        qec, std::move(child), std::move(leftSide), std::move(rightSide),
        minDist, maxDist, std::move(activeGraphs), graphVariable);
  } else {
    return std::make_shared<TransitivePathHashMap>(
        qec, std::move(child), std::move(leftSide), std::move(rightSide),
        minDist, maxDist, std::move(activeGraphs), graphVariable);
  }
}

// _____________________________________________________________________________
std::vector<QueryExecutionTree*> TransitivePathBase::getChildren() {
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
std::shared_ptr<QueryExecutionTree> TransitivePathBase::matchWithKnowledgeGraph(
    size_t& inputCol, std::shared_ptr<QueryExecutionTree> leftOrRightOp) const {
  auto [originalVar, info] =
      leftOrRightOp->getVariableAndInfoByColumnIndex(inputCol);

  // If we don't include the empty path, then inputs which don't originate in
  // the graph will be automatically filtered out because they cannot appear in
  // the `subtree_`.
  if (minDist_ > 0) {
    return leftOrRightOp;
  }

  if (graphVariable_.has_value()) {
    // Join with the starting side of a clone of the subtree to get the proper
    // graph values.
    if (!leftOrRightOp->getVariableColumnOrNullopt(graphVariable_.value())
             .has_value()) {
      auto completeScan = makeEmptyPathSide(
          getExecutionContext(), activeGraphs_, graphVariable_, originalVar);
      leftOrRightOp = ad_utility::makeExecutionTree<Join>(
          getExecutionContext(), std::move(leftOrRightOp), completeScan,
          inputCol, 0);
      inputCol = leftOrRightOp->getVariableColumn(originalVar);
    }

    AD_CORRECTNESS_CHECK(
        leftOrRightOp->getVariableColumnOrNullopt(graphVariable_.value())
            .has_value());
  }

  bool graphIsJoin = originalVar == graphVariable_;

  // If we cannot guarantee the values are part of the graph, we have to join
  // with it first.
  if (!leftOrRightOp->getRootOperation()->columnOriginatesFromGraphOrUndef(
          originalVar)) {
    auto completeScan = makeEmptyPathSide(
        getExecutionContext(), activeGraphs_,
        graphIsJoin ? internalGraphHelper_ : graphVariable_, originalVar);
    if (graphVariable_.has_value() && !graphIsJoin) {
      leftOrRightOp = ad_utility::makeExecutionTree<MultiColumnJoin>(
          getExecutionContext(), std::move(leftOrRightOp),
          std::move(completeScan));
    } else {
      leftOrRightOp = ad_utility::makeExecutionTree<Join>(
          getExecutionContext(), std::move(leftOrRightOp),
          std::move(completeScan), inputCol, 0);
    }
    inputCol = leftOrRightOp->getVariableColumn(originalVar);
  } else if (graphIsJoin) {
    // If the join column is a subject or object anywhere in the graph, we still
    // don't know for sure if it is also a valid graph name. Hence, we need to
    // join it with actual graphs. To get actual matching graph ids if they
    // exist. We don't need a filter here, because
    // `TransitivePathImpl::transitiveHull` already does the comparison.
    auto completeScan = makeEmptyPathSide(getExecutionContext(), activeGraphs_,
                                          internalGraphHelper_, originalVar);
    leftOrRightOp = ad_utility::makeExecutionTree<Join>(
        getExecutionContext(), std::move(leftOrRightOp),
        std::move(completeScan), inputCol, 0);
    inputCol = leftOrRightOp->getVariableColumn(originalVar);
  }
  return leftOrRightOp;
}

// _____________________________________________________________________________
std::shared_ptr<TransitivePathBase> TransitivePathBase::bindLeftOrRightSide(
    std::shared_ptr<QueryExecutionTree> leftOrRightOp, size_t inputCol,
    bool isLeft) const {
  leftOrRightOp = matchWithKnowledgeGraph(inputCol, std::move(leftOrRightOp));
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
    // Remove placeholder tree if binding actual tree.
    if (!rhs.isVariable()) {
      rhs.treeAndCol_ = std::nullopt;
    }
  } else {
    // Remove placeholder tree if binding actual tree.
    if (boundVariableIsForEmptyPath_ || !lhs.isVariable()) {
      lhs.treeAndCol_ = std::nullopt;
    }
    rhs.treeAndCol_ = {leftOrRightOp, inputCol};
  }

  // We use the cheapest tree that can be created using any of the alternative
  // subtrees. This has the effect that the `TransitivePathBinSearch` will
  // never re-sort an index scan (which should not happen because we can just
  // take the appropriate index scan in the first place).
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

  auto& p = *ql::ranges::min_element(
      candidates, {}, [](const auto& tree) { return tree->getCostEstimate(); });

  // Note: The `variable` in the following structured binding is `const`, even
  // if we bind by value. We deliberately make one unnecessary copy of the
  // `variable` to keep the code simpler.
  for (auto [variable, columnIndexWithType] :
       leftOrRightOp->getVariableColumns()) {
    ColumnIndex columnIndex = columnIndexWithType.columnIndex_;
    if (columnIndex == inputCol || variable == graphVariable_) {
      continue;
    }

    columnIndexWithType.columnIndex_ += columnIndex > inputCol ? 1 : 2;

    // When we have a graph variable, we write it last, so we have to account
    // for that.
    if (graphVariable_.has_value()) {
      auto optGraphIndex =
          leftOrRightOp->getVariableColumnOrNullopt(graphVariable_.value());
      if (columnIndex >
          optGraphIndex.value_or(std::numeric_limits<size_t>::max())) {
        columnIndexWithType.columnIndex_ -= 1;
      }
    }

    AD_CORRECTNESS_CHECK(!p->variableColumns_.contains(variable));
    p->variableColumns_[variable] = columnIndexWithType;
  }
  p->resultWidth_ += leftOrRightOp->getResultWidth() -
                     numJoinColumnsWith(leftOrRightOp, inputCol);
  // Make sure mapping actually points to the last column if it's not one of the
  // regular variables.
  if (graphVariable_.has_value()) {
    auto& graphIndex = p->variableColumns_[graphVariable_.value()].columnIndex_;
    if (graphIndex == 2) {
      graphIndex = p->resultWidth_ - 1;
    }
  }
  return std::move(p);
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
  // The first two columns are both sides of the transitive path, then they are
  // followed by the payload columns (if present) and then the (optional) graph
  // column follows (but it is not written in this function).
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
  return variable == lhs_.value_ || variable == rhs_.value_;
}

// _____________________________________________________________________________
// Don't check the name because this leads to segfaults during static
// initialization.
const Variable TransitivePathBase::internalGraphHelper_{
    "?_Qlever_internal_transitive_path_graph", false};

#endif
