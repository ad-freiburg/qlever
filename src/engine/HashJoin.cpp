// Copyright 2026, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Mark Veser (veser@informatik.uni-freiburg.de)

#include "engine/HashJoin.h"

#include <sstream>
#include <vector>

#include "JoinWithIndexScanHelpers.h"
#include "backports/functional.h"
#include "backports/type_traits.h"
#include "engine/AddCombinedRowToTable.h"
#include "engine/CallFixedSize.h"
#include "engine/IndexScan.h"
#include "engine/JoinHelpers.h"
#include "engine/OperationBindPushDownImpl.h"
#include "engine/Service.h"
#include "global/Constants.h"
#include "global/Id.h"
#include "global/RuntimeParameters.h"
#include "util/Algorithm.h"
#include "util/Exception.h"
#include "util/Generators.h"
#include "util/HashMap.h"
#include "util/Iterators.h"
#include "util/JoinAlgorithms/JoinAlgorithms.h"

using namespace qlever::joinHelpers;
using namespace qlever::joinWithIndexScanHelpers;
using std::endl;
using std::string;

// _____________________________________________________________________________
HashJoin::HashJoin(QueryExecutionContext* qec, std::shared_ptr<QueryExecutionTree> t1,
           std::shared_ptr<QueryExecutionTree> t2, ColumnIndex t1JoinCol,
           ColumnIndex t2JoinCol, bool keepJoinColumn,
           bool allowSwappingChildrenOnlyForTesting)
    : Operation(qec), keepJoinColumn_{keepJoinColumn} {
  AD_CONTRACT_CHECK(t1 && t2);

  // Make the order of the two subtrees deterministic. That way, queries that
  // are identical except for the order of the join operands, are easier to
  // identify.
  auto swapChildren = [&]() {
    // This can be disabled by tests to fix the order of the subtrees.
    if (allowSwappingChildrenOnlyForTesting) {
      std::swap(t1, t2);
      std::swap(t1JoinCol, t2JoinCol);
    }
  };
  if (t1->getCacheKey() > t2->getCacheKey()) {
    swapChildren();
  }
  // If one of the inputs is a SCAN and the other one is not, always make the
  // SCAN the right child (which also gives a deterministic order of the
  // subtrees). This simplifies several branches in the `computeResult` method.
  if (std::dynamic_pointer_cast<IndexScan>(t1->getRootOperation()) &&
      !std::dynamic_pointer_cast<IndexScan>(t2->getRootOperation())) {
    swapChildren();
  }
  left_ = std::move(t1);
  leftJoinCol_ = t1JoinCol;
  right_ = std::move(t2);
  rightJoinCol_ = t2JoinCol;
  sizeEstimate_ = 0;
  sizeEstimateComputed_ = false;
  multiplicities_.clear();
  auto findJoinVar = [](const QueryExecutionTree& tree,
                        ColumnIndex joinCol) -> Variable {
    return tree.getVariableAndInfoByColumnIndex(joinCol).first;
  };
  joinVar_ = findJoinVar(*left_, leftJoinCol_);
  AD_CONTRACT_CHECK(joinVar_ == findJoinVar(*right_, rightJoinCol_));
}

// _____________________________________________________________________________
string HashJoin::getCacheKeyImpl() const {
  std::ostringstream os;
  os << "JOIN\n"
     << left_->getCacheKey() << " join-column: [" << leftJoinCol_ << "]\n";
  os << "|X|\n"
     << right_->getCacheKey() << " join-column: [" << rightJoinCol_ << "]";
  os << "keep join Col " << keepJoinColumn_;
  return std::move(os).str();
}

// _____________________________________________________________________________
string HashJoin::getDescriptor() const { return "HashJoin on " + joinVar_.name(); }

// _____________________________________________________________________________
Result HashJoin::computeResult(bool requestLaziness) {
  AD_LOG_DEBUG << "Getting sub-results for join result computation..." << endl;
  if (left_->knownEmptyResult() || right_->knownEmptyResult()) {
    left_->getRootOperation()->updateRuntimeInformationWhenOptimizedOut();
    right_->getRootOperation()->updateRuntimeInformationWhenOptimizedOut();
    return createEmptyResult();
  }

  // TODO: consider different cases (IndexScan, Service, etc.). Respectively
  // should probably be considered in the cost estimation and not end up here.
  auto leftResult = left_->getResult(requestLaziness);
  auto rightResult = right_->getResult(requestLaziness);

  auto leftIsSmaller = left_->getSizeEstimate() <= right_->getSizeEstimate();
  return hashJoin(std::move(leftResult), std::move(rightResult), leftIsSmaller);
}

// _____________________________________________________________________________
Result HashJoin::hashJoin(std::shared_ptr<const Result> left,
                          std::shared_ptr<const Result> right,
                          bool leftIsSmaller) {
  // Build Map from smaller Input. I want to keep left and right as they are
  // because in the resulting Table left input should come first.
  const auto& small = leftIsSmaller ? left : right;
  const auto& large = leftIsSmaller ? right : left;
  auto jcSmall = leftIsSmaller ? leftJoinCol_ : rightJoinCol_;
  auto jcLarge = leftIsSmaller ? rightJoinCol_ : leftJoinCol_;
  // Helper function for hash map creation
  ad_utility::HashMap<Id, std::vector<IdTable::row_type>> map;
  LocalVocab localVocab;
  auto buildHashMap = [&map, jcSmall](const auto& table) {
    for (const auto& row : table) {
      map[row[jcSmall]].push_back(row);
    }
  };
  // Build Hash Map using the smaller result.
  // both fully materialized and lazy tables are supported
  if (small->isFullyMaterialized()) {
    buildHashMap(small->idTable());
    localVocab.mergeWith(small->localVocab());
  } else {
    for (const auto& table : small->idTables()) {
      buildHashMap(table.idTable_);
      localVocab.mergeWith(table.localVocab_);
    }
  }
  // Probing phase: check for matching rows in the larger table
  IdTable result{getResultWidth(), allocator()};
  auto probeRow = [this, &map, jcLarge, jcSmall, leftIsSmaller,
                   &result](const auto& table) {
    for (const auto& row : table) {
      auto entry = map.find(row[jcLarge]);
      if (entry != map.end()) {
        for (const auto& rowSmall : entry->second) {
          // the order of the resulting table has to be in the following order:
          // left table + right table. Since the right table could be the smaller one
          // we have to cover both cases and make sure the original left side remains
          // on the left side.
          if (leftIsSmaller) {
            addCombinedRowToIdTable(rowSmall, row, jcSmall, jcLarge, keepJoinColumn_, &result);
          } else {
            addCombinedRowToIdTable(row, rowSmall, jcLarge, jcSmall, keepJoinColumn_, &result);
          }
        }
      }
    }
  };
  // both fully materialized and lazy tables are supported
  if (large->isFullyMaterialized()) {
    probeRow(large->idTable());
    localVocab.mergeWith(large->localVocab());
  } else {
    for (const auto& table : large->idTables()) {
      probeRow(table.idTable_);
      localVocab.mergeWith(table.localVocab_);
    }
  }
  return Result{std::move(result), resultSortedOn(),
                std::move(localVocab)};
}

// _____________________________________________________________________________
VariableToColumnMap HashJoin::computeVariableToColumnMap() const {
  return makeVarToColMapForJoinOperation(
      left_->getVariableColumns(), right_->getVariableColumns(),
      {{leftJoinCol_, rightJoinCol_}}, BinOpType::Join, left_->getResultWidth(),
      keepJoinColumn_);
}

// _____________________________________________________________________________
size_t HashJoin::getResultWidth() const {
  auto sumOfChildWidths = left_->getResultWidth() + right_->getResultWidth();
  AD_CORRECTNESS_CHECK(sumOfChildWidths >= 2);
  return sumOfChildWidths - 1 - static_cast<size_t>(!keepJoinColumn_);
}

// _____________________________________________________________________________
std::vector<ColumnIndex> HashJoin::resultSortedOn() const {
  // TODO: Needs to be consider if smaller side is sorted
  return {};
}

// _____________________________________________________________________________
float HashJoin::getMultiplicity(size_t col) {
  if (multiplicities_.empty()) {
    computeSizeEstimateAndMultiplicities();
    sizeEstimateComputed_ = true;
  }
  return multiplicities_.at(col);
}

// _____________________________________________________________________________
size_t HashJoin::getCostEstimate() {
  // 1. Fetch necessary information
  // Set cost estimate to infinity if hash join is not supported due to:
  // compare estimated size of hash map with available memory.
  auto leftSize = left_->getSizeEstimate();
  auto rightSize = right_->getSizeEstimate();
  auto leftCost = left_->getCostEstimate();
  auto rightCost = right_->getCostEstimate();
  bool leftIsSmaller = leftSize <= rightSize;
  size_t buildSize = leftIsSmaller ? leftSize : rightSize;
  size_t numCols = leftIsSmaller ? 
                   left_->getResultWidth() : right_->getResultWidth();
  ad_utility::MemorySize estimatedHashMapSize = 
    ad_utility::MemorySize::bytes(buildSize * numCols * sizeof(Id));

  // Max value to return for excludes.
  // We want to return a value, that is bigger than any possible combination
  // of Sort + Join of children, but not bigger than `std::numeric_limits<size_t>::max()`, because then we can get an
  // overflow.
  auto calculateMaxSize = [this, leftSize, rightSize, leftCost, rightCost]() {
    size_t sortCost = leftSize * log(leftSize) + rightSize * log(rightSize);
    size_t joinCost = getSizeEstimateBeforeLimit() + leftCost + rightCost;
    return sortCost + joinCost + 1;
  };
  size_t maxSize = calculateMaxSize();

  // 2. Excludes
  if (estimatedHashMapSize > allocator().amountMemoryLeft()) {
    return maxSize;
  }

  // If both sides are sorted on the join column, prefer the merge join (Join
  // class) which can exploit the sortedness for free.
  auto isSortedOnJoinCol = [](const auto& tree, ColumnIndex joinCol) {
    auto sorted = tree->resultSortedOn();
    return !sorted.empty() && sorted[0] == joinCol;
  };
  if (isSortedOnJoinCol(left_, leftJoinCol_) &&
      isSortedOnJoinCol(right_, rightJoinCol_)) {
    return maxSize;
  }

  // If the bigger side is already sorted on the join column, a merge join is
  // more efficient than hashing.
  auto biggerResultSortedOn =
      leftIsSmaller ? right_->resultSortedOn() : left_->resultSortedOn();
  if (!biggerResultSortedOn.empty()) {
    auto biggerJoinCol = leftIsSmaller ? rightJoinCol_ : leftJoinCol_;
    if (biggerResultSortedOn[0] == biggerJoinCol) {
      return maxSize;
    }
  }

  // Exclude if both sides are IndexScans
  if (std::dynamic_pointer_cast<IndexScan>(left_->getRootOperation()) &&
      std::dynamic_pointer_cast<IndexScan>(right_->getRootOperation())) {
    return maxSize;
  }

  // 3. Actual calculation of cost estimate
  // Build HashMap + Probe HashMap + Cost of Subtrees
  size_t costBuild = leftSize;
  size_t costProbe = rightSize;
  size_t costSubtrees = left_->getCostEstimate() + right_->getCostEstimate();
  return costBuild + costProbe + costSubtrees;
}

// _____________________________________________________________________________
void HashJoin::computeSizeEstimateAndMultiplicities() {
  multiplicities_.clear();
  if (left_->getSizeEstimate() == 0 || right_->getSizeEstimate() == 0) {
    for (size_t i = 0; i < getResultWidth(); ++i) {
      multiplicities_.emplace_back(1);
    }
    return;
  }

  // nof := number of
  size_t nofDistinctLeft = std::max(
      size_t(1), static_cast<size_t>(left_->getSizeEstimate() /
                                     left_->getMultiplicity(leftJoinCol_)));
  size_t nofDistinctRight = std::max(
      size_t(1), static_cast<size_t>(right_->getSizeEstimate() /
                                     right_->getMultiplicity(rightJoinCol_)));

  size_t nofDistinctInResult = std::min(nofDistinctLeft, nofDistinctRight);

  double adaptSizeLeft =
      left_->getSizeEstimate() *
      (static_cast<double>(nofDistinctInResult) / nofDistinctLeft);
  double adaptSizeRight =
      right_->getSizeEstimate() *
      (static_cast<double>(nofDistinctInResult) / nofDistinctRight);

  double corrFactor = _executionContext
                          ? _executionContext->getCostFactor(
                                "JOIN_SIZE_ESTIMATE_CORRECTION_FACTOR")
                          : 1.0;

  double jcMultiplicityInResult = left_->getMultiplicity(leftJoinCol_) *
                                  right_->getMultiplicity(rightJoinCol_);
  sizeEstimate_ = std::max(
      size_t(1), static_cast<size_t>(corrFactor * jcMultiplicityInResult *
                                     nofDistinctInResult));

  AD_LOG_TRACE << "Estimated size as: " << sizeEstimate_ << " := " << corrFactor
               << " * " << jcMultiplicityInResult << " * "
               << nofDistinctInResult << std::endl;

  for (auto i = ColumnIndex{0}; i < left_->getResultWidth(); ++i) {
    double oldMult = left_->getMultiplicity(i);
    double m = std::max(
        1.0, oldMult * right_->getMultiplicity(rightJoinCol_) * corrFactor);
    if (i != leftJoinCol_ && nofDistinctLeft != nofDistinctInResult) {
      double oldDist = left_->getSizeEstimate() / oldMult;
      double newDist = std::min(oldDist, adaptSizeLeft);
      m = (sizeEstimate_ / corrFactor) / newDist;
    }
    if (i != leftJoinCol_ || keepJoinColumn_) {
      multiplicities_.emplace_back(m);
    }
  }
  for (auto i = ColumnIndex{0}; i < right_->getResultWidth(); ++i) {
    if (i == rightJoinCol_) {
      continue;
    }
    double oldMult = right_->getMultiplicity(i);
    double m = std::max(
        1.0, oldMult * left_->getMultiplicity(leftJoinCol_) * corrFactor);
    if (i != rightJoinCol_ && nofDistinctRight != nofDistinctInResult) {
      double oldDist = right_->getSizeEstimate() / oldMult;
      double newDist = std::min(oldDist, adaptSizeRight);
      m = (sizeEstimate_ / corrFactor) / newDist;
    }
    multiplicities_.emplace_back(m);
  }
  assert(multiplicities_.size() == getResultWidth());
}


// ___________________________________________________________________________
template <typename ROW_A, typename ROW_B, int TABLE_WIDTH>
void HashJoin::addCombinedRowToIdTable(const ROW_A& rowA, const ROW_B& rowB,
                                   ColumnIndex jcRowA, ColumnIndex jcRowB,
                                   bool keepJoinColumn,
                                   IdTableStatic<TABLE_WIDTH>* table) {
  // Add a new, empty row.
  const size_t backIndex = table->size();
  table->emplace_back();
  ColumnIndex resultCol = 0;

  // Copy the entire rowA in the table.
  for (auto h = ColumnIndex{0}; h < rowA.numColumns(); h++) {
    if (!keepJoinColumn && h == jcRowA) continue;
    (*table)(backIndex, resultCol++) = rowA[h];
  }
  // Copy rowB except the join column.
  for (auto h = ColumnIndex{0}; h < rowB.numColumns(); h++) {
    if (h == jcRowB) continue;
    (*table)(backIndex, resultCol++) = rowB[h];
  }
}

// _____________________________________________________________________________
Result HashJoin::createEmptyResult() const {
  return {IdTable{getResultWidth(), allocator()}, resultSortedOn(),
          LocalVocab{}};
}

// _____________________________________________________________________________
ad_utility::JoinColumnMapping HashJoin::getJoinColumnMapping() const {
  return ad_utility::JoinColumnMapping{{{leftJoinCol_, rightJoinCol_}},
                                       left_->getResultWidth(),
                                       right_->getResultWidth(),
                                       keepJoinColumn_};
}

// _____________________________________________________________________________
ad_utility::AddCombinedRowToIdTable HashJoin::makeRowAdder(
    std::function<void(IdTable&, LocalVocab&)> callback) const {
  return ad_utility::AddCombinedRowToIdTable{
      1,
      IdTable{getResultWidth(), allocator()},
      cancellationHandle_,
      keepJoinColumn_,
      CHUNK_SIZE,
      std::move(callback)};
}

// _____________________________________________________________________________
std::unique_ptr<Operation> HashJoin::cloneImpl() const {
  auto copy = std::make_unique<HashJoin>(*this);
  copy->left_ = left_->clone();
  copy->right_ = right_->clone();
  return copy;
}

// _____________________________________________________________________________
bool HashJoin::columnOriginatesFromGraphOrUndef(const Variable& variable) const {
  AD_CONTRACT_CHECK(getExternallyVisibleVariableColumns().contains(variable));
  // For the join column we don't union the elements, we intersect them so we
  // can have a more efficient implementation.
  if (variable == joinVar_) {
    return doesJoinProduceGuaranteedGraphValuesOrUndef(left_, right_, variable);
  }
  return Operation::columnOriginatesFromGraphOrUndef(variable);
}

// _____________________________________________________________________________
std::optional<std::shared_ptr<QueryExecutionTree>>
HashJoin::makeTreeWithStrippedColumns(const std::set<Variable>& variables) const {
  std::set<Variable> newVariables;
  const auto* vars = &variables;
  if (!ad_utility::contains(variables, joinVar_)) {
    newVariables = variables;
    newVariables.insert(joinVar_);
    vars = &newVariables;
  }

  // TODO<joka921> Code duplication including a former copy-paste bug.
  auto left = QueryExecutionTree::makeTreeWithStrippedColumns(left_, *vars);
  auto right = QueryExecutionTree::makeTreeWithStrippedColumns(right_, *vars);
  auto leftCol = left->getVariableColumn(joinVar_);
  auto rightCol = right->getVariableColumn(joinVar_);
  return ad_utility::makeExecutionTree<HashJoin>(
      getExecutionContext(), std::move(left), std::move(right), leftCol,
      rightCol, ad_utility::contains(variables, joinVar_));
}

// _____________________________________________________________________________
std::optional<std::shared_ptr<QueryExecutionTree>> HashJoin::makeTreeWithBindColumn(
    const parsedQuery::Bind& bind) const {
  return pushDownBindToAnyChild(
      bind, {left_, right_},
      [this](std::vector<std::shared_ptr<QueryExecutionTree>> newChildren) {
        auto& left = newChildren.at(0);
        auto& right = newChildren.at(1);
        auto leftCol = left->getVariableColumn(joinVar_);
        auto rightCol = right->getVariableColumn(joinVar_);
        return ad_utility::makeExecutionTree<HashJoin>(
            getExecutionContext(), std::move(left), std::move(right), leftCol,
            rightCol);
      });
}
