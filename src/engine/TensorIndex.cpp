// Copyright 2026, Technical University of Graz, University of Padova
// Institute for Visual Computing, Department of Information Engineering
// Authors: Benedikt Kantz <benedikt.kantz@tugraz.at>

#include <absl/container/flat_hash_set.h>
#include <absl/strings/charconv.h>

#include <cstdint>
#include <limits>
#include <optional>
#include <queue>
#include <tuple>
#include <unordered_set>
#include <variant>

#include "backports/type_traits.h"
#include "engine/ExportQueryExecutionTrees.h"
#include "engine/NamedResultCache.h"
#include "engine/TensorIndex.h"
#include "engine/TensorIndexConfig.h"
#include "engine/VariableToColumnMap.h"
#include "engine/idTable/IdTable.h"
#include "global/Constants.h"
#include "global/ValueId.h"
#include "parser/ParsedQuery.h"
#include "util/AllocatorWithLimit.h"
#include "util/Exception.h"
#include "util/MemorySize/MemorySize.h"
// ____________________________________________________________________________
TensorIndex::TensorIndex(
    QueryExecutionContext* qec, TensorIndexConfiguration config,
    std::optional<std::shared_ptr<QueryExecutionTree>> childLeft,
    std::optional<std::shared_ptr<QueryExecutionTree>> childRight,
    bool substitutesFilterOp)
    : Operation(qec),
      config_{std::move(config)},
      substitutesFilterOp_{substitutesFilterOp} {
  if (childLeft.has_value()) {
    childLeft_ = std::move(childLeft.value());
  }
  if (childRight.has_value()) {
    childRight_ = std::move(childRight.value());
  }
}

// ____________________________________________________________________________
std::shared_ptr<TensorIndex> TensorIndex::addChild(
    std::shared_ptr<QueryExecutionTree> child,
    const Variable& varOfChild) const {
  std::shared_ptr<TensorIndex> sj;
  if (varOfChild == config_.left_) {
    sj = std::make_shared<TensorIndex>(getExecutionContext(), config_,
                                        std::move(child), childRight_,
                                        substitutesFilterOp_);
  } else if (varOfChild == config_.right_) {
    sj = std::make_shared<TensorIndex>(getExecutionContext(), config_,
                                        childLeft_, std::move(child),
                                        substitutesFilterOp_);
  } else {
    AD_THROW("variable does not match");
  }

  // The new tensor search after adding a child needs to inherit the warnings of
  // its predecessor.
  auto warnings = getWarnings().rlock();
  for (const auto& warning : *warnings) {
    sj->addWarning(warning);
  }

  return sj;
}

// ____________________________________________________________________________
bool TensorIndex::isConstructed() const { return childLeft_ && childRight_; }

// ____________________________________________________________________________
std::optional<size_t> TensorIndex::getMaxResults() const {
  return config_.maxResults_;
}

// ____________________________________________________________________________
std::vector<QueryExecutionTree*> TensorIndex::getChildren() {
  std::vector<QueryExecutionTree*> result;
  auto addChild = [&](std::shared_ptr<QueryExecutionTree> child) {
    if (child) {
      result.push_back(child.get());
    }
  };
  addChild(childLeft_);
  addChild(childRight_);
  return result;
}

// ____________________________________________________________________________
std::string TensorIndex::getCacheKeyImpl() const {
  if (childLeft_ && childRight_) {
    std::ostringstream os;
    // This includes all attributes that change the result

    // Children
    os << "TensorIndex\nChild1:\n" << childLeft_->getCacheKey() << "\n";
    os << "Child2:\n" << childRight_->getCacheKey() << "\n";

    // Join Variables
    os << "JoinColLeft:" << childLeft_->getVariableColumn(config_.left_);
    os << "JoinColRight:" << childRight_->getVariableColumn(config_.right_);

    auto maxResults = getMaxResults();

    os << "Max Results: " << (int)maxResults.value_or(-1) << "\n";
    os << "kTrees" << (int)config_.searchK_.value_or(-1) << "\n";
    os << "kIVF" << (int)config_.kIVF_.value_or(-1) << "\n";

    os << "TensorIndex in distance: " << (int)config_.algo_ << "with algorithm"
       << (int)config_.dist_ << "\n";

    // Uses distance variable?
    if (config_.distanceVariable_.has_value()) {
      os << "withDistanceVariable \n";
    }

    // Selected payload variables
    os << "payload:";
    auto rightVarColFiltered =
        copySortedByColumnIndex(getVarColMapPayloadVars());
    for (const auto& [var, info] : rightVarColFiltered) {
      os << " " << info.columnIndex_;
    }
    os << "\n";

    // If we use the s2-point-polyline algorithm, we also need to add the cache
    // entry name to our own cache key.
    if (config_.rightCacheName_.has_value()) {
      os << "right cache name:" << config_.rightCacheName_.value() << "\n";
    }

    // Algorithm is not included here because it should not have any impact on
    // the result.
    return std::move(os).str();
  } else {
    return "incomplete TensorIndex class";
  }
}

// ____________________________________________________________________________
std::string TensorIndex::getDescriptor() const {
  auto left = config_.left_.name();
  auto right = config_.right_.name();
  auto algo = getAlgorithm();
  auto algorithmString = "";
  switch (algo) {
    case TensorIndexAlgorithm::NAIVE:
      algorithmString = "naive";
      break;
    case TensorIndexAlgorithm::FAISS_HSNW:
      algorithmString = "hsnw";
      break;
    case TensorIndexAlgorithm::FAISS_IVF:
      algorithmString = "ivf";
      break;
  }
  auto distanceString = [this]() {
    switch (config_.dist_) {
      case TensorDistanceAlgorithm::DOT_PRODUCT:
        return "dot product";
      case TensorDistanceAlgorithm::COSINE_SIMILARITY:
        return "cosine similarity";
      case TensorDistanceAlgorithm::ANGULAR_DISTANCE:
        return "angular distance";
      case TensorDistanceAlgorithm::EUCLIDEAN_DISTANCE:
        return "euclidean distance";
      case TensorDistanceAlgorithm::MANHATTAN_DISTANCE:
        return "manhattan distance";
      case TensorDistanceAlgorithm::HAMMING_DISTANCE:
        return "hamming distance";
      default:
        AD_FAIL();
    }
  }();

  return absl::StrCat("Nearest Neighbour Tensor Search of ", left, " to ",
                      right, " of max. ", getMaxResults().value_or(0),
                      " using ", algorithmString, " and ", distanceString);
}

// ____________________________________________________________________________
size_t TensorIndex::getResultWidth() const {
  if (childLeft_ && childRight_) {
    // don't subtract anything because of a common join column. In the case
    // of the tensor search, the join column is different for both sides (e.g.
    // objects with a distance of at most 500m to each other, then each object
    // might contain different positions, which should be kept).
    // For the right join table we only use the selected columns.
    size_t sizeRight;
    if (config_.payloadVariables_.isAll()) {
      sizeRight = childRight_->getResultWidth();
    } else {
      // Derive the actual set of columns we will include from the right child
      // by relying on `getVarColMapPayloadVars()` which filters missing
      // variables and always includes the join column. This avoids a
      // mismatch between the configured payload variables and the actual
      // available columns that can lead to out_of_range accesses later.
      auto varColMapRightFiltered = getVarColMapPayloadVars();
      sizeRight = varColMapRightFiltered.size();
    }
    auto widthChildren = childLeft_->getResultWidth() + sizeRight;

    if (config_.distanceVariable_.has_value()) {
      return widthChildren + 1;
    } else {
      return widthChildren;
    }
  } else if (childLeft_ || childRight_) {
    // if only one of the children is added yet, the "dummy result table" only
    // consists of one result column, the not yet added variable
    return 1;
  } else {
    // if none of the children has been added yet, the "dummy result table"
    // consists of two columns: the variables, which need to be added
    return 2;
  }
}

// ____________________________________________________________________________
size_t TensorIndex::getCostEstimate() {
  using enum TensorIndexAlgorithm;
  if (!childLeft_ || !childRight_) {
    return 1;  // dummy return, as the class does not have its children yet
  }

  size_t TensorIndexCostEst = [this]() {
    auto n = childLeft_->getSizeEstimate();
    auto m = childRight_->getSizeEstimate();
    switch (config_.algo_) {
      case NAIVE:
        return n * m;
      case FAISS_IVF:
      case FAISS_HSNW: {
        auto logn = n > 0
                        ? static_cast<size_t>(std::sqrt(static_cast<double>(n)))
                        : size_t{1};
        return logn * m;
      }
    }
    AD_FAIL();
  }();

  // The cost to compute the children needs to be taken into account.
  return TensorIndexCostEst + childLeft_->getCostEstimate() +
         childRight_->getCostEstimate();
}

// ____________________________________________________________________________
uint64_t TensorIndex::getSizeEstimateBeforeLimit() {
  if (childLeft_ && childRight_) {
    // If we limit the number of results to k, even in the largest scenario, the
    // result can be at most `|childLeft| * k`
    auto maxResults = getMaxResults();
    if (maxResults.has_value()) {
      return childLeft_->getSizeEstimate() * maxResults.value();
    }

    // If we don't limit the number of results, we cannot draw conclusions about
    // the size, other than the worst case `|childLeft| * |childRight|`.
    return (childLeft_->getSizeEstimate() * childRight_->getSizeEstimate());
  }
  return 1;  // dummy return if not both children are added
}

// ____________________________________________________________________________
float TensorIndex::getMultiplicity(size_t col) {
  auto getDistinctness = [](std::shared_ptr<QueryExecutionTree> child,
                            ColumnIndex ind) {
    auto size = (u_int)child->getSizeEstimate();
    auto multiplicity = child->getMultiplicity(ind);
    return static_cast<float>(size) / multiplicity;
  };

  if (col >= getResultWidth()) {
    AD_FAIL();
  }

  if (childLeft_ && childRight_) {
    std::shared_ptr<QueryExecutionTree> child;
    size_t column = col;
    if (config_.distanceVariable_.has_value() && col == getResultWidth() - 1) {
      // as each max results value is very likely to be unique, no
      // multiplicities are assumed
      return 1;
    } else if (col < childLeft_->getResultWidth()) {
      child = childLeft_;
    } else {
      child = childRight_;
      // Since we only select the payload variables into the result and
      // potentially omit some columns from the right child, we need to
      // translate the column index on the tensor search to a column index in
      // the right child.
      auto filteredColumns = copySortedByColumnIndex(getVarColMapPayloadVars());
      column = filteredColumns.at(column - childLeft_->getResultWidth())
                   .second.columnIndex_;
    }
    auto distinctnessChild = getDistinctness(child, column);
    return static_cast<float>(childLeft_->getSizeEstimate() *
                              childRight_->getSizeEstimate()) /
           distinctnessChild;
  } else {
    return 1;
  }
}

// ____________________________________________________________________________
bool TensorIndex::knownEmptyResult() {
  // return false if either both children don't have an empty result, only one
  // child is available, which doesn't have a known empty result or neither
  // child is available
  return (childLeft_ && childLeft_->knownEmptyResult()) ||
         (childRight_ && childRight_->knownEmptyResult());
}

// ____________________________________________________________________________
std::vector<ColumnIndex> TensorIndex::resultSortedOn() const {
  // the baseline (with O(n^2) runtime) can have some sorted columns, but as
  // the "true" computeResult method will use bounding boxes, which can't
  // guarantee that a sorted column stays sorted, this will return no sorted
  // column in all cases.
  return {};
}

// ____________________________________________________________________________
VariableToColumnMap TensorIndex::getVarColMapPayloadVars() const {
  AD_CONTRACT_CHECK(childRight_,
                    "Child right must be added before payload variable to "
                    "column map can be computed.");

  const auto& varColMap = childRight_->getVariableColumns();
  VariableToColumnMap newVarColMap;

  if (config_.payloadVariables_.isAll()) {
    newVarColMap = VariableToColumnMap{varColMap};
  } else {
    for (const auto& var : config_.payloadVariables_.getVariables()) {
      if (varColMap.contains(var)) {
        newVarColMap.insert_or_assign(var, varColMap.at(var));
      } else {
        addWarningOrThrow(
            absl::StrCat("Variable '", var.name(),
                         "' selected as payload to tensor search but not "
                         "present in right child."));
      }
    }
    AD_CONTRACT_CHECK(
        varColMap.contains(config_.right_),
        "Right variable is not defined in right child of tensor search.");
    newVarColMap.insert_or_assign(config_.right_, varColMap.at(config_.right_));
  }

  return newVarColMap;
}

// ____________________________________________________________________________
PreparedTensorIndexParams TensorIndex::prepareJoin() const {
  auto getIdTable = [](std::shared_ptr<QueryExecutionTree> child) {
    std::shared_ptr<const Result> resTable = child->getResult();
    auto idTablePtr = &resTable->idTable();
    return std::pair{idTablePtr, std::move(resTable)};
  };

  // Input tables
  auto [idTableLeft, resultLeft] = getIdTable(childLeft_);
  auto [idTableRight, resultRight] = getIdTable(childRight_);

  // Input table columns for the join
  ColumnIndex leftJoinCol = childLeft_->getVariableColumn(config_.left_);
  ColumnIndex rightJoinCol = childRight_->getVariableColumn(config_.right_);

  // Payload cols and join col
  auto varsAndColInfo = copySortedByColumnIndex(getVarColMapPayloadVars());
  std::vector<ColumnIndex> rightSelectedCols;
  for (const auto& [var, colInfo] : varsAndColInfo) {
    rightSelectedCols.push_back(colInfo.columnIndex_);
  }

  // Size of output table
  size_t numColumns = getResultWidth();

  auto cacheKey = config_.rightCacheName_.value_or(getCacheKey());
  return PreparedTensorIndexParams{idTableLeft,       std::move(resultLeft),
                                    idTableRight,      std::move(resultRight),
                                    leftJoinCol,       rightJoinCol,
                                    rightSelectedCols, cacheKey,
                                    numColumns,        config_};
}

// ____________________________________________________________________________
Result TensorIndex::computeResult([[maybe_unused]] bool requestLaziness) {
  auto params = prepareJoin();
  auto searchImpl = TensorIndexImpl(params, getExecutionContext());
  return searchImpl.computeTensorIndexResult();
}

// ____________________________________________________________________________
VariableToColumnMap TensorIndex::computeVariableToColumnMap() const {
  VariableToColumnMap variableToColumnMap;
  auto makeUndefCol = makePossiblyUndefinedColumn;

  if (!(childLeft_ || childRight_)) {
    // none of the children has been added
    variableToColumnMap[config_.left_] = makeUndefCol(ColumnIndex{0});
    variableToColumnMap[config_.right_] = makeUndefCol(ColumnIndex{1});
  } else if (childLeft_ && !childRight_) {
    // only the left child has been added
    variableToColumnMap[config_.right_] = makeUndefCol(ColumnIndex{1});
  } else if (!childLeft_ && childRight_) {
    // only the right child has been added
    variableToColumnMap[config_.left_] = makeUndefCol(ColumnIndex{0});
  } else {
    auto addColumns = [&](const VariableToColumnMap& varColMap, size_t offset) {
      auto varColsVec = copySortedByColumnIndex(varColMap);
      for (size_t i = 0; i < varColsVec.size(); i++) {
        auto [var, colAndType] = varColsVec[i];
        colAndType.columnIndex_ = offset + i;
        variableToColumnMap[var] = colAndType;
      }
    };

    // We add all columns from the left table, but only those from the right
    // table that are actually selected by the payload variables, plus the join
    // column
    auto sizeLeft = childLeft_->getResultWidth();
    auto varColMapLeft = childLeft_->getVariableColumns();
    AD_CONTRACT_CHECK(
        !varColMapLeft.contains(config_.right_),
        "Right variable must not be defined in left child of tensor search.");
    AD_CONTRACT_CHECK(
        varColMapLeft.contains(config_.left_),
        "Left variable is not defined in left child of tensor search.");
    addColumns(varColMapLeft, 0);

    auto varColMapRightFiltered = getVarColMapPayloadVars();
    auto sizeRight = varColMapRightFiltered.size();
    AD_CONTRACT_CHECK(
        !varColMapRightFiltered.contains(config_.left_),
        "Left variable must not be defined in right child of tensor search.");
    addColumns(varColMapRightFiltered, sizeLeft);

    // Column for the distance
    if (config_.distanceVariable_.has_value()) {
      AD_CONTRACT_CHECK(
          !variableToColumnMap.contains(config_.distanceVariable_.value()),
          "The distance variable of a tensor search must not be previously "
          "defined.");
      variableToColumnMap[config_.distanceVariable_.value()] =
          makeUndefCol(ColumnIndex{sizeLeft + sizeRight});
    }
  }

  return variableToColumnMap;
}

// _____________________________________________________________________________
std::unique_ptr<Operation> TensorIndex::cloneImpl() const {
  return std::make_unique<TensorIndex>(
      _executionContext, config_,
      childLeft_ ? std::optional{childLeft_->clone()} : std::nullopt,
      childRight_ ? std::optional{childRight_->clone()} : std::nullopt);
}
