//  Copyright 2024, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: @Jonathan24680
//  Author: Christoph Ullinger <ullingec@informatik.uni-freiburg.de>

#include "engine/SpatialJoin.h"

#include <absl/strings/charconv.h>

#include <cstdint>
#include <ctre-unicode.hpp>
#include <limits>
#include <optional>
#include <queue>
#include <tuple>

#include "engine/ExportQueryExecutionTrees.h"
#include "engine/SpatialJoinAlgorithms.h"
#include "engine/VariableToColumnMap.h"
#include "engine/idTable/IdTable.h"
#include "global/Constants.h"
#include "global/ValueId.h"
#include "parser/ParsedQuery.h"
#include "util/AllocatorWithLimit.h"
#include "util/Exception.h"
#include "util/GeoSparqlHelpers.h"
#include "util/MemorySize/MemorySize.h"

// ____________________________________________________________________________
SpatialJoin::SpatialJoin(
    QueryExecutionContext* qec,
    std::shared_ptr<SpatialJoinConfiguration> config,
    std::optional<std::shared_ptr<QueryExecutionTree>> childLeft,
    std::optional<std::shared_ptr<QueryExecutionTree>> childRight)
    : Operation(qec), config_{config} {
  if (childLeft.has_value()) {
    childLeft_ = std::move(childLeft.value());
  }
  if (childRight.has_value()) {
    childRight_ = std::move(childRight.value());
  }
}

// ____________________________________________________________________________
std::shared_ptr<SpatialJoin> SpatialJoin::addChild(
    std::shared_ptr<QueryExecutionTree> child,
    const Variable& varOfChild) const {
  if (varOfChild == config_->left_) {
    return std::make_shared<SpatialJoin>(getExecutionContext(), config_,
                                         std::move(child), childRight_);
  } else if (varOfChild == config_->right_) {
    return std::make_shared<SpatialJoin>(getExecutionContext(), config_,
                                         childLeft_, std::move(child));
  } else {
    AD_THROW("variable does not match");
  }
}

// ____________________________________________________________________________
bool SpatialJoin::isConstructed() const {
  if (childLeft_ && childRight_) {
    return true;
  }
  return false;
}

// ____________________________________________________________________________
std::optional<size_t> SpatialJoin::getMaxDist() const {
  auto visitor = []<typename T>(const T& config) -> std::optional<size_t> {
    return config.maxDist_;
  };
  return std::visit(visitor, config_->task_);
}

// ____________________________________________________________________________
std::optional<size_t> SpatialJoin::getMaxResults() const {
  auto visitor = []<typename T>(const T& config) -> std::optional<size_t> {
    if constexpr (std::is_same_v<T, MaxDistanceConfig>) {
      return std::nullopt;
    } else {
      static_assert(std::is_same_v<T, NearestNeighborsConfig>);
      return config.maxResults_;
    }
  };
  return std::visit(visitor, config_->task_);
}

// ____________________________________________________________________________
std::vector<QueryExecutionTree*> SpatialJoin::getChildren() {
  std::vector<QueryExecutionTree*> result;
  if (childLeft_) {
    result.push_back(childLeft_.get());
  }
  if (childRight_) {
    result.push_back(childRight_.get());
  }
  return result;
}

// ____________________________________________________________________________
string SpatialJoin::getCacheKeyImpl() const {
  if (childLeft_ && childRight_) {
    std::ostringstream os;
    os << "SpatialJoin\nChild1:\n" << childLeft_->getCacheKey() << "\n";
    os << "Child2:\n" << childRight_->getCacheKey() << "\n";
    auto maxDist = getMaxDist();
    if (maxDist.has_value()) {
      os << "maxDist: " << maxDist.value() << "\n";
    }
    auto maxResults = getMaxResults();
    if (maxResults.has_value()) {
      os << "maxResults: " << maxResults.value() << "\n";
    }
    return std::move(os).str();
  } else {
    return "incomplete SpatialJoin class";
  }
}

// ____________________________________________________________________________
string SpatialJoin::getDescriptor() const {
  // Build different descriptors depending on the configuration
  auto visitor = [this]<typename T>(const T& config) -> std::string {
    // Joined Variables
    auto left = config_->left_.name();
    auto right = config_->right_.name();

    // Config type
    if constexpr (std::is_same_v<T, MaxDistanceConfig>) {
      return absl::StrCat("MaxDistJoin ", left, " to ", right, " of ",
                          config.maxDist_, " meter(s)");
    } else {
      static_assert(std::is_same_v<T, NearestNeighborsConfig>);
      return absl::StrCat("NearestNeighborsJoin ", left, " to ", right,
                          " of max. ", config.maxResults_);
    }
  };
  return std::visit(visitor, config_->task_);
}

// ____________________________________________________________________________
size_t SpatialJoin::getResultWidth() const {
  if (childLeft_ && childRight_) {
    // don't subtract anything because of a common join column. In the case
    // of the spatial join, the join column is different for both sides (e.g.
    // objects with a distance of at most 500m to each other, then each object
    // might contain different positions, which should be kept).
    auto widthChildren =
        childLeft_->getResultWidth() + childRight_->getResultWidth();

    if (config_->bindDist_.has_value()) {
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
size_t SpatialJoin::getCostEstimate() {
  if (childLeft_ && childRight_) {
    size_t inputEstimate =
        childLeft_->getSizeEstimate() * childRight_->getSizeEstimate();
    if (config_->algo_ == SpatialJoinAlgorithm::BASELINE) {
      return inputEstimate * inputEstimate;
    } else if (config_->algo_ == SpatialJoinAlgorithm::S2_GEOMETRY) {
      // Let n be the size of the left table and m the size of the right table.
      // When using the S2Point index, we first create the index for the right
      // table in O(m * log(m)). We then iterate over the left table in O(n) and
      // for each item do a lookup on the index for the right table in O(log m).
      // Together we have O(n log(m) + m log(m)), because in general we can't
      // draw conclusions about the relation between the sizes of n and m.
      auto n = childLeft_->getSizeEstimate();
      auto m = childRight_->getSizeEstimate();
      auto logm = static_cast<size_t>(
          log(static_cast<double>(childRight_->getSizeEstimate())));
      return (n * logm) + (m * logm);
    } else {
      AD_THROW("Unknown SpatialJoin Algorithm.");
    }
  }
  return 1;  // dummy return, as the class does not have its children yet
}

// ____________________________________________________________________________
uint64_t SpatialJoin::getSizeEstimateBeforeLimit() {
  if (childLeft_ && childRight_) {
    // If we limit the number of results to k, even in the largest scenario, the
    // result can be at most `|childLeft| * k`
    auto maxResults = getMaxResults();
    if (maxResults.has_value()) {
      return childLeft_->getSizeEstimate() * maxResults.value();
    }

    // If we don't limit the number of results, we cannot draw conclusions about
    // the size, other than the worst case `|childLeft| * |childRight|`. However
    // to improve query planning for the average case, we apply a constant
    // factor (the asymptotic behavior remains unchanged).
    return (childLeft_->getSizeEstimate() * childRight_->getSizeEstimate()) /
           SPATIAL_JOIN_MAX_DIST_SIZE_ESTIMATE;
  }
  return 1;  // dummy return if not both children are added
}

// ____________________________________________________________________________
float SpatialJoin::getMultiplicity(size_t col) {
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
    if (config_->bindDist_.has_value() && col == getResultWidth() - 1) {
      // as each distance is very likely to be unique (even if only after
      // a few decimal places), no multiplicities are assumed
      return 1;
    } else if (col < childLeft_->getResultWidth()) {
      child = childLeft_;
    } else {
      child = childRight_;
      column -= childLeft_->getResultWidth();
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
bool SpatialJoin::knownEmptyResult() {
  // return false if either both children don't have an empty result, only one
  // child is available, which doesn't have a known empty result or neither
  // child is available
  return (childLeft_ && childLeft_->knownEmptyResult()) ||
         (childRight_ && childRight_->knownEmptyResult());
}

// ____________________________________________________________________________
vector<ColumnIndex> SpatialJoin::resultSortedOn() const {
  // the baseline (with O(n^2) runtime) can have some sorted columns, but as
  // the "true" computeResult method will use bounding boxes, which can't
  // guarantee that a sorted column stays sorted, this will return no sorted
  // column in all cases.
  return {};
}

// ____________________________________________________________________________
PreparedSpatialJoinParams SpatialJoin::prepareJoin() const {
  auto getIdTable = [](std::shared_ptr<QueryExecutionTree> child) {
    std::shared_ptr<const Result> resTable = child->getResult();
    auto idTablePtr = &resTable->idTable();
    return std::pair{idTablePtr, std::move(resTable)};
  };

  auto getJoinCol = [](const std::shared_ptr<const QueryExecutionTree>& child,
                       const Variable& childVariable) {
    auto varColMap =
        child->getRootOperation()->getExternallyVisibleVariableColumns();
    return varColMap[childVariable].columnIndex_;
  };

  // Input tables
  auto [idTableLeft, resultLeft] = getIdTable(childLeft_);
  auto [idTableRight, resultRight] = getIdTable(childRight_);

  // Input table columns for the join
  ColumnIndex leftJoinCol = getJoinCol(childLeft_, config_->left_);
  ColumnIndex rightJoinCol = getJoinCol(childRight_, config_->right_);

  // Size of output table
  size_t numColumns = getResultWidth();
  return PreparedSpatialJoinParams{idTableLeft,    std::move(resultLeft),
                                   idTableRight,   std::move(resultRight),
                                   leftJoinCol,    rightJoinCol,
                                   numColumns,     getMaxDist(),
                                   getMaxResults()};
}

// ____________________________________________________________________________
Result SpatialJoin::computeResult([[maybe_unused]] bool requestLaziness) {
  AD_CONTRACT_CHECK(
      isConstructed(),
      "SpatialJoin needs two children, but at least one is missing");
  SpatialJoinAlgorithms algorithms{_executionContext, prepareJoin(), config_};
  if (config_->algo_ == SpatialJoinAlgorithm::BASELINE) {
    return algorithms.BaselineAlgorithm();
  } else if (config_->algo_ == SpatialJoinAlgorithm::S2_GEOMETRY) {
    return algorithms.S2geometryAlgorithm();
  } else {
    AD_THROW("Unknown SpatialJoin Algorithm.");
  }
}

// ____________________________________________________________________________
VariableToColumnMap SpatialJoin::computeVariableToColumnMap() const {
  VariableToColumnMap variableToColumnMap;
  auto makeUndefCol = makePossiblyUndefinedColumn;

  if (!(childLeft_ || childRight_)) {
    // none of the children has been added
    variableToColumnMap[config_->left_] = makeUndefCol(ColumnIndex{0});
    variableToColumnMap[config_->right_] = makeUndefCol(ColumnIndex{1});
  } else if (childLeft_ && !childRight_) {
    // only the left child has been added
    variableToColumnMap[config_->right_] = makeUndefCol(ColumnIndex{1});
  } else if (!childLeft_ && childRight_) {
    // only the right child has been added
    variableToColumnMap[config_->left_] = makeUndefCol(ColumnIndex{0});
  } else {
    auto addColumns = [&](std::shared_ptr<QueryExecutionTree> child,
                          size_t offset) {
      auto varColsmap = child->getVariableColumns();
      auto varColsVec = copySortedByColumnIndex(varColsmap);
      std::ranges::for_each(
          varColsVec,
          [&](const std::pair<Variable, ColumnIndexAndTypeInfo>& varColEntry) {
            auto colAndType = varColEntry.second;  // Type info already correct
            colAndType.columnIndex_ += offset;
            variableToColumnMap[varColEntry.first] = colAndType;
          });
    };

    auto sizeLeft = childLeft_->getResultWidth();
    auto sizeRight = childRight_->getResultWidth();
    addColumns(childLeft_, 0);
    addColumns(childRight_, sizeLeft);

    if (config_->bindDist_.has_value()) {
      AD_CONTRACT_CHECK(variableToColumnMap.find(config_->bindDist_.value()) ==
                        variableToColumnMap.end());
      variableToColumnMap[config_->bindDist_.value()] =
          makeUndefCol(ColumnIndex{sizeLeft + sizeRight});
    }
  }

  return variableToColumnMap;
}
