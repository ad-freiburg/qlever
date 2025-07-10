// Copyright 2024 - 2025, University of Freiburg
// Chair of Algorithms and Data Structures
// Authors: Jonathan Zeller github@Jonathan24680
//          Christoph Ullinger <ullingec@cs.uni-freiburg.de>
//          Patrick Brosi <brosi@cs.uni-freiburg.de>
//
// Copyright 2025, Bayerische Motoren Werke Aktiengesellschaft (BMW AG)

#include "engine/SpatialJoin.h"

#include <absl/container/flat_hash_set.h>
#include <absl/strings/charconv.h>

#include <cstdint>
#include <limits>
#include <optional>
#include <queue>
#include <tuple>
#include <type_traits>
#include <unordered_set>
#include <variant>

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
    QueryExecutionContext* qec, SpatialJoinConfiguration config,
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
std::shared_ptr<SpatialJoin> SpatialJoin::addChild(
    std::shared_ptr<QueryExecutionTree> child,
    const Variable& varOfChild) const {
  std::shared_ptr<SpatialJoin> sj;
  if (varOfChild == config_.left_) {
    sj = std::make_shared<SpatialJoin>(getExecutionContext(), config_,
                                       std::move(child), childRight_,
                                       substitutesFilterOp_);
  } else if (varOfChild == config_.right_) {
    sj = std::make_shared<SpatialJoin>(getExecutionContext(), config_,
                                       childLeft_, std::move(child),
                                       substitutesFilterOp_);
  } else {
    AD_THROW("variable does not match");
  }

  // The new spatial join after adding a child needs to inherit the warnings of
  // its predecessor.
  auto warnings = getWarnings().rlock();
  for (const auto& warning : *warnings) {
    sj->addWarning(warning);
  }

  return sj;
}

// ____________________________________________________________________________
bool SpatialJoin::isConstructed() const { return childLeft_ && childRight_; }

// ____________________________________________________________________________
std::optional<double> SpatialJoin::getMaxDist() const {
  auto visitor = [](const auto& config) -> std::optional<double> {
    return config.maxDist_;
  };
  return std::visit(visitor, config_.task_);
}

// ____________________________________________________________________________
std::optional<size_t> SpatialJoin::getMaxResults() const {
  auto visitor = [](const auto& config) -> std::optional<size_t> {
    using T = std::decay_t<decltype(config)>;
    if constexpr (std::is_same_v<T, MaxDistanceConfig>) {
      return std::nullopt;
    } else if constexpr (std::is_same_v<T, SpatialJoinConfig>) {
      return std::nullopt;
    } else {
      static_assert(std::is_same_v<T, NearestNeighborsConfig>);
      return config.maxResults_;
    }
  };
  return std::visit(visitor, config_.task_);
}

// ____________________________________________________________________________
std::vector<QueryExecutionTree*> SpatialJoin::getChildren() {
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
std::string SpatialJoin::getCacheKeyImpl() const {
  if (childLeft_ && childRight_) {
    std::ostringstream os;
    // This includes all attributes that change the result

    // Children
    os << "SpatialJoin\nChild1:\n" << childLeft_->getCacheKey() << "\n";
    os << "Child2:\n" << childRight_->getCacheKey() << "\n";

    // Join Variables
    os << "JoinColLeft:" << childLeft_->getVariableColumn(config_.left_);
    os << "JoinColRight:" << childRight_->getVariableColumn(config_.right_);

    // Task
    auto maxDist = getMaxDist();
    if (maxDist.has_value()) {
      os << "maxDist: " << maxDist.value() << "\n";
    }
    auto maxResults = getMaxResults();
    if (maxResults.has_value()) {
      os << "maxResults: " << maxResults.value() << "\n";
    }

    // Uses libspatialjoin?
    auto algo = getAlgorithm();
    if (algo == SpatialJoinAlgorithm::LIBSPATIALJOIN) {
      auto joinType = getJoinType();
      os << "libspatialjoin on: "
         << (int)joinType.value_or(SpatialJoinType::INTERSECTS) << "\n";
    }

    // Uses distance variable?
    if (config_.distanceVariable_.has_value()) {
      os << "withDistanceVariable\n";
    }

    // Selected payload variables
    os << "payload:";
    auto rightVarColFiltered =
        copySortedByColumnIndex(getVarColMapPayloadVars());
    for (const auto& [var, info] : rightVarColFiltered) {
      os << " " << info.columnIndex_;
    }
    os << "\n";

    // Algorithm is not included here because it should not have any impact on
    // the result.
    return std::move(os).str();
  } else {
    return "incomplete SpatialJoin class";
  }
}

// ____________________________________________________________________________
std::string SpatialJoin::getDescriptor() const {
  // Build different descriptors depending on the configuration
  auto visitor = [this](const auto& config) -> std::string {
    using T = std::decay_t<decltype(config)>;
    // Joined Variables
    auto left = config_.left_.name();
    auto right = config_.right_.name();

    // Config type
    if constexpr (std::is_same_v<T, MaxDistanceConfig>) {
      return absl::StrCat("MaxDistJoin ", left, " to ", right, " of ",
                          config.maxDist_, " meter(s)");
    } else if constexpr (std::is_same_v<T, SpatialJoinConfig>) {
      return absl::StrCat("Spatial Join ", left, " to ", right, " of type ",
                          config.joinType_);
    } else {
      static_assert(std::is_same_v<T, NearestNeighborsConfig>);
      return absl::StrCat("NearestNeighborsJoin ", left, " to ", right,
                          " of max. ", config.maxResults_);
    }
  };
  return std::visit(visitor, config_.task_);
}

// ____________________________________________________________________________
size_t SpatialJoin::getResultWidth() const {
  if (childLeft_ && childRight_) {
    // don't subtract anything because of a common join column. In the case
    // of the spatial join, the join column is different for both sides (e.g.
    // objects with a distance of at most 500m to each other, then each object
    // might contain different positions, which should be kept).
    // For the right join table we only use the selected columns.
    size_t sizeRight;
    if (config_.payloadVariables_.isAll()) {
      sizeRight = childRight_->getResultWidth();
    } else {
      // We convert to a set here, because we allow multiple occurrences of
      // variables in payloadVariables_
      std::vector<Variable> pv = config_.payloadVariables_.getVariables();
      absl::flat_hash_set<Variable> pvSet{pv.begin(), pv.end()};

      // The payloadVariables_ may contain the right join variable
      sizeRight = pvSet.size() + (pvSet.contains(config_.right_) ? 0 : 1);
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
size_t SpatialJoin::getCostEstimate() {
  if (!childLeft_ || !childRight_) {
    return 1;  // dummy return, as the class does not have its children yet
  }

  size_t spatialJoinCostEst = [this]() {
    auto n = childLeft_->getSizeEstimate();
    auto m = childRight_->getSizeEstimate();

    if (config_.algo_ == SpatialJoinAlgorithm::BASELINE) {
      return n * m;
    } else if (config_.algo_ == SpatialJoinAlgorithm::LIBSPATIALJOIN) {
      // We take the cost estimate to be `4 * (n + m)`, where `n` and `m` are
      // the size of the left and right table, respectively. Reasoning:
      //
      // Let `N = n + m` be the number of the union of the two sets of objects.
      // We first sort these objects in time `O(N log n)`, and then iterate
      // over these objects, keeping track of the active boxes. This can be done
      // in time `O(N log M)`, where `M` is the maximum number of active boxes
      // at any time. For a full self-join on the complete OSM data, `M` is at
      // most 10'000, so for all practical purposes we can consider `log M` to
      // be a constant of 4.
      //
      // The actual cost of comparing the candidate cannot be meaningfully
      // estimated here, as we know nothing about the invidiual geometries.
      auto numObjects = n + m;
      return numObjects * 4;
    } else {
      AD_CORRECTNESS_CHECK(
          config_.algo_ == SpatialJoinAlgorithm::S2_GEOMETRY ||
              config_.algo_ == SpatialJoinAlgorithm::BOUNDING_BOX,
          "Unknown SpatialJoin Algorithm.");

      // Let n be the size of the left table and m the size of the right table.
      // When using the S2Point index, we first create the index for the right
      // table in O(m * log(m)). We then iterate over the left table in O(n) and
      // for each item do a lookup on the index for the right table in O(log m).
      // Together we have O(n log(m) + m log(m)), because in general we can't
      // draw conclusions about the relation between the sizes of n and m.
      auto logm = static_cast<size_t>(std::log(static_cast<double>(m)));
      return (n * logm) + (m * logm);
    }
  }();

  // The cost to compute the children needs to be taken into account.
  return spatialJoinCostEst + childLeft_->getCostEstimate() +
         childRight_->getCostEstimate();
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
    if (config_.distanceVariable_.has_value() && col == getResultWidth() - 1) {
      // as each distance is very likely to be unique (even if only after
      // a few decimal places), no multiplicities are assumed
      return 1;
    } else if (col < childLeft_->getResultWidth()) {
      child = childLeft_;
    } else {
      child = childRight_;
      // Since we only select the payload variables into the result and
      // potentially omit some columns from the right child, we need to
      // translate the column index on the spatial join to a column index in the
      // right child.
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
VariableToColumnMap SpatialJoin::getVarColMapPayloadVars() const {
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
                         "' selected as payload to spatial join but not "
                         "present in right child."));
      }
    }
    AD_CONTRACT_CHECK(
        varColMap.contains(config_.right_),
        "Right variable is not defined in right child of spatial join.");
    newVarColMap.insert_or_assign(config_.right_, varColMap.at(config_.right_));
  }

  return newVarColMap;
}

// ____________________________________________________________________________
PreparedSpatialJoinParams SpatialJoin::prepareJoin() const {
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
  return PreparedSpatialJoinParams{idTableLeft,       std::move(resultLeft),
                                   idTableRight,      std::move(resultRight),
                                   leftJoinCol,       rightJoinCol,
                                   rightSelectedCols, numColumns,
                                   getMaxDist(),      getMaxResults(),
                                   config_.joinType_};
}

// ____________________________________________________________________________
Result SpatialJoin::computeResult([[maybe_unused]] bool requestLaziness) {
  AD_CONTRACT_CHECK(
      isConstructed(),
      "SpatialJoin needs two children, but at least one is missing");
  SpatialJoinAlgorithms algorithms{_executionContext, prepareJoin(), config_,
                                   this};
  if (config_.algo_ == SpatialJoinAlgorithm::BASELINE) {
    return algorithms.BaselineAlgorithm();
  } else if (config_.algo_ == SpatialJoinAlgorithm::S2_GEOMETRY) {
    return algorithms.S2geometryAlgorithm();
  } else if (config_.algo_ == SpatialJoinAlgorithm::LIBSPATIALJOIN) {
    return algorithms.LibspatialjoinAlgorithm();
  } else {
    AD_CORRECTNESS_CHECK(config_.algo_ == SpatialJoinAlgorithm::BOUNDING_BOX,
                         "Unknown SpatialJoin Algorithm.");
    // as the BoundingBoxAlgorithms only works for max distance and not for
    // nearest neighbors, S2geometry gets called as a backup, if the query is
    // asking for the nearest neighbors
    if (std::get_if<MaxDistanceConfig>(&config_.task_)) {
      return algorithms.BoundingBoxAlgorithm();
    } else {
      addWarning(
          "The bounding box spatial join algorithm does not support nearest "
          "neighbor search. Using s2 geometry algorithm instead.");
      return algorithms.S2geometryAlgorithm();
    }
  }
}

// ____________________________________________________________________________
VariableToColumnMap SpatialJoin::computeVariableToColumnMap() const {
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
        "Right variable must not be defined in left child of spatial join.");
    AD_CONTRACT_CHECK(
        varColMapLeft.contains(config_.left_),
        "Left variable is not defined in left child of spatial join.");
    addColumns(varColMapLeft, 0);

    auto varColMapRightFiltered = getVarColMapPayloadVars();
    auto sizeRight = varColMapRightFiltered.size();
    AD_CONTRACT_CHECK(
        !varColMapRightFiltered.contains(config_.left_),
        "Left variable must not be defined in right child of spatial join.");
    addColumns(varColMapRightFiltered, sizeLeft);

    // Column for the distance
    if (config_.distanceVariable_.has_value()) {
      AD_CONTRACT_CHECK(
          !variableToColumnMap.contains(config_.distanceVariable_.value()),
          "The distance variable of a spatial join must not be previously "
          "defined.");
      variableToColumnMap[config_.distanceVariable_.value()] =
          makeUndefCol(ColumnIndex{sizeLeft + sizeRight});
    }
  }

  return variableToColumnMap;
}

// _____________________________________________________________________________
std::unique_ptr<Operation> SpatialJoin::cloneImpl() const {
  return std::make_unique<SpatialJoin>(
      _executionContext, config_,
      childLeft_ ? std::optional{childLeft_->clone()} : std::nullopt,
      childRight_ ? std::optional{childRight_->clone()} : std::nullopt);
}
