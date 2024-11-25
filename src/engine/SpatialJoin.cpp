//  Copyright 2024, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: @Jonathan24680
//  Author: Christoph Ullinger <ullingec@informatik.uni-freiburg.de>

#include "engine/SpatialJoin.h"

#include <absl/strings/charconv.h>
#include <s2/s2closest_point_query.h>
#include <s2/s2earth.h>
#include <s2/s2point.h>
#include <s2/s2point_index.h>
#include <s2/util/units/length-units.h>

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
    QueryExecutionContext* qec, SparqlTriple triple,
    std::optional<std::shared_ptr<QueryExecutionTree>> childLeft,
    std::optional<std::shared_ptr<QueryExecutionTree>> childRight)
    : Operation(qec),
      triple_{std::move(triple)},
      leftChildVariable_{triple_.s_.getVariable()},
      rightChildVariable_{triple_.o_.getVariable()} {
  // A `SpatialJoin` can be constructed from different system predicates, parse
  parseConfigFromTriple();

  if (childLeft) {
    childLeft_ = std::move(childLeft.value());
  }
  if (childRight) {
    childRight_ = std::move(childRight.value());
  }

  AD_CONTRACT_CHECK(triple_.s_.isVariable() && triple_.o_.isVariable(),
                    "Currently, SpatialJoin needs two variables");
}

// ____________________________________________________________________________
void SpatialJoin::parseConfigFromTriple() {
  // Helper to convert a ctre match to an integer
  auto matchToInt = [](std::string_view match) -> std::optional<size_t> {
    if (match.size() > 0) {
      size_t res = 0;
      std::from_chars(match.data(), match.data() + match.size(), res);
      return res;
    }
    return std::nullopt;
  };

  const std::string& input = triple_.p_._iri;

  if (auto match = ctre::match<MAX_DIST_IN_METERS_REGEX>(input)) {
    auto maxDist = matchToInt(match.get<"dist">());
    AD_CORRECTNESS_CHECK(maxDist.has_value());
    config_ = MaxDistanceConfig{maxDist.value()};
  } else if (auto match = ctre::search<NEAREST_NEIGHBORS_REGEX>(input)) {
    auto maxResults = matchToInt(match.get<"results">());
    auto maxDist = matchToInt(match.get<"dist">());
    AD_CORRECTNESS_CHECK(maxResults.has_value());
    config_ = NearestNeighborsConfig{maxResults.value(), maxDist};
  } else {
    AD_THROW(
        absl::StrCat("Tried to perform spatial join with unknown triple ",
                     input, ". This must be a valid spatial condition like ",
                     "<max-distance-in-meters:50> or <nearest-neighbors:3>."));
  }
}

// ____________________________________________________________________________
std::shared_ptr<SpatialJoin> SpatialJoin::addChild(
    std::shared_ptr<QueryExecutionTree> child,
    const Variable& varOfChild) const {
  if (varOfChild == leftChildVariable_) {
    return std::make_shared<SpatialJoin>(getExecutionContext(), triple_,
                                         std::move(child), childRight_);
  } else if (varOfChild == rightChildVariable_) {
    return std::make_shared<SpatialJoin>(getExecutionContext(), triple_,
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
  return std::visit(visitor, config_);
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
  return std::visit(visitor, config_);
}

// ____________________________________________________________________________
std::vector<QueryExecutionTree*> SpatialJoin::getChildren() {
  if (!(childLeft_ && childRight_)) {
    AD_THROW("SpatialJoin needs two children, but at least one is missing");
  }

  return {childLeft_.get(), childRight_.get()};
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
    auto left = triple_.s_.getVariable().name();
    auto right = triple_.o_.getVariable().name();

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
  return std::visit(visitor, config_);
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
    if (addDistToResult_) {
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
    if (algorithm_ == Algorithm::Baseline) {
      return inputEstimate * inputEstimate;
    } else {
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
    if (addDistToResult_ && col == getResultWidth() - 1) {
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
  ColumnIndex leftJoinCol = getJoinCol(childLeft_, leftChildVariable_);
  ColumnIndex rightJoinCol = getJoinCol(childRight_, rightChildVariable_);

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
  SpatialJoinAlgorithms algorithms{_executionContext, prepareJoin(),
                                   addDistToResult_, config_, this};
  if (algorithm_ == Algorithm::Baseline) {
    return algorithms.BaselineAlgorithm();
  } else if (algorithm_ == Algorithm::S2Geometry) {
    return algorithms.S2geometryAlgorithm();
  } else {
    AD_CORRECTNESS_CHECK(algorithm_ == Algorithm::BoundingBox);
    // as the BoundingBoxAlgorithms only works for max distance and not for
    // nearest neighbors, S2geometry gets called as a backup, if the query is
    // asking for the nearest neighbors
    if (std::get_if<MaxDistanceConfig>(&config_)) {
      return algorithms.BoundingBoxAlgorithm();
    } else {
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
    variableToColumnMap[leftChildVariable_] = makeUndefCol(ColumnIndex{0});
    variableToColumnMap[rightChildVariable_] = makeUndefCol(ColumnIndex{1});
  } else if (childLeft_ && !childRight_) {
    // only the left child has been added
    variableToColumnMap[rightChildVariable_] = makeUndefCol(ColumnIndex{1});
  } else if (!childLeft_ && childRight_) {
    // only the right child has been added
    variableToColumnMap[leftChildVariable_] = makeUndefCol(ColumnIndex{0});
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

    if (addDistToResult_) {
      variableToColumnMap[Variable{nameDistanceInternal_}] =
          makeUndefCol(ColumnIndex{sizeLeft + sizeRight});
    }
  }

  return variableToColumnMap;
}
