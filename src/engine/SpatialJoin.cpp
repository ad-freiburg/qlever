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
#include <queue>
#include <tuple>

#include "engine/ExportQueryExecutionTrees.h"
#include "engine/VariableToColumnMap.h"
#include "engine/idTable/IdTable.h"
#include "global/Constants.h"
#include "global/ValueId.h"
#include "parser/ParsedQuery.h"
#include "util/AllocatorWithLimit.h"
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
  if (triple_.p_._iri.starts_with(MAX_DIST_IN_METERS)) {
    parseMaxDistance();
  } else if (triple_.p_._iri.starts_with(NEAREST_NEIGHBORS)) {
    parseNearestNeighbors();
  } else {
    AD_THROW(absl::StrCat("Invalid predicate used to construct SpatialJoin: ",
                          triple_.p_._iri));
  }

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
void SpatialJoin::parseMaxDistance() {
  const std::string& input = triple_.p_._iri;

  if (ctre::match<MAX_DIST_IN_METERS_REGEX>(input)) {
    std::string number = input.substr(
        MAX_DIST_IN_METERS.size(),
        input.size() - MAX_DIST_IN_METERS.size() - 1);  // -1: compensate for >
    maxDist_ = std::stoll(number);
  } else {
    AD_THROW(
        "parsing of the maximum distance for the "
        "SpatialJoin operation was not possible");
  }

  if (maxDist_ < 0) {
    AD_THROW("the maximum distance between two objects must be >= 0");
  }
}

// ____________________________________________________________________________
void SpatialJoin::parseNearestNeighbors() {
  const std::string& input = triple_.p_._iri;
  if (auto match = ctre::search<NEAREST_NEIGHBORS_REGEX>(input)) {
    double maxResults = -1;
    double maxDist = -1;

    std::string_view maxResultsMatch = match.get<"results">();
    std::string_view maxDistMatch = match.get<"dist">();

    absl::from_chars(maxResultsMatch.data(),
                     maxResultsMatch.data() + maxResultsMatch.size(),
                     maxResults);
    if (maxDistMatch.size() > 0) {
      absl::from_chars(maxDistMatch.data(),
                       maxDistMatch.data() + maxDistMatch.size(), maxDist);
    }

    maxResults_ = static_cast<long long>(maxResults);
    maxDist_ = static_cast<long long>(maxDist);
  } else {
    AD_THROW("invalid arguments for <nearest-neighbors>");
  }

  if (maxResults_ < 0) {
    // We accept `maxDist_ == -1` here: deactivate `maxDist_`
    AD_THROW("the maximum number of results must be >= 0");
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
    os << "maxDist: " << maxDist_ << "\n";
    os << "maxResults: " << maxResults_ << "\n";
    return std::move(os).str();
  } else {
    return "incomplete SpatialJoin class";
  }
}

// ____________________________________________________________________________
string SpatialJoin::getDescriptor() const {
  std::string properties = absl::StrCat(triple_.s_.getVariable().name(), " to ",
                                        triple_.o_.getVariable().name());
  if (maxDist_ >= 0) {
    properties = absl::StrCat(properties, " with max distance of ",
                              std::to_string(maxDist_), " ");
  }
  if (maxResults_ >= 0) {
    properties = absl::StrCat(properties, " with max number of results of ",
                              std::to_string(maxResults_));
  }
  return absl::StrCat("SpatialJoin: ", properties);
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
    if (useBaselineAlgorithm_) {
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
    return childLeft_->getSizeEstimate() * childRight_->getSizeEstimate();
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
    return static_cast<float>(getSizeEstimate()) / distinctnessChild;
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
std::string SpatialJoin::betweenQuotes(std::string extractFrom) const {
  // returns everything between the first two quotes. If the string does
  // not contain two quotes, the string is returned as a whole
  //
  size_t pos1 = extractFrom.find("\"", 0);
  size_t pos2 = extractFrom.find("\"", pos1 + 1);
  if (pos1 != std::string::npos && pos2 != std::string::npos) {
    return extractFrom.substr(pos1 + 1, pos2 - pos1 - 1);
  } else {
    return extractFrom;
  }
}

// ____________________________________________________________________________
std::optional<GeoPoint> SpatialJoin::getPoint(const IdTable* restable,
                                              size_t row,
                                              ColumnIndex col) const {
  auto id = restable->at(row, col);
  return id.getDatatype() == Datatype::GeoPoint
             ? std::optional{id.getGeoPoint()}
             : std::nullopt;
};

// ____________________________________________________________________________
Id SpatialJoin::computeDist(const IdTable* resLeft, const IdTable* resRight,
                            size_t rowLeft, size_t rowRight,
                            ColumnIndex leftPointCol,
                            ColumnIndex rightPointCol) const {
  auto point1 = getPoint(resLeft, rowLeft, leftPointCol);
  auto point2 = getPoint(resRight, rowRight, rightPointCol);
  if (!point1.has_value() || !point2.has_value()) {
    return Id::makeUndefined();
  }
  return Id::makeFromInt(
      ad_utility::detail::wktDistImpl(point1.value(), point2.value()) * 1000);
}

// ____________________________________________________________________________
void SpatialJoin::addResultTableEntry(IdTable* result,
                                      const IdTable* resultLeft,
                                      const IdTable* resultRight,
                                      size_t rowLeft, size_t rowRight,
                                      Id distance) const {
  // this lambda function copies elements from copyFrom
  // into the table res. It copies them into the row
  // rowIndRes and column column colIndRes. It returns the column number
  // until which elements were copied
  auto addColumns = [](IdTable* res, const IdTable* copyFrom, size_t rowIndRes,
                       size_t colIndRes, size_t rowIndCopy) {
    size_t col = 0;
    while (col < copyFrom->numColumns()) {
      res->at(rowIndRes, colIndRes) = (*copyFrom).at(rowIndCopy, col);
      colIndRes += 1;
      col += 1;
    }
    return colIndRes;
  };

  auto resrow = result->numRows();
  result->emplace_back();
  // add columns to result table
  size_t rescol = 0;
  rescol = addColumns(result, resultLeft, resrow, rescol, rowLeft);
  rescol = addColumns(result, resultRight, resrow, rescol, rowRight);

  if (addDistToResult_) {
    result->at(resrow, rescol) = distance;
    // rescol isn't used after that in this function, but future updates,
    // which add additional columns, would need to remember to increase
    // rescol at this place otherwise. If they forget to do this, the
    // distance column will be overwritten, the variableToColumnMap will
    // not work and so on
    // rescol += 1;
  }
}

// ____________________________________________________________________________
std::tuple<const IdTable* const, const IdTable* const, unsigned long,
           unsigned long, size_t>
SpatialJoin::prepareJoin() const {
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
  const auto [resLeft, keepAliveLeft] = getIdTable(childLeft_);
  const auto [resRight, keepAliveRight] = getIdTable(childRight_);

  // Input table columns for the join
  ColumnIndex leftJoinCol = getJoinCol(childLeft_, leftChildVariable_);
  ColumnIndex rightJoinCol = getJoinCol(childRight_, rightChildVariable_);

  // Size of output table
  size_t numColumns = getResultWidth();
  return std::make_tuple(resLeft, resRight, leftJoinCol, rightJoinCol,
                         numColumns);
}

// ____________________________________________________________________________
Result SpatialJoin::baselineAlgorithm() {
  const auto [resLeft, resRight, leftJoinCol, rightJoinCol, numColumns] =
      prepareJoin();
  IdTable result{numColumns, _executionContext->getAllocator()};

  // cartesian product between the two tables, pairs are restricted according to
  // `maxDistance_` and `maxResults_`
  for (size_t rowLeft = 0; rowLeft < resLeft->size(); rowLeft++) {
    // This priority queue stores the intermediate best results if `maxResults_`
    // is used. Each intermediate result is stored as a pair of its `rowRight`
    // and distance. Since the queue will hold at most `maxResults_ + 1` items,
    // it is not a memory concern.
    auto compare = [](std::pair<size_t, int64_t> a,
                      std::pair<size_t, int64_t> b) {
      return a.second > b.second;
    };
    std::priority_queue<std::pair<size_t, int64_t>,
                        std::vector<std::pair<size_t, int64_t>>,
                        decltype(compare)>
        intermediate(compare);

    // Inner loop of cartesian product
    for (size_t rowRight = 0; rowRight < resRight->size(); rowRight++) {
      Id dist = computeDist(resLeft, resRight, rowLeft, rowRight, leftJoinCol,
                            rightJoinCol);

      // Ensure `maxDist_` constraint
      if (dist.getDatatype() != Datatype::Int || maxDist_ < 0 ||
          dist.getInt() > maxDist_) {
        continue;
      }

      // If there is no `maxResults_` we can add the result row immediately
      if (maxResults_ < 0) {
        addResultTableEntry(&result, resLeft, resRight, rowLeft, rowRight,
                            dist);
        continue;
      }

      // Ensure `maxResults_` constraint using priority queue
      intermediate.push(std::pair{rowRight, dist.getInt()});
      // Too many results? Drop the worst one
      if (intermediate.size() > static_cast<size_t>(maxResults_)) {
        intermediate.pop();
      }
    }

    // If we are using the priority queue, we didn't add the results in the
    // inner loop, so we do it now.
    if (maxResults_ >= 0) {
      size_t numResults = intermediate.size();
      for (size_t item = 0; item < numResults; item++) {
        // Get and remove largest item from priority queue
        auto [rowRight, dist] = intermediate.top();
        intermediate.pop();

        addResultTableEntry(&result, resLeft, resRight, rowLeft, rowRight,
                            Id::makeFromInt(dist));
      }
    }
  }
  return Result(std::move(result), std::vector<ColumnIndex>{}, LocalVocab{});
}

// ____________________________________________________________________________
Result SpatialJoin::s2geometryAlgorithm() {
  const auto [resLeft, resRight, leftJoinCol, rightJoinCol, numColumns] =
      prepareJoin();
  IdTable result{numColumns, _executionContext->getAllocator()};

  // Helper function to convert `GeoPoint` to `S2Point`
  auto constexpr toS2Point = [](const GeoPoint& p) {
    auto lat = p.getLat();
    auto lng = p.getLng();
    auto latlng = S2LatLng::FromDegrees(lat, lng);
    return S2Point{latlng};
  };

  // Build an index that contains all elements of the right table
  S2PointIndex<size_t> s2index;
  for (size_t rowRight = 0; rowRight < resRight->size(); rowRight++) {
    auto p = getPoint(resRight, rowRight, rightJoinCol);
    if (p.has_value()) {
      s2index.Add(toS2Point(p.value()), rowRight);
    }
  }

  // Performs a nearest neighbor search on the index containing the right table
  // for each row in the left table and returns the closest points that satisfy
  // the criteria given by `maxDist_` and `maxResults_`.

  // Construct a query object with the given constraints
  auto s2query = S2ClosestPointQuery<size_t>{&s2index};

  if (maxResults_ >= 0) {
    s2query.mutable_options()->set_max_results(static_cast<int>(maxResults_));
  }
  if (maxDist_ >= 0) {
    s2query.mutable_options()->set_max_distance(
        S2Earth::ToAngle(util::units::Meters(static_cast<float>(maxDist_))));
  }

  for (size_t rowLeft = 0; rowLeft < resLeft->size(); rowLeft++) {
    auto p = getPoint(resLeft, rowLeft, leftJoinCol);
    if (!p.has_value()) {
      continue;
    }
    auto s2target =
        S2ClosestPointQuery<size_t>::PointTarget{toS2Point(p.value())};

    for (const auto& neighbor : s2query.FindClosestPoints(&s2target)) {
      // In this loop we only receive points that already satisfy the given
      // criteria
      auto rowRight = neighbor.data();
      auto dist = static_cast<int64_t>(S2Earth::ToMeters(neighbor.distance()));
      addResultTableEntry(&result, resLeft, resRight, rowLeft, rowRight,
                          Id::makeFromInt(dist));
    }
  }

  return Result(std::move(result), std::vector<ColumnIndex>{}, LocalVocab{});
}

// ____________________________________________________________________________
Result SpatialJoin::computeResult([[maybe_unused]] bool requestLaziness) {
  if (useBaselineAlgorithm_) {
    return baselineAlgorithm();
  } else {
    return s2geometryAlgorithm();
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
