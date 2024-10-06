#include "engine/SpatialJoin.h"

#include "engine/ExportQueryExecutionTrees.h"
#include "engine/VariableToColumnMap.h"
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
  parseMaxDistance();

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
  std::string errormessage =
      "parsing of the maximum distance for the "
      "SpatialJoin operation was not possible";

  if (ctre::match<MAX_DIST_IN_METERS_REGEX>(input)) {
    std::string number = input.substr(
        MAX_DIST_IN_METERS.size(),
        input.size() - MAX_DIST_IN_METERS.size() - 1);  // -1: compensate for >
    maxDist_ = std::stoll(number);
  } else {
    AD_THROW(errormessage);
  }

  if (maxDist_ < 0) {
    AD_THROW("the maximum distance between two objects must be >= 0");
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
    return std::move(os).str();
  } else {
    return "incomplete SpatialJoin class";
  }
}

// ____________________________________________________________________________
string SpatialJoin::getDescriptor() const {
  return absl::StrCat("SpatialJoin: ", triple_.s_.getVariable().name(),
                      " max distance of ", std::to_string(maxDist_), " to ",
                      triple_.o_.getVariable().name());
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
      // check after implementation, if it is correct, for now it remains
      // here because otherwise SonarQube complains about costEstimate and
      // sizeEstimate having the same implementation
      return inputEstimate *
             static_cast<size_t>(log(static_cast<double>(inputEstimate)));
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

long long SpatialJoin::getMaxDist() const { return maxDist_; }

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
Id SpatialJoin::computeDist(const IdTable* resLeft, const IdTable* resRight,
                            size_t rowLeft, size_t rowRight,
                            ColumnIndex leftPointCol,
                            ColumnIndex rightPointCol) const {
  auto getPoint = [&](const IdTable* restable, size_t row,
                      ColumnIndex col) -> std::optional<GeoPoint> {
    auto id = restable->at(row, col);
    /*
    std::cerr << "id is " << id << std::endl;
     */
    return id.getDatatype() == Datatype::GeoPoint
               ? std::optional{id.getGeoPoint()}
               : std::nullopt;
  };

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
Result SpatialJoin::baselineAlgorithm() {
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

  const auto [resLeft, keepAliveLeft] = getIdTable(childLeft_);
  const auto [resRight, keepAliveRight] = getIdTable(childRight_);
  ColumnIndex leftJoinCol = getJoinCol(childLeft_, leftChildVariable_);
  ColumnIndex rightJoinCol = getJoinCol(childRight_, rightChildVariable_);
  size_t numColumns = getResultWidth();
  IdTable result{numColumns, _executionContext->getAllocator()};

  // cartesian product with a distance of at most maxDist between the two
  // objects
  for (size_t rowLeft = 0; rowLeft < resLeft->size(); rowLeft++) {
    for (size_t rowRight = 0; rowRight < resRight->size(); rowRight++) {
      Id dist = computeDist(resLeft, resRight, rowLeft, rowRight, leftJoinCol,
                            rightJoinCol);
      if (dist.getDatatype() == Datatype::Int && dist.getInt() <= maxDist_) {
        addResultTableEntry(&result, resLeft, resRight, rowLeft, rowRight,
                            dist);
      }
    }
  }
  return Result(std::move(result), std::vector<ColumnIndex>{}, LocalVocab{});
}

// ____________________________________________________________________________
Result SpatialJoin::computeResult([[maybe_unused]] bool requestLaziness) {
  if (useBaselineAlgorithm_) {
    return baselineAlgorithm();
  } else {
    AD_THROW("Not yet implemented");
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
