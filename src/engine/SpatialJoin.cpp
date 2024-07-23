#include "SpatialJoin.h"

#include "VariableToColumnMap.h"
#include "engine/ExportQueryExecutionTrees.h"
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
    : Operation(qec) {
  triple_ = triple;
  const std::string input = triple.p_._iri;
  std::string errormessage =
      "parsing of the maximum distance for the "
      "SpatialJoin operation was not possible";
  if (input.substr(0, MAX_DIST_IN_METERS.size()) == MAX_DIST_IN_METERS &&
      input[input.size() - 1] == '>') {
    try {
      std::string number =
          input.substr(MAX_DIST_IN_METERS.size(),
                       input.size() - MAX_DIST_IN_METERS.size() -
                           1);  // -1: compensate for >
      for (size_t i = 0; i < number.size(); i++) {
        if (!isdigit(number.at(i))) {
          AD_THROW(errormessage);
        }
      }
      maxDist_ = std::stoll(number);
    } catch (const std::exception& e) {
      LOG(INFO) << "exception: " << e.what() << std::endl;
      AD_THROW(errormessage);
    }
  } else {
    AD_THROW(errormessage);
  }

  if (maxDist_ < 0) {
    AD_THROW("the maximum distance between two objects must be > 0");
  }

  if (childLeft) {
    childLeft_ = childLeft.value();
  }
  if (childRight) {
    childRight_ = childRight.value();
  }

  if (triple.s_.isVariable() && triple.o_.isVariable()) {
    leftChildVariable_ = triple.s_.getVariable();
    rightChildVariable_ = triple.o_.getVariable();
  } else {
    AD_THROW("SpatialJoin needs two variables");
  }
}

// ____________________________________________________________________________
std::shared_ptr<SpatialJoin> SpatialJoin::addChild(
    std::shared_ptr<QueryExecutionTree> child, Variable varOfChild) {
  if (varOfChild == leftChildVariable_) {
    return std::make_shared<SpatialJoin>(getExecutionContext(), triple_.value(),
                                         child, childRight_);
  } else if (varOfChild == rightChildVariable_) {
    return std::make_shared<SpatialJoin>(getExecutionContext(), triple_.value(),
                                         childLeft_, child);
  } else {
    AD_THROW("variable does not match");
  }
}

// ____________________________________________________________________________
bool SpatialJoin::isConstructed() {
  if (childLeft_ && childRight_) {
    return true;
  }
  return false;
}

// ____________________________________________________________________________
std::vector<QueryExecutionTree*> SpatialJoin::getChildren() {
  if (!(childLeft_ && childRight_)) {
    AD_THROW("SpatialJoin needs two variables");
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
  // return "Descriptor of SpatialJoin";
  return "SpatialJoin: " + triple_.value().s_.getVariable().name() +
         " max distance of " + std::to_string(maxDist_) + " to " +
         triple_.value().o_.getVariable().name();
}

// ____________________________________________________________________________
size_t SpatialJoin::getResultWidth() const {
  if (childLeft_ && childRight_) {
    // don't subtract anything because of a common join column. In the case
    // of the spatial join, the join column is different for both sides (e.g.
    // objects with a distance of at most 500m to each other, then each object
    // might contain different positions, which should be kept)
    auto widthChildren =
        childLeft_->getResultWidth() + childRight_->getResultWidth();
    if (addDistToResult_) {
      return widthChildren + 1;
    } else {
      return widthChildren;
    }
  } else {
    return 1;  // dummy return, as the class does not have its children yet
  }
}

// ____________________________________________________________________________
size_t SpatialJoin::getCostEstimate() {
  if (childLeft_ && childRight_) {
    return childLeft_->getSizeEstimate() * childRight_->getSizeEstimate();
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
    return size / multiplicity;
  };

  if (childLeft_ && childRight_) {
    auto varColsLeftMap = childLeft_->getVariableColumns();
    auto varColsRightMap = childRight_->getVariableColumns();
    auto varColMap = computeVariableToColumnMap();
    auto colVarMap = copySortedByColumnIndex(varColMap);
    Variable var = colVarMap.at(col).first;
    auto left = varColsLeftMap.find(var);
    auto right = varColsRightMap.find(var);

    if (var.name() == nameDistanceInternal_) {
      // as each distance is very likely to be unique (even if only after
      // a few decimal places), no multiplicities are assumed
      return 1;
    } else if (left != varColsLeftMap.end()) {
      auto distinctnessChild =
          getDistinctness(childLeft_, left->second.columnIndex_);
      return getSizeEstimate() / distinctnessChild;
    } else if (right != varColsRightMap.end()) {
      auto distinctnessChild =
          getDistinctness(childRight_, right->second.columnIndex_);
      return getSizeEstimate() / distinctnessChild;
    } else {
      AD_FAIL();  // this should not be reachable
    }
    return 1;  // to prevent compiler warning
  } else {
    return 1;  // dummy return, as the class does not have its children yet
  }
}

// ____________________________________________________________________________
bool SpatialJoin::knownEmptyResult() {
  if (childLeft_) {
    if (childLeft_->knownEmptyResult()) {
      return true;
    }
  }

  if (childRight_) {
    if (childRight_->knownEmptyResult()) {
      return true;
    }
  }

  // return false if either both children don't have an empty result, only one
  // child is available, which doesn't have a known empty result or neither
  // child is available
  return false;
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
Result SpatialJoin::computeResult([[maybe_unused]] bool requestLaziness) {
  auto betweenQuotes = [](std::string extractFrom) {
    // returns everything between the first two quotes. If the string does not
    // contain two quotes, the string is returned as a whole
    //
    size_t pos1 = extractFrom.find("\"", 0);
    size_t pos2 = extractFrom.find("\"", pos1 + 1);
    if (pos1 != std::string::npos && pos2 != std::string::npos) {
      return extractFrom.substr(pos1 + 1, pos2 - pos1 - 1);
    } else {
      return extractFrom;
    }
  };

  auto computeDist = [&](const IdTable* resLeft, const IdTable* resRight,
                         size_t rowLeft, size_t rowRight,
                         ColumnIndex leftPointCol, ColumnIndex rightPointCol) {
    std::string point1 = ExportQueryExecutionTrees::idToStringAndType(
                             getExecutionContext()->getIndex(),
                             resLeft->at(rowLeft, leftPointCol), {})
                             .value()
                             .first;
    std::string point2 = ExportQueryExecutionTrees::idToStringAndType(
                             getExecutionContext()->getIndex(),
                             resRight->at(rowRight, rightPointCol), {})
                             .value()
                             .first;
    point1 = betweenQuotes(point1);
    point2 = betweenQuotes(point2);
    double dist = ad_utility::detail::wktDistImpl(point1, point2);
    return static_cast<long long>(dist * 1000);  // convert to meters
  };

  // a maximum distance of 0 encodes infinity -> return cross product
  if (maxDist_ == 0) {
    auto idtable = IdTable(0, allocator_);
    idtable.setNumColumns(getResultWidth());

    std::shared_ptr<const Result> resLeft = childLeft_->getResult();
    std::shared_ptr<const Result> resRight = childRight_->getResult();

    const IdTable* idLeft = &resLeft->idTable();
    const IdTable* idRight = &resRight->idTable();
    size_t numRowsLeft = idLeft->size();

    auto varColMapLeft =
        childLeft_->getRootOperation()->getExternallyVisibleVariableColumns();
    auto varColMapRight =
        childRight_->getRootOperation()->getExternallyVisibleVariableColumns();
    ColumnIndex leftJoinCol =
        varColMapLeft[leftChildVariable_.value()].columnIndex_;
    ColumnIndex rightJoinCol =
        varColMapRight[rightChildVariable_.value()].columnIndex_;

    // iterate through all rows of the left id table
    for (size_t i = 0; i < numRowsLeft; i++) {
      // for each row of the left idtable add all rows of the right idtable
      for (size_t k = 0; k < idRight->size(); k++) {
        idtable.emplace_back();
        size_t col = 0;
        if (addDistToResult_) {
          long long dist =
              computeDist(idLeft, idRight, i, k, leftJoinCol, rightJoinCol);
          idtable[i * numRowsLeft + k][col] = ValueId::makeFromInt(dist);
          col += 1;
        }
        // add coloms from the left id table
        for (size_t l = 0; l < idLeft->numColumns(); l++) {
          idtable[i * numRowsLeft + k][col] = idLeft->at(i, l);
          col += 1;
        }
        // add columns from the right id table
        for (size_t m = 0; m < idRight->numColumns(); m++) {
          idtable[i * numRowsLeft + k][col] = idRight->at(k, m);
          col += 1;
        }
      }
    }

    LocalVocab lv = {};  // let's assume, this has no local vocabs
    std::vector<ColumnIndex> sortedBy = {0};
    return Result(std::move(idtable), {}, std::move(lv));
  } else {
    std::shared_ptr<const Result> resLeft_ = childLeft_->getResult();
    std::shared_ptr<const Result> resRight_ = childRight_->getResult();
    const IdTable* resLeft = &resLeft_->idTable();
    const IdTable* resRight = &resRight_->idTable();
    size_t numColumns = getResultWidth();
    IdTable result{numColumns, allocator_};

    // this lambda function copies elements from copyFrom
    // into the table res. It copies them into the row
    // rowIndRes and column column colIndRes. It returns the column number
    // until which elements were copied
    auto addColumns = [](IdTable* res, const IdTable* copyFrom,
                         size_t rowIndRes, size_t colIndRes,
                         size_t rowIndCopy) {
      size_t col = 0;
      while (col < copyFrom->numColumns()) {
        res->at(rowIndRes, colIndRes) = (*copyFrom).at(rowIndCopy, col);
        colIndRes += 1;
        col += 1;
      }
      return colIndRes;
    };

    // cartesian product with a distance of at most maxDist between the two
    // objects
    size_t resrow = 0;
    auto varColMapLeft =
        childLeft_->getRootOperation()->getExternallyVisibleVariableColumns();
    auto varColMapRight =
        childRight_->getRootOperation()->getExternallyVisibleVariableColumns();
    ColumnIndex leftJoinCol =
        varColMapLeft[leftChildVariable_.value()].columnIndex_;
    ColumnIndex rightJoinCol =
        varColMapRight[rightChildVariable_.value()].columnIndex_;
    for (size_t rowLeft = 0; rowLeft < resLeft->size(); rowLeft++) {
      for (size_t rowRight = 0; rowRight < resRight->size(); rowRight++) {
        size_t rescol = 0;
        long long dist = computeDist(resLeft, resRight, rowLeft, rowRight,
                                     leftJoinCol, rightJoinCol);
        if (dist < maxDist_) {
          result.emplace_back();
          if (addDistToResult_) {
            result.at(resrow, rescol) = ValueId::makeFromInt(dist);
            rescol += 1;
          }
          // add columns to result table
          rescol = addColumns(&result, resLeft, resrow, rescol, rowLeft);
          rescol = addColumns(&result, resRight, resrow, rescol, rowRight);
          resrow += 1;
        }
      }
    }
    return Result(std::move(result), std::vector<ColumnIndex>{}, LocalVocab{});
  }
}

// ____________________________________________________________________________
VariableToColumnMap SpatialJoin::computeVariableToColumnMap() const {
  VariableToColumnMap variableToColumnMap;
  auto makeCol = makePossiblyUndefinedColumn;

  if (!(childLeft_ || childRight_)) {
    // none of the children has been added
    variableToColumnMap[leftChildVariable_.value()] = makeCol(ColumnIndex{0});
    variableToColumnMap[rightChildVariable_.value()] = makeCol(ColumnIndex{1});
  } else if (childLeft_ && !childRight_) {
    // only the left child has been added
    variableToColumnMap[rightChildVariable_.value()] = makeCol(ColumnIndex{1});
  } else if (!childLeft_ && childRight_) {
    // only the right child has been added
    variableToColumnMap[leftChildVariable_.value()] = makeCol(ColumnIndex{0});
  } else {
    // both children have been added to the Operation
    auto varColsLeftMap = childLeft_->getVariableColumns();
    auto varColsRightMap = childRight_->getVariableColumns();
    auto varColsLeftVec = copySortedByColumnIndex(varColsLeftMap);
    auto varColsRightVec = copySortedByColumnIndex(varColsRightMap);
    size_t index = 0;
    if (addDistToResult_) {
      variableToColumnMap[Variable{nameDistanceInternal_}] =
          makeCol(ColumnIndex{index});
      index++;
    }
    for (size_t i = 0; i < varColsLeftVec.size(); i++) {
      variableToColumnMap[varColsLeftVec.at(i).first] =
          makeCol(ColumnIndex{index});
      index++;
    }
    for (size_t i = 0; i < varColsRightVec.size(); i++) {
      variableToColumnMap[varColsRightVec.at(i).first] =
          makeCol(ColumnIndex{index});
      index++;
    }
  }

  return variableToColumnMap;
}
