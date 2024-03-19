#include "SpatialJoin.h"
#include "util/AllocatorWithLimit.h"
#include "util/MemorySize/MemorySize.h"
#include "global/ValueId.h"
#include "ValuesForTesting.h"
#include "parser/ParsedQuery.h"
#include "VariableToColumnMap.h"
#include "engine/ExportQueryExecutionTrees.h"
#include "util/GeoSparqlHelpers.h"

// ____________________________________________________________________________
SpatialJoin::SpatialJoin(QueryExecutionContext* qec, SparqlTriple triple,
  std::optional<std::shared_ptr<QueryExecutionTree>> childLeft,
  std::optional<std::shared_ptr<QueryExecutionTree>> childRight)
            : Operation(qec) {
  triple_ = triple;
  const std::string input = triple._p._iri;
  std::string errormessage = "parsing of the maximum distance for the "
      "SpatialJoin operation was not possible";
  if (input.substr(0, MAX_DIST_IN_METERS.size()) == MAX_DIST_IN_METERS &&
          input[input.size() - 1] == '>' ) {
    try {
      std::string number = input.substr(MAX_DIST_IN_METERS.size(),
        input.size() - MAX_DIST_IN_METERS.size()-1);  // -1: compensate for >
      for (size_t i = 0; i < number.size(); i++) {
        if (!isdigit(number.at(i))) {
          AD_THROW(errormessage);
        }
      }
      maxDist = std::stoll(number);
    } catch(const std::exception& e) {
      LOG(INFO) << "exception: " << e.what() << std::endl;
      AD_THROW(errormessage);
    }
  } else {
    AD_THROW(errormessage);
  }

  if (maxDist < 0) {
    AD_THROW("the maximum distance between two objects must be > 0");
  }

  if (childLeft) {
    childLeft_ = childLeft.value();
  }
  if (childRight) {
    childRight_ = childRight.value();
  }
  
  if (triple._s.isVariable() && triple._o.isVariable()) {
    leftChildVariable = triple._s.getVariable();
    rightChildVariable = triple._o.getVariable();
  } else {
    AD_THROW("SpatialJoin needs two variables");
  }
}

// ____________________________________________________________________________
std::shared_ptr<SpatialJoin> SpatialJoin::addChild(
      std::shared_ptr<QueryExecutionTree> child, Variable varOfChild) {
  if (varOfChild == leftChildVariable) {
    childLeft_ = child;
    return std::make_shared<SpatialJoin>(getExecutionContext(), triple_.value(),
          childLeft_, childRight_);
  } else if (varOfChild == rightChildVariable) {
    childRight_ = child;
    return std::make_shared<SpatialJoin>(getExecutionContext(), triple_.value(),
          childLeft_, childRight_);
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
    return std::move(os).str();
  } else {
    return "incomplete SpatialJoin class";
  }
}

// ____________________________________________________________________________
string SpatialJoin::getDescriptor() const {
  return "Descriptor of SpatialJoin";
}

// ____________________________________________________________________________
size_t SpatialJoin::getResultWidth() const {
  if (childLeft_ && childRight_) {
    // don't subtract anything because of a common join column. In the case
    // of the spatial join, the join column is different for both sides (e.g.
    // objects with a distance of at most 500m to each other, then each object
    // might contain different positions, which should be kept)
    auto widthChildren = childLeft_->getResultWidth()
        + childRight_->getResultWidth();
    if (addDistToResult) {
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
    if (maxDist % 1000 == 2) {
      return 500;
    }
  } else {
    LOG(INFO) << "called before both children are added ============================" << std::endl;
  }
  return 1;  // dummy return for now
}

void dummy_print_var_cols_map(VariableToColumnMap vars) {
  for (auto i : vars) {
    LOG(INFO) << "Variable " << i.first.name()
          << " column " << i.second.columnIndex_ << std::endl;
  }
}

void dummy_print_col_var_map(std::vector<std::pair<Variable, ColumnIndexAndTypeInfo>> vars) {
  for (unsigned int i = 0; i < vars.size(); i++) {
    LOG(INFO) << "Variable: " << vars[i].first.name() 
          << " Column: " << vars[i].second.columnIndex_ << std::endl;
  }
}

// ____________________________________________________________________________
float SpatialJoin::getMultiplicity(size_t col) {
  if (childLeft_ && childRight_) {
    auto varColsLeftMap = childLeft_->getVariableColumns();
    auto varColsRightMap = childRight_->getVariableColumns();
    auto sizeLeft = childLeft_->getSizeEstimate();
    auto sizeRight = childRight_->getSizeEstimate();
    auto varColMap = computeVariableToColumnMap();
    auto colVarMap = copySortedByColumnIndex(varColMap);
    Variable var = colVarMap.at(col).first;
    auto left = varColsLeftMap.find(var);
    // testing start
    LOG(INFO) << "starting debugging statements var cols left map" << std::endl;
    dummy_print_var_cols_map(varColsLeftMap);
    LOG(INFO) << "starting debugging statements var cols right map" << std::endl;
    dummy_print_var_cols_map(varColsRightMap);
    LOG(INFO) << "starting debugging statements var col map" << std::endl;
    dummy_print_var_cols_map(varColMap);
    LOG(INFO) << "starting debugging statements col var map" << std::endl;
    dummy_print_col_var_map(colVarMap);
    LOG(INFO) << "size Left " << sizeLeft << std::endl;
    LOG(INFO) << "size right"  << sizeRight << std::endl;
    LOG(INFO) << "Variable " << var.name() << std::endl;
    // testing end
    /*if (left != varColsLeftMap.end()) {
      // multiplicity of var times size of right
      LOG(INFO) << "=============MULTIPLICITY 1" << childLeft_->getMultiplicity(
                    varColsLeftMap[var].columnIndex_) * sizeRight << std::endl;
      return childLeft_->getMultiplicity(
                        varColsLeftMap[var].columnIndex_) * sizeRight;
    } else {
      auto right = varColsRightMap.find(var);
      if (right != varColsRightMap.end()) {
        // multiplicity of right times size of left
        LOG(INFO) << "==========MULTIPLICITY 2" << childRight_->getMultiplicity(
                varColsRightMap[var].columnIndex_) * sizeLeft << std::endl;
        return childRight_->getMultiplicity(
                varColsRightMap[var].columnIndex_) * sizeLeft;
      } else if (var.name().compare(nameDistanceInternal) == 0) {
        LOG(INFO) << "==============MULTIPLICITY 3" << std::endl;
        return 1;  // dummy return for now
      } else {
        AD_THROW("Variable does not match");
      }
    }*/
    // seems like it only works, when this method returns 1, maybe it's
    // because then the join is done on ?geometry instead of ?wkt1
    // return sizeLeft * sizeRight; // this line is only for testing, seems to introduce mistakes elsewhere
    // return 1;
    // testing starting
    if (maxDist % 1000 == 1) {
      return 1;
    } else {
      return sizeLeft * sizeRight;
    }
    // testing ending
  } else {
    LOG(INFO) << "==============MULTIPLICITY 4" << std::endl;
    return 1;  // dummy return, as the class does not have its children yet
  }
}

// ____________________________________________________________________________
bool SpatialJoin::knownEmptyResult() {
  if (childLeft_ && childRight_) {
    return childLeft_->knownEmptyResult() || childRight_->knownEmptyResult();
  }
  return false;  // dummy return, as the class does not have its children yet
}

// ____________________________________________________________________________
vector<ColumnIndex> SpatialJoin::resultSortedOn() const {
  return {};  // dummy return for now
}

long long SpatialJoin::getMaxDist() {
  return maxDist;
}

// ____________________________________________________________________________
ResultTable SpatialJoin::computeResult() {
  auto betweenQuotes = [] (std::string extractFrom) {
    // returns everything between the first two quotes. If the string does not
    // contain two quotes, the string is returned as a whole
    //
    size_t pos1 = extractFrom.find("\"", 0);
    size_t pos2 = extractFrom.find("\"", pos1 + 1);
    if (pos1 != std::string::npos && pos2 != std::string::npos) {
      return extractFrom.substr(pos1 + 1, pos2-pos1-1);
    } else {
      return extractFrom;
    }
  };
  
  auto computeDist = [&](const IdTable* res_left, const IdTable* res_right,
                        size_t rowLeft, size_t rowRight,
                        ColumnIndex leftPointCol, ColumnIndex rightPointCol) {
    std::string point1 = ExportQueryExecutionTrees::idToStringAndType(
            getExecutionContext()->getIndex(),
            res_left->at(rowLeft, leftPointCol), {}).value().first;
    std::string point2 = ExportQueryExecutionTrees::idToStringAndType(
            getExecutionContext()->getIndex(),
            res_right->at(rowRight, rightPointCol), {}).value().first;
    point1 = betweenQuotes(point1);
    point2 = betweenQuotes(point2);
    double dist = ad_utility::detail::wktDistImpl(point1, point2);
    return static_cast<long long>(dist * 1000);  // convert to meters
  };

  // a maximum distance of 0 encodes infinity -> return cross product
  if (maxDist == 0) {
    IdTable idtable = IdTable(0, _allocator);
    idtable.setNumColumns(getResultWidth());

    std::shared_ptr<const ResultTable> res_left = childLeft_->getResult();
    std::shared_ptr<const ResultTable> res_right = childRight_->getResult();

    const IdTable* idLeft = &res_left->idTable();
    const IdTable* idRight = &res_right->idTable();
    size_t numRowsLeft = idLeft->size();

    auto varColMapLeft = childLeft_->getRootOperation()
          ->getExternallyVisibleVariableColumns();
    auto varColMapRight = childRight_->getRootOperation()
          ->getExternallyVisibleVariableColumns();
    ColumnIndex leftJoinCol
            = varColMapLeft[leftChildVariable.value()].columnIndex_;
    ColumnIndex rightJoinCol
            = varColMapRight[rightChildVariable.value()].columnIndex_;
    
    // iterate through all rows of the left id table
    for (size_t i = 0; i < numRowsLeft; i++) {
      // for each row of the left idtable add all rows of the right idtable
      for (size_t k = 0; k < idRight->size(); k++) {
        idtable.emplace_back();
        size_t col = 0;
        if (addDistToResult) {
          long long dist = computeDist(idLeft, idRight, i, k,
                          leftJoinCol, rightJoinCol);
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

    LocalVocab lv = {}; // let's assume, this has no local vocabs
    std::vector<ColumnIndex> sortedBy = {0};
    return ResultTable(std::move(idtable), {}, std::move(lv));
  } else {
    std::shared_ptr<const ResultTable> res_left_ = childLeft_->getResult();
    std::shared_ptr<const ResultTable> res_right_ = childRight_->getResult();
    const IdTable* res_left = &res_left_->idTable();
    const IdTable* res_right = &res_right_->idTable();
    size_t numColumns = getResultWidth();
    IdTable result{numColumns, _allocator};

    // this lambda function copies elements from copyFrom
    // into the table res. It copies them into the row
    // row_ind_res and column column col_ind_res. It returns the column number
    // until which elements were copied
    auto addColumns = [](IdTable* res,
                        const IdTable* copyFrom,
                        size_t row_ind_res,
                        size_t col_ind_res,
                        size_t row_ind_copy) {
      size_t col = 0;
      while (col < copyFrom->numColumns()) {
        res->at(row_ind_res, col_ind_res) = 
                (*copyFrom).at(row_ind_copy, col);
        col_ind_res += 1;
        col += 1;
      }
      return col_ind_res;
    };

    // cartesian product with a distance of at most maxDist between the two
    // objects
    size_t resrow = 0;
    auto varColMapLeft = childLeft_->getRootOperation()
          ->getExternallyVisibleVariableColumns();
    auto varColMapRight = childRight_->getRootOperation()
          ->getExternallyVisibleVariableColumns();
    ColumnIndex leftJoinCol
            = varColMapLeft[leftChildVariable.value()].columnIndex_;
    ColumnIndex rightJoinCol
            = varColMapRight[rightChildVariable.value()].columnIndex_;
    for (size_t rowLeft = 0; rowLeft < res_left->size(); rowLeft++) {
      for (size_t rowRight = 0; rowRight < res_right->size(); rowRight++) {
        size_t rescol = 0;
        long long dist = computeDist(res_left, res_right, rowLeft, rowRight,
              leftJoinCol, rightJoinCol);
        if (dist < maxDist) {
          result.emplace_back();
          if (addDistToResult) {
            result.at(resrow, rescol) = ValueId::makeFromInt(dist);
            rescol += 1;
          }
          // add columns to result table
          rescol = addColumns(&result, res_left, resrow, rescol,
                              rowLeft);
          rescol = addColumns(&result, res_right, resrow, rescol,
                              rowRight);
          resrow += 1;
        }
      }
    }
    return ResultTable(std::move(result),
            std::vector<ColumnIndex>{}, LocalVocab{});
  }
  AD_THROW("SpatialJoin: this line should never be reached");
}

// ____________________________________________________________________________
VariableToColumnMap SpatialJoin::computeVariableToColumnMap() const {
  VariableToColumnMap variableToColumnMap;
  auto makeCol = makePossiblyUndefinedColumn;
  
  if (!(childLeft_ || childRight_)) {
    // none of the children has been added
    variableToColumnMap[leftChildVariable.value()] = makeCol(ColumnIndex{0});
    variableToColumnMap[rightChildVariable.value()] = makeCol(ColumnIndex{1});
  } else if (childLeft_ && !childRight_) {
    // only the left child has been added
    variableToColumnMap[rightChildVariable.value()] = makeCol(ColumnIndex{1});
  } else if (!childLeft_ && childRight_) {
    // only the right child has been added
    variableToColumnMap[leftChildVariable.value()] = makeCol(ColumnIndex{0});
  } else {
    // both children have been added to the Operation
    auto varColsLeftMap = childLeft_->getVariableColumns();
    auto varColsRightMap = childRight_->getVariableColumns();
    auto varColsLeftVec = copySortedByColumnIndex(varColsLeftMap);
    auto varColsRightVec = copySortedByColumnIndex(varColsRightMap);
    size_t index = 0;
    if (addDistToResult) {
      variableToColumnMap[Variable{nameDistanceInternal}]
        = makeCol(ColumnIndex{index});
      index++;
    }
    for (size_t i = 0; i < varColsLeftVec.size(); i++) {
      variableToColumnMap[varColsLeftVec.at(i).first]
        = makeCol(ColumnIndex{index});
      index++;
    }
    for (size_t i = 0; i < varColsRightVec.size(); i++) {
      variableToColumnMap[varColsRightVec.at(i).first]
        = makeCol(ColumnIndex{index});
      index++;
    }
  }
  
  return variableToColumnMap;
}
