#include "SpatialJoin.h"
#include "util/AllocatorWithLimit.h"
#include "util/MemorySize/MemorySize.h"
#include "global/ValueId.h"
#include "ValuesForTesting.h"
#include "parser/ParsedQuery.h"
#include "VariableToColumnMap.h"
#include "engine/ExportQueryExecutionTrees.h"
#include "util/GeoSparqlHelpers.h"

// soll eine Klasse sein, welche aus dem nichts ein Ergebnis erzeugt

// note that this class is taking some unnecessary parameters for testing
/* SpatialJoin::SpatialJoin(QueryExecutionContext* qec,
            std::shared_ptr<QueryExecutionTree> t1,
            std::shared_ptr<QueryExecutionTree> t2,
            ColumnIndex t1JoinCol, ColumnIndex t2JoinCol,
            bool keepJoinColumn,
            IdTable leftChildTable,
            std::vector<std::optional<Variable>> variablesLeft,
            IdTable rightChildTable,
            std::vector<std::optional<Variable>> variablesRight)
          : Operation(qec){ */
  /* ValuesForTesting operationLeft = ValuesForTesting(qec,
      std::move(leftChildTable), std::move(variablesLeft));
  ValuesForTesting operationRight = ValuesForTesting(qec,
      std::move(rightChildTable), std::move(variablesRight)); */
  /*_left = ad_utility::makeExecutionTree<ValuesForTesting>(qec,
      std::move(leftChildTable),variablesLeft);
  _right = ad_utility::makeExecutionTree<ValuesForTesting>(qec,
      std::move(rightChildTable), variablesRight);
  _leftJoinCol = t1JoinCol;
  _rightJoinCol = t2JoinCol;
  _keepJoinColumn = keepJoinColumn;
  _variablesLeft = std::move(variablesLeft);
  _variablesRight = std::move(variablesRight);
  _verbose_init = true;
  std::cout << "test in constructor" << std::endl;
}

SpatialJoin::SpatialJoin(QueryExecutionContext* qec,
            std::shared_ptr<QueryExecutionTree> t1,
            std::shared_ptr<QueryExecutionTree> t2,
            ColumnIndex t1JoinCol, ColumnIndex t2JoinCol,
            bool keepJoinColumn)
        : Operation(qec),
        _left(t1),
        _right(t2),
        _leftJoinCol(t1JoinCol),
        _rightJoinCol(t2JoinCol),
        _keepJoinColumn(keepJoinColumn) {
  std::cout << "another test in constructor" << std::endl;
}

SpatialJoin::SpatialJoin() {  // never use as qec of operation is not initialized
    std::cout << "yet another test in constructor" << std::endl;
}

SpatialJoin::SpatialJoin(QueryExecutionContext* qec, SparqlTriple triple) // delete this constructor
      : Operation(qec) {
  std::cout << "so many tests in constructor" << std::endl;
  std::cout << triple._s << " " << triple._p << " " << triple._o
            << " " << std::endl;
    
  if (triple._s.isVariable() && triple._o.isVariable()) {
    leftChildVariable = triple._s.getVariable();
    rightChildVariable = triple._o.getVariable();
  } else {
    AD_THROW("SpatialJoin needs two variables");
  }
} */

SpatialJoin::SpatialJoin(QueryExecutionContext* qec, SparqlTriple triple,
  std::optional<std::shared_ptr<QueryExecutionTree>> childLeft,
  std::optional<std::shared_ptr<QueryExecutionTree>> childRight,
  int maxDist)
            : Operation(qec) {
  this->triple = triple;
  this->maxDist = maxDist;
  LOG(INFO) << "maxDist: " << maxDist << std::endl;
  if (childLeft) {
    this->childLeft = childLeft.value();
  }
  if (childRight) {
    this->childRight = childRight.value();
  }
  
  if (triple._s.isVariable() && triple._o.isVariable()) {
    leftChildVariable = triple._s.getVariable();
    rightChildVariable = triple._o.getVariable();
  } else {
    AD_THROW("SpatialJoin needs two variables");
  }
}

std::shared_ptr<SpatialJoin> SpatialJoin::addChild(
      std::shared_ptr<QueryExecutionTree> child, Variable varOfChild){
  if (varOfChild == leftChildVariable) {
    childLeft = child;
    return std::make_shared<SpatialJoin>(getExecutionContext(), triple.value(),
          childLeft, childRight, maxDist);
  } else if (varOfChild == rightChildVariable) {
    childRight = child;
    return std::make_shared<SpatialJoin>(getExecutionContext(), triple.value(),
          childLeft, childRight, maxDist);
  } else {
    LOG(INFO) << "variable does not match" << varOfChild._name << std::endl;
    AD_THROW("variable does not match");
  }
}

bool SpatialJoin::isConstructed() {
  // if the spatialJoin has both children its construction is done. Then true
  // is returned. This is needed in the QueryPlanner, so that the QueryPlanner
  // stops trying to add children, after it is already constructed
  if (childLeft && childRight) {
    return true;
  }
  return false;
}

std::vector<QueryExecutionTree*> SpatialJoin::getChildren() {
  if (!(childLeft || childRight)) {
    AD_THROW("SpatialJoin needs two variables");
  }

  return {childLeft.get(), childRight.get()};
}


string SpatialJoin::getCacheKeyImpl() const {
  if (childLeft && childRight) {
    std::ostringstream os;
    os << "SpatialJoin\nChild1:\n" << childLeft->getCacheKey() << "\n";
    os << "Child2:\n" << childRight->getCacheKey() << "\n";
    return std::move(os).str();
  } else {
    return "incomplete SpatialJoin class";
  }
}

string SpatialJoin::getDescriptor() const {
  return "Descriptor of SpatialJoin";
}

size_t SpatialJoin::getResultWidth() const {
  if (childLeft && childRight) {
    // don't subtract anything because of a common join column. In the case
    // of the spatial join, the join column is different for both sides (e.g.
    // objects with a distance of at most 500m to each other, then each object
    // might contain different positions, which should be kept)
    if (addDistToResult) {
      return childLeft->getResultWidth() + childRight->getResultWidth() + 1;
    } else {
      return childLeft->getResultWidth() + childRight->getResultWidth();
    }
  } else {
    return 1;  // dummy return, as the class does not have its children yet
  }
}

size_t SpatialJoin::getCostEstimate() {
  return 1;  // dummy return for now
}

uint64_t SpatialJoin::getSizeEstimateBeforeLimit() {
  return 1;  // dummy return for now
}

float SpatialJoin::getMultiplicity(size_t col) {
  return 1;  // dummy return for now
}

bool SpatialJoin::knownEmptyResult() {
  return false;  // dummy return for now
}

vector<ColumnIndex> SpatialJoin::resultSortedOn() const {
  return {};  // dummy return for now
}

ResultTable SpatialJoin::computeResult() {
  /* returns everything between the first two quotes. If the string does not
  * contain two quotes, the string is returned as a whole
  */
  auto betweenQuotes = [] (std::string extractFrom) {
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
                        ColumnIndex leftJoinCol, ColumnIndex rightJoinCol) {
    std::string point1 = ExportQueryExecutionTrees::idToStringAndType(
            getExecutionContext()->getIndex(),
            res_left->at(rowLeft, leftJoinCol), {}).value().first;
    std::string point2 = ExportQueryExecutionTrees::idToStringAndType(
            getExecutionContext()->getIndex(),
            res_right->at(rowRight, rightJoinCol), {}).value().first;
    point1 = betweenQuotes(point1);
    point2 = betweenQuotes(point2);
    double dist = ad_utility::detail::wktDistImpl(point1, point2);
    return dist * 1000;  // convert to meters
  };

  // a maximum distance of 0 encodes infinity -> return cross product
  if (maxDist == 0) {
    LOG(INFO) << "if case" << std::endl;
    IdTable idtable = IdTable(0, _allocator);
    idtable.setNumColumns(getResultWidth());

    std::shared_ptr<const ResultTable> res_left = childLeft->getResult();
    std::shared_ptr<const ResultTable> res_right = childRight->getResult();

    const IdTable* idLeft = &res_left->idTable();
    const IdTable* idRight = &res_right->idTable();
    size_t numRowsLeft = idLeft->size();

    auto varColMapLeft = childLeft->getRootOperation()
          ->getExternallyVisibleVariableColumns();
    auto varColMapRight = childRight->getRootOperation()
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
          double dist = computeDist(idLeft, idRight, i, k,
                          leftJoinCol, rightJoinCol);
          idtable[i * numRowsLeft + k][col] = ValueId::makeFromDouble(dist);
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
    LOG(INFO) << "else case " << std::endl;
    std::shared_ptr<const ResultTable> res_left_ = childLeft->getResult();
    std::shared_ptr<const ResultTable> res_right_ = childRight->getResult();
    const IdTable* res_left = &res_left_->idTable();
    const IdTable* res_right = &res_right_->idTable();
    size_t numColumns = getResultWidth();
    IdTable result{numColumns, _allocator};

    /* this lambda function copies elements from copyFrom
    * into the table res. It copies them into the row
    * row_ind_res and column column col_ind_res. It returns the column number
    * until which elements were copied
    */
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
    auto varColMapLeft = childLeft->getRootOperation()
          ->getExternallyVisibleVariableColumns();
    auto varColMapRight = childRight->getRootOperation()
          ->getExternallyVisibleVariableColumns();
    ColumnIndex leftJoinCol
            = varColMapLeft[leftChildVariable.value()].columnIndex_;
    ColumnIndex rightJoinCol
            = varColMapRight[rightChildVariable.value()].columnIndex_;
    for (size_t rowLeft = 0; rowLeft < res_left->size(); rowLeft++) {
      for (size_t rowRight = 0; rowRight < res_right->size(); rowRight++) {
        size_t rescol = 0;
        double dist = computeDist(res_left, res_right, rowLeft, rowRight,
              leftJoinCol, rightJoinCol);
        if (dist < maxDist) {  // no need to check if maxDist == 0, see if above
          result.emplace_back();
          if (addDistToResult) {
            result.at(resrow, rescol) = ValueId::makeFromDouble(dist);
            rescol += 1;
          }
          // add other columns to result table
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
  LOG(INFO) << "this should never be reached" << std::endl;
  IdTable idtable = IdTable(0, _allocator);
  idtable.setNumColumns(getResultWidth());
  return ResultTable(std::move(idtable), {}, {});
}

/*shared_ptr<const ResultTable> SpatialJoin::geoJoinTest() {
  std::shared_ptr<ResultTable> res = std::make_shared<ResultTable>(
    ResultTable(IdTable(1, _allocator),
          std::vector<ColumnIndex>{}, LocalVocab{}));
  return res;
} */

VariableToColumnMap SpatialJoin::computeVariableToColumnMap() const {
  // welche Variable kommt zu welcher Spalte
  std::cout << "varColMap SpatialJoin " << std::endl;
  VariableToColumnMap variableToColumnMap;
  auto makeCol = makePossiblyUndefinedColumn;
  
  /*Depending on the amount of children the operation returns a different
  VariableToColumnMap. If the operation doesn't have both children it needs
  to aggressively push the queryplanner to add the children, because the
  operation can't exist without them. If it has both children, it can return
  the variable to column map, which will be present, after the operation has
  computed its result*/
  if (!(childLeft || childRight)) {
    // none of the children has been added
    variableToColumnMap[leftChildVariable.value()] = makeCol(ColumnIndex{0});
    variableToColumnMap[rightChildVariable.value()] = makeCol(ColumnIndex{1});
  } else if (childLeft && !childRight) {
    // only the left child has been added
    variableToColumnMap[rightChildVariable.value()] = makeCol(ColumnIndex{1});
  } else if (!childLeft && childRight) {
    // only the right child has been added
    variableToColumnMap[leftChildVariable.value()] = makeCol(ColumnIndex{0});
  } else {
    // both children have been added to the Operation
    auto varColsLeftMap = childLeft->getVariableColumns();
    auto varColsRightMap = childRight->getVariableColumns();
    auto varColsLeftVec = copySortedByColumnIndex(varColsLeftMap);
    auto varColsRightVec = copySortedByColumnIndex(varColsRightMap);
    size_t index = 0;
    if (addDistToResult) {
      variableToColumnMap[Variable{"?distOfTheTwoObjectsAddedInternally"}]
        = makeCol(ColumnIndex{index});
      index++;
    }
    for (size_t i = 0; i < varColsLeftVec.size(); i++) {
      variableToColumnMap[varColsLeftVec.at(i).first] = makeCol(ColumnIndex{index});
      index++;
    }
    for (size_t i = 0; i < varColsRightVec.size(); i++) {
      variableToColumnMap[varColsRightVec.at(i).first] = makeCol(ColumnIndex{index});
      index++;
    }
  }
  return variableToColumnMap;
}