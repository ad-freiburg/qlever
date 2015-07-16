// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)

#include <sstream>
#include <unordered_map>
#include "./QueryExecutionTree.h"
#include "./Filter.h"


using std::string;
using std::unordered_map;

// _____________________________________________________________________________
size_t Filter::getResultWidth() const {
  return _subtree->getResultWidth();
}

// _____________________________________________________________________________
Filter::Filter(QueryExecutionContext* qec, const QueryExecutionTree& subtree,
               SparqlFilter::FilterType type, size_t lhsInd, size_t rhsInd) :
    Operation(qec),
    _subtree(new QueryExecutionTree(subtree)),
    _type(type),
    _lhsInd(lhsInd),
    _rhsInd(rhsInd) {
}

// _____________________________________________________________________________
Filter::Filter(const Filter& other) :
    Operation(other._executionContext),
    _subtree(new QueryExecutionTree(*other._subtree)),
    _type(other._type),
    _lhsInd(other._lhsInd),
    _rhsInd(other._rhsInd) {
}

// _____________________________________________________________________________
Filter& Filter::operator=(const Filter& other) {
  delete _subtree;
  _executionContext = other._executionContext;
  _subtree = new QueryExecutionTree(*other._subtree);
  _type = other._type;
  _lhsInd = other._lhsInd;
  _rhsInd = other._rhsInd;
  return *this;
}

// _____________________________________________________________________________
Filter::~Filter() {
  delete _subtree;
}

// _____________________________________________________________________________
string Filter::asString() const {
  std::ostringstream os;
  os << "Filter " << _subtree->asString() << " with ";
  switch (_type) {
    case SparqlFilter::EQ :
      os << "col " << _lhsInd << " == col " << _rhsInd << '\n';
      break;
    case SparqlFilter::NE :
      os << "col " << _lhsInd << " != col " << _rhsInd << '\n';
      break;
    case SparqlFilter::LT :
      os << "col " << _lhsInd << " < col " << _rhsInd << '\n';
      break;
    case SparqlFilter::LE :
      os << "col " << _lhsInd << " <= col " << _rhsInd << '\n';
      break;
    case SparqlFilter::GT :
      os << "col " << _lhsInd << " > col " << _rhsInd << '\n';
      break;
    case SparqlFilter::GE :
      os << "col " << _lhsInd << " >= col " << _rhsInd << '\n';
      break;
  }

  return os.str();
}

// _____________________________________________________________________________
void Filter::computeResult(ResultTable* result) const {
  LOG(DEBUG) << "Filter result computation..." << endl;
  const ResultTable& subRes = _subtree->getResult();
  result->_nofColumns = subRes._nofColumns;
  size_t l = _lhsInd;
  size_t r = _rhsInd;
  switch (subRes._nofColumns) {
    case 1: {
      typedef array<Id, 1> RT;
      auto res = new vector<RT>();
      result->_fixedSizeData = res;
      switch (_type) {
        case SparqlFilter::EQ:
          getEngine()
              .filter(
                  *static_cast<vector<RT>*>(subRes._fixedSizeData),
                  [&l, &r](const RT& e) {
                    return e[l] == e[r];
                  }, res);
          break;
        case SparqlFilter::NE:
          getEngine()
              .filter(
                  *static_cast<vector<RT>*>(subRes._fixedSizeData),
                  [&l, &r](const RT& e) {
                    return e[l] != e[r];
                  }, res);
          break;
        case SparqlFilter::LT:
          getEngine()
              .filter(
                  *static_cast<vector<RT>*>(subRes._fixedSizeData),
                  [&l, &r](const RT& e) {
                    return e[l] < e[r];
                  }, res);
          break;
        case SparqlFilter::LE:
          getEngine()
              .filter(
                  *static_cast<vector<RT>*>(subRes._fixedSizeData),
                  [&l, &r](const RT& e) {
                    return e[l] <= e[r];
                  }, res);
          break;
        case SparqlFilter::GT:
          getEngine()
              .filter(
                  *static_cast<vector<RT>*>(subRes._fixedSizeData),
                  [&l, &r](const RT& e) {
                    return e[l] > e[r];
                  }, res);
          break;
        case SparqlFilter::GE:
          getEngine()
              .filter(
                  *static_cast<vector<RT>*>(subRes._fixedSizeData),
                  [&l, &r](const RT& e) {
                    return e[l] >= e[r];
                  }, res);
          break;
      }
      break;
    }
    case 2: {
      typedef array<Id, 2> RT;
      auto res = new vector<RT>();
      result->_fixedSizeData = res;
      switch (_type) {
        case SparqlFilter::EQ:
          getEngine()
              .filter(
                  *static_cast<vector<RT>*>(subRes._fixedSizeData),
                  [&l, &r](const RT& e) {
                    return e[l] == e[r];
                  }, res);
          break;
        case SparqlFilter::NE:
          getEngine()
              .filter(
                  *static_cast<vector<RT>*>(subRes._fixedSizeData),
                  [&l, &r](const RT& e) {
                    return e[l] != e[r];
                  }, res);
          break;
        case SparqlFilter::LT:
          getEngine()
              .filter(
                  *static_cast<vector<RT>*>(subRes._fixedSizeData),
                  [&l, &r](const RT& e) {
                    return e[l] < e[r];
                  }, res);
          break;
        case SparqlFilter::LE:
          getEngine()
              .filter(
                  *static_cast<vector<RT>*>(subRes._fixedSizeData),
                  [&l, &r](const RT& e) {
                    return e[l] <= e[r];
                  }, res);
          break;
        case SparqlFilter::GT:
          getEngine()
              .filter(
                  *static_cast<vector<RT>*>(subRes._fixedSizeData),
                  [&l, &r](const RT& e) {
                    return e[l] > e[r];
                  }, res);
          break;
        case SparqlFilter::GE:
          getEngine()
              .filter(
                  *static_cast<vector<RT>*>(subRes._fixedSizeData),
                  [&l, &r](const RT& e) {
                    return e[l] >= e[r];
                  }, res);
          break;
      }
      break;
    }
    case 3: {
      typedef array<Id, 3> RT;
      auto res = new vector<RT>();
      result->_fixedSizeData = res;
      switch (_type) {
        case SparqlFilter::EQ:
          getEngine()
              .filter(
                  *static_cast<vector<RT>*>(subRes._fixedSizeData),
                  [&l, &r](const RT& e) {
                    return e[l] == e[r];
                  }, res);
          break;
        case SparqlFilter::NE:
          getEngine()
              .filter(
                  *static_cast<vector<RT>*>(subRes._fixedSizeData),
                  [&l, &r](const RT& e) {
                    return e[l] != e[r];
                  }, res);
          break;
        case SparqlFilter::LT:
          getEngine()
              .filter(
                  *static_cast<vector<RT>*>(subRes._fixedSizeData),
                  [&l, &r](const RT& e) {
                    return e[l] < e[r];
                  }, res);
          break;
        case SparqlFilter::LE:
          getEngine()
              .filter(
                  *static_cast<vector<RT>*>(subRes._fixedSizeData),
                  [&l, &r](const RT& e) {
                    return e[l] <= e[r];
                  }, res);
          break;
        case SparqlFilter::GT:
          getEngine()
              .filter(
                  *static_cast<vector<RT>*>(subRes._fixedSizeData),
                  [&l, &r](const RT& e) {
                    return e[l] > e[r];
                  }, res);
          break;
        case SparqlFilter::GE:
          getEngine()
              .filter(
                  *static_cast<vector<RT>*>(subRes._fixedSizeData),
                  [&l, &r](const RT& e) {
                    return e[l] >= e[r];
                  }, res);
          break;
      }
      break;
    }
    case 4: {
      typedef array<Id, 4> RT;
      auto res = new vector<RT>();
      result->_fixedSizeData = res;
      switch (_type) {
        case SparqlFilter::EQ:
          getEngine()
              .filter(
                  *static_cast<vector<RT>*>(subRes._fixedSizeData),
                  [&l, &r](const RT& e) {
                    return e[l] == e[r];
                  }, res);
          break;
        case SparqlFilter::NE:
          getEngine()
              .filter(
                  *static_cast<vector<RT>*>(subRes._fixedSizeData),
                  [&l, &r](const RT& e) {
                    return e[l] != e[r];
                  }, res);
          break;
        case SparqlFilter::LT:
          getEngine()
              .filter(
                  *static_cast<vector<RT>*>(subRes._fixedSizeData),
                  [&l, &r](const RT& e) {
                    return e[l] < e[r];
                  }, res);
          break;
        case SparqlFilter::LE:
          getEngine()
              .filter(
                  *static_cast<vector<RT>*>(subRes._fixedSizeData),
                  [&l, &r](const RT& e) {
                    return e[l] <= e[r];
                  }, res);
          break;
        case SparqlFilter::GT:
          getEngine()
              .filter(
                  *static_cast<vector<RT>*>(subRes._fixedSizeData),
                  [&l, &r](const RT& e) {
                    return e[l] > e[r];
                  }, res);
          break;
        case SparqlFilter::GE:
          getEngine()
              .filter(
                  *static_cast<vector<RT>*>(subRes._fixedSizeData),
                  [&l, &r](const RT& e) {
                    return e[l] >= e[r];
                  }, res);
          break;
      }
      break;
    }
    case 5: {
      typedef array<Id, 5> RT;
      auto res = new vector<RT>();
      result->_fixedSizeData = res;
      switch (_type) {
        case SparqlFilter::EQ:
          getEngine()
              .filter(
                  *static_cast<vector<RT>*>(subRes._fixedSizeData),
                  [&l, &r](const RT& e) {
                    return e[l] == e[r];
                  }, res);
          break;
        case SparqlFilter::NE:
          getEngine()
              .filter(
                  *static_cast<vector<RT>*>(subRes._fixedSizeData),
                  [&l, &r](const RT& e) {
                    return e[l] != e[r];
                  }, res);
          break;
        case SparqlFilter::LT:
          getEngine()
              .filter(
                  *static_cast<vector<RT>*>(subRes._fixedSizeData),
                  [&l, &r](const RT& e) {
                    return e[l] < e[r];
                  }, res);
          break;
        case SparqlFilter::LE:
          getEngine()
              .filter(
                  *static_cast<vector<RT>*>(subRes._fixedSizeData),
                  [&l, &r](const RT& e) {
                    return e[l] <= e[r];
                  }, res);
          break;
        case SparqlFilter::GT:
          getEngine()
              .filter(
                  *static_cast<vector<RT>*>(subRes._fixedSizeData),
                  [&l, &r](const RT& e) {
                    return e[l] > e[r];
                  }, res);
          break;
        case SparqlFilter::GE:
          getEngine()
              .filter(
                  *static_cast<vector<RT>*>(subRes._fixedSizeData),
                  [&l, &r](const RT& e) {
                    return e[l] >= e[r];
                  }, res);
          break;
      }
      break;
    }
    default: {
      typedef vector<Id> RT;
      switch (_type) {
        case SparqlFilter::EQ:
          getEngine()
              .filter(
                  subRes._varSizeData,
                  [&l, &r](const RT& e) {
                    return e[l] == e[r];
                  }, &result->_varSizeData);
          break;
        case SparqlFilter::NE:
          getEngine()
              .filter(
                  subRes._varSizeData,
                  [&l, &r](const RT& e) {
                    return e[l] != e[r];
                  }, &result->_varSizeData);
          break;
        case SparqlFilter::LT:
          getEngine()
              .filter(
                  subRes._varSizeData,
                  [&l, &r](const RT& e) {
                    return e[l] < e[r];
                  }, &result->_varSizeData);
          break;
        case SparqlFilter::LE:
          getEngine()
              .filter(
                  subRes._varSizeData,
                  [&l, &r](const RT& e) {
                    return e[l] <= e[r];
                  }, &result->_varSizeData);
          break;
        case SparqlFilter::GT:
          getEngine()
              .filter(
                  subRes._varSizeData,
                  [&l, &r](const RT& e) {
                    return e[l] > e[r];
                  }, &result->_varSizeData);
          break;
        case SparqlFilter::GE:
          getEngine()
              .filter(
                  subRes._varSizeData,
                  [&l, &r](const RT& e) {
                    return e[l] >= e[r];
                  }, &result->_varSizeData);
          break;
      }
      break;
    }
  }
  result->_status = ResultTable::FINISHED;
  LOG(DEBUG) << "Filter result computation done." << endl;
}
