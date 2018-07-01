// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)

#include "./Filter.h"
#include <sstream>
#include "./QueryExecutionTree.h"

using std::string;

// _____________________________________________________________________________
size_t Filter::getResultWidth() const { return _subtree->getResultWidth(); }

// _____________________________________________________________________________
Filter::Filter(QueryExecutionContext* qec,
               std::shared_ptr<QueryExecutionTree> subtree,
               SparqlFilter::FilterType type, size_t lhsInd, size_t rhsInd,
               Id rhsId)
    : Operation(qec),
      _subtree(subtree),
      _type(type),
      _lhsInd(lhsInd),
      _rhsInd(rhsInd),
      _rhsId(rhsId) {
  AD_CHECK(rhsId == std::numeric_limits<Id>::max() ||
           rhsInd == std::numeric_limits<size_t>::max());
}

// _____________________________________________________________________________
string Filter::asString(size_t indent) const {
  std::ostringstream os;
  for (size_t i = 0; i < indent; ++i) {
    os << " ";
  }
  os << "FILTER " << _subtree->asString(indent) << "\n";
  for (size_t i = 0; i < indent; ++i) {
    os << " ";
  }
  os << " with col " << _lhsInd;
  switch (_type) {
    case SparqlFilter::EQ:
      os << " == ";
      break;
    case SparqlFilter::NE:
      os << " != ";
      break;
    case SparqlFilter::LT:
      os << " < ";
      break;
    case SparqlFilter::LE:
      os << " <= ";
      break;
    case SparqlFilter::GT:
      os << " > ";
      break;
    case SparqlFilter::GE:
      os << " <= ";
      break;
  }
  if (_rhsInd != std::numeric_limits<size_t>::max()) {
    os << "col " << _rhsInd;
  } else {
    os << "entity Id " << _rhsId;
  }

  return os.str();
}

// _____________________________________________________________________________
void Filter::computeResult(ResultTable* result) const {
  LOG(DEBUG) << "Getting sub-result for Filter result computation..." << endl;
  shared_ptr<const ResultTable> subRes = _subtree->getResult();
  LOG(DEBUG) << "Filter result computation..." << endl;
  result->_nofColumns = subRes->_nofColumns;
  result->_resultTypes.insert(result->_resultTypes.end(),
                              subRes->_resultTypes.begin(),
                              subRes->_resultTypes.end());
  size_t l = _lhsInd;
  size_t r = _rhsInd;
  if (r == std::numeric_limits<size_t>::max()) {
    AD_CHECK(_rhsId != std::numeric_limits<size_t>::max());
    return computeResultFixedValue(result);
  }
  switch (subRes->_nofColumns) {
    case 1: {
      typedef array<Id, 1> RT;
      auto res = new vector<RT>();
      result->_fixedSizeData = res;
      switch (_type) {
        case SparqlFilter::EQ:
          getEngine().filter(*static_cast<vector<RT>*>(subRes->_fixedSizeData),
                             [&l, &r](const RT& e) { return e[l] == e[r]; },
                             res);
          break;
        case SparqlFilter::NE:
          getEngine().filter(*static_cast<vector<RT>*>(subRes->_fixedSizeData),
                             [&l, &r](const RT& e) { return e[l] != e[r]; },
                             res);
          break;
        case SparqlFilter::LT:
          getEngine().filter(*static_cast<vector<RT>*>(subRes->_fixedSizeData),
                             [&l, &r](const RT& e) { return e[l] < e[r]; },
                             res);
          break;
        case SparqlFilter::LE:
          getEngine().filter(*static_cast<vector<RT>*>(subRes->_fixedSizeData),
                             [&l, &r](const RT& e) { return e[l] <= e[r]; },
                             res);
          break;
        case SparqlFilter::GT:
          getEngine().filter(*static_cast<vector<RT>*>(subRes->_fixedSizeData),
                             [&l, &r](const RT& e) { return e[l] > e[r]; },
                             res);
          break;
        case SparqlFilter::GE:
          getEngine().filter(*static_cast<vector<RT>*>(subRes->_fixedSizeData),
                             [&l, &r](const RT& e) { return e[l] >= e[r]; },
                             res);
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
          getEngine().filter(*static_cast<vector<RT>*>(subRes->_fixedSizeData),
                             [&l, &r](const RT& e) { return e[l] == e[r]; },
                             res);
          break;
        case SparqlFilter::NE:
          getEngine().filter(*static_cast<vector<RT>*>(subRes->_fixedSizeData),
                             [&l, &r](const RT& e) { return e[l] != e[r]; },
                             res);
          break;
        case SparqlFilter::LT:
          getEngine().filter(*static_cast<vector<RT>*>(subRes->_fixedSizeData),
                             [&l, &r](const RT& e) { return e[l] < e[r]; },
                             res);
          break;
        case SparqlFilter::LE:
          getEngine().filter(*static_cast<vector<RT>*>(subRes->_fixedSizeData),
                             [&l, &r](const RT& e) { return e[l] <= e[r]; },
                             res);
          break;
        case SparqlFilter::GT:
          getEngine().filter(*static_cast<vector<RT>*>(subRes->_fixedSizeData),
                             [&l, &r](const RT& e) { return e[l] > e[r]; },
                             res);
          break;
        case SparqlFilter::GE:
          getEngine().filter(*static_cast<vector<RT>*>(subRes->_fixedSizeData),
                             [&l, &r](const RT& e) { return e[l] >= e[r]; },
                             res);
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
          getEngine().filter(*static_cast<vector<RT>*>(subRes->_fixedSizeData),
                             [&l, &r](const RT& e) { return e[l] == e[r]; },
                             res);
          break;
        case SparqlFilter::NE:
          getEngine().filter(*static_cast<vector<RT>*>(subRes->_fixedSizeData),
                             [&l, &r](const RT& e) { return e[l] != e[r]; },
                             res);
          break;
        case SparqlFilter::LT:
          getEngine().filter(*static_cast<vector<RT>*>(subRes->_fixedSizeData),
                             [&l, &r](const RT& e) { return e[l] < e[r]; },
                             res);
          break;
        case SparqlFilter::LE:
          getEngine().filter(*static_cast<vector<RT>*>(subRes->_fixedSizeData),
                             [&l, &r](const RT& e) { return e[l] <= e[r]; },
                             res);
          break;
        case SparqlFilter::GT:
          getEngine().filter(*static_cast<vector<RT>*>(subRes->_fixedSizeData),
                             [&l, &r](const RT& e) { return e[l] > e[r]; },
                             res);
          break;
        case SparqlFilter::GE:
          getEngine().filter(*static_cast<vector<RT>*>(subRes->_fixedSizeData),
                             [&l, &r](const RT& e) { return e[l] >= e[r]; },
                             res);
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
          getEngine().filter(*static_cast<vector<RT>*>(subRes->_fixedSizeData),
                             [&l, &r](const RT& e) { return e[l] == e[r]; },
                             res);
          break;
        case SparqlFilter::NE:
          getEngine().filter(*static_cast<vector<RT>*>(subRes->_fixedSizeData),
                             [&l, &r](const RT& e) { return e[l] != e[r]; },
                             res);
          break;
        case SparqlFilter::LT:
          getEngine().filter(*static_cast<vector<RT>*>(subRes->_fixedSizeData),
                             [&l, &r](const RT& e) { return e[l] < e[r]; },
                             res);
          break;
        case SparqlFilter::LE:
          getEngine().filter(*static_cast<vector<RT>*>(subRes->_fixedSizeData),
                             [&l, &r](const RT& e) { return e[l] <= e[r]; },
                             res);
          break;
        case SparqlFilter::GT:
          getEngine().filter(*static_cast<vector<RT>*>(subRes->_fixedSizeData),
                             [&l, &r](const RT& e) { return e[l] > e[r]; },
                             res);
          break;
        case SparqlFilter::GE:
          getEngine().filter(*static_cast<vector<RT>*>(subRes->_fixedSizeData),
                             [&l, &r](const RT& e) { return e[l] >= e[r]; },
                             res);
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
          getEngine().filter(*static_cast<vector<RT>*>(subRes->_fixedSizeData),
                             [&l, &r](const RT& e) { return e[l] == e[r]; },
                             res);
          break;
        case SparqlFilter::NE:
          getEngine().filter(*static_cast<vector<RT>*>(subRes->_fixedSizeData),
                             [&l, &r](const RT& e) { return e[l] != e[r]; },
                             res);
          break;
        case SparqlFilter::LT:
          getEngine().filter(*static_cast<vector<RT>*>(subRes->_fixedSizeData),
                             [&l, &r](const RT& e) { return e[l] < e[r]; },
                             res);
          break;
        case SparqlFilter::LE:
          getEngine().filter(*static_cast<vector<RT>*>(subRes->_fixedSizeData),
                             [&l, &r](const RT& e) { return e[l] <= e[r]; },
                             res);
          break;
        case SparqlFilter::GT:
          getEngine().filter(*static_cast<vector<RT>*>(subRes->_fixedSizeData),
                             [&l, &r](const RT& e) { return e[l] > e[r]; },
                             res);
          break;
        case SparqlFilter::GE:
          getEngine().filter(*static_cast<vector<RT>*>(subRes->_fixedSizeData),
                             [&l, &r](const RT& e) { return e[l] >= e[r]; },
                             res);
          break;
      }
      break;
    }
    default: {
      typedef vector<Id> RT;
      switch (_type) {
        case SparqlFilter::EQ:
          getEngine().filter(subRes->_varSizeData,
                             [&l, &r](const RT& e) { return e[l] == e[r]; },
                             &result->_varSizeData);
          break;
        case SparqlFilter::NE:
          getEngine().filter(subRes->_varSizeData,
                             [&l, &r](const RT& e) { return e[l] != e[r]; },
                             &result->_varSizeData);
          break;
        case SparqlFilter::LT:
          getEngine().filter(subRes->_varSizeData,
                             [&l, &r](const RT& e) { return e[l] < e[r]; },
                             &result->_varSizeData);
          break;
        case SparqlFilter::LE:
          getEngine().filter(subRes->_varSizeData,
                             [&l, &r](const RT& e) { return e[l] <= e[r]; },
                             &result->_varSizeData);
          break;
        case SparqlFilter::GT:
          getEngine().filter(subRes->_varSizeData,
                             [&l, &r](const RT& e) { return e[l] > e[r]; },
                             &result->_varSizeData);
          break;
        case SparqlFilter::GE:
          getEngine().filter(subRes->_varSizeData,
                             [&l, &r](const RT& e) { return e[l] >= e[r]; },
                             &result->_varSizeData);
          break;
      }
      break;
    }
  }
  result->finish();
  LOG(DEBUG) << "Filter result computation done." << endl;
}

// _____________________________________________________________________________
void Filter::computeResultFixedValue(ResultTable* result) const {
  LOG(DEBUG) << "Filter result computation..." << endl;
  shared_ptr<const ResultTable> subRes = _subtree->getResult();
  result->_nofColumns = subRes->_nofColumns;
  size_t l = _lhsInd;
  Id r = _rhsId;
  switch (subRes->_nofColumns) {
    case 1: {
      typedef array<Id, 1> RT;
      auto res = new vector<RT>();
      result->_fixedSizeData = res;
      switch (_type) {
        case SparqlFilter::EQ:
          getEngine().filter(*static_cast<vector<RT>*>(subRes->_fixedSizeData),
                             [&l, &r](const RT& e) { return e[l] == r; }, res);
          break;
        case SparqlFilter::NE:
          getEngine().filter(*static_cast<vector<RT>*>(subRes->_fixedSizeData),
                             [&l, &r](const RT& e) { return e[l] != r; }, res);
          break;
        case SparqlFilter::LT:
          getEngine().filter(*static_cast<vector<RT>*>(subRes->_fixedSizeData),
                             [&l, &r](const RT& e) { return e[l] < r; }, res);
          break;
        case SparqlFilter::LE:
          getEngine().filter(*static_cast<vector<RT>*>(subRes->_fixedSizeData),
                             [&l, &r](const RT& e) { return e[l] <= r; }, res);
          break;
        case SparqlFilter::GT:
          getEngine().filter(*static_cast<vector<RT>*>(subRes->_fixedSizeData),
                             [&l, &r](const RT& e) { return e[l] > r; }, res);
          break;
        case SparqlFilter::GE:
          getEngine().filter(*static_cast<vector<RT>*>(subRes->_fixedSizeData),
                             [&l, &r](const RT& e) { return e[l] >= r; }, res);
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
          getEngine().filter(*static_cast<vector<RT>*>(subRes->_fixedSizeData),
                             [&l, &r](const RT& e) { return e[l] == r; }, res);
          break;
        case SparqlFilter::NE:
          getEngine().filter(*static_cast<vector<RT>*>(subRes->_fixedSizeData),
                             [&l, &r](const RT& e) { return e[l] != r; }, res);
          break;
        case SparqlFilter::LT:
          getEngine().filter(*static_cast<vector<RT>*>(subRes->_fixedSizeData),
                             [&l, &r](const RT& e) { return e[l] < r; }, res);
          break;
        case SparqlFilter::LE:
          getEngine().filter(*static_cast<vector<RT>*>(subRes->_fixedSizeData),
                             [&l, &r](const RT& e) { return e[l] <= r; }, res);
          break;
        case SparqlFilter::GT:
          getEngine().filter(*static_cast<vector<RT>*>(subRes->_fixedSizeData),
                             [&l, &r](const RT& e) { return e[l] > r; }, res);
          break;
        case SparqlFilter::GE:
          getEngine().filter(*static_cast<vector<RT>*>(subRes->_fixedSizeData),
                             [&l, &r](const RT& e) { return e[l] >= r; }, res);
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
          getEngine().filter(*static_cast<vector<RT>*>(subRes->_fixedSizeData),
                             [&l, &r](const RT& e) { return e[l] == r; }, res);
          break;
        case SparqlFilter::NE:
          getEngine().filter(*static_cast<vector<RT>*>(subRes->_fixedSizeData),
                             [&l, &r](const RT& e) { return e[l] != r; }, res);
          break;
        case SparqlFilter::LT:
          getEngine().filter(*static_cast<vector<RT>*>(subRes->_fixedSizeData),
                             [&l, &r](const RT& e) { return e[l] < r; }, res);
          break;
        case SparqlFilter::LE:
          getEngine().filter(*static_cast<vector<RT>*>(subRes->_fixedSizeData),
                             [&l, &r](const RT& e) { return e[l] <= r; }, res);
          break;
        case SparqlFilter::GT:
          getEngine().filter(*static_cast<vector<RT>*>(subRes->_fixedSizeData),
                             [&l, &r](const RT& e) { return e[l] > r; }, res);
          break;
        case SparqlFilter::GE:
          getEngine().filter(*static_cast<vector<RT>*>(subRes->_fixedSizeData),
                             [&l, &r](const RT& e) { return e[l] >= r; }, res);
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
          getEngine().filter(*static_cast<vector<RT>*>(subRes->_fixedSizeData),
                             [&l, &r](const RT& e) { return e[l] == r; }, res);
          break;
        case SparqlFilter::NE:
          getEngine().filter(*static_cast<vector<RT>*>(subRes->_fixedSizeData),
                             [&l, &r](const RT& e) { return e[l] != r; }, res);
          break;
        case SparqlFilter::LT:
          getEngine().filter(*static_cast<vector<RT>*>(subRes->_fixedSizeData),
                             [&l, &r](const RT& e) { return e[l] < r; }, res);
          break;
        case SparqlFilter::LE:
          getEngine().filter(*static_cast<vector<RT>*>(subRes->_fixedSizeData),
                             [&l, &r](const RT& e) { return e[l] <= r; }, res);
          break;
        case SparqlFilter::GT:
          getEngine().filter(*static_cast<vector<RT>*>(subRes->_fixedSizeData),
                             [&l, &r](const RT& e) { return e[l] > r; }, res);
          break;
        case SparqlFilter::GE:
          getEngine().filter(*static_cast<vector<RT>*>(subRes->_fixedSizeData),
                             [&l, &r](const RT& e) { return e[l] >= r; }, res);
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
          getEngine().filter(*static_cast<vector<RT>*>(subRes->_fixedSizeData),
                             [&l, &r](const RT& e) { return e[l] == r; }, res);
          break;
        case SparqlFilter::NE:
          getEngine().filter(*static_cast<vector<RT>*>(subRes->_fixedSizeData),
                             [&l, &r](const RT& e) { return e[l] != r; }, res);
          break;
        case SparqlFilter::LT:
          getEngine().filter(*static_cast<vector<RT>*>(subRes->_fixedSizeData),
                             [&l, &r](const RT& e) { return e[l] < r; }, res);
          break;
        case SparqlFilter::LE:
          getEngine().filter(*static_cast<vector<RT>*>(subRes->_fixedSizeData),
                             [&l, &r](const RT& e) { return e[l] <= r; }, res);
          break;
        case SparqlFilter::GT:
          getEngine().filter(*static_cast<vector<RT>*>(subRes->_fixedSizeData),
                             [&l, &r](const RT& e) { return e[l] > r; }, res);
          break;
        case SparqlFilter::GE:
          getEngine().filter(*static_cast<vector<RT>*>(subRes->_fixedSizeData),
                             [&l, &r](const RT& e) { return e[l] >= r; }, res);
          break;
      }
      break;
    }
    default: {
      typedef vector<Id> RT;
      switch (_type) {
        case SparqlFilter::EQ:
          getEngine().filter(subRes->_varSizeData,
                             [&l, &r](const RT& e) { return e[l] == r; },
                             &result->_varSizeData);
          break;
        case SparqlFilter::NE:
          getEngine().filter(subRes->_varSizeData,
                             [&l, &r](const RT& e) { return e[l] != r; },
                             &result->_varSizeData);
          break;
        case SparqlFilter::LT:
          getEngine().filter(subRes->_varSizeData,
                             [&l, &r](const RT& e) { return e[l] < r; },
                             &result->_varSizeData);
          break;
        case SparqlFilter::LE:
          getEngine().filter(subRes->_varSizeData,
                             [&l, &r](const RT& e) { return e[l] <= r; },
                             &result->_varSizeData);
          break;
        case SparqlFilter::GT:
          getEngine().filter(subRes->_varSizeData,
                             [&l, &r](const RT& e) { return e[l] > r; },
                             &result->_varSizeData);
          break;
        case SparqlFilter::GE:
          getEngine().filter(subRes->_varSizeData,
                             [&l, &r](const RT& e) { return e[l] >= r; },
                             &result->_varSizeData);
          break;
      }
      break;
    }
  }
  result->finish();
  LOG(DEBUG) << "Filter result computation done." << endl;
}
