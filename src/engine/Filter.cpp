// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)

#include "./Filter.h"
#include <optional>
#include <sstream>
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
      os << " >= ";
      break;
    case SparqlFilter::LANG_MATCHES:
      os << " LANG_MATCHES " << _rhsString;
      break;
    case SparqlFilter::REGEX:
      os << " REGEX ";
      if (_regexIgnoreCase) {
        os << "ignoring case ";
      }
      os << _rhsString;
      break;
  }
  if (_type != SparqlFilter::LANG_MATCHES && _type != SparqlFilter::REGEX) {
    if (_rhsInd != std::numeric_limits<size_t>::max()) {
      os << "col " << _rhsInd;
    } else {
      os << "entity Id " << _rhsId;
    }
  }

  return os.str();
}

// _____________________________________________________________________________
template <class RT>
vector<RT>* Filter::computeFilter(vector<RT>* res, size_t l, size_t r,
                                  shared_ptr<const ResultTable> subRes) const {
  switch (_type) {
    case SparqlFilter::EQ:
      getEngine().filter(*static_cast<vector<RT>*>(subRes->_fixedSizeData),
                         [l, r](const RT& e) { return e[l] == e[r]; }, res);
      break;
    case SparqlFilter::NE:
      getEngine().filter(*static_cast<vector<RT>*>(subRes->_fixedSizeData),
                         [l, r](const RT& e) { return e[l] != e[r]; }, res);
      break;
    case SparqlFilter::LT:
      getEngine().filter(*static_cast<vector<RT>*>(subRes->_fixedSizeData),
                         [l, r](const RT& e) { return e[l] < e[r]; }, res);
      break;
    case SparqlFilter::LE:
      getEngine().filter(*static_cast<vector<RT>*>(subRes->_fixedSizeData),
                         [l, r](const RT& e) { return e[l] <= e[r]; }, res);
      break;
    case SparqlFilter::GT:
      getEngine().filter(*static_cast<vector<RT>*>(subRes->_fixedSizeData),
                         [l, r](const RT& e) { return e[l] > e[r]; }, res);
      break;
    case SparqlFilter::GE:
      getEngine().filter(*static_cast<vector<RT>*>(subRes->_fixedSizeData),
                         [l, r](const RT& e) { return e[l] >= e[r]; }, res);
      break;
    case SparqlFilter::LANG_MATCHES:
      AD_THROW(ad_semsearch::Exception::NOT_YET_IMPLEMENTED,
               "Language filtering with a dynamic right side has not yet "
               "been implemented.");
      break;
  }
  return res;
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
      result->_fixedSizeData = computeFilter(new vector<RT>(), l, r, subRes);
      break;
    }
    case 2: {
      typedef array<Id, 2> RT;
      result->_fixedSizeData = computeFilter(new vector<RT>(), l, r, subRes);
      break;
    }
    case 3: {
      typedef array<Id, 3> RT;
      result->_fixedSizeData = computeFilter(new vector<RT>(), l, r, subRes);
      break;
    }
    case 4: {
      typedef array<Id, 4> RT;
      result->_fixedSizeData = computeFilter(new vector<RT>(), l, r, subRes);
      break;
    }
    case 5: {
      typedef array<Id, 5> RT;
      result->_fixedSizeData = computeFilter(new vector<RT>(), l, r, subRes);
      break;
    }
    default:
      computeFilter(&result->_varSizeData, l, r, subRes);
      break;
  }
  result->finish();
  LOG(DEBUG) << "Filter result computation done." << endl;
}

// _____________________________________________________________________________
template <class RT>
vector<RT>* Filter::computeFilterFixedValue(
    vector<RT>* res, size_t l, Id r,
    shared_ptr<const ResultTable> subRes) const {
  switch (_type) {
    case SparqlFilter::EQ:
      getEngine().filter(*static_cast<vector<RT>*>(subRes->_fixedSizeData),
                         [l, r](const RT& e) { return e[l] == r; }, res);
      break;
    case SparqlFilter::NE:
      getEngine().filter(*static_cast<vector<RT>*>(subRes->_fixedSizeData),
                         [l, r](const RT& e) { return e[l] != r; }, res);
      break;
    case SparqlFilter::LT:
      getEngine().filter(*static_cast<vector<RT>*>(subRes->_fixedSizeData),
                         [l, r](const RT& e) { return e[l] < r; }, res);
      break;
    case SparqlFilter::LE:
      getEngine().filter(*static_cast<vector<RT>*>(subRes->_fixedSizeData),
                         [l, r](const RT& e) { return e[l] <= r; }, res);
      break;
    case SparqlFilter::GT:
      getEngine().filter(*static_cast<vector<RT>*>(subRes->_fixedSizeData),
                         [l, r](const RT& e) { return e[l] > r; }, res);
      break;
    case SparqlFilter::GE:
      getEngine().filter(*static_cast<vector<RT>*>(subRes->_fixedSizeData),
                         [l, r](const RT& e) { return e[l] >= r; }, res);
      break;
    case SparqlFilter::LANG_MATCHES:
      getEngine().filter(
          *static_cast<vector<RT>*>(subRes->_fixedSizeData),
          [this, l](const RT& e) {
            std::optional<string> entity = getIndex().idToOptionalString(e[l]);
            if (!entity) {
              return true;
            }
            return ad_utility::endsWith(entity.value(), this->_rhsString);
          },
          res);
      break;
  }
  return res;
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
      result->_fixedSizeData =
          computeFilterFixedValue(new vector<RT>(), l, r, subRes);
      break;
    }
    case 2: {
      typedef array<Id, 2> RT;
      result->_fixedSizeData =
          computeFilterFixedValue(new vector<RT>(), l, r, subRes);
      break;
    }
    case 3: {
      typedef array<Id, 3> RT;
      result->_fixedSizeData =
          computeFilterFixedValue(new vector<RT>(), l, r, subRes);
      break;
    }
    case 4: {
      typedef array<Id, 4> RT;
      result->_fixedSizeData =
          computeFilterFixedValue(new vector<RT>(), l, r, subRes);
      break;
    }
    case 5: {
      typedef array<Id, 5> RT;
      result->_fixedSizeData =
          computeFilterFixedValue(new vector<RT>(), l, r, subRes);
      break;
    }
    default: {
      computeFilterFixedValue(&result->_varSizeData, l, r, subRes);
      break;
    }
  }
  result->finish();
  LOG(DEBUG) << "Filter result computation done." << endl;
}
