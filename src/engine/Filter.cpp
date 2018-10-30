// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)

#include "./Filter.h"
#include <optional>
#include <regex>
#include <sstream>
#include "./QueryExecutionTree.h"

using std::string;

// _____________________________________________________________________________
size_t Filter::getResultWidth() const { return _subtree->getResultWidth(); }

// _____________________________________________________________________________
Filter::Filter(QueryExecutionContext* qec,
               std::shared_ptr<QueryExecutionTree> subtree,
               SparqlFilter::FilterType type, string lhs, string rhs)
    : Operation(qec), _subtree(subtree), _type(type), _lhs(lhs), _rhs(rhs) {}

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
  os << " with " << _lhs;
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
      os << " LANG_MATCHES ";
      break;
    case SparqlFilter::REGEX:
      os << " REGEX ";
      if (_regexIgnoreCase) {
        os << "ignoring case ";
      };
      break;
    case SparqlFilter::PREFIX:
      os << " PREFIX ";
      break;
  }
  os << _rhs;
  return os.str();
}

// _____________________________________________________________________________
template <class RT, ResultTable::ResultType T>
vector<RT>* Filter::computeFilterForResultType(
    vector<RT>* res, size_t l, size_t r,
    shared_ptr<const ResultTable> subRes) const {
  switch (_type) {
    case SparqlFilter::EQ:
      getEngine().filter(*static_cast<vector<RT>*>(subRes->_fixedSizeData),
                         [l, r](const RT& e) {
                           return ValueReader<T>::get(e[l]) ==
                                  ValueReader<T>::get(e[r]);
                         },
                         res);
      break;
    case SparqlFilter::NE:
      getEngine().filter(*static_cast<vector<RT>*>(subRes->_fixedSizeData),
                         [l, r](const RT& e) {
                           return ValueReader<T>::get(e[l]) !=
                                  ValueReader<T>::get(e[r]);
                         },
                         res);
      break;
    case SparqlFilter::LT:
      getEngine().filter(*static_cast<vector<RT>*>(subRes->_fixedSizeData),
                         [l, r](const RT& e) {
                           return ValueReader<T>::get(e[l]) <
                                  ValueReader<T>::get(e[r]);
                         },
                         res);
      break;
    case SparqlFilter::LE:
      getEngine().filter(*static_cast<vector<RT>*>(subRes->_fixedSizeData),
                         [l, r](const RT& e) {
                           return ValueReader<T>::get(e[l]) <=
                                  ValueReader<T>::get(e[r]);
                         },
                         res);
      break;
    case SparqlFilter::GT:
      getEngine().filter(*static_cast<vector<RT>*>(subRes->_fixedSizeData),
                         [l, r](const RT& e) {
                           return ValueReader<T>::get(e[l]) >
                                  ValueReader<T>::get(e[r]);
                         },
                         res);
      break;
    case SparqlFilter::GE:
      getEngine().filter(*static_cast<vector<RT>*>(subRes->_fixedSizeData),
                         [l, r](const RT& e) {
                           return ValueReader<T>::get(e[l]) >=
                                  ValueReader<T>::get(e[r]);
                         },
                         res);
      break;
    case SparqlFilter::LANG_MATCHES:
      AD_THROW(ad_semsearch::Exception::NOT_YET_IMPLEMENTED,
               "Language filtering with a dynamic right side has not yet "
               "been implemented.");
      break;
    case SparqlFilter::REGEX:
      AD_THROW(ad_semsearch::Exception::NOT_YET_IMPLEMENTED,
               "Regex filtering with a dynamic right side has not yet "
               "been implemented.");
      break;
    case SparqlFilter::PREFIX:
      AD_THROW(ad_semsearch::Exception::NOT_YET_IMPLEMENTED,
               "Prefix filtering with a dynamic right side has not yet "
               "been implemented.");
      break;
  }
  return res;
}

template <class RT>
vector<RT>* Filter::computeFilter(vector<RT>* res, size_t l, size_t r,
                                  shared_ptr<const ResultTable> subRes) const {
  switch (subRes->getResultType(l)) {
    case ResultTable::ResultType::KB:
      return computeFilterForResultType<RT, ResultTable::ResultType::KB>(
          res, l, r, subRes);
    case ResultTable::ResultType::VERBATIM:
      return computeFilterForResultType<RT, ResultTable::ResultType::VERBATIM>(
          res, l, r, subRes);
    case ResultTable::ResultType::FLOAT:
      return computeFilterForResultType<RT, ResultTable::ResultType::FLOAT>(
          res, l, r, subRes);
    case ResultTable::ResultType::LOCAL_VOCAB:
      return computeFilterForResultType<RT,
                                        ResultTable::ResultType::LOCAL_VOCAB>(
          res, l, r, subRes);
    case ResultTable::ResultType::TEXT:
      return computeFilterForResultType<RT, ResultTable::ResultType::TEXT>(
          res, l, r, subRes);
  }
  AD_THROW(ad_semsearch::Exception::NOT_YET_IMPLEMENTED,
           "Tried to compute a filter on an unknown result type " +
               std::to_string(static_cast<int>(subRes->getResultType(l))));
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
  result->_localVocab = subRes->_localVocab;
  size_t lhsInd = _subtree->getVariableColumn(_lhs);

  if (_rhs[0] == '?') {
    size_t rhsInd = _subtree->getVariableColumn(_rhs);
    // compare two columns
    switch (subRes->_nofColumns) {
      case 1: {
        typedef array<Id, 1> RT;
        result->_fixedSizeData =
            computeFilter(new vector<RT>(), lhsInd, rhsInd, subRes);
        break;
      }
      case 2: {
        typedef array<Id, 2> RT;
        result->_fixedSizeData =
            computeFilter(new vector<RT>(), lhsInd, rhsInd, subRes);
        break;
      }
      case 3: {
        typedef array<Id, 3> RT;
        result->_fixedSizeData =
            computeFilter(new vector<RT>(), lhsInd, rhsInd, subRes);
        break;
      }
      case 4: {
        typedef array<Id, 4> RT;
        result->_fixedSizeData =
            computeFilter(new vector<RT>(), lhsInd, rhsInd, subRes);
        break;
      }
      case 5: {
        typedef array<Id, 5> RT;
        result->_fixedSizeData =
            computeFilter(new vector<RT>(), lhsInd, rhsInd, subRes);
        break;
      }
      default:
        computeFilter(&result->_varSizeData, lhsInd, rhsInd, subRes);
        break;
    }
  } else {
    // compare the left column to a fixed value
    return computeResultFixedValue(result, subRes);
  }
  result->finish();
  LOG(DEBUG) << "Filter result computation done." << endl;
}

template <class RT, ResultTable::ResultType T>
vector<RT>* Filter::computeFilterFixedValueForResultType(
    vector<RT>* res, size_t l, Id r,
    shared_ptr<const ResultTable> subRes) const {
  switch (_type) {
    case SparqlFilter::EQ:
      getEngine().filter(*static_cast<vector<RT>*>(subRes->_fixedSizeData),
                         [l, r, &subRes](const RT& e) { return e[l] == r; },
                         res);
      break;
    case SparqlFilter::NE:
      getEngine().filter(*static_cast<vector<RT>*>(subRes->_fixedSizeData),
                         [l, r](const RT& e) { return e[l] != r; }, res);
      break;
    case SparqlFilter::LT:
      getEngine().filter(*static_cast<vector<RT>*>(subRes->_fixedSizeData),
                         [l, r](const RT& e) {
                           return ValueReader<T>::get(e[l]) <
                                  ValueReader<T>::get(r);
                         },
                         res);
      break;
    case SparqlFilter::LE:
      getEngine().filter(*static_cast<vector<RT>*>(subRes->_fixedSizeData),
                         [l, r](const RT& e) {
                           return ValueReader<T>::get(e[l]) <=
                                  ValueReader<T>::get(r);
                         },
                         res);
      break;
    case SparqlFilter::GT:
      getEngine().filter(*static_cast<vector<RT>*>(subRes->_fixedSizeData),
                         [l, r](const RT& e) {
                           return ValueReader<T>::get(e[l]) >
                                  ValueReader<T>::get(r);
                         },
                         res);
      break;
    case SparqlFilter::GE:
      getEngine().filter(*static_cast<vector<RT>*>(subRes->_fixedSizeData),
                         [l, r](const RT& e) {
                           return ValueReader<T>::get(e[l]) >=
                                  ValueReader<T>::get(r);
                         },
                         res);
      break;
    case SparqlFilter::LANG_MATCHES:
      getEngine().filter(*static_cast<vector<RT>*>(subRes->_fixedSizeData),
                         [this, l](const RT& e) {
                           std::optional<string> entity =
                               getIndex().idToOptionalString(e[l]);
                           if (!entity) {
                             return true;
                           }
                           return ad_utility::endsWith(entity.value(), _rhs);
                         },
                         res);
      break;
    case SparqlFilter::REGEX: {
      std::regex self_regex;
      if (_regexIgnoreCase) {
        self_regex.assign(_rhs, std::regex_constants::ECMAScript |
                                    std::regex_constants::icase);
      } else {
        self_regex.assign(_rhs, std::regex_constants::ECMAScript);
      }
      getEngine().filter(*static_cast<vector<RT>*>(subRes->_fixedSizeData),
                         [this, self_regex, &l](const RT& e) {
                           std::optional<string> entity =
                               getIndex().idToOptionalString(e[l]);
                           if (!entity) {
                             return true;
                           }
                           return std::regex_search(entity.value(), self_regex);
                         },
                         res);
    } break;
    case SparqlFilter::PREFIX: {
      // remove the leading '^' symbol
      std::string rhs = _rhs.substr(1);
      size_t lowerBound = getIndex().getVocab().getValueIdForGE(rhs);
      std::string upperBoundStr = rhs;
      // TODO(florian): This only works for ascii but could break unicode
      upperBoundStr[upperBoundStr.size() - 1]++;
      size_t upperBound = getIndex().getVocab().getValueIdForLT(upperBoundStr);
      getEngine().filter(*static_cast<vector<RT>*>(subRes->_fixedSizeData),
                         [this, l, lowerBound, upperBound](const RT& e) {
                           return lowerBound <= e[l] && e[l] < upperBound;
                         },
                         res);
    } break;
  }
  return res;
}

// _____________________________________________________________________________
template <class RT>
vector<RT>* Filter::computeFilterFixedValue(
    vector<RT>* res, size_t l, Id r,
    shared_ptr<const ResultTable> subRes) const {
  ResultTable::ResultType resultType = subRes->getResultType(l);
  // Catch some unsupported combinations
  if (resultType != ResultTable::ResultType::KB &&
      (_type == SparqlFilter::PREFIX || _type == SparqlFilter::LANG_MATCHES ||
       _type == SparqlFilter::REGEX)) {
    AD_THROW(
        ad_semsearch::Exception::BAD_QUERY,
        "Requested to apply a string based filter on a non string column: " +
            asString());
  }
  switch (resultType) {
    case ResultTable::ResultType::KB:
      return computeFilterFixedValueForResultType<RT,
                                                  ResultTable::ResultType::KB>(
          res, l, r, subRes);
    case ResultTable::ResultType::VERBATIM:
      return computeFilterFixedValueForResultType<
          RT, ResultTable::ResultType::VERBATIM>(res, l, r, subRes);
    case ResultTable::ResultType::FLOAT:
      return computeFilterFixedValueForResultType<
          RT, ResultTable::ResultType::FLOAT>(res, l, r, subRes);
    case ResultTable::ResultType::LOCAL_VOCAB:
      return computeFilterFixedValueForResultType<
          RT, ResultTable::ResultType::LOCAL_VOCAB>(res, l, r, subRes);
    case ResultTable::ResultType::TEXT:
      return computeFilterFixedValueForResultType<
          RT, ResultTable::ResultType::TEXT>(res, l, r, subRes);
  }
  AD_THROW(ad_semsearch::Exception::NOT_YET_IMPLEMENTED,
           "Tried to compute a filter on an unknown result type " +
               std::to_string(static_cast<int>(subRes->getResultType(l))));
}

// _____________________________________________________________________________
void Filter::computeResultFixedValue(
    ResultTable* result,
    const std::shared_ptr<const ResultTable> subRes) const {
  LOG(DEBUG) << "Filter result computation..." << endl;

  // interpret the filters right hand side
  size_t l = _subtree->getVariableColumn(_lhs);
  Id r;
  switch (subRes->getResultType(l)) {
    case ResultTable::ResultType::KB: {
      std::string rhs = _rhs;
      if (ad_utility::isXsdValue(rhs)) {
        rhs = ad_utility::convertValueLiteralToIndexWord(rhs);
      } else if (ad_utility::isNumeric(_rhs)) {
        rhs = ad_utility::convertNumericToIndexWord(rhs);
      }
      if (_type == SparqlFilter::EQ || _type == SparqlFilter::NE) {
        if (!getIndex().getVocab().getId(_rhs, &r)) {
          r = std::numeric_limits<size_t>::max() - 1;
        }
      } else if (_type == SparqlFilter::GE) {
        r = getIndex().getVocab().getValueIdForGE(rhs);
      } else if (_type == SparqlFilter::GT) {
        r = getIndex().getVocab().getValueIdForGT(rhs);
      } else if (_type == SparqlFilter::LT) {
        r = getIndex().getVocab().getValueIdForLT(rhs);
      } else if (_type == SparqlFilter::LE) {
        r = getIndex().getVocab().getValueIdForLE(rhs);
      }
      // All other types of filters do not use r and work on _rhs directly
      break;
    }
    case ResultTable::ResultType::VERBATIM:
      try {
        r = std::stoull(_rhs);
      } catch (const std::logic_error& e) {
        AD_THROW(ad_semsearch::Exception::BAD_QUERY,
                 "A filter filters on an unsigned integer column, but its "
                 "right hand side '" +
                     _rhs + "' could not be parsed as an unsigned integer.");
      }
      break;
    case ResultTable::ResultType::FLOAT:
      try {
        float f = std::stof(_rhs);
        std::memcpy(&r, &f, sizeof(float));
      } catch (const std::logic_error& e) {
        AD_THROW(
            ad_semsearch::Exception::BAD_QUERY,
            "A filter filters on a float column, but its right hand side '" +
                _rhs + "' could not be parsed as a float.");
      }
      break;
    case ResultTable::ResultType::TEXT:
      AD_THROW(ad_semsearch::Exception::NOT_YET_IMPLEMENTED,
               "Filtering on text type columns is not supported but required "
               "by filter: " +
                   asString());
      break;
    case ResultTable::ResultType::LOCAL_VOCAB:
      if (_type == SparqlFilter::EQ || _type == SparqlFilter::NE) {
        // Find a matching entry in subRes' _localVocab. If _rhs is not in the
        // _localVocab of subRes r will be equal to  _localVocab.size() and
        // not match the index of any entry in _localVocab.
        for (r = 0; r < subRes->_localVocab->size(); r++) {
          if ((*subRes->_localVocab)[r] == _rhs) {
            break;
          }
        }
      } else {
        AD_THROW(ad_semsearch::Exception::NOT_YET_IMPLEMENTED,
                 "Only equality and inequality filters are allowed on "
                 "dynamicaly assembled strings, but the following filter "
                 "requires another type of filter operation:" +
                     asString());
      }
      break;
    default:
      AD_THROW(ad_semsearch::Exception::NOT_YET_IMPLEMENTED,
               "Trying to filter on a not yet supported column type.");
      break;
  }
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
