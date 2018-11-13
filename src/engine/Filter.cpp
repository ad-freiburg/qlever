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
    vector<RT>* res, size_t lhs, size_t rhs,
    shared_ptr<const ResultTable> subRes) const {
  switch (_type) {
    case SparqlFilter::EQ:
      getEngine().filter(*static_cast<vector<RT>*>(subRes->_fixedSizeData),
                         [lhs, rhs](const RT& e) {
                           return ValueReader<T>::get(e[lhs]) ==
                                  ValueReader<T>::get(e[rhs]);
                         },
                         res);
      break;
    case SparqlFilter::NE:
      getEngine().filter(*static_cast<vector<RT>*>(subRes->_fixedSizeData),
                         [lhs, rhs](const RT& e) {
                           return ValueReader<T>::get(e[lhs]) !=
                                  ValueReader<T>::get(e[rhs]);
                         },
                         res);
      break;
    case SparqlFilter::LT:
      getEngine().filter(*static_cast<vector<RT>*>(subRes->_fixedSizeData),
                         [lhs, rhs](const RT& e) {
                           return ValueReader<T>::get(e[lhs]) <
                                  ValueReader<T>::get(e[rhs]);
                         },
                         res);
      break;
    case SparqlFilter::LE:
      getEngine().filter(*static_cast<vector<RT>*>(subRes->_fixedSizeData),
                         [lhs, rhs](const RT& e) {
                           return ValueReader<T>::get(e[lhs]) <=
                                  ValueReader<T>::get(e[rhs]);
                         },
                         res);
      break;
    case SparqlFilter::GT:
      getEngine().filter(*static_cast<vector<RT>*>(subRes->_fixedSizeData),
                         [lhs, rhs](const RT& e) {
                           return ValueReader<T>::get(e[lhs]) >
                                  ValueReader<T>::get(e[rhs]);
                         },
                         res);
      break;
    case SparqlFilter::GE:
      getEngine().filter(*static_cast<vector<RT>*>(subRes->_fixedSizeData),
                         [lhs, rhs](const RT& e) {
                           return ValueReader<T>::get(e[lhs]) >=
                                  ValueReader<T>::get(e[rhs]);
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
vector<RT>* Filter::computeFilter(vector<RT>* res, size_t lhs, size_t rhs,
                                  shared_ptr<const ResultTable> subRes) const {
  switch (subRes->getResultType(lhs)) {
    case ResultTable::ResultType::KB:
      return computeFilterForResultType<RT, ResultTable::ResultType::KB>(
          res, lhs, rhs, subRes);
    case ResultTable::ResultType::VERBATIM:
      return computeFilterForResultType<RT, ResultTable::ResultType::VERBATIM>(
          res, lhs, rhs, subRes);
    case ResultTable::ResultType::FLOAT:
      return computeFilterForResultType<RT, ResultTable::ResultType::FLOAT>(
          res, lhs, rhs, subRes);
    case ResultTable::ResultType::LOCAL_VOCAB:
      return computeFilterForResultType<RT,
                                        ResultTable::ResultType::LOCAL_VOCAB>(
          res, lhs, rhs, subRes);
    case ResultTable::ResultType::TEXT:
      return computeFilterForResultType<RT, ResultTable::ResultType::TEXT>(
          res, lhs, rhs, subRes);
  }
  AD_THROW(ad_semsearch::Exception::NOT_YET_IMPLEMENTED,
           "Tried to compute a filter on an unknown result type " +
               std::to_string(static_cast<int>(subRes->getResultType(lhs))));
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
    vector<RT>* res, size_t lhs, Id rhs,
    shared_ptr<const ResultTable> subRes) const {
  bool lhs_is_sorted =
      subRes->_sortedBy.size() > 0 && subRes->_sortedBy[0] == lhs;
  switch (_type) {
    case SparqlFilter::EQ:
      if (lhs_is_sorted) {
        // The input data is sorted, use binary search to locate the first
        // and last element that match rhs and copy the range.
        RT rhs_array;
        rhs_array[lhs] = rhs;
        vector<RT>* data = static_cast<vector<RT>*>(subRes->_fixedSizeData);
        const typename vector<RT>::iterator& lower = std::lower_bound(
            data->begin(), data->end(), rhs_array,
            [lhs](const RT& l, const RT& r) { return l[lhs] < r[lhs]; });
        if (lower != data->end() && (*lower)[lhs] == rhs) {
          // an element equal to rhs exists in the vector
          const auto& upper = std::upper_bound(
              lower, data->end(), rhs_array,
              [lhs](const RT& l, const RT& r) { return l[lhs] < r[lhs]; });
          res->insert(res->end(), lower, upper);
        }
      } else {
        getEngine().filter(
            *static_cast<vector<RT>*>(subRes->_fixedSizeData),
            [lhs, rhs, &subRes](const RT& e) { return e[lhs] == rhs; }, res);
      }
      break;
    case SparqlFilter::NE:
      if (lhs_is_sorted) {
        // The input data is sorted, use binary search to locate the first
        // and last element that match rhs and copy the range.
        RT rhs_array;
        rhs_array[lhs] = rhs;
        vector<RT>* data = static_cast<vector<RT>*>(subRes->_fixedSizeData);
        const typename vector<RT>::iterator& lower = std::lower_bound(
            data->begin(), data->end(), rhs_array,
            [lhs](const RT& l, const RT& r) { return l[lhs] < r[lhs]; });
        if (lower != data->end() && (*lower)[lhs] == rhs) {
          // rhs appears within the data, take all elements before and after it
          const typename vector<RT>::iterator& upper = std::upper_bound(
              lower, data->end(), rhs_array,
              [lhs](const RT& l, const RT& r) { return l[lhs] < r[lhs]; });
          res->insert(res->end(), data->begin(), lower);
          res->insert(res->end(), upper, data->end());
        } else {
          // rhs does not appear within the data
          res->insert(res->end(), data->begin(), data->end());
        }
      } else {
        getEngine().filter(*static_cast<vector<RT>*>(subRes->_fixedSizeData),
                           [lhs, rhs](const RT& e) { return e[lhs] != rhs; },
                           res);
      }
      break;
    case SparqlFilter::LT:
      if (lhs_is_sorted) {
        // The input data is sorted, use binary search to locate the first
        // and last element that match rhs and copy the range.
        RT rhs_array;
        rhs_array[lhs] = rhs;
        vector<RT>* data = static_cast<vector<RT>*>(subRes->_fixedSizeData);
        const typename vector<RT>::iterator& lower = std::lower_bound(
            data->begin(), data->end(), rhs_array,
            [lhs](const RT& l, const RT& r) { return l[lhs] < r[lhs]; });
        res->insert(res->end(), data->begin(), lower);
      } else {
        getEngine().filter(*static_cast<vector<RT>*>(subRes->_fixedSizeData),
                           [lhs, rhs](const RT& e) {
                             return ValueReader<T>::get(e[lhs]) <
                                    ValueReader<T>::get(rhs);
                           },
                           res);
      }
      break;
    case SparqlFilter::LE:
      if (lhs_is_sorted) {
        // The input data is sorted, use binary search to locate the first
        // and last element that match rhs and copy the range.
        RT rhs_array;
        rhs_array[lhs] = rhs;
        vector<RT>* data = static_cast<vector<RT>*>(subRes->_fixedSizeData);
        const typename vector<RT>::iterator& upper = std::upper_bound(
            data->begin(), data->end(), rhs_array,
            [lhs](const RT& l, const RT& r) { return l[lhs] < r[lhs]; });
        res->insert(res->end(), data->begin(), upper);
      } else {
        getEngine().filter(*static_cast<vector<RT>*>(subRes->_fixedSizeData),
                           [lhs, rhs](const RT& e) {
                             return ValueReader<T>::get(e[lhs]) <=
                                    ValueReader<T>::get(rhs);
                           },
                           res);
      }
      break;
    case SparqlFilter::GT:
      if (lhs_is_sorted) {
        // The input data is sorted, use binary search to locate the first
        // and last element that match rhs and copy the range.
        RT rhs_array;
        rhs_array[lhs] = rhs;
        vector<RT>* data = static_cast<vector<RT>*>(subRes->_fixedSizeData);
        const typename vector<RT>::iterator& upper = std::upper_bound(
            data->begin(), data->end(), rhs_array,
            [lhs](const RT& l, const RT& r) { return l[lhs] < r[lhs]; });
        // an element equal to rhs exists in the vector
        res->insert(res->end(), upper, data->end());
      } else {
        getEngine().filter(*static_cast<vector<RT>*>(subRes->_fixedSizeData),
                           [lhs, rhs](const RT& e) {
                             return ValueReader<T>::get(e[lhs]) >
                                    ValueReader<T>::get(rhs);
                           },
                           res);
      }
      break;
    case SparqlFilter::GE:
      if (lhs_is_sorted) {
        // The input data is sorted, use binary search to locate the first
        // and last element that match rhs and copy the range.
        RT rhs_array;
        rhs_array[lhs] = rhs;
        vector<RT>* data = static_cast<vector<RT>*>(subRes->_fixedSizeData);
        const typename vector<RT>::iterator& lower = std::lower_bound(
            data->begin(), data->end(), rhs_array,
            [lhs](const RT& l, const RT& r) { return l[lhs] < r[lhs]; });
        // an element equal to rhs exists in the vector
        res->insert(res->end(), lower, data->end());
      } else {
        getEngine().filter(*static_cast<vector<RT>*>(subRes->_fixedSizeData),
                           [lhs, rhs](const RT& e) {
                             return ValueReader<T>::get(e[lhs]) >=
                                    ValueReader<T>::get(rhs);
                           },
                           res);
      }
      break;
    case SparqlFilter::LANG_MATCHES:
      getEngine().filter(
          *static_cast<vector<RT>*>(subRes->_fixedSizeData),
          [this, lhs, &subRes](const RT& e) {
            std::optional<string> entity;
            if constexpr (T == ResultTable::ResultType::KB) {
              entity = getIndex().idToOptionalString(e[lhs]);
            } else if (T == ResultTable::ResultType::LOCAL_VOCAB) {
              entity = subRes->idToOptionalString(e[lhs]);
            }
            if (!entity) {
              return true;
            }
            return ad_utility::endsWith(entity.value(), _rhs);
          },
          res);
      break;
    case SparqlFilter::PREFIX:
      // Check if the prefix filter can be applied. Use the regex filter
      // otherwise.
      if constexpr (T == ResultTable::ResultType::KB) {
        // remove the leading '^' symbol
        std::string rhs = _rhs.substr(1);
        std::string upperBoundStr = rhs;
        upperBoundStr[upperBoundStr.size() - 1]++;
        size_t upperBound =
            getIndex().getVocab().getValueIdForLT(upperBoundStr);
        size_t lowerBound = getIndex().getVocab().getValueIdForGE(rhs);
        if (lhs_is_sorted) {
          // The input data is sorted, use binary search to locate the first
          // and last element that match rhs and copy the range.
          RT rhs_array;
          rhs_array[lhs] = lowerBound;
          vector<RT>* data = static_cast<vector<RT>*>(subRes->_fixedSizeData);
          const typename vector<RT>::iterator& lower = std::lower_bound(
              data->begin(), data->end(), rhs_array,
              [lhs](const RT& l, const RT& r) { return l[lhs] < r[lhs]; });
          if (lower != data->end()) {
            // rhs appears within the data, take all elements before and after
            // it
            rhs_array[lhs] = upperBound;
            const typename vector<RT>::iterator& upper = std::upper_bound(
                lower, data->end(), rhs_array,
                [lhs](const RT& l, const RT& r) { return l[lhs] < r[lhs]; });
            res->insert(res->end(), lower, upper);
          }
        } else {
          getEngine().filter(*static_cast<vector<RT>*>(subRes->_fixedSizeData),
                             [this, lhs, lowerBound, upperBound](const RT& e) {
                               return lowerBound <= e[lhs] &&
                                      e[lhs] < upperBound;
                             },
                             res);
        }
        break;
      }
    case SparqlFilter::REGEX: {
      std::regex self_regex;
      if (_regexIgnoreCase) {
        self_regex.assign(_rhs, std::regex_constants::ECMAScript |
                                    std::regex_constants::icase);
      } else {
        self_regex.assign(_rhs, std::regex_constants::ECMAScript);
      }
      getEngine().filter(
          *static_cast<vector<RT>*>(subRes->_fixedSizeData),
          [this, self_regex, &lhs, &subRes](const RT& e) {
            std::optional<string> entity;
            if constexpr (T == ResultTable::ResultType::KB) {
              entity = getIndex().idToOptionalString(e[lhs]);
            } else if (T == ResultTable::ResultType::LOCAL_VOCAB) {
              entity = subRes->idToOptionalString(e[lhs]);
            }
            if (!entity) {
              return true;
            }
            return std::regex_search(entity.value(), self_regex);
          },
          res);
    } break;
  }
  return res;
}

// _____________________________________________________________________________
template <class RT>
vector<RT>* Filter::computeFilterFixedValue(
    vector<RT>* res, size_t lhs, Id rhs,
    shared_ptr<const ResultTable> subRes) const {
  ResultTable::ResultType resultType = subRes->getResultType(lhs);
  // Catch some unsupported combinations
  if (resultType != ResultTable::ResultType::KB &&
      resultType != ResultTable::ResultType::LOCAL_VOCAB &&
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
          res, lhs, rhs, subRes);
    case ResultTable::ResultType::VERBATIM:
      return computeFilterFixedValueForResultType<
          RT, ResultTable::ResultType::VERBATIM>(res, lhs, rhs, subRes);
    case ResultTable::ResultType::FLOAT:
      return computeFilterFixedValueForResultType<
          RT, ResultTable::ResultType::FLOAT>(res, lhs, rhs, subRes);
    case ResultTable::ResultType::LOCAL_VOCAB:
      return computeFilterFixedValueForResultType<
          RT, ResultTable::ResultType::LOCAL_VOCAB>(res, lhs, rhs, subRes);
    case ResultTable::ResultType::TEXT:
      return computeFilterFixedValueForResultType<
          RT, ResultTable::ResultType::TEXT>(res, lhs, rhs, subRes);
  }
  AD_THROW(ad_semsearch::Exception::NOT_YET_IMPLEMENTED,
           "Tried to compute a filter on an unknown result type " +
               std::to_string(static_cast<int>(subRes->getResultType(lhs))));
}

// _____________________________________________________________________________
void Filter::computeResultFixedValue(
    ResultTable* result,
    const std::shared_ptr<const ResultTable> subRes) const {
  LOG(DEBUG) << "Filter result computation..." << endl;

  // interpret the filters right hand side
  size_t lhs = _subtree->getVariableColumn(_lhs);
  Id rhs;
  switch (subRes->getResultType(lhs)) {
    case ResultTable::ResultType::KB: {
      std::string rhs_string = _rhs;
      if (ad_utility::isXsdValue(rhs_string)) {
        rhs_string = ad_utility::convertValueLiteralToIndexWord(rhs_string);
      } else if (ad_utility::isNumeric(_rhs)) {
        rhs_string = ad_utility::convertNumericToIndexWord(rhs_string);
      }
      if (_type == SparqlFilter::EQ || _type == SparqlFilter::NE) {
        if (!getIndex().getVocab().getId(_rhs, &rhs)) {
          rhs = std::numeric_limits<size_t>::max() - 1;
        }
      } else if (_type == SparqlFilter::GE) {
        rhs = getIndex().getVocab().getValueIdForGE(rhs_string);
      } else if (_type == SparqlFilter::GT) {
        rhs = getIndex().getVocab().getValueIdForGT(rhs_string);
      } else if (_type == SparqlFilter::LT) {
        rhs = getIndex().getVocab().getValueIdForLT(rhs_string);
      } else if (_type == SparqlFilter::LE) {
        rhs = getIndex().getVocab().getValueIdForLE(rhs_string);
      }
      // All other types of filters do not use r and work on _rhs directly
      break;
    }
    case ResultTable::ResultType::VERBATIM:
      try {
        rhs = std::stoull(_rhs);
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
        std::memcpy(&rhs, &f, sizeof(float));
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
        for (rhs = 0; rhs < subRes->_localVocab->size(); rhs++) {
          if ((*subRes->_localVocab)[rhs] == _rhs) {
            break;
          }
        }
      } else if (_type != SparqlFilter::LANG_MATCHES &&
                 _type != SparqlFilter::PREFIX &&
                 _type != SparqlFilter::REGEX) {
        // Comparison based filters on the local vocab are hard, as the
        // vocabularyis not sorted.
        AD_THROW(
            ad_semsearch::Exception::NOT_YET_IMPLEMENTED,
            "Only equality, inequality and string based filters are allowed on "
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
          computeFilterFixedValue(new vector<RT>(), lhs, rhs, subRes);
      break;
    }
    case 2: {
      typedef array<Id, 2> RT;
      result->_fixedSizeData =
          computeFilterFixedValue(new vector<RT>(), lhs, rhs, subRes);
      break;
    }
    case 3: {
      typedef array<Id, 3> RT;
      result->_fixedSizeData =
          computeFilterFixedValue(new vector<RT>(), lhs, rhs, subRes);
      break;
    }
    case 4: {
      typedef array<Id, 4> RT;
      result->_fixedSizeData =
          computeFilterFixedValue(new vector<RT>(), lhs, rhs, subRes);
      break;
    }
    case 5: {
      typedef array<Id, 5> RT;
      result->_fixedSizeData =
          computeFilterFixedValue(new vector<RT>(), lhs, rhs, subRes);
      break;
    }
    default: {
      computeFilterFixedValue(&result->_varSizeData, lhs, rhs, subRes);
      break;
    }
  }
  result->finish();
  LOG(DEBUG) << "Filter result computation done." << endl;
}
