// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)

#include "Filter.h"

#include <algorithm>
#include <optional>
#include <regex>
#include <sstream>

#include "CallFixedSize.h"
#include "QueryExecutionTree.h"

using std::string;

// _____________________________________________________________________________
size_t Filter::getResultWidth() const { return _subtree->getResultWidth(); }

// _____________________________________________________________________________
Filter::Filter(QueryExecutionContext* qec,
               std::shared_ptr<QueryExecutionTree> subtree,
               SparqlFilter::FilterType type, string lhs, string rhs,
               vector<string> additionalLhs, vector<string> additionalPrefixes)
    : Operation(qec),
      _subtree(std::move(subtree)),
      _type(type),
      _lhs(std::move(lhs)),
      _rhs(std::move(rhs)),
      _additionalLhs(std::move(additionalLhs)),
      _additionalPrefixRegexes(std::move(additionalPrefixes)),
      _regexIgnoreCase(false),
      _lhsAsString(false) {
  AD_CHECK(_additionalPrefixRegexes.empty() || _type == SparqlFilter::PREFIX);
  AD_CHECK(_additionalLhs.size() == _additionalPrefixRegexes.size());
  // TODO<joka921> Make the _additionalRegexes and _additionalLhs a pair to
  // safely deal with then. then and ONLY then we could maybe sort them hear for
  // consistent cache keys, when filters are only reordered within the pair.
}

// _____________________________________________________________________________
string Filter::asString(size_t indent) const {
  std::ostringstream os;
  for (size_t i = 0; i < indent; ++i) {
    os << " ";
  }
  os << "FILTER " << _subtree->asString(indent);
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
  if (_lhsAsString) {
    os << "LHS_AS_STR ";
  }
  os << _rhs;
  for (size_t i = 0; i < _additionalLhs.size(); ++i) {
    os << " || " << _additionalLhs[i] << " " << _additionalPrefixRegexes[i];
  }
  os << '\n';
  return os.str();
}

string Filter::getDescriptor() const {
  std::ostringstream os;
  os << "Filter ";
  os << _lhs;
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
      os << " langMatches ";
      break;
    case SparqlFilter::REGEX:
      os << " regex ";
      if (_regexIgnoreCase) {
        os << "ignoring case ";
      };
      break;
    case SparqlFilter::PREFIX:
      os << " PREFIX ";
      break;
  }
  os << _rhs;
  for (size_t i = 0; i < _additionalLhs.size(); ++i) {
    os << " || " << _additionalLhs[i] << " " << _additionalPrefixRegexes[i];
  }
  return os.str();
}

// _____________________________________________________________________________
template <ResultTable::ResultType T, int WIDTH>
void Filter::computeFilter(IdTableStatic<WIDTH>* result, size_t lhs, size_t rhs,
                           const IdTableView<WIDTH>& input) const {
  switch (_type) {
    case SparqlFilter::EQ:
      getEngine().filter(
          input,
          [lhs, rhs](const auto& e) {
            return ValueReader<T>::get(e[lhs]) == ValueReader<T>::get(e[rhs]);
          },
          result);
      break;
    case SparqlFilter::NE:
      getEngine().filter(
          input,
          [lhs, rhs](const auto& e) {
            return ValueReader<T>::get(e[lhs]) != ValueReader<T>::get(e[rhs]);
          },
          result);
      break;
    case SparqlFilter::LT:
      getEngine().filter(
          input,
          [lhs, rhs](const auto& e) {
            return ValueReader<T>::get(e[lhs]) < ValueReader<T>::get(e[rhs]);
          },
          result);
      break;
    case SparqlFilter::LE:
      getEngine().filter(
          input,
          [lhs, rhs](const auto& e) {
            return ValueReader<T>::get(e[lhs]) <= ValueReader<T>::get(e[rhs]);
          },
          result);
      break;
    case SparqlFilter::GT:
      getEngine().filter(
          input,
          [lhs, rhs](const auto& e) {
            return ValueReader<T>::get(e[lhs]) > ValueReader<T>::get(e[rhs]);
          },
          result);
      break;
    case SparqlFilter::GE:
      getEngine().filter(
          input,
          [lhs, rhs](const auto& e) {
            return ValueReader<T>::get(e[lhs]) >= ValueReader<T>::get(e[rhs]);
          },
          result);
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
}

template <int WIDTH>
void Filter::computeResultDynamicValue(IdTable* dynResult, size_t lhsInd,
                                       size_t rhsInd, const IdTable& dynInput,
                                       ResultTable::ResultType lhsType) {
  const IdTableView<WIDTH> input = dynInput.asStaticView<WIDTH>();
  IdTableStatic<WIDTH> result = dynResult->moveToStatic<WIDTH>();
  switch (lhsType) {
    case ResultTable::ResultType::KB:
      computeFilter<ResultTable::ResultType::KB>(&result, lhsInd, rhsInd,
                                                 input);
      break;
    case ResultTable::ResultType::VERBATIM:
      computeFilter<ResultTable::ResultType::VERBATIM>(&result, lhsInd, rhsInd,
                                                       input);
      break;
    case ResultTable::ResultType::FLOAT:
      computeFilter<ResultTable::ResultType::FLOAT>(&result, lhsInd, rhsInd,
                                                    input);
      break;
    case ResultTable::ResultType::LOCAL_VOCAB:
      computeFilter<ResultTable::ResultType::LOCAL_VOCAB>(&result, lhsInd,
                                                          rhsInd, input);
      break;
    case ResultTable::ResultType::TEXT:
      computeFilter<ResultTable::ResultType::TEXT>(&result, lhsInd, rhsInd,
                                                   input);
      break;
    default:
      AD_THROW(ad_semsearch::Exception::NOT_YET_IMPLEMENTED,
               "Tried to compute a filter on an unknown result type " +
                   std::to_string(static_cast<int>(lhsType)));
      break;
  }
  *dynResult = result.moveToDynamic();
}

// _____________________________________________________________________________
void Filter::computeResult(ResultTable* result) {
  LOG(DEBUG) << "Getting sub-result for Filter result computation..." << endl;
  shared_ptr<const ResultTable> subRes = _subtree->getResult();
  RuntimeInformation& runtimeInfo = getRuntimeInfo();
  runtimeInfo.setDescriptor(getDescriptor());
  runtimeInfo.addChild(_subtree->getRootOperation()->getRuntimeInfo());
  LOG(DEBUG) << "Filter result computation..." << endl;
  result->_idTable.setCols(subRes->_idTable.cols());
  result->_resultTypes.insert(result->_resultTypes.end(),
                              subRes->_resultTypes.begin(),
                              subRes->_resultTypes.end());
  result->_localVocab = subRes->_localVocab;
  size_t lhsInd = _subtree->getVariableColumn(_lhs);
  int width = result->_idTable.cols();
  if (_rhs[0] == '?') {
    size_t rhsInd = _subtree->getVariableColumn(_rhs);
    CALL_FIXED_SIZE_1(width, computeResultDynamicValue, &result->_idTable,
                      lhsInd, rhsInd, subRes->_idTable,
                      subRes->getResultType(lhsInd));
  } else {
    // compare the left column to a fixed value
    CALL_FIXED_SIZE_1(width, computeResultFixedValue, result, subRes);
  }

  LOG(DEBUG) << "Filter result computation done." << endl;
}

// _____________________________________________________________________________
template <ResultTable::ResultType T, int WIDTH, bool INVERSE>
void Filter::computeFilterRange(IdTableStatic<WIDTH>* res, size_t lhs,
                                Id rhs_lower, Id rhs_upper,
                                const IdTableView<WIDTH>& input,
                                shared_ptr<const ResultTable> subRes) const {
  bool lhs_is_sorted =
      subRes->_sortedBy.size() > 0 && subRes->_sortedBy[0] == lhs;
  if (lhs_is_sorted) {
    // The input data is sorted, use binary search to locate the first
    // and last element that match rhs and copy the range.

    const auto& lower = std::lower_bound(input.begin(), input.end(), rhs_lower,
                                         [lhs](const auto& l, const auto& r) {
                                           return ValueReader<T>::get(l[lhs]) <
                                                  ValueReader<T>::get(r);
                                         });
    const auto& upper = std::lower_bound(
        lower, input.end(), rhs_upper, [lhs](const auto& l, const auto& r) {
          return ValueReader<T>::get(l[lhs]) < ValueReader<T>::get(r);
        });
    if constexpr (!INVERSE) {
      res->insert(res->end(), lower, upper);
    } else {
      res->insert(res->end(), input.begin(), lower);
      res->insert(res->end(), upper, res->end());
    }
  } else {
    const auto inv = [&](const bool b) { return INVERSE ? !b : b; };
    getEngine().filter(
        input,
        [lhs, rhs_lower, rhs_upper, &inv](const auto& e) {
          return inv(
              ValueReader<T>::get(e[lhs]) >= ValueReader<T>::get(rhs_lower) &&
              ValueReader<T>::get(e[lhs]) < ValueReader<T>::get(rhs_upper));
        },
        res);
  }
}

// _____________________________________________________________________________
template <ResultTable::ResultType T, int WIDTH>
void Filter::computeFilterFixedValue(
    IdTableStatic<WIDTH>* res, size_t lhs, Id rhs,
    const IdTableView<WIDTH>& input,
    shared_ptr<const ResultTable> subRes) const {
  bool lhs_is_sorted =
      subRes->_sortedBy.size() > 0 && subRes->_sortedBy[0] == lhs;
  Id* rhs_array = new Id[res->cols()];
  IdTable::row_type rhs_row(rhs_array, res->cols());
  switch (_type) {
    case SparqlFilter::EQ:
      if (lhs_is_sorted) {
        // The input data is sorted, use binary search to locate the first
        // and last element that match rhs and copy the range.
        rhs_array[lhs] = rhs;
        const auto& lower = std::lower_bound(
            input.begin(), input.end(), rhs_row,
            [lhs](const auto& l, const auto& r) {
              return ValueReader<T>::get(l[lhs]) < ValueReader<T>::get(r[lhs]);
            });
        if (lower != input.end() && (*lower)[lhs] == rhs) {
          // an element equal to rhs exists in the vector
          const auto& upper = std::upper_bound(
              lower, input.end(), rhs_row, [lhs](const auto& l, const auto& r) {
                return ValueReader<T>::get(l[lhs]) <
                       ValueReader<T>::get(r[lhs]);
              });
          res->insert(res->end(), lower, upper);
        }
      } else {
        getEngine().filter(
            input, [lhs, rhs](const auto& e) { return e[lhs] == rhs; }, res);
      }
      break;
    case SparqlFilter::NE:
      if (lhs_is_sorted) {
        // The input data is sorted, use binary search to locate the first
        // and last element that match rhs and copy the range.
        rhs_array[lhs] = rhs;
        const auto& lower = std::lower_bound(
            input.begin(), input.end(), rhs_row,
            [lhs](const auto& l, const auto& r) {
              return ValueReader<T>::get(l[lhs]) < ValueReader<T>::get(r[lhs]);
            });
        if (lower != input.end() && (*lower)[lhs] == rhs) {
          // rhs appears within the input, take all elements before and after it
          const auto& upper = std::upper_bound(
              lower, input.end(), rhs_row, [lhs](const auto& l, const auto& r) {
                return ValueReader<T>::get(l[lhs]) <
                       ValueReader<T>::get(r[lhs]);
              });
          res->insert(res->end(), input.begin(), lower);
          res->insert(res->end(), upper, input.end());
        } else {
          // rhs does not appear within the input
          res->insert(res->end(), input.begin(), input.end());
        }
      } else {
        getEngine().filter(
            input, [lhs, rhs](const auto& e) { return e[lhs] != rhs; }, res);
      }
      break;
    case SparqlFilter::LT:
      if (lhs_is_sorted) {
        // The input data is sorted, use binary search to locate the first
        // and last element that match rhs and copy the range.
        rhs_array[lhs] = rhs;
        const auto& lower = std::lower_bound(
            input.begin(), input.end(), rhs_row,
            [lhs](const auto& l, const auto& r) {
              return ValueReader<T>::get(l[lhs]) < ValueReader<T>::get(r[lhs]);
            });
        res->insert(res->end(), input.begin(), lower);
      } else {
        getEngine().filter(
            input,
            [lhs, rhs](const auto& e) {
              return ValueReader<T>::get(e[lhs]) < ValueReader<T>::get(rhs);
            },
            res);
      }
      break;
    case SparqlFilter::LE:
      if (lhs_is_sorted) {
        // The input data is sorted, use binary search to locate the first
        // and last element that match rhs and copy the range.
        rhs_array[lhs] = rhs;
        const auto& upper = std::upper_bound(
            input.begin(), input.end(), rhs_row,
            [lhs](const auto& l, const auto& r) {
              return ValueReader<T>::get(l[lhs]) < ValueReader<T>::get(r[lhs]);
            });
        res->insert(res->end(), input.begin(), upper);
      } else {
        getEngine().filter(
            input,
            [lhs, rhs](const auto& e) {
              return ValueReader<T>::get(e[lhs]) <= ValueReader<T>::get(rhs);
            },
            res);
      }
      break;
    case SparqlFilter::GT:
      if (lhs_is_sorted) {
        // The input data is sorted, use binary search to locate the first
        // and last element that match rhs and copy the range.
        rhs_array[lhs] = rhs;
        const auto& upper = std::upper_bound(
            input.begin(), input.end(), rhs_row,
            [lhs](const auto& l, const auto& r) {
              return ValueReader<T>::get(l[lhs]) < ValueReader<T>::get(r[lhs]);
            });
        // an element equal to rhs exists in the vector
        res->insert(res->end(), upper, input.end());
      } else {
        getEngine().filter(
            input,
            [lhs, rhs](const auto& e) {
              return ValueReader<T>::get(e[lhs]) > ValueReader<T>::get(rhs);
            },
            res);
      }
      break;
    case SparqlFilter::GE:
      if (lhs_is_sorted) {
        // The input data is sorted, use binary search to locate the first
        // and last element that match rhs and copy the range.
        rhs_array[lhs] = rhs;
        const auto& lower = std::lower_bound(
            input.begin(), input.end(), rhs_row,
            [lhs](const auto& l, const auto& r) {
              return ValueReader<T>::get(l[lhs]) < ValueReader<T>::get(r[lhs]);
            });
        // an element equal to rhs exists in the vector
        res->insert(res->end(), lower, input.end());
      } else {
        getEngine().filter(
            input,
            [lhs, rhs](const auto& e) {
              return ValueReader<T>::get(e[lhs]) >= ValueReader<T>::get(rhs);
            },
            res);
      }
      break;
    case SparqlFilter::LANG_MATCHES:
      getEngine().filter(
          input,
          [this, lhs, &subRes](const auto& e) {
            std::optional<string> entity;
            if constexpr (T == ResultTable::ResultType::KB) {
              entity = getIndex().idToOptionalString(e[lhs]);
              (void)subRes;  // Silence unused warning
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
        ad_utility::HashMap<Id, vector<string>> lhsRhsMap;
        lhsRhsMap[lhs].push_back(_rhs.substr(1));
        for (size_t i = 0; i < _additionalPrefixRegexes.size(); ++i) {
          lhsRhsMap[_subtree->getVariableColumn(_additionalLhs[i])].push_back(
              _additionalPrefixRegexes[i].substr(1));
        }
        ad_utility::HashMap<Id, std::vector<std::pair<Id, Id>>> prefixRanges;
        // TODO<joka921>: handle Levels correctly;
        for (const auto& [l, r] : lhsRhsMap) {
          for (const auto& pref : r) {
            prefixRanges[l].push_back(getIndex().getVocab().prefix_range(pref));
          }
        }
        // remove overlap in the ranges
        for (auto& [l, range] : prefixRanges) {
          (void)l;
          std::sort(
              range.begin(), range.end(),
              [](const auto& a, const auto& b) { return a.first < b.first; });
          for (size_t i = 1; i < range.size(); ++i) {
            range[i].first = std::max(range[i - 1].second, range[i].first);
            range[i].second = std::max(range[i].second, range[i].first);
          }
        }

        const std::optional<Id> sortedLhs = [&prefixRanges,
                                             &subRes]() -> std::optional<Id> {
          if (prefixRanges.size() > 1 || subRes->_sortedBy.empty() ||
              !prefixRanges.contains(subRes->_sortedBy[0])) {
            return std::nullopt;
          }
          return subRes->_sortedBy[0];
        }();

        using IT = typename std::decay_t<decltype(input)>::const_iterator;
        std::vector<std::pair<IT, IT>> iterators;
        if (sortedLhs) {
          // The input data is sorted, use binary search to locate the first
          // and last element that match rhs and copy the range.
          for (auto [lowerBound, upperBound] : prefixRanges[*sortedLhs]) {
            AD_CHECK(*sortedLhs == lhs);
            rhs_array[lhs] = lowerBound;
            const auto& lower =
                std::lower_bound(input.begin(), input.end(), rhs_row,
                                 [lhs](const auto& l, const auto& r) {
                                   return l[lhs] < r[lhs];
                                 });
            if (lower != input.end()) {
              // There is at least one element in the input that is also within
              // the range, look for the upper boundary and then copy all
              // elements within the range.
              rhs_array[lhs] = upperBound;
              const auto& upper =
                  std::lower_bound(lower, input.end(), rhs_row,
                                   [lhs](const auto& l, const auto& r) {
                                     return l[lhs] < r[lhs];
                                   });
              iterators.emplace_back(lower, upper);
            } else {
              // the bounds are sorted in ascending order
              break;
            }
          }
          auto sz = std::accumulate(iterators.begin(), iterators.end(), 0ul,
                                    [](const auto& a, const auto& b) {
                                      return a + (b.second - b.first);
                                    });
          res->reserve(sz);
          for (const auto& [lower, upper] : iterators) {
            res->insert(res->end(), lower, upper);
          }
        } else {
          // optimization for a single filter
          if (prefixRanges.size() == 1 && prefixRanges[lhs].size() == 1) {
            getEngine().filter(
                input,
                [lhs, p = prefixRanges[lhs][0]](const auto& e) {
                  return p.first <= e[lhs] && e[lhs] < p.second;
                },
                res);
          } else {
            getEngine().filter(
                input,
                [&prefixRanges](const auto& e) {
                  return std::any_of(prefixRanges.begin(), prefixRanges.end(),
                                     [&e](const auto& x) {
                                       const auto& vec = x.second;
                                       return std::any_of(
                                           vec.begin(), vec.end(),
                                           [&e, &l = x.first](const auto& p) {
                                             return p.first <= e[l] &&
                                                    e[l] < p.second;
                                           });
                                     });
                },
                res);
          }
        }
        break;
      }
    case SparqlFilter::REGEX: {
      if (!_additionalPrefixRegexes.empty()) {
        AD_THROW(
            ad_semsearch::Exception::BAD_QUERY,
            "Encountered multiple prefix filters concatenated with ||, but "
            "their input was not of type KB, this is not supported as of now");
      }
      std::regex self_regex;
      try {
        if (_regexIgnoreCase) {
          self_regex.assign(_rhs, std::regex_constants::ECMAScript |
                                      std::regex_constants::icase);
        } else {
          self_regex.assign(_rhs, std::regex_constants::ECMAScript);
        }
      } catch (const std::regex_error& e) {
        // Rethrow the regex error with more information. Can't use the
        // regex_error here as the constructor does not allow setting the
        // error message.
        throw std::runtime_error(
            "The regex '" + _rhs +
            "'is not an ECMAScript regex: " + std::string(e.what()));
      }
      getEngine().filter(
          input,
          [self_regex, &lhs, &subRes, this](const auto& e) {
            std::optional<string> entity;
            if constexpr (T == ResultTable::ResultType::KB) {
              entity = getIndex().idToOptionalString(e[lhs]);
            } else if (T == ResultTable::ResultType::LOCAL_VOCAB) {
              entity = subRes->idToOptionalString(e[lhs]);
            }
            (void)subRes;  // Silence unused warning.
            (void)this;
            if (!entity) {
              return true;
            }
            return std::regex_search(entity.value(), self_regex);
          },
          res);
    } break;
  }
  delete[] rhs_array;
}

// _____________________________________________________________________________
template <int WIDTH>
void Filter::computeResultFixedValue(
    ResultTable* resultTable,
    const std::shared_ptr<const ResultTable> subRes) const {
  LOG(DEBUG) << "Filter result computation..." << endl;
  IdTableStatic<WIDTH> result = resultTable->_idTable.moveToStatic<WIDTH>();
  const IdTableView<WIDTH> input = subRes->_idTable.asStaticView<WIDTH>();

  if (_lhsAsString) {
    AD_THROW(ad_semsearch::Exception::NOT_YET_IMPLEMENTED,
             "The str function is not yet supported within filters.");
  }

  // interpret the filters right hand side
  size_t lhs = _subtree->getVariableColumn(_lhs);
  Id rhs;
  Id rhs_upper_for_range;
  bool apply_range_filter = false;
  bool range_filter_inverse = false;
  switch (subRes->getResultType(lhs)) {
    case ResultTable::ResultType::KB: {
      std::string rhs_string = _rhs;
      if (ad_utility::isXsdValue(rhs_string)) {
        rhs_string = ad_utility::convertValueLiteralToIndexWord(rhs_string);
      } else if (ad_utility::isNumeric(_rhs)) {
        rhs_string = ad_utility::convertNumericToIndexWord(rhs_string);
      } else {
        // TODO: This is not standard conform, but currently required due to
        // our vocabulary storing iris with the greater than and
        // literals with their quotation marks.
        if (rhs_string.size() > 2 && rhs_string[1] == '<' &&
            rhs_string[0] == '"' && rhs_string.back() == '"') {
          // Remove the quotation marks surrounding the string.
          rhs_string = rhs_string.substr(1, rhs_string.size() - 2);
        } else if (std::count(rhs_string.begin(), rhs_string.end(), '"') > 2 &&
                   rhs_string.back() == '"') {
          // Remove the quotation marks surrounding the string.
          rhs_string = rhs_string.substr(1, rhs_string.size() - 2);
        }
      }

      // TODO<joka921> which level do we want for these filters
      auto level = TripleComponentComparator::Level::QUARTERNARY;
      if (_type == SparqlFilter::EQ || _type == SparqlFilter::NE) {
        rhs = getIndex().getVocab().lower_bound(rhs_string, level);
        rhs_upper_for_range =
            getIndex().getVocab().upper_bound(rhs_string, level);
        apply_range_filter = true;
        range_filter_inverse = _type == SparqlFilter::NE;
      } else if (_type == SparqlFilter::GE) {
        rhs = getIndex().getVocab().getValueIdForGE(rhs_string, level);
      } else if (_type == SparqlFilter::GT) {
        rhs = getIndex().getVocab().getValueIdForGT(rhs_string, level);
      } else if (_type == SparqlFilter::LT) {
        rhs = getIndex().getVocab().getValueIdForLT(rhs_string, level);
      } else if (_type == SparqlFilter::LE) {
        rhs = getIndex().getVocab().getValueIdForLE(rhs_string, level);
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

  if (apply_range_filter) {
    if (resultType != ResultTable::ResultType::KB) {
      AD_THROW(ad_semsearch::Exception::BAD_REQUEST,
               "Applying a range filter where datatype is not KB, this "
               "indicates a programming error, please report this");
    }
    if (range_filter_inverse) {
      computeFilterRange<ResultTable::ResultType::KB, WIDTH, true>(
          &result, lhs, rhs, rhs_upper_for_range, input, subRes);
    } else {
      computeFilterRange<ResultTable::ResultType::KB, WIDTH, false>(
          &result, lhs, rhs, rhs_upper_for_range, input, subRes);
    }
  } else {
    switch (resultType) {
      case ResultTable::ResultType::KB:
        computeFilterFixedValue<ResultTable::ResultType::KB>(&result, lhs, rhs,
                                                             input, subRes);
        break;
      case ResultTable::ResultType::VERBATIM:
        computeFilterFixedValue<ResultTable::ResultType::VERBATIM>(
            &result, lhs, rhs, input, subRes);
        break;
      case ResultTable::ResultType::FLOAT:
        computeFilterFixedValue<ResultTable::ResultType::FLOAT>(
            &result, lhs, rhs, input, subRes);
        break;
      case ResultTable::ResultType::LOCAL_VOCAB:
        computeFilterFixedValue<ResultTable::ResultType::LOCAL_VOCAB>(
            &result, lhs, rhs, input, subRes);
        break;
      case ResultTable::ResultType::TEXT:
        computeFilterFixedValue<ResultTable::ResultType::TEXT>(
            &result, lhs, rhs, input, subRes);
        break;
      default:
        AD_THROW(
            ad_semsearch::Exception::NOT_YET_IMPLEMENTED,
            "Tried to compute a filter on an unknown result type " +
                std::to_string(static_cast<int>(subRes->getResultType(lhs))));
        break;
    }
  }
  LOG(DEBUG) << "Filter result computation done." << endl;
  resultTable->_idTable = result.moveToDynamic();
}
