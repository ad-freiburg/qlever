// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)

#include "Filter.h"

#include <algorithm>
#include <optional>
#include <regex>
#include <sstream>

#include "../global/ValueIdComparators.h"
#include "../parser/TurtleParser.h"
#include "../util/Iterators.h"
#include "../util/LambdaHelpers.h"
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
string Filter::asStringImpl(size_t indent) const {
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
  return std::move(os).str();
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
  return std::move(os).str();
}

namespace {
[[noreturn]] void throwNotSupported(std::string_view filterType) {
  AD_THROW(ad_semsearch::Exception::NOT_YET_IMPLEMENTED,
           absl::StrCat(filterType,
                        " filtering with a variable right side has not yet "
                        "been implemented."));
}
}  // namespace

template <int WIDTH>
void Filter::computeResultDynamicValue(IdTable* dynResult, size_t lhsInd,
                                       size_t rhsInd, const IdTable& dynInput) {
  const IdTableView<WIDTH> input = dynInput.asStaticView<WIDTH>();
  IdTableStatic<WIDTH> result = dynResult->moveToStatic<WIDTH>();
  switch (_type) {
    case SparqlFilter::LT:
    case SparqlFilter::LE:
    case SparqlFilter::EQ:
    case SparqlFilter::NE:
    case SparqlFilter::GT:
    case SparqlFilter::GE: {
      auto comparison = toComparison(_type);
      getEngine().filter(
          input,
          [lhsInd, rhsInd, comparison](const auto& e) {
            return valueIdComparators::compareIds(e[lhsInd], e[rhsInd],
                                                  comparison);
          },
          &result);
      break;
    }
      // Note: It is okay to have no `break` after the following cases, because
      // `throwNotSupported` always throws and never returns. The compiler is
      // aware of this because of the `[[noreturn]]` attribute and thus doesn't
      // emit a fallthrough warning.
    case SparqlFilter::LANG_MATCHES:
      throwNotSupported("Language");
    case SparqlFilter::REGEX:
      throwNotSupported("Regex");
    case SparqlFilter::PREFIX:
      throwNotSupported("Prefix");
  }
  *dynResult = result.moveToDynamic();
}

// _____________________________________________________________________________
void Filter::computeResult(ResultTable* result) {
  LOG(DEBUG) << "Getting sub-result for Filter result computation..." << endl;
  shared_ptr<const ResultTable> subRes = _subtree->getResult();
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
                      lhsInd, rhsInd, subRes->_idTable);
  } else {
    // compare the left column to a fixed value
    CALL_FIXED_SIZE_1(width, computeResultFixedValue, result, subRes);
  }

  LOG(DEBUG) << "Filter result computation done." << endl;
}

// _____________________________________________________________________________
template <int WIDTH>
void Filter::computeFilterRange(IdTableStatic<WIDTH>* res, size_t lhs,
                                Id rhs_lower, Id rhs_upper,
                                const IdTableView<WIDTH>& input,
                                shared_ptr<const ResultTable> subRes) const {
  bool lhs_is_sorted =
      subRes->_sortedBy.size() > 0 && subRes->_sortedBy[0] == lhs;
  switch (_type) {
    case SparqlFilter::LT:
    case SparqlFilter::LE:
    case SparqlFilter::EQ:
    case SparqlFilter::NE:
    case SparqlFilter::GT:
    case SparqlFilter::GE: {
      const auto comparison = toComparison(_type);
      if (lhs_is_sorted) {
        // The input data is sorted, use binary search to locate the first
        // and last element that match rhs and copy the range.
        auto accessColumnLambda = ad_utility::makeAssignableLambda(
            [lhs](const auto& idTable, auto i) { return idTable(i, lhs); });

        using Iterator =
            ad_utility::IteratorForAccessOperator<std::decay_t<decltype(input)>,
                                                  decltype(accessColumnLambda)>;
        auto begin = Iterator{&input, 0, accessColumnLambda};
        auto end = Iterator{&input, input.size(), accessColumnLambda};

        auto resultRanges = valueIdComparators::getRangesForEqualIds(
            begin, end, rhs_lower, rhs_upper, comparison);

        auto resultSize =
            std::accumulate(resultRanges.begin(), resultRanges.end(), 0ul,
                            [](const auto& value, const auto& range) {
                              return value + (range.second - range.first);
                            });
        res->reserve(resultSize);
        for (auto range : resultRanges) {
          auto actualBegin = input.begin() + (range.first - begin);
          auto actualEnd = input.begin() + (range.second - begin);
          res->insert(res->end(), actualBegin, actualEnd);
        }
      } else {
        // The input is not sorted, compare each element.
        getEngine().filter(
            input,
            [lhs, rhs_lower, rhs_upper, comparison](const auto& e) {
              return valueIdComparators::compareWithEqualIds(
                  e[lhs], rhs_lower, rhs_upper, comparison);
            },
            res);
      }
      break;
    }
    default:
      // This should be unreachable.
      AD_FAIL();
  }
}

// _____________________________________________________________________________
template <int WIDTH>
void Filter::computeFilterFixedValue(
    IdTableStatic<WIDTH>* res, size_t lhs, Id rhs,
    const IdTableView<WIDTH>& input,
    shared_ptr<const ResultTable> subRes) const {
  bool lhs_is_sorted =
      subRes->_sortedBy.size() > 0 && subRes->_sortedBy[0] == lhs;
  Id* rhs_array = new Id[res->cols()];
  IdTable::row_type rhs_row(rhs_array, res->cols());

  switch (_type) {
    case SparqlFilter::LT:
    case SparqlFilter::LE:
    case SparqlFilter::EQ:
    case SparqlFilter::NE:
    case SparqlFilter::GT:
    case SparqlFilter::GE: {
      const auto comparison = toComparison(_type);
      if (lhs_is_sorted) {
        auto accessColumnLambda = ad_utility::makeAssignableLambda(
            [lhs](const auto& idTable, auto i) { return idTable(i, lhs); });

        using Iterator =
            ad_utility::IteratorForAccessOperator<std::decay_t<decltype(input)>,
                                                  decltype(accessColumnLambda)>;
        auto begin = Iterator{&input, 0, accessColumnLambda};
        auto end = Iterator{&input, input.size(), accessColumnLambda};

        auto resultRanges =
            valueIdComparators::getRangesForId(begin, end, rhs, comparison);

        auto resultSize =
            std::accumulate(resultRanges.begin(), resultRanges.end(), 0ul,
                            [](const auto& value, const auto& range) {
                              return value + (range.second - range.first);
                            });
        res->reserve(resultSize);

        for (auto range : resultRanges) {
          auto actualBegin = input.begin() + (range.first - begin);
          auto actualEnd = input.begin() + (range.second - begin);
          res->insert(res->end(), actualBegin, actualEnd);
        }
      } else {
        getEngine().filter(
            input,
            [lhs, rhs, comparison](const auto& e) {
              return valueIdComparators::compareIds(e[lhs], rhs, comparison);
            },
            res);
      }
      break;
    }
    case SparqlFilter::LANG_MATCHES:
      getEngine().filter(
          input,
          [this, lhs, &subRes](const auto& e) {
            std::optional<string> entity;
            if (e[lhs].getDatatype() == Datatype::VocabIndex) {
              entity = getIndex().idToOptionalString(e[lhs]);
            } else if (e[lhs].getDatatype() == Datatype::LocalVocabIndex) {
              entity =
                  subRes->indexToOptionalString(e[lhs].getLocalVocabIndex());
            }
            if (!entity) {
              return true;
            }
            return entity.value().ends_with(_rhs);
          },
          res);
      break;
    case SparqlFilter::PREFIX:
      // Check if the prefix filter can be applied. Use the regex filter
      // otherwise.
      {
        // remove the leading '^' symbol
        // TODO<joka921> Is this a "columnIndex"?
        ad_utility::HashMap<UnknownIndex, vector<string>> lhsRhsMap;
        lhsRhsMap[lhs].push_back(_rhs.substr(1));
        for (size_t i = 0; i < _additionalPrefixRegexes.size(); ++i) {
          lhsRhsMap[_subtree->getVariableColumn(_additionalLhs[i])].push_back(
              _additionalPrefixRegexes[i].substr(1));
        }
        // TODO<joka921> Is this a "columnIndex"?
        ad_utility::HashMap<UnknownIndex,
                            std::vector<std::pair<VocabIndex, VocabIndex>>>
            prefixRanges;
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

        const std::optional<ColumnIndex> sortedLhs =
            [&prefixRanges, &subRes]() -> std::optional<ColumnIndex> {
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
            rhs_array[lhs] = Id::makeFromVocabIndex(lowerBound);
            const auto& lower =
                std::lower_bound(input.begin(), input.end(), rhs_row,
                                 [lhs](const auto& l, const auto& r) {
                                   return l[lhs] < r[lhs];
                                 });
            if (lower != input.end()) {
              // There is at least one element in the input that is also within
              // the range, look for the upper boundary and then copy all
              // elements within the range.
              rhs_array[lhs] = Id::makeFromVocabIndex(upperBound);
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
                // TODO<joka921> prefixRanges on Ids
                [lhs, p = prefixRanges[lhs][0]](const auto& e) {
                  return Id::makeFromVocabIndex(p.first) <= e[lhs] &&
                         e[lhs] < Id::makeFromVocabIndex(p.second);
                },
                res);
          } else {
            getEngine().filter(
                input,
                [&prefixRanges](const auto& e) {
                  return std::any_of(
                      prefixRanges.begin(), prefixRanges.end(),
                      [&e](const auto& x) {
                        const auto& vec = x.second;
                        return std::any_of(
                            vec.begin(), vec.end(),
                            [&e, &l = x.first](const auto& p) {
                              return Id::makeFromVocabIndex(p.first) <= e[l] &&
                                     e[l] < Id::makeFromVocabIndex(p.second);
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
            if (e[lhs].getDatatype() == Datatype::VocabIndex) {
              entity = getIndex().idToOptionalString(e[lhs]);
            } else if (e[lhs].getDatatype() == Datatype::LocalVocabIndex) {
              entity =
                  subRes->indexToOptionalString(e[lhs].getLocalVocabIndex());
            }
            // TODO<joka921> Should this be true or false? There is a similar
            // place for the prefix filter.
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

  // Interpret the right hand side of the filters.
  size_t lhs = _subtree->getVariableColumn(_lhs);
  Id rhs;
  std::optional<Id> rhs_upper_for_range;
  switch (subRes->getResultType(lhs)) {
      // TODO<joka921> Eliminate the "ResultType" completely
    case ResultTable::ResultType::KB:
    case ResultTable::ResultType::VERBATIM:
    case ResultTable::ResultType::FLOAT: {
      std::string rhs_string = _rhs;
      // TODO<joka921> This parsing should really be in the sparql parser

      if (!(_type == SparqlFilter::EQ || _type == SparqlFilter::NE ||
            _type == SparqlFilter::LE || _type == SparqlFilter::GE ||
            _type == SparqlFilter::GT || _type == SparqlFilter::LT)) {
        // all other filters (e.g. regexes) don't use the `Id` in `rhs`
        break;
      }
      TripleComponent rhsObject =
          TurtleStringParser<TokenizerCtre>::parseTripleObject(rhs_string);
      if (rhsObject.isInt()) {
        rhs = Id::makeFromInt(rhsObject.getInt());
      } else if (rhsObject.isDouble()) {
        rhs = Id::makeFromDouble(rhsObject.getDouble());
      } else {
        // TODO<joka921> give the `TripleComponent` a visit function such that
        // this becomes a compile time check.
        AD_CHECK(rhsObject.isString());
        rhs_string = rhsObject.getString();
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

        // TODO<joka921> which level do we want for these filters
        auto level = TripleComponentComparator::Level::QUARTERNARY;
        if (_type == SparqlFilter::EQ || _type == SparqlFilter::NE ||
            _type == SparqlFilter::LE || _type == SparqlFilter::GE ||
            _type == SparqlFilter::GT || _type == SparqlFilter::LT) {
          rhs = Id::makeFromVocabIndex(
              getIndex().getVocab().lower_bound(rhs_string, level));
          rhs_upper_for_range = Id::makeFromVocabIndex(
              getIndex().getVocab().upper_bound(rhs_string, level));
        }
        // All other types of filters do not use r and work on _rhs directly
      }
      break;
    }
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
        rhs = Id::makeFromLocalVocabIndex(
            LocalVocabIndex::make(subRes->_localVocab->size()));
        for (size_t i = 0; i < subRes->_localVocab->size(); ++i) {
          if ((*subRes->_localVocab)[i] == _rhs) {
            rhs = Id::makeFromLocalVocabIndex(LocalVocabIndex::make(i));
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

  // TODO<joka921> handle the case where we have multiple "equal" IDs because of
  // The sort level for Literals and IDs (the `rhs_for_upper...` variable is
  // then set
  if (rhs_upper_for_range.has_value()) {
    computeFilterRange(&result, lhs, rhs, rhs_upper_for_range.value(), input,
                       subRes);
  } else {
    computeFilterFixedValue(&result, lhs, rhs, input, subRes);
  }
  LOG(DEBUG) << "Filter result computation done." << endl;
  resultTable->_idTable = result.moveToDynamic();
}
