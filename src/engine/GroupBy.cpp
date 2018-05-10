// Copyright 2018, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Florian Kramer (florian.kramer@mail.uni-freiburg.de)

#include "GroupBy.h"

GroupBy::GroupBy(QueryExecutionContext *qec,
                 std::shared_ptr<QueryExecutionTree> subtree,
                 const vector<string>& groupByVariables,
                 const std::unordered_map<std::string, ParsedQuery::Alias>& aliases) :
  Operation(qec),
  _subtree(subtree),
  _groupByVariables(groupByVariables) {

  _aliases.reserve(aliases.size());
  for (auto it : aliases) {
    if (it.second._isAggregate) {
      _aliases.push_back(it.second);
    }
  }
  std::sort(_aliases.begin(), _aliases.end(),
            [](const ParsedQuery::Alias& a1, const ParsedQuery::Alias& a2) {
    return a1._varName < a2._varName;
  });

  // sort the groupByVariables to ensure the cache key is order invariant
  std::sort(_groupByVariables.begin(), _groupByVariables.end());

  // The returned columns are all groupByVariables followed by aggregrates
  size_t colIndex = 0;
  for (std::string var : _groupByVariables) {
    _varColMap[var] = colIndex;
    colIndex++;
  }
  for (const ParsedQuery::Alias& a : _aliases) {
    _varColMap[a._varName] = colIndex;
    colIndex++;
  }
}

string GroupBy::asString(size_t indent) const {
  std::ostringstream os;
  for (size_t i = 0; i < indent; ++i) { os << " "; }
  os << "GROUP_BY" << std::endl;
  for (const std::string var : _groupByVariables) {
    os << var << ", ";
  }
  for (auto p : _aliases) {
    os << p._function << ", ";
  }
  os << std::endl;
  os << _subtree->asString(indent);
  return os.str();
}

size_t GroupBy::getResultWidth() const {
  //TODO(Florian): stub
  return _varColMap.size();
}

size_t GroupBy::resultSortedOn() const {
  //TODO(Florian): stub
  return 0;
}

vector<pair<size_t, bool>> GroupBy::computeSortColumns(
    std::shared_ptr<QueryExecutionTree> subtree,
    const vector<string>& groupByVariables,
    const std::unordered_map<std::string, ParsedQuery::Alias>& aliasMap) {
  // Create sorted lists of the aliases and the group by variables to determine
  // the output column order, on which the sorting depends. Then populate
  // the vector of columns which should be sorted by using the subtrees
  // variable column map.

  std::vector<ParsedQuery::Alias> aliases;
  aliases.reserve(aliases.size());
  for (auto it : aliasMap) {
    if (it.second._isAggregate) {
      aliases.push_back(it.second);
    }
  }
  std::sort(aliases.begin(), aliases.end(),
            [](const ParsedQuery::Alias& a1, const ParsedQuery::Alias& a2) {
    return a1._varName < a2._varName;
  });

  std::vector<std::string> sortedGroupByVars;
  sortedGroupByVars.insert(sortedGroupByVars.end(), groupByVariables.begin(),
                           groupByVariables.end());

  // sort the groupByVariables to ensure the cache key is order invariant
  std::sort(sortedGroupByVars.begin(), sortedGroupByVars.end());


  std::unordered_map<string, size_t> inVarColMap = subtree->getVariableColumnMap();
  vector<pair<size_t, bool>> cols;

  // The returned columns are all groupByVariables followed by aggregrates
  for (std::string var : sortedGroupByVars) {
    cols.push_back({inVarColMap[var], true});
  }
  for (const ParsedQuery::Alias& a : aliases) {
    cols.push_back({inVarColMap[a._varName], true});
  }
  return cols;
}


std::unordered_map<string, size_t> GroupBy::getVariableColumns() const {
  return _varColMap;
}

float GroupBy::getMultiplicity(size_t col) {
  //TODO(Florian): stub
  return 0;
}

size_t GroupBy::getSizeEstimate() {
  //TODO(Florian): stub
  return 0;
}

size_t GroupBy::getCostEstimate() {
  //TODO(Florian): stub
  return 0;
}

template<typename T, typename C>
struct resizeIfVec {
  static void resize(T& t, int size) { (void) t; (void) size; }
};

template<typename C>
struct resizeIfVec<vector<C>, C> {
  static void resize(vector<C>& t, int size) {
    t.resize(size);
  }
};

template<typename A, typename R>
void doGroupBy(const vector<A>* input,
               const vector<size_t>& groupByCols,
               const vector<GroupBy::Aggregate>& aggregates,
               vector<R>* result) {
  if (input->size() == 0) {
    return;
  }

  std::vector<std::pair<size_t, Id>> currentGroupBlock;
  for (size_t col : groupByCols) {
    currentGroupBlock.push_back(std::pair<size_t, Id>(col, (*input)[0][col]));
  }
  size_t blockStart = 0;
  size_t blockEnd = 0;
  for (size_t pos = 1; pos < input->size(); pos++) {
    bool rowMatchesCurrentBlock = true;
    for (size_t i = 0; i < currentGroupBlock.size(); i++) {
      if ((*input)[pos][currentGroupBlock[i].first] != currentGroupBlock[i].second) {
        rowMatchesCurrentBlock = false;
        break;
      }
    }
    if (!rowMatchesCurrentBlock) {
      result->emplace_back();
      R& resultRow = result->back();
      // does nothing for arrays, resizes vectors
      resizeIfVec<R, typename R::value_type>::resize(resultRow, aggregates.size());
      blockEnd = pos - 1;
      for (const GroupBy::Aggregate &a : aggregates) {
        switch (a._type) {
        case GroupBy::AggregateType::AVG:
          AD_THROW(ad_semsearch::Exception::NOT_YET_IMPLEMENTED, "Average aggregation is not yet implemented.");
          break;
        case GroupBy::AggregateType::COUNT:
          resultRow[a._outCol] = blockEnd - blockStart + 1;
          break;
        case GroupBy::AggregateType::GROUP_CONCAT:
          AD_THROW(ad_semsearch::Exception::NOT_YET_IMPLEMENTED, "Group concatenation aggregation is not yet implemented.");
          break;
        case GroupBy::AggregateType::MAX:
          AD_THROW(ad_semsearch::Exception::NOT_YET_IMPLEMENTED, "Maximum aggregation is not yet implemented.");
          break;
        case GroupBy::AggregateType::MIN:
          AD_THROW(ad_semsearch::Exception::NOT_YET_IMPLEMENTED, "Minimum aggregation is not yet implemented.");
          break;
        case GroupBy::AggregateType::SAMPLE:
          resultRow[a._outCol] = (*input)[blockEnd][a._inCol];
          break;
        case GroupBy::AggregateType::SUM:
          AD_THROW(ad_semsearch::Exception::NOT_YET_IMPLEMENTED, "Sum aggregation is not yet implemented.");
          break;
        case GroupBy::AggregateType::FIRST:
          // This does the same as sample, as the non grouping rows have no
          // inherent order.
          resultRow[a._outCol] = (*input)[blockStart][a._inCol];
          break;
        case GroupBy::AggregateType::LAST:
          // This does the same as sample, as the non grouping rows have no
          // inherent order.
          resultRow[a._outCol] = (*input)[blockEnd][a._inCol];
          break;
        }
      }
      // setup for processing the next block
      blockStart = pos;
      for (size_t i = 0; i < currentGroupBlock.size(); i++) {
        currentGroupBlock[i].second = (*input)[pos][currentGroupBlock[i].first];
      }
    }
  }
  blockEnd = input->size() - 1;
  {
    result->emplace_back();
    R& resultRow = result->back();
    // does nothing for arrays, resizes vectors
    resizeIfVec<R, typename R::value_type>::resize(resultRow, aggregates.size());
    for (const GroupBy::Aggregate &a : aggregates) {
      switch (a._type) {
      case GroupBy::AggregateType::AVG:
        AD_THROW(ad_semsearch::Exception::NOT_YET_IMPLEMENTED, "Average aggregation is not yet implemented.");
        break;
      case GroupBy::AggregateType::COUNT:
        resultRow[a._outCol] = blockEnd - blockStart + 1;
        break;
      case GroupBy::AggregateType::GROUP_CONCAT:
        AD_THROW(ad_semsearch::Exception::NOT_YET_IMPLEMENTED, "Group concatenation aggregation is not yet implemented.");
        break;
      case GroupBy::AggregateType::MAX:
        AD_THROW(ad_semsearch::Exception::NOT_YET_IMPLEMENTED, "Maximum aggregation is not yet implemented.");
        break;
      case GroupBy::AggregateType::MIN:
        AD_THROW(ad_semsearch::Exception::NOT_YET_IMPLEMENTED, "Minimum aggregation is not yet implemented.");
        break;
      case GroupBy::AggregateType::SAMPLE:
        resultRow[a._outCol] = (*input)[blockEnd][a._inCol];
        break;
      case GroupBy::AggregateType::SUM:
        AD_THROW(ad_semsearch::Exception::NOT_YET_IMPLEMENTED, "Sum aggregation is not yet implemented.");
        break;
      case GroupBy::AggregateType::FIRST:
        // This does the same as sample, as the non grouping rows have no
        // inherent order.
        resultRow[a._outCol] = (*input)[blockStart][a._inCol];
        break;
      case GroupBy::AggregateType::LAST:
        // This does the same as sample, as the non grouping rows have no
        // inherent order.
        resultRow[a._outCol] = (*input)[blockEnd][a._inCol];
        break;
      }
    }
  }
}


// This struct is used to call doGroupBy with the correct template parameters.
// Using it is equivalent to a structure of nested if clauses, checking if
// the inputColCount and resultColCount are of a certain value, and then
// calling doGroupBy.
template<int InputColCount, int ResultColCount>
struct callDoGroupBy {
  static void call(int inputColCount, int resultColCount,
                   std::shared_ptr<const ResultTable> subresult,
                   const vector<size_t>& groupByCols,
                   const vector<GroupBy::Aggregate>& aggregates,
                   ResultTable* result) {
    if (InputColCount == inputColCount) {
      if (resultColCount == ResultColCount) {
        result->_fixedSizeData = new vector<array<Id, ResultColCount>>();
        doGroupBy<array<Id, InputColCount>, array<Id, ResultColCount>>(
              static_cast<vector<array<Id, InputColCount>>*>(subresult->_fixedSizeData),
              groupByCols,
              aggregates,
              static_cast<vector<array<Id, ResultColCount>>*>(result->_fixedSizeData));
      } else {
        callDoGroupBy<InputColCount, ResultColCount + 1>::call(inputColCount,
                                                               resultColCount,
                                                               subresult,
                                                               groupByCols,
                                                               aggregates,
                                                               result);
      }
    } else {
      callDoGroupBy<InputColCount + 1, ResultColCount>::call(inputColCount,
                                                             resultColCount,
                                                             subresult,
                                                             groupByCols,
                                                             aggregates,
                                                             result);
    }
  }
};

template<int ResultColCount>
struct callDoGroupBy<6, ResultColCount> {
  static void call(int inputColCount, int resultColCount,
                   std::shared_ptr<const ResultTable> subresult,
                   const vector<size_t>& groupByCols,
                   const vector<GroupBy::Aggregate>& aggregates,
                   ResultTable* result) {
    if (resultColCount == ResultColCount) {
      result->_fixedSizeData = new vector<array<Id, ResultColCount>>();
      doGroupBy<vector<Id>, array<Id, ResultColCount>>(
            &subresult->_varSizeData,
            groupByCols,
            aggregates,
            static_cast<vector<array<Id, ResultColCount>>*>(result->_fixedSizeData));
    } else {
      callDoGroupBy<6, ResultColCount + 1>::call(inputColCount,
                                                 resultColCount,
                                                 subresult,
                                                 groupByCols,
                                                 aggregates,
                                                 result);
    }
  }
};

template<int InputColCount>
struct callDoGroupBy<InputColCount, 6> {
  static void call(int inputColCount, int resultColCount,
                   std::shared_ptr<const ResultTable> subresult,
                   const vector<size_t>& groupByCols,
                   const vector<GroupBy::Aggregate>& aggregates,
                   ResultTable* result) {
    // avoid the quite numerous warnings about unused parameters
    (void) inputColCount;
    (void) resultColCount;
    doGroupBy<array<Id, InputColCount>, vector<Id>>(
          static_cast<vector<array<Id, InputColCount>>*>(subresult->_fixedSizeData),
          groupByCols,
          aggregates,
          &result->_varSizeData);
  }
};

template<>
struct callDoGroupBy<6, 6> {
  static void call(int inputColCount, int resultColCount,
                   std::shared_ptr<const ResultTable> subresult,
                   const vector<size_t>& groupByCols,
                   const vector<GroupBy::Aggregate>& aggregates,
                   ResultTable* result) {
    // avoid the quite numerous warnings about unused parameters
    (void) inputColCount;
    (void) resultColCount;
    doGroupBy<vector<Id>, vector<Id>>(
          &subresult->_varSizeData,
          groupByCols,
          aggregates,
          &result->_varSizeData);
  }
};

void GroupBy::computeResult(ResultTable* result) const {
  std::vector<size_t> groupByColumns;

  result->_sortedBy = resultSortedOn();
  result->_nofColumns = getResultWidth();

  // TODO(florian): return correct result types for other aggregates such as
  // COUNT.
  result->_resultTypes.resize(result->_nofColumns);
  for (int i = 0; i < result->_nofColumns; i++) {
    result->_resultTypes[i] = ResultTable::ResultType::KB;
  }

  std::vector<Aggregate> aggregates;
  aggregates.reserve(_aliases.size() + _groupByVariables.size());

  // parse the group by columns
  std::unordered_map<string, Id> subtreeVarCols = _subtree->getVariableColumnMap();
  for (const string& var : _groupByVariables) {
    auto it = subtreeVarCols.find(var);
    if (it == subtreeVarCols.end()) {
      LOG(WARN) << "Group by variable " << var << " is not part of the query."
                << std::endl;
      if (result->_nofColumns == 1) {
        result->_fixedSizeData = new vector<array<Id, 1>>();
      } else if (result->_nofColumns == 2) {
        result->_fixedSizeData = new vector<array<Id, 2>>();
      } else if (result->_nofColumns == 3) {
        result->_fixedSizeData = new vector<array<Id, 3>>();
      } else if (result->_nofColumns == 4) {
        result->_fixedSizeData = new vector<array<Id, 4>>();
      } else if (result->_nofColumns == 5) {
        result->_fixedSizeData = new vector<array<Id, 5>>();
      }
      result->finish();
      return;
    }
    groupByColumns.push_back(it->second);
    // Add an "identity" aggregate in the form of a sample aggregate to
    // facilitate the passthrough of the groupBy columns into the result
    aggregates.emplace_back();
    aggregates.back()._type = AggregateType::SAMPLE;
    aggregates.back()._inCol = it->second;
    aggregates.back()._outCol = _varColMap.find(var)->second;
    aggregates.back()._userData = nullptr;
  }

  // parse the aggregate aliases
  for (const ParsedQuery::Alias& alias : _aliases) {
    if (alias._isAggregate) {
      aggregates.emplace_back();
      if (ad_utility::startsWith(alias._function, "COUNT")) {
        aggregates.back()._type = AggregateType::COUNT;
      } else if (ad_utility::startsWith(alias._function, "GROUP_CONCAT")) {
        aggregates.back()._type = AggregateType::GROUP_CONCAT;
      } else if (ad_utility::startsWith(alias._function, "SAMPLE")) {
        aggregates.back()._type = AggregateType::SAMPLE;
      } else if (ad_utility::startsWith(alias._function, "MIN")) {
        aggregates.back()._type = AggregateType::MIN;
      } else if (ad_utility::startsWith(alias._function, "MAX")) {
        aggregates.back()._type = AggregateType::MAX;
      } else if (ad_utility::startsWith(alias._function, "SUM")) {
        aggregates.back()._type = AggregateType::SUM;
      } else if (ad_utility::startsWith(alias._function, "AVG")) {
        aggregates.back()._type = AggregateType::AVG;
      } else {
        LOG(WARN) << "Unknown aggregate " << alias._function << std::endl;
        aggregates.pop_back();
        continue;
      }

      std::string inVarName;
      if (aggregates.back()._type == AggregateType::GROUP_CONCAT) {
        size_t varStart = alias._function.find('(');
        size_t varStop = alias._function.rfind(')');
        size_t delimitorPos = alias._function.find(';');
        if (varStop > varStart && varStop != std::string::npos
            && varStart != std::string::npos) {
          // found a matching pair of brackets
          if (delimitorPos != std::string::npos) {
            // found a delimiter, need to look for a separator assignment
            inVarName = alias._function.substr(varStart + 1,
                                               delimitorPos - varStart - 1);
            std::string concatString = alias._function.substr(delimitorPos + 1,
                                                              varStop - delimitorPos - 1);
            concatString = ad_utility::strip(concatString, " ");
            size_t startConcat = concatString.find('"');
            size_t stopConcat = concatString.rfind('"');
            if (stopConcat > startConcat && stopConcat != std::string::npos
                && startConcat != std::string::npos) {
              aggregates.back()._userData
                  = new std::string(alias._function.substr(startConcat + 1,
                                                           stopConcat - startConcat - 1));
            } else {
              LOG(WARN) << "Unable to parse the delimiter in GROUP_CONCAT"
                           "aggregrate " << alias._function;
              aggregates.back()._userData = new std::string(" ");
            }
          } else {
            // found no delimiter, using the default separator ' '
            inVarName = alias._function.substr(varStart + 1,
                                               varStop - varStart - 1);
            aggregates.back()._userData = new std::string(" ");
          }

        }
      } else {
        size_t varStart = alias._function.find('(');
        size_t varStop = alias._function.rfind(')');
        if (varStop > varStart && varStop != std::string::npos
            && varStart != std::string::npos) {
          inVarName = alias._function.substr(varStart + 1,
                                             varStop - varStart - 1);
        }
      }
      inVarName = ad_utility::strip(inVarName, " ");
      auto inIt = subtreeVarCols.find(inVarName);
      if (inIt == subtreeVarCols.end()) {
        LOG(WARN) << "The aggregate alias " << alias._function << " refers to "
                  << "a column not present in the query." << std::endl;
        if (result->_nofColumns == 1) {
          result->_fixedSizeData = new vector<array<Id, 1>>();
        } else if (result->_nofColumns == 2) {
          result->_fixedSizeData = new vector<array<Id, 2>>();
        } else if (result->_nofColumns == 3) {
          result->_fixedSizeData = new vector<array<Id, 3>>();
        } else if (result->_nofColumns == 4) {
          result->_fixedSizeData = new vector<array<Id, 4>>();
        } else if (result->_nofColumns == 5) {
          result->_fixedSizeData = new vector<array<Id, 5>>();
        }
        result->finish();
        return;
      }
      aggregates.back()._inCol = inIt->second;
      aggregates.back()._outCol = _varColMap.find(alias._varName)->second;

    }
  }

  std::vector<size_t> groupByCols;
  groupByCols.reserve(_groupByVariables.size());
  for (const string& var : _groupByVariables) {
    groupByCols.push_back(subtreeVarCols[var]);
  }

  std::shared_ptr<const ResultTable> subresult = _subtree->getResult();

  callDoGroupBy<1, 1>::call(subresult->_nofColumns, aggregates.size(), subresult,
                            groupByCols, aggregates, result);

  std::cout << "Group by result size " << result->size() << std::endl;

  // Free the user data used by GROUP_CONCAT aggregates.
  for (Aggregate& a : aggregates) {
    if (a._type == AggregateType::GROUP_CONCAT) {
      delete static_cast<std::string*>(a._userData);
    }
  }
  result->finish();
}
