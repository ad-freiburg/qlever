// Copyright 2018, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Florian Kramer (florian.kramer@mail.uni-freiburg.de)

#include "GroupBy.h"

#include "../index/Index.h"
#include "../util/Conversions.h"
#include "../util/HashSet.h"
#include "./sparqlExpressions/SparqlExpression.h"
#include "CallFixedSize.h"

GroupBy::GroupBy(QueryExecutionContext* qec,
                 const vector<string>& groupByVariables,
                 const std::vector<ParsedQuery::Alias>& aliases)
    : Operation(qec), _subtree(nullptr), _groupByVariables(groupByVariables) {
  _aliases.reserve(aliases.size());
  for (const ParsedQuery::Alias& a : aliases) {
    _aliases.push_back(a);
  }

  std::sort(_aliases.begin(), _aliases.end(),
            [](const ParsedQuery::Alias& a1, const ParsedQuery::Alias& a2) {
              return a1._outVarName < a2._outVarName;
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
    _varColMap[a._outVarName] = colIndex;
    colIndex++;
  }
}

void GroupBy::setSubtree(std::shared_ptr<QueryExecutionTree> subtree) {
  _subtree = std::move(subtree);
}

string GroupBy::asString(size_t indent) const {
  const auto varMap = getVariableColumns();
  const auto varMapInput = _subtree->getVariableColumns();
  std::ostringstream os;
  for (size_t i = 0; i < indent; ++i) {
    os << " ";
  }
  os << "GROUP_BY ";
  for (const std::string& var : _groupByVariables) {
    os << varMap.at(var) << ", ";
  }
  for (const auto& p : _aliases) {
    os << p._expression.getCacheKey(varMapInput) << " AS "
       << varMap.at(p._outVarName);
  }
  os << std::endl;
  os << _subtree->asString(indent);
  return os.str();
}

string GroupBy::getDescriptor() const {
  return "GroupBy on " + ad_utility::join(_groupByVariables, ' ');
}

size_t GroupBy::getResultWidth() const { return _varColMap.size(); }

vector<size_t> GroupBy::resultSortedOn() const {
  auto varCols = getVariableColumns();
  vector<size_t> sortedOn;
  sortedOn.reserve(_groupByVariables.size());
  for (const std::string& var : _groupByVariables) {
    sortedOn.push_back(varCols[var]);
  }
  return sortedOn;
}

vector<pair<size_t, bool>> GroupBy::computeSortColumns(
    const QueryExecutionTree* inputTree) {
  vector<pair<size_t, bool>> cols;
  if (_groupByVariables.empty()) {
    // the entire input is a single group, no sorting needs to be done
    return cols;
  }

  ad_utility::HashMap<string, size_t> inVarColMap =
      inputTree->getVariableColumns();

  std::unordered_set<size_t> sortColSet;

  for (std::string var : _groupByVariables) {
    size_t col = inVarColMap[var];
    // avoid sorting by a column twice
    if (sortColSet.find(col) == sortColSet.end()) {
      sortColSet.insert(col);
      cols.push_back({col, false});
    }
  }

  // TODO<joka921> What does this do? is it needed? the groupByVariables should
  // be all there.
  /*
  for (const ParsedQuery::Alias& a : _aliases) {
    size_t col = inVarColMap[a._inVarName];
    if (sortColSet.find(col) == sortColSet.end()) {
      sortColSet.insert(col);
      cols.push_back({inVarColMap[a._inVarName], false});
    }
  }
   */
  return cols;
}

ad_utility::HashMap<string, size_t> GroupBy::getVariableColumns() const {
  return _varColMap;
}

float GroupBy::getMultiplicity(size_t col) {
  // Group by should currently not be used in the optimizer, unless
  // it is part of a subquery. In that case multiplicities may only be
  // taken from the actual result
  (void)col;
  return 1;
}

size_t GroupBy::getSizeEstimate() {
  // TODO: stub implementation of getSizeEstimate()
  return _subtree->getSizeEstimate();
}

size_t GroupBy::getCostEstimate() {
  // TODO: add the cost of the actual group by operation to the cost.
  // Currently group by is only added to the optimizer as a terminal operation
  // and its cost should not affect the optimizers results.
  return _subtree->getCostEstimate();
}

template <typename T, typename C>
struct resizeIfVec {
  static void resize(T& t, int size) {
    (void)t;
    (void)size;
  }
};

template <typename C>
struct resizeIfVec<vector<C>, C> {
  static void resize(vector<C>& t, int size) { t.resize(size); }
};

template <int OUT_WIDTH>
void GroupBy::processGroup(
    const GroupBy::Aggregate& aggregate,
    const sparqlExpression::EvaluationContext* evaluationContextIn,
    size_t blockStart, size_t blockEnd, IdTableStatic<OUT_WIDTH>* result,
    size_t resultRow, size_t resultColumn, ResultTable* outTable,
    ResultTable::ResultType* resultType) const {
  auto evaluationContext = *evaluationContextIn;
  evaluationContext._beginIndex = blockStart;
  evaluationContext._endIndex = blockEnd;

  sparqlExpression::ExpressionResult expressionResult =
      aggregate._expression.getPimpl()->evaluate(&evaluationContext);

  auto& resultEntry = result->operator()(resultRow, resultColumn);

  auto visitor = [&]<sparqlExpression::SingleExpressionResult T>(
                     T&& singleResult) mutable {
    constexpr static bool isStrongId =
        std::is_same_v<T, sparqlExpression::StrongIdWithResultType>;
    AD_CHECK(sparqlExpression::isConstantResult<T>);
    if constexpr (isStrongId) {
      resultEntry = singleResult._id._value;
      *resultType = singleResult._type;
    } else if constexpr (sparqlExpression::isConstantResult<T>) {
      *resultType =
          sparqlExpression::detail::expressionResultTypeToQleverResultType<T>();
      resultEntry = sparqlExpression::detail::constantExpressionResultToId(
          singleResult, *(outTable->_localVocab), false);
    }
  };

  std::visit(visitor, std::move(expressionResult));
}

/**
 * @brief This method takes a single group and computes the output for the
 *        given aggregate.
 * @param a The aggregate which should be used to create the output
 * @param blockStart Where the group start.
 * @param blockEnd Where the group ends.
 * @param input The input Table.
 * @param result
 * @param inTable The input ResultTable, which is required for its local
 *                vocabulary
 * @param outTable The output ResultTable, the vocabulary of which needs to be
 *                 expanded for GROUP_CONCAT aggregates
 * @param distinctHashSet An empty hash set. This is only passed in as an
 *                        argument to allow for efficient reusage of its
 *                        its already allocated storage.
 */

template <int IN_WIDTH, int OUT_WIDTH>
void GroupBy::doGroupBy(const IdTable& dynInput,
                        const vector<size_t>& groupByCols,
                        const vector<GroupBy::Aggregate>& aggregates,
                        IdTable* dynResult, const ResultTable* inTable,
                        ResultTable* outTable) const {
  LOG(DEBUG) << "Group by input size " << dynInput.size() << std::endl;
  if (dynInput.size() == 0) {
    return;
  }
  const IdTableView<IN_WIDTH> input = dynInput.asStaticView<IN_WIDTH>();
  IdTableStatic<OUT_WIDTH> result = dynResult->moveToStatic<OUT_WIDTH>();

  sparqlExpression::VariableToColumnAndResultTypeMap columnMap;
  for (const auto& [variable, columnIndex] : _subtree->getVariableColumns()) {
    columnMap[variable] =
        std::pair(columnIndex, inTable->getResultType(columnIndex));
  }

  sparqlExpression::EvaluationContext evaluationInput(
      *getExecutionContext(), columnMap, inTable->_data,
      getExecutionContext()->getAllocator(), *outTable->_localVocab);

  if (groupByCols.empty()) {
    // The entire input is a single group
    size_t blockStart = 0;
    size_t blockEnd = input.size();

    result.emplace_back();
    size_t resIdx = result.size() - 1;
    for (const GroupBy::Aggregate& a : aggregates) {
      processGroup(a, &evaluationInput, blockStart, blockEnd, &result, resIdx,
                   a._outCol, outTable, &outTable->_resultTypes[a._outCol]);
    }
    *dynResult = result.moveToDynamic();
    return;
  }

  // This stores the values of the group by cols for the current block. A block
  // ends when one of these values changes.
  std::vector<std::pair<size_t, Id>> currentGroupBlock;
  for (size_t col : groupByCols) {
    currentGroupBlock.push_back(std::pair<size_t, Id>(col, input(0, col)));
  }
  size_t blockStart = 0;
  size_t blockEnd = 0;
  auto checkTimeoutAfterNCalls = checkTimeoutAfterNCallsFactory(32000);
  for (size_t pos = 1; pos < input.size(); pos++) {
    checkTimeoutAfterNCalls(currentGroupBlock.size());
    bool rowMatchesCurrentBlock = true;
    for (size_t i = 0; i < currentGroupBlock.size(); i++) {
      if (input(pos, currentGroupBlock[i].first) !=
          currentGroupBlock[i].second) {
        rowMatchesCurrentBlock = false;
        break;
      }
    }
    if (!rowMatchesCurrentBlock) {
      blockEnd = pos;

      result.emplace_back();
      size_t resIdx = result.size() - 1;
      for (size_t i = 0; i < groupByCols.size(); ++i) {
        result(resIdx, i) = input(blockStart, groupByCols[i]);
      }
      for (const GroupBy::Aggregate& a : aggregates) {
        processGroup(a, &evaluationInput, blockStart, blockEnd, &result, resIdx,
                     a._outCol, outTable, &outTable->_resultTypes[a._outCol]);
      }
      // setup for processing the next block
      blockStart = pos;
      for (size_t i = 0; i < currentGroupBlock.size(); i++) {
        currentGroupBlock[i].second = input(pos, currentGroupBlock[i].first);
      }
    }
  }
  blockEnd = input.size();
  {
    result.emplace_back();
    size_t resIdx = result.size() - 1;
    for (size_t i = 0; i < groupByCols.size(); ++i) {
      result(resIdx, i) = input(blockStart, groupByCols[i]);
    }
    for (const GroupBy::Aggregate& a : aggregates) {
      processGroup(a, &evaluationInput, blockStart, blockEnd, &result, resIdx,
                   a._outCol, outTable, &outTable->_resultTypes[a._outCol]);
      for (size_t i = 0; i < groupByCols.size(); ++i) {
        result(resIdx, i) = input(blockStart, groupByCols[i]);
      }
    }
  }
  *dynResult = result.moveToDynamic();
}

void GroupBy::computeResult(ResultTable* result) {
  LOG(DEBUG) << "GroupBy result computation..." << std::endl;
  std::vector<size_t> groupByColumns;

  result->_sortedBy = resultSortedOn();
  result->_data.setCols(getResultWidth());

  std::vector<Aggregate> aggregates;
  aggregates.reserve(_aliases.size() + _groupByVariables.size());

  // parse the group by columns
  ad_utility::HashMap<string, size_t> subtreeVarCols =
      _subtree->getVariableColumns();
  for (const string& var : _groupByVariables) {
    auto it = subtreeVarCols.find(var);
    if (it == subtreeVarCols.end()) {
      LOG(WARN) << "Group by variable " << var << " is not groupable."
                << std::endl;
      AD_THROW(ad_semsearch::Exception::BAD_QUERY,
               "Groupby variable " + var + " is not groupable");
    }

    groupByColumns.push_back(it->second);
    /*
    // Add an "identity" aggregate in the form of a sample aggregate to
    // facilitate the passthrough of the groupBy columns into the result
    sparqlExpression::detail::Variable v;
    v._variable = var;
    shared_ptr<sparqlExpression::SparqlExpression> ptr =
    std::make_shared<sparqlExpression::VariableExpression>(std::move(v));
    aggregates.push_back(Aggregate{sparqlExpression::SparqlExpressionPimpl{std::move(ptr)},
    _varColMap.find(var)->second});
     */
  }

  // parse the aggregate aliases
  for (const ParsedQuery::Alias& alias : _aliases) {
    aggregates.push_back(Aggregate{alias._expression,
                                   _varColMap.find(alias._outVarName)->second});
  }

  std::shared_ptr<const ResultTable> subresult = _subtree->getResult();
  LOG(DEBUG) << "GroupBy subresult computation done" << std::endl;

  RuntimeInformation& runtimeInfo = getRuntimeInfo();
  runtimeInfo.addChild(_subtree->getRootOperation()->getRuntimeInfo());

  // populate the result type vector
  result->_resultTypes.resize(result->_data.cols());

  std::vector<size_t> groupByCols;
  groupByCols.reserve(_groupByVariables.size());
  for (const string& var : _groupByVariables) {
    groupByCols.push_back(subtreeVarCols[var]);
  }

  std::vector<ResultTable::ResultType> inputResultTypes;
  inputResultTypes.reserve(subresult->_data.cols());
  for (size_t i = 0; i < subresult->_data.cols(); i++) {
    inputResultTypes.push_back(subresult->getResultType(i));
  }

  int inWidth = subresult->_data.cols();
  int outWidth = result->_data.cols();

  CALL_FIXED_SIZE_2(inWidth, outWidth, doGroupBy, subresult->_data, groupByCols,
                    aggregates, &result->_data, subresult.get(), result);
  LOG(DEBUG) << "GroupBy result computation done." << std::endl;
}
