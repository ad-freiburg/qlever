// Copyright 2018, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author:
//   2018      Florian Kramer (florian.kramer@mail.uni-freiburg.de)
//   2020-     Johannes Kalmbach (kalmbach@informatik.uni-freiburg.de)

#include <absl/strings/str_join.h>
#include <engine/CallFixedSize.h>
#include <engine/GroupBy.h>
#include <engine/sparqlExpressions/SparqlExpression.h>
#include <index/Index.h>
#include <parser/Alias.h>
#include <util/Conversions.h>
#include <util/HashSet.h>

// _______________________________________________________________________________________________
GroupBy::GroupBy(QueryExecutionContext* qec, vector<Variable> groupByVariables,
                 std::vector<Alias> aliases,
                 std::shared_ptr<QueryExecutionTree> subtree)
    : Operation{qec},
      _groupByVariables{std::move(groupByVariables)},
      _aliases{std::move(aliases)} {
  // sort the aliases and groupByVariables to ensure the cache key is order
  // invariant.
  std::ranges::sort(_aliases, std::less<>{},
                    [](const Alias& a) { return a._target.name(); });

  std::ranges::sort(_groupByVariables, std::less<>{}, &Variable::name);

  auto sortColumns = computeSortColumns(subtree.get());
  _subtree =
      QueryExecutionTree::createSortedTree(std::move(subtree), sortColumns);
}

string GroupBy::asStringImpl(size_t indent) const {
  const auto varMap = getInternallyVisibleVariableColumns();
  const auto varMapInput = _subtree->getVariableColumns();
  std::ostringstream os;
  for (size_t i = 0; i < indent; ++i) {
    os << " ";
  }
  os << "GROUP_BY ";
  for (const auto& var : _groupByVariables) {
    os << varMap.at(var) << ", ";
  }
  for (const auto& alias : _aliases) {
    os << alias._expression.getCacheKey(varMapInput) << " AS "
       << varMap.at(alias._target);
  }
  os << std::endl;
  os << _subtree->asString(indent);
  return std::move(os).str();
}

string GroupBy::getDescriptor() const {
  // TODO<C++20 Views (clang16): Do this lazily using std::views::transform.
  // TODO<C++23>:: Use std::views::join_with.
  return "GroupBy on " + absl::StrJoin(_groupByVariables, " ",
                                       [](std::string* out, const Variable& v) {
                                         absl::StrAppend(out, v.name());
                                       });
}

size_t GroupBy::getResultWidth() const {
  return getInternallyVisibleVariableColumns().size();
}

vector<size_t> GroupBy::resultSortedOn() const {
  auto varCols = getInternallyVisibleVariableColumns();
  vector<size_t> sortedOn;
  sortedOn.reserve(_groupByVariables.size());
  for (const auto& var : _groupByVariables) {
    sortedOn.push_back(varCols[var]);
  }
  return sortedOn;
}

vector<size_t> GroupBy::computeSortColumns(const QueryExecutionTree* subtree) {
  vector<size_t> cols;
  if (_groupByVariables.empty()) {
    // the entire input is a single group, no sorting needs to be done
    return cols;
  }

  const auto& inVarColMap = subtree->getVariableColumns();

  std::unordered_set<size_t> sortColSet;

  for (const auto& var : _groupByVariables) {
    size_t col = inVarColMap.at(var);
    // avoid sorting by a column twice
    if (sortColSet.find(col) == sortColSet.end()) {
      sortColSet.insert(col);
      cols.push_back(col);
    }
  }
  return cols;
}

VariableToColumnMap GroupBy::computeVariableToColumnMap() const {
  VariableToColumnMap result;
  // The returned columns are all groupByVariables followed by aggregrates.
  size_t colIndex = 0;
  for (const auto& var : _groupByVariables) {
    result[var] = colIndex;
    colIndex++;
  }
  for (const Alias& a : _aliases) {
    result[a._target] = colIndex;
    colIndex++;
  }
  return result;
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
    sparqlExpression::EvaluationContext evaluationContext, size_t blockStart,
    size_t blockEnd, IdTableStatic<OUT_WIDTH>* result, size_t resultRow,
    size_t resultColumn, ResultTable* outTable,
    ResultTable::ResultType* resultType) const {
  evaluationContext._beginIndex = blockStart;
  evaluationContext._endIndex = blockEnd;

  sparqlExpression::ExpressionResult expressionResult =
      aggregate._expression.getPimpl()->evaluate(&evaluationContext);

  auto& resultEntry = result->operator()(resultRow, resultColumn);

  auto visitor = [&]<sparqlExpression::SingleExpressionResult T>(
                     T&& singleResult) mutable {
    constexpr static bool isStrongId = std::is_same_v<T, Id>;
    AD_CHECK(sparqlExpression::isConstantResult<T>);
    if constexpr (isStrongId) {
      resultEntry = singleResult;
      *resultType = qlever::ResultType::KB;
    } else if constexpr (sparqlExpression::isConstantResult<T>) {
      *resultType =
          sparqlExpression::detail::expressionResultTypeToQleverResultType<T>();
      resultEntry = sparqlExpression::detail::constantExpressionResultToId(
          singleResult, *(outTable->_localVocab), false);
    } else {
      // This should never happen since aggregates always return constants.
      AD_FAIL()
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

  sparqlExpression::EvaluationContext evaluationContext(
      *getExecutionContext(), columnMap, inTable->_idTable,
      getExecutionContext()->getAllocator(), *outTable->_localVocab);

  auto processNextBlock = [&](size_t blockStart, size_t blockEnd) {
    result.emplace_back();
    size_t rowIdx = result.size() - 1;
    for (size_t i = 0; i < groupByCols.size(); ++i) {
      result(rowIdx, i) = input(blockStart, groupByCols[i]);
    }
    for (const GroupBy::Aggregate& a : aggregates) {
      processGroup(a, evaluationContext, blockStart, blockEnd, &result, rowIdx,
                   a._outCol, outTable, &outTable->_resultTypes[a._outCol]);
    }
  };

  if (groupByCols.empty()) {
    // The entire input is a single group
    processNextBlock(0, input.size());
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
  auto checkTimeoutAfterNCalls = checkTimeoutAfterNCallsFactory(32000);

  for (size_t pos = 1; pos < input.size(); pos++) {
    checkTimeoutAfterNCalls(currentGroupBlock.size());
    bool rowMatchesCurrentBlock =
        std::all_of(currentGroupBlock.begin(), currentGroupBlock.end(),
                    [&](const auto& columns) {
                      return input(pos, columns.first) == columns.second;
                    });
    if (!rowMatchesCurrentBlock) {
      processNextBlock(blockStart, pos);
      // setup for processing the next block
      blockStart = pos;
      for (auto& columnPair : currentGroupBlock) {
        columnPair.second = input(pos, columnPair.first);
      }
    }
  }
  processNextBlock(blockStart, input.size());
  *dynResult = result.moveToDynamic();
}

void GroupBy::computeResult(ResultTable* result) {
  LOG(DEBUG) << "GroupBy result computation..." << std::endl;

  std::shared_ptr<const ResultTable> subresult = _subtree->getResult();
  LOG(DEBUG) << "GroupBy subresult computation done" << std::endl;

  std::vector<size_t> groupByColumns;

  result->_sortedBy = resultSortedOn();
  result->_idTable.setCols(getResultWidth());

  std::vector<Aggregate> aggregates;
  aggregates.reserve(_aliases.size() + _groupByVariables.size());

  // parse the group by columns
  const auto& subtreeVarCols = _subtree->getVariableColumns();
  for (const auto& var : _groupByVariables) {
    auto it = subtreeVarCols.find(var);
    if (it == subtreeVarCols.end()) {
      LOG(WARN) << "Group by variable " << var.name() << " is not groupable."
                << std::endl;
      AD_THROW(ad_semsearch::Exception::BAD_QUERY,
               "Groupby variable " + var.name() + " is not groupable");
    }

    groupByColumns.push_back(it->second);
  }

  // parse the aggregate aliases
  const auto& varColMap = getInternallyVisibleVariableColumns();
  for (const Alias& alias : _aliases) {
    aggregates.push_back(
        Aggregate{alias._expression, varColMap.at(alias._target)});
  }

  // populate the result type vector
  result->_resultTypes.resize(result->_idTable.cols());

  // The `_groupByVariables` are simply copied, so their result type is
  // also copied. The result type of the other columns is set when the
  // values are computed.
  for (const auto& var : _groupByVariables) {
    result->_resultTypes[varColMap.at(var)] =
        subresult->getResultType(subtreeVarCols.at(var));
  }

  std::vector<size_t> groupByCols;
  groupByCols.reserve(_groupByVariables.size());
  for (const auto& var : _groupByVariables) {
    groupByCols.push_back(subtreeVarCols.at(var));
  }

  std::vector<ResultTable::ResultType> inputResultTypes;
  inputResultTypes.reserve(subresult->_idTable.cols());
  for (size_t i = 0; i < subresult->_idTable.cols(); i++) {
    inputResultTypes.push_back(subresult->getResultType(i));
  }

  int inWidth = subresult->_idTable.cols();
  int outWidth = result->_idTable.cols();

  CALL_FIXED_SIZE_2(inWidth, outWidth, doGroupBy, subresult->_idTable,
                    groupByCols, aggregates, &result->_idTable, subresult.get(),
                    result);
  LOG(DEBUG) << "GroupBy result computation done." << std::endl;
}
