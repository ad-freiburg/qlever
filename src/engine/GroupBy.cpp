// Copyright 2018, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author:
//   2018      Florian Kramer (florian.kramer@mail.uni-freiburg.de)
//   2020-     Johannes Kalmbach (kalmbach@informatik.uni-freiburg.de)

#include "engine/GroupBy.h"

#include "absl/strings/str_join.h"
#include "engine/CallFixedSize.h"
#include "engine/IndexScan.h"
#include "engine/Join.h"
#include "engine/Sort.h"
#include "engine/Values.h"
#include "engine/sparqlExpressions/SparqlExpression.h"
#include "index/Index.h"
#include "index/IndexImpl.h"
#include "parser/Alias.h"
#include "util/Conversions.h"
#include "util/HashSet.h"

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
  if (_groupByVariables.empty()) {
    return 1;
  }
  // Assume that the total number of groups is the input size divided
  // by the minimal multiplicity of one of the grouped variables.
  auto varToMultiplicity = [this](const Variable& var) -> float {
    return _subtree->getMultiplicity(_subtree->getVariableColumn(var));
  };

  // TODO<joka921> Once we can use `std::views` this can be solved
  // more elegantly.
  std::vector<float> multiplicites;
  std::ranges::transform(_groupByVariables, std::back_inserter(multiplicites),
                         varToMultiplicity);
  return _subtree->getSizeEstimate() / std::ranges::min(multiplicites);
}

size_t GroupBy::getCostEstimate() {
  // TODO: add the cost of the actual group by operation to the cost.
  // Currently group by is only added to the optimizer as a terminal operation
  // and its cost should not affect the optimizers results.
  return _subtree->getCostEstimate();
}

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
          singleResult, *(outTable->_localVocab));
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
  IdTableStatic<OUT_WIDTH> result = std::move(*dynResult).toStatic<OUT_WIDTH>();

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
    *dynResult = std::move(result).toDynamic();
    return;
  }

  // This stores the values of the group by numColumns for the current block. A
  // block ends when one of these values changes.
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
  *dynResult = std::move(result).toDynamic();
}

void GroupBy::computeResult(ResultTable* result) {
  LOG(DEBUG) << "GroupBy result computation..." << std::endl;

  if (computeOptimizedGroupByIfPossible(result)) {
    return;
  }

  std::shared_ptr<const ResultTable> subresult = _subtree->getResult();
  LOG(DEBUG) << "GroupBy subresult computation done" << std::endl;

  // Make a copy of the local vocab from the sub-result and then add to it (in
  // case GROUP_CONCAT adds something).
  //
  // NOTE: If we did `result->_localVocab = subresult->_localVocab` here, only a
  // shared pointer would be copied. The the additions made in this operation
  // would also affect the `subresult`, which leads to all kinds of unexpected
  // behavior.
  result->_localVocab =
      std::make_shared<LocalVocab>(subresult->_localVocab->clone());

  std::vector<size_t> groupByColumns;

  result->_sortedBy = resultSortedOn();
  result->_idTable.setNumColumns(getResultWidth());

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
  result->_resultTypes.resize(result->_idTable.numColumns());

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
  inputResultTypes.reserve(subresult->_idTable.numColumns());
  for (size_t i = 0; i < subresult->_idTable.numColumns(); i++) {
    inputResultTypes.push_back(subresult->getResultType(i));
  }

  int inWidth = subresult->_idTable.numColumns();
  int outWidth = result->_idTable.numColumns();

  CALL_FIXED_SIZE((std::array{inWidth, outWidth}), &GroupBy::doGroupBy, this,
                  subresult->_idTable, groupByCols, aggregates,
                  &result->_idTable, subresult.get(), result);

  LOG(DEBUG) << "GroupBy result computation done." << std::endl;
}

// _____________________________________________________________________________
bool GroupBy::computeGroupByForSingleIndexScan(ResultTable* result) {
  // The child must be an `IndexScan` for this optimization.
  auto* indexScan =
      dynamic_cast<const IndexScan*>(_subtree->getRootOperation().get());

  if (!indexScan) {
    return false;
  }

  if (indexScan->getResultWidth() <= 1 || !_groupByVariables.empty()) {
    return false;
  }

  // Alias must be a single count of a variable
  auto varAndDistinctness = getVariableForCountOfSingleAlias();
  if (!varAndDistinctness.has_value()) {
    return false;
  }

  // Distinct counts are only supported for triples with three variables.
  bool countIsDistinct = varAndDistinctness.value().isDistinct_;
  if (countIsDistinct && indexScan->getResultWidth() != 3) {
    return false;
  }

  result->_idTable.setNumColumns(1);
  // TODO<joka921> The `resultTypes` are not really in use, but if they are
  // not present, segfaults might occur in upstream `Operation`s.
  result->_resultTypes.push_back(qlever::ResultType::VERBATIM);
  result->_idTable.emplace_back();
  // For `IndexScan`s with at least two variables the size estimates are
  // exact as they are read directly from the index metadata.
  if (indexScan->getResultWidth() == 3) {
    if (countIsDistinct) {
      const auto& var = varAndDistinctness.value().variable_;
      auto permutation =
          getPermutationForThreeVariableTriple(*_subtree, var, var);
      AD_CHECK(permutation.has_value());
      result->_idTable(0, 0) = Id::makeFromInt(
          getIndex().getImpl().numDistinctCol0(permutation.value()).normal_);
    } else {
      result->_idTable(0, 0) = Id::makeFromInt(getIndex().numTriples().normal_);
    }
  } else {
    // TODO<joka921> The two variables IndexScans should also account for the
    // additionally added triples.
    result->_idTable(0, 0) = Id::makeFromInt(indexScan->getSizeEstimate());
  }
  return true;
}

// _____________________________________________________________________________
bool GroupBy::computeGroupByForFullIndexScan(ResultTable* result) {
  if (_groupByVariables.size() != 1) {
    return false;
  }
  const auto& groupByVariable = _groupByVariables.at(0);

  // The child must be an `IndexScan` with three variables that contains
  // the grouped variable.
  auto permutation = getPermutationForThreeVariableTriple(
      *_subtree, groupByVariable, groupByVariable);

  if (!permutation.has_value()) {
    return false;
  }

  // Check that all the aliases are non-distinct counts. We currently support
  // only one or no such count. Redundant additional counts will lead to an
  // exception (it is easy to reformulate the query to trigger this
  // optimization).
  size_t numCounts = 0;
  for (size_t i = 0; i < _aliases.size(); ++i) {
    const auto& alias = _aliases[i];
    if (auto count = alias._expression.getVariableForCount()) {
      if (count.value().isDistinct_) {
        return false;
      }
      numCounts++;
    } else {
      return false;
    }
  }

  if (numCounts > 1) {
    throw std::runtime_error{
        "This query contains two or more COUNT expressions in the same GROUP "
        "BY that would lead to identical values. This redundancy is currently "
        "not supported."};
  }

  // Prepare the `result`
  size_t numCols = numCounts + 1;
  result->_idTable.setNumColumns(numCols);
  for (size_t i = 0; i < numCols; ++i) {
    result->_resultTypes.push_back(qlever::ResultType::VERBATIM);
  }
  _subtree->getRootOperation()->updateRuntimeInformationWhenOptimizedOut({});

  // A nested lambda that computes the actual result. The outer lambda is
  // templated on the number of columns (1 or 2) and will be passed to
  // `callFixedSize`.
  DISABLE_WARNINGS_CLANG_13
  auto doComputationForNumberOfColumns = [&]<int NUM_COLS>(
                                             IdTable* idTable) mutable {
    ENABLE_WARNINGS_CLANG_13
    // TODO<joka921> The `resultTypes` are not really in use, but if they
    // are not present, segfaults might occur in upstream `Operation`s.
    auto ignoredRanges =
        getIndex().getImpl().getIgnoredIdRanges(permutation.value()).first;
    // The permutations in the `Index` class have different types, so we
    // also have to write a generic lambda here that will be passed to
    // `IndexImpl::applyToPermutation`.
    auto applyForPermutation = [&](const auto& permutation) mutable {
      IdTableStatic<NUM_COLS> table = std::move(*idTable).toStatic<NUM_COLS>();
      const auto& metaData = permutation._meta.data();
      // TODO<joka921> the reserve is too large because of the ignored
      // triples. We would need to incorporate the information how many
      // added "relations" are in each permutation during index building.
      table.reserve(metaData.size());
      for (auto it = metaData.ordered_begin(); it != metaData.ordered_end();
           ++it) {
        Id id = decltype(metaData.ordered_begin())::getIdFromElement(*it);

        // Check whether this is an `@en@...` predicate in a `Pxx`
        // permutation, a literal in a `Sxx` permutation or some other
        // entity that was added only for internal reasons.
        if (std::ranges::any_of(ignoredRanges, [&id](const auto& pair) {
              return id >= pair.first && id < pair.second;
            })) {
          continue;
        }
        Id count = Id::makeFromInt(
            decltype(metaData.ordered_begin())::getNumRowsFromElement(*it));
        // TODO<joka921> The count is actually not accurate at least for the
        // `Sxx` and `Oxx` permutations because it contains the triples with
        // predicate
        // `@en@rdfs:label` etc. The probably easiest way to fix this is to
        // exclude these triples from those permutations (they are only
        // relevant for queries with a fixed subject), but then we would
        // need to make sure, that we don't accidentally break the language
        // filters for queries like
        // `<fixedSubject> @en@rdfs:label ?labels`, for which the best
        // query plan potentially goes through the `SPO` relation.
        // Alternatively we would have to write an additional number
        // `numNonAddedTriples` to the `IndexMetaData` which would further
        // increase their size.
        // TODO<joka921> Discuss this with Hannah.
        table.emplace_back();
        table(table.size() - 1, 0) = id;
        if (numCounts == 1) {
          table(table.size() - 1, 1) = count;
        }
      }
      *idTable = std::move(table).toDynamic();
    };

    getExecutionContext()->getIndex().getPimpl().applyToPermutation(
        permutation.value(), applyForPermutation);
  };
  ad_utility::callFixedSize(numCols, doComputationForNumberOfColumns,
                            &result->_idTable);

  // TODO<joka921> This optimization should probably also apply if
  // the query is `SELECT DISTINCT ?s WHERE {?s ?p ?o} ` without a
  // GROUP BY, but that needs to be implemented in the `DISTINCT` operation.
  return true;
}

// _____________________________________________________________________________
std::optional<Index::Permutation> GroupBy::getPermutationForThreeVariableTriple(
    const QueryExecutionTree& tree, const Variable& variableByWhichToSort,
    const Variable& variableThatMustBeContained) {
  auto indexScan =
      dynamic_cast<const IndexScan*>(tree.getRootOperation().get());

  if (!indexScan || indexScan->getResultWidth() != 3) {
    return std::nullopt;
  }
  {
    auto v = variableThatMustBeContained;
    if (v != indexScan->getSubject() && v.name() != indexScan->getPredicate() &&
        v != indexScan->getObject()) {
      return std::nullopt;
    }
  }

  if (variableByWhichToSort == indexScan->getSubject()) {
    return Index::Permutation::SPO;
  } else if (variableByWhichToSort.name() == indexScan->getPredicate()) {
    return Index::Permutation::POS;
  } else if (variableByWhichToSort == indexScan->getObject()) {
    return Index::Permutation::OSP;
  } else {
    return std::nullopt;
  }
};

// _____________________________________________________________________________
std::optional<GroupBy::OptimizedGroupByData> GroupBy::checkIfJoinWithFullScan(
    const Join* join) {
  if (_groupByVariables.size() != 1) {
    return std::nullopt;
  }
  const Variable& groupByVariable = _groupByVariables.front();

  auto countedVariable = getVariableForNonDistinctCountOfSingleAlias();
  if (!countedVariable.has_value()) {
    return std::nullopt;
  }

  // Determine if any of the two children of the join operation is a
  // triple with three variables that fulfills the condition.
  auto* child1 = static_cast<const Operation*>(join)->getChildren().at(0);
  auto* child2 = static_cast<const Operation*>(join)->getChildren().at(1);

  // TODO<joka921, C++23> Use `optional::or_else`
  auto permutation = getPermutationForThreeVariableTriple(
      *child1, groupByVariable, countedVariable.value());
  if (!permutation.has_value()) {
    std::swap(child1, child2);
    permutation = getPermutationForThreeVariableTriple(*child1, groupByVariable,
                                                       countedVariable.value());
  }
  if (!permutation.has_value()) {
    return std::nullopt;
  }

  // TODO<joka921> This  is rather implicit. We should have a (soft) check,
  // that the join column is correct, and a HARD check, that the result is
  // sorted.
  // This check fails if we ever decide to not eagerly sort the children of
  // a JOIN. We can detect this case and change something here then.
  if (child2->getPrimarySortKeyVariable() != groupByVariable) {
    return std::nullopt;
  }
  auto columnIndex = child2->getVariableColumn(groupByVariable);

  return OptimizedGroupByData{*child1, *child2, permutation.value(),
                              columnIndex};
}

// _____________________________________________________________________________
bool GroupBy::computeGroupByForJoinWithFullScan(ResultTable* result) {
  auto join = dynamic_cast<Join*>(_subtree->getRootOperation().get());
  if (!join) {
    return false;
  }

  auto optimizedAggregateData = checkIfJoinWithFullScan(join);
  if (!optimizedAggregateData.has_value()) {
    return false;
  }
  const auto& [threeVarSubtree, subtree, permutation, columnIndex] =
      optimizedAggregateData.value();

  auto subresult = subtree.getResult();
  threeVarSubtree.getRootOperation()->updateRuntimeInformationWhenOptimizedOut(
      {});

  join->updateRuntimeInformationWhenOptimizedOut(
      {subtree.getRootOperation()->getRuntimeInfo(),
       threeVarSubtree.getRootOperation()->getRuntimeInfo()});
  result->_idTable.setNumColumns(2);
  // TODO<joka921> The `resultTypes` are not really in use, but if they are
  // not present, segfaults might occur in upstream `Operation`s.
  result->_resultTypes.push_back(qlever::ResultType::KB);
  result->_resultTypes.push_back(qlever::ResultType::VERBATIM);
  if (subresult->_idTable.size() == 0) {
    return true;
  }

  auto idTable = std::move(result->_idTable).toStatic<2>();
  const auto& index = getExecutionContext()->getIndex();

  // TODO<joka921, C++23> Simplify the following pattern by using
  // `std::views::chunk_by` and implement a lazy version of this view for input
  // iterators.

  // Take care of duplicate values in the input.
  Id currentId = subresult->_idTable(0, columnIndex);
  size_t currentCount = 0;
  size_t currentCardinality = index.getCardinality(currentId, permutation);

  auto pushRow = [&]() {
    // If the count is 0 this means that the element with the `currentId`
    // doesn't exist in the knowledge graph. Thus, the join with a three
    // variable triple would have filtered it out and we don't include it in the
    // final result.
    if (currentCount > 0) {
      // TODO<C++20, as soon as Clang supports it>: use `emplace_back(id1, id2)`
      // (requires parenthesized initialization of aggregates.
      idTable.push_back({currentId, Id::makeFromInt(currentCount)});
    }
  };
  for (size_t i = 0; i < subresult->_idTable.size(); ++i) {
    auto id = subresult->_idTable(i, columnIndex);
    if (id != currentId) {
      pushRow();
      currentId = id;
      currentCount = 0;
      // TODO<joka921> This is also not quite correct, we want the cardinality
      // without the internally added triples, but that is not easy to
      // retrieve right now.
      currentCardinality = index.getCardinality(id, permutation);
    }
    currentCount += currentCardinality;
  }
  pushRow();
  result->_idTable = std::move(idTable).toDynamic();
  return true;
}

// _____________________________________________________________________________
bool GroupBy::computeOptimizedGroupByIfPossible(ResultTable* result) {
  if (computeGroupByForSingleIndexScan(result)) {
    return true;
  } else if (computeGroupByForFullIndexScan(result)) {
    return true;
  } else {
    return computeGroupByForJoinWithFullScan(result);
  }
}

// _____________________________________________________________________________
std::optional<Variable> GroupBy::getVariableForNonDistinctCountOfSingleAlias()
    const {
  auto varAndDistinctness = getVariableForCountOfSingleAlias();
  if (!varAndDistinctness.has_value() ||
      varAndDistinctness.value().isDistinct_) {
    return std::nullopt;
  }
  return std::move(varAndDistinctness.value().variable_);
}

// _____________________________________________________________________________
std::optional<sparqlExpression::SparqlExpressionPimpl::VariableAndDistinctness>
GroupBy::getVariableForCountOfSingleAlias() const {
  return _aliases.size() == 1
             ? _aliases.front()._expression.getVariableForCount()
             : std::nullopt;
}
