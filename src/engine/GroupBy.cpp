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
#include "engine/sparqlExpressions/AggregateExpression.h"
#include "engine/sparqlExpressions/GroupConcatExpression.h"
#include "engine/sparqlExpressions/LiteralExpression.h"
#include "engine/sparqlExpressions/SparqlExpression.h"
#include "engine/sparqlExpressions/SparqlExpressionGenerators.h"
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
  // Sort the groupByVariables to ensure that the cache key is order
  // invariant.
  // Note: It is tempting to do the same also for the aliases, but that would
  // break the case when an alias reuses a variable that was bound by a previous
  // alias.
  std::ranges::sort(_groupByVariables, std::less<>{}, &Variable::name);

  auto sortColumns = computeSortColumns(subtree.get());
  _subtree =
      QueryExecutionTree::createSortedTree(std::move(subtree), sortColumns);
}

string GroupBy::getCacheKeyImpl() const {
  const auto& varMap = getInternallyVisibleVariableColumns();
  auto varMapInput = _subtree->getVariableColumns();

  // We also have to encode the variables to which alias results are stored in
  // the cache key of the expressions in case they reuse a variable from the
  // previous result.
  auto numColumnsInput = _subtree->getResultWidth();
  for (const auto& [var, column] : varMap) {
    if (!varMapInput.contains(var)) {
      // It is important that the cache keys for the variables from the aliases
      // do not collide with the query body, and that they are consistent. The
      // constant `1000` has no deeper meaning but makes debugging easier.
      varMapInput[var].columnIndex_ =
          column.columnIndex_ + 1000 + numColumnsInput;
    }
  }

  std::ostringstream os;
  os << "GROUP_BY ";
  for (const auto& var : _groupByVariables) {
    os << varMap.at(var).columnIndex_ << ", ";
  }
  for (const auto& alias : _aliases) {
    os << alias._expression.getCacheKey(varMapInput) << " AS "
       << varMap.at(alias._target).columnIndex_;
  }
  os << std::endl;
  os << _subtree->getCacheKey();
  return std::move(os).str();
}

string GroupBy::getDescriptor() const {
  // TODO<C++23>:: Use std::views::join_with.
  return "GroupBy on " +
         absl::StrJoin(_groupByVariables, " ", &Variable::AbslFormatter);
}

size_t GroupBy::getResultWidth() const {
  return getInternallyVisibleVariableColumns().size();
}

vector<ColumnIndex> GroupBy::resultSortedOn() const {
  auto varCols = getInternallyVisibleVariableColumns();
  vector<ColumnIndex> sortedOn;
  sortedOn.reserve(_groupByVariables.size());
  for (const auto& var : _groupByVariables) {
    sortedOn.push_back(varCols[var].columnIndex_);
  }
  return sortedOn;
}

vector<ColumnIndex> GroupBy::computeSortColumns(
    const QueryExecutionTree* subtree) {
  vector<ColumnIndex> cols;
  if (_groupByVariables.empty()) {
    // the entire input is a single group, no sorting needs to be done
    return cols;
  }

  const auto& inVarColMap = subtree->getVariableColumns();

  std::unordered_set<ColumnIndex> sortColSet;

  for (const auto& var : _groupByVariables) {
    ColumnIndex col = inVarColMap.at(var).columnIndex_;
    // avoid sorting by a column twice
    if (sortColSet.find(col) == sortColSet.end()) {
      sortColSet.insert(col);
      cols.push_back(col);
    }
  }
  return cols;
}

// ____________________________________________________________________
VariableToColumnMap GroupBy::computeVariableToColumnMap() const {
  VariableToColumnMap result;
  // The returned columns are all groupByVariables followed by aggregrates.
  const auto& subtreeVars = _subtree->getVariableColumns();
  size_t colIndex = 0;
  for (const auto& var : _groupByVariables) {
    result[var] = ColumnIndexAndTypeInfo{
        colIndex, subtreeVars.at(var).mightContainUndef_};
    colIndex++;
  }
  for (const Alias& a : _aliases) {
    // TODO<joka921> This currently pessimistically assumes that all (aggregate)
    // expressions can produce undefined values. This might impact the
    // performance when the result of this GROUP BY is joined on one or more of
    // the aggregating columns. Implement an interface in the expressions that
    // allows to check, whether an expression can never produce an undefined
    // value.
    result[a._target] = makePossiblyUndefinedColumn(colIndex);
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

uint64_t GroupBy::getSizeEstimateBeforeLimit() {
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
  float minMultiplicity = std::ranges::min(
      _groupByVariables | std::views::transform(varToMultiplicity));
  return _subtree->getSizeEstimate() / minMultiplicity;
}

size_t GroupBy::getCostEstimate() {
  // TODO: add the cost of the actual group by operation to the cost.
  // Currently group by is only added to the optimizer as a terminal operation
  // and its cost should not affect the optimizers results.
  return _subtree->getCostEstimate();
}

template <size_t OUT_WIDTH>
void GroupBy::processGroup(
    const GroupBy::Aggregate& aggregate,
    sparqlExpression::EvaluationContext& evaluationContext, size_t blockStart,
    size_t blockEnd, IdTableStatic<OUT_WIDTH>* result, size_t resultRow,
    size_t resultColumn, LocalVocab* localVocab) const {
  evaluationContext._beginIndex = blockStart;
  evaluationContext._endIndex = blockEnd;

  sparqlExpression::ExpressionResult expressionResult =
      aggregate._expression.getPimpl()->evaluate(&evaluationContext);

  auto& resultEntry = result->operator()(resultRow, resultColumn);

  // Copy the result to the evaluation context in case one of the following
  // aliases has to reuse it.
  evaluationContext._previousResultsFromSameGroup.at(resultColumn) =
      sparqlExpression::copyExpressionResult(expressionResult);

  auto visitor = [&]<sparqlExpression::SingleExpressionResult T>(
                     T&& singleResult) mutable {
    constexpr static bool isStrongId = std::is_same_v<T, Id>;
    AD_CONTRACT_CHECK(sparqlExpression::isConstantResult<T>);
    if constexpr (isStrongId) {
      resultEntry = singleResult;
    } else if constexpr (sparqlExpression::isConstantResult<T>) {
      resultEntry = sparqlExpression::detail::constantExpressionResultToId(
          AD_FWD(singleResult), *localVocab);
    } else {
      // This should never happen since aggregates always return constants.
      AD_FAIL();
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

template <size_t IN_WIDTH, size_t OUT_WIDTH>
void GroupBy::doGroupBy(const IdTable& dynInput,
                        const vector<size_t>& groupByCols,
                        const vector<GroupBy::Aggregate>& aggregates,
                        IdTable* dynResult, const IdTable* inTable,
                        LocalVocab* outLocalVocab) const {
  LOG(DEBUG) << "Group by input size " << dynInput.size() << std::endl;
  if (dynInput.empty()) {
    return;
  }
  const IdTableView<IN_WIDTH> input = dynInput.asStaticView<IN_WIDTH>();
  IdTableStatic<OUT_WIDTH> result = std::move(*dynResult).toStatic<OUT_WIDTH>();

  sparqlExpression::EvaluationContext evaluationContext(
      *getExecutionContext(), _subtree->getVariableColumns(), *inTable,
      getExecutionContext()->getAllocator(), *outLocalVocab);

  // In a GROUP BY evaluation, the expressions need to know which variables are
  // grouped, and to which columns the results of the aliases are written. The
  // latter information is needed if the expression of an alias reuses the
  // result variable from a previous alias as an input.
  evaluationContext._groupedVariables = ad_utility::HashSet<Variable>{
      _groupByVariables.begin(), _groupByVariables.end()};
  evaluationContext._variableToColumnMapPreviousResults =
      getInternallyVisibleVariableColumns();
  evaluationContext._previousResultsFromSameGroup.resize(getResultWidth());

  // Let the evaluation know that we are part of a GROUP BY.
  evaluationContext._isPartOfGroupBy = true;

  auto processNextBlock = [&](size_t blockStart, size_t blockEnd) {
    result.emplace_back();
    size_t rowIdx = result.size() - 1;
    for (size_t i = 0; i < groupByCols.size(); ++i) {
      result(rowIdx, i) = input(blockStart, groupByCols[i]);
    }
    for (const GroupBy::Aggregate& a : aggregates) {
      processGroup<OUT_WIDTH>(a, evaluationContext, blockStart, blockEnd,
                              &result, rowIdx, a._outCol, outLocalVocab);
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

  for (size_t pos = 1; pos < input.size(); pos++) {
    checkCancellation();
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

ResultTable GroupBy::computeResult() {
  LOG(DEBUG) << "GroupBy result computation..." << std::endl;

  IdTable idTable{getExecutionContext()->getAllocator()};

  if (computeOptimizedGroupByIfPossible(&idTable)) {
    // Note: The optimized group bys currently all include index scans and thus
    // can never produce local vocab entries. If this should ever change, then
    // we also have to take care of the local vocab here.
    return {std::move(idTable), resultSortedOn(), LocalVocab{}};
  }

  std::vector<Aggregate> aggregates;
  aggregates.reserve(_aliases.size() + _groupByVariables.size());

  // parse the aggregate aliases
  const auto& varColMap = getInternallyVisibleVariableColumns();
  for (const Alias& alias : _aliases) {
    aggregates.emplace_back(alias._expression,
                            varColMap.at(alias._target).columnIndex_);
  }

  // Check if optimization for explicitly sorted child can be applied
  auto hashMapOptimizationParams =
      checkIfHashMapOptimizationPossible(aggregates);

  std::shared_ptr<const ResultTable> subresult;
  if (hashMapOptimizationParams.has_value()) {
    const auto* child = _subtree->getRootOperation()->getChildren().at(0);
    // Skip sorting
    subresult = child->getResult();
    // Update runtime information
    auto runTimeInfoChildren =
        child->getRootOperation()->getRuntimeInfoPointer();
    _subtree->getRootOperation()->updateRuntimeInformationWhenOptimizedOut(
        {runTimeInfoChildren}, RuntimeInformation::Status::optimizedOut);
  } else {
    subresult = _subtree->getResult();
  }

  LOG(DEBUG) << "GroupBy subresult computation done" << std::endl;

  // Make a deep copy of the local vocab from `subresult` and then add to it (in
  // case GROUP_CONCAT adds a new word or words).
  //
  // TODO: In most GROUP BY operations, nothing is added to the local
  // vocabulary, so it would be more efficient to first share the pointer here
  // (like with `shareLocalVocabFrom`) and only copy it when a new word is about
  // to be added. Same for BIND.

  auto localVocab = subresult->getCopyOfLocalVocab();

  std::vector<size_t> groupByColumns;

  idTable.setNumColumns(getResultWidth());

  // parse the group by columns
  const auto& subtreeVarCols = _subtree->getVariableColumns();
  for (const auto& var : _groupByVariables) {
    auto it = subtreeVarCols.find(var);
    if (it == subtreeVarCols.end()) {
      AD_THROW("Groupby variable " + var.name() + " is not groupable");
    }

    groupByColumns.push_back(it->second.columnIndex_);
  }

  std::vector<size_t> groupByCols;
  groupByCols.reserve(_groupByVariables.size());
  for (const auto& var : _groupByVariables) {
    groupByCols.push_back(subtreeVarCols.at(var).columnIndex_);
  }

  size_t inWidth = subresult->idTable().numColumns();
  size_t outWidth = idTable.numColumns();

  if (hashMapOptimizationParams.has_value()) {
    computeGroupByForHashMapOptimization(
        &idTable, hashMapOptimizationParams->aggregateAliases_,
        subresult->idTable(), hashMapOptimizationParams->subtreeColumnIndex_,
        &localVocab);

    return {std::move(idTable), resultSortedOn(), std::move(localVocab)};
  }

  CALL_FIXED_SIZE((std::array{inWidth, outWidth}), &GroupBy::doGroupBy, this,
                  subresult->idTable(), groupByCols, aggregates, &idTable,
                  &(subresult->idTable()), &localVocab);

  LOG(DEBUG) << "GroupBy result computation done." << std::endl;
  return {std::move(idTable), resultSortedOn(), std::move(localVocab)};
}

// _____________________________________________________________________________
bool GroupBy::computeGroupByForSingleIndexScan(IdTable* result) {
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

  auto& table = *result;
  table.setNumColumns(1);
  table.emplace_back();
  // For `IndexScan`s with at least two variables the size estimates are
  // exact as they are read directly from the index metadata.
  if (indexScan->getResultWidth() == 3) {
    if (countIsDistinct) {
      const auto& var = varAndDistinctness.value().variable_;
      auto permutation =
          getPermutationForThreeVariableTriple(*_subtree, var, var);
      AD_CONTRACT_CHECK(permutation.has_value());
      table(0, 0) = Id::makeFromInt(
          getIndex().getImpl().numDistinctCol0(permutation.value()).normal_);
    } else {
      table(0, 0) = Id::makeFromInt(getIndex().numTriples().normal_);
    }
  } else {
    // TODO<joka921> The two variables IndexScans should also account for the
    // additionally added triples.
    table(0, 0) = Id::makeFromInt(indexScan->getExactSize());
  }
  return true;
}

// _____________________________________________________________________________
bool GroupBy::computeGroupByForFullIndexScan(IdTable* result) {
  if (_groupByVariables.size() != 1) {
    return false;
  }
  const auto& groupByVariable = _groupByVariables.at(0);

  // The child must be an `IndexScan` with three variables that contains
  // the grouped variable.
  auto permutationEnum = getPermutationForThreeVariableTriple(
      *_subtree, groupByVariable, groupByVariable);

  if (!permutationEnum.has_value()) {
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
  result->setNumColumns(numCols);
  _subtree->getRootOperation()->updateRuntimeInformationWhenOptimizedOut({});

  // A nested lambda that computes the actual result. The outer lambda is
  // templated on the number of columns (1 or 2) and will be passed to
  // `callFixedSize`.
  auto doComputationForNumberOfColumns = [&]<int NUM_COLS>(
                                             IdTable* idTable) mutable {
    auto ignoredRanges =
        getIndex().getImpl().getIgnoredIdRanges(permutationEnum.value()).first;
    const auto& permutation =
        getExecutionContext()->getIndex().getPimpl().getPermutation(
            permutationEnum.value());
    IdTableStatic<NUM_COLS> table = std::move(*idTable).toStatic<NUM_COLS>();
    const auto& metaData = permutation.meta_.data();
    // TODO<joka921> the reserve is too large because of the ignored
    // triples. We would need to incorporate the information how many
    // added "relations" are in each permutationEnum during index building.
    table.reserve(metaData.size());
    for (auto it = metaData.ordered_begin(); it != metaData.ordered_end();
         ++it) {
      Id id = decltype(metaData.ordered_begin())::getIdFromElement(*it);

      // Check whether this is an `@en@...` predicate in a `Pxx`
      // permutationEnum, a literal in a `Sxx` permutationEnum or some other
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
  ad_utility::callFixedSize(numCols, doComputationForNumberOfColumns, result);

  // TODO<joka921> This optimization should probably also apply if
  // the query is `SELECT DISTINCT ?s WHERE {?s ?p ?o} ` without a
  // GROUP BY, but that needs to be implemented in the `DISTINCT` operation.
  return true;
}

// _____________________________________________________________________________
std::optional<Permutation::Enum> GroupBy::getPermutationForThreeVariableTriple(
    const QueryExecutionTree& tree, const Variable& variableByWhichToSort,
    const Variable& variableThatMustBeContained) {
  auto indexScan =
      dynamic_cast<const IndexScan*>(tree.getRootOperation().get());

  if (!indexScan || indexScan->getResultWidth() != 3) {
    return std::nullopt;
  }
  {
    auto v = variableThatMustBeContained;
    if (v != indexScan->getSubject() && v != indexScan->getPredicate() &&
        v != indexScan->getObject()) {
      return std::nullopt;
    }
  }

  if (variableByWhichToSort == indexScan->getSubject()) {
    return Permutation::SPO;
  } else if (variableByWhichToSort == indexScan->getPredicate()) {
    return Permutation::POS;
  } else if (variableByWhichToSort == indexScan->getObject()) {
    return Permutation::OSP;
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
bool GroupBy::computeGroupByForJoinWithFullScan(IdTable* result) {
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
      {subtree.getRootOperation()->getRuntimeInfoPointer(),
       threeVarSubtree.getRootOperation()->getRuntimeInfoPointer()});
  result->setNumColumns(2);
  if (subresult->idTable().size() == 0) {
    return true;
  }

  auto idTable = std::move(*result).toStatic<2>();
  const auto& index = getExecutionContext()->getIndex();

  // TODO<joka921, C++23> Simplify the following pattern by using
  // `std::views::chunk_by` and implement a lazy version of this view for input
  // iterators.

  // Take care of duplicate values in the input.
  Id currentId = subresult->idTable()(0, columnIndex);
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
  for (size_t i = 0; i < subresult->idTable().size(); ++i) {
    auto id = subresult->idTable()(i, columnIndex);
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
  *result = std::move(idTable).toDynamic();
  return true;
}

// _____________________________________________________________________________
bool GroupBy::computeOptimizedGroupByIfPossible(IdTable* result) {
  if (computeGroupByForSingleIndexScan(result)) {
    return true;
  } else if (computeGroupByForFullIndexScan(result)) {
    return true;
  } else {
    return computeGroupByForJoinWithFullScan(result);
  }
}

// _____________________________________________________________________________
std::optional<GroupBy::HashMapOptimizationData>
GroupBy::checkIfHashMapOptimizationPossible(std::vector<Aggregate>& aliases) {
  if (!RuntimeParameters().get<"use-group-by-hash-map-optimization">()) {
    return std::nullopt;
  }

  auto* sort = dynamic_cast<const Sort*>(_subtree->getRootOperation().get());
  if (!sort) {
    return std::nullopt;
  }

  // Only allow one group by variable
  if (_groupByVariables.size() != 1) {
    return std::nullopt;
  }

  // Get pointers to all aggregate expressions and their parents
  size_t numAggregates = 0;
  std::vector<HashMapAliasInformation> aliasesWithAggregateInfo;
  for (auto& alias : aliases) {
    auto expr = alias._expression.getPimpl();

    // Find all aggregates in the expression of the current alias.
    auto foundAggregates = findAggregates(expr);

    // TODO<kcaliban> Remove as soon as all aggregates are supported
    if (!foundAggregates.has_value()) return std::nullopt;

    for (auto& aggregate : foundAggregates.value()) {
      aggregate.aggregateDataIndex_ = numAggregates++;
    }

    aliasesWithAggregateInfo.emplace_back(alias._expression, alias._outCol,
                                          foundAggregates.value());
  }

  const Variable& groupByVariable = _groupByVariables.front();
  auto child = _subtree->getRootOperation()->getChildren().at(0);
  auto columnIndex = child->getVariableColumn(groupByVariable);

  return HashMapOptimizationData{columnIndex, aliasesWithAggregateInfo};
}

// _____________________________________________________________________________
std::variant<std::vector<GroupBy::ParentAndChildIndex>,
             GroupBy::OccurenceAsRoot>
GroupBy::findGroupedVariable(sparqlExpression::SparqlExpression* expr) {
  AD_CONTRACT_CHECK(_groupByVariables.size() == 1);
  std::variant<std::vector<ParentAndChildIndex>, OccurenceAsRoot> substitutions;
  findGroupedVariableImpl(expr, std::nullopt, substitutions);
  return substitutions;
}

// _____________________________________________________________________________
void GroupBy::findGroupedVariableImpl(
    sparqlExpression::SparqlExpression* expr,
    std::optional<ParentAndChildIndex> parentAndChildIndex,
    std::variant<std::vector<ParentAndChildIndex>, OccurenceAsRoot>&
        substitutions) {
  if (auto value = hasType<sparqlExpression::VariableExpression>(expr)) {
    auto variable = value.value()->value();
    for (const auto& groupedVariable : _groupByVariables) {
      if (variable != groupedVariable) continue;

      if (parentAndChildIndex.has_value()) {
        auto vector =
            std::get_if<std::vector<ParentAndChildIndex>>(&substitutions);
        AD_CONTRACT_CHECK(vector != nullptr);
        vector->emplace_back(parentAndChildIndex.value());
      } else {
        substitutions = OccurenceAsRoot{};
        return;
      }
    }
  }

  auto children = expr->children();

  // TODO<C++23> use views::enumerate
  size_t childIndex = 0;
  for (const auto& child : children) {
    ParentAndChildIndex parentAndChildIndexForChild{expr, childIndex++};
    findGroupedVariableImpl(child.get(), parentAndChildIndexForChild,
                            substitutions);
  }
}

// _____________________________________________________________________________
std::optional<std::vector<GroupBy::HashMapAggregateInformation>>
GroupBy::findAggregates(sparqlExpression::SparqlExpression* expr) {
  std::vector<HashMapAggregateInformation> result;
  if (!findAggregatesImpl(expr, std::nullopt, result))
    return std::nullopt;
  else
    return result;
}

// _____________________________________________________________________________
template <class T>
std::optional<T*> GroupBy::hasType(sparqlExpression::SparqlExpression* expr) {
  auto value = dynamic_cast<T*>(expr);
  if (value == nullptr)
    return std::nullopt;
  else
    return value;
}

// _____________________________________________________________________________
template <typename... Exprs>
bool GroupBy::hasAnyType(const auto& expr) {
  return (... || hasType<Exprs>(expr));
}

// _____________________________________________________________________________
std::optional<GroupBy::HashMapAggregateType> GroupBy::isSupportedAggregate(
    sparqlExpression::SparqlExpression* expr) {
  using namespace sparqlExpression;

  // `expr` is not a distinct aggregate
  if (expr->isDistinct()) return std::nullopt;

  // `expr` is not a nested aggregated
  if (expr->children().front()->containsAggregate()) return std::nullopt;

  if (hasType<AvgExpression>(expr)) return HashMapAggregateType::AVG;
  if (hasType<CountExpression>(expr)) return HashMapAggregateType::COUNT;

  // `expr` is an unsupported aggregate
  return std::nullopt;
}

// _____________________________________________________________________________
bool GroupBy::findAggregatesImpl(
    sparqlExpression::SparqlExpression* expr,
    std::optional<ParentAndChildIndex> parentAndChildIndex,
    std::vector<GroupBy::HashMapAggregateInformation>& info) {
  if (expr->isAggregate()) {
    if (auto aggregateType = isSupportedAggregate(expr)) {
      info.emplace_back(expr, 0, aggregateType.value(), parentAndChildIndex);
      return true;
    } else {
      return false;
    }
  }

  auto children = expr->children();

  bool childrenContainOnlySupportedAggregates = true;
  // TODO<C++23> use views::enumerate
  size_t childIndex = 0;
  for (const auto& child : children) {
    ParentAndChildIndex parentAndChildIndexForChild{expr, childIndex++};
    childrenContainOnlySupportedAggregates =
        childrenContainOnlySupportedAggregates &&
        findAggregatesImpl(child.get(), parentAndChildIndexForChild, info);
  }

  return childrenContainOnlySupportedAggregates;
}

// _____________________________________________________________________________
void GroupBy::extractValues(
    sparqlExpression::ExpressionResult&& expressionResult,
    sparqlExpression::EvaluationContext& evaluationContext,
    IdTable* resultTable, LocalVocab* localVocab, size_t outCol) {
  auto visitor = [&evaluationContext, &resultTable, &localVocab,
                  &outCol]<sparqlExpression::SingleExpressionResult T>(
                     T&& singleResult) mutable {
    auto generator = sparqlExpression::detail::makeGenerator(
        std::forward<T>(singleResult),
        evaluationContext._endIndex - evaluationContext._beginIndex,
        &evaluationContext);

    auto targetIterator =
        resultTable->getColumn(outCol).begin() + evaluationContext._beginIndex;
    for (sparqlExpression::IdOrString val : generator) {
      *targetIterator = sparqlExpression::detail::constantExpressionResultToId(
          std::move(val), *localVocab);
      ++targetIterator;
    }
  };

  std::visit(visitor, std::move(expressionResult));
}

// _____________________________________________________________________________
sparqlExpression::VectorWithMemoryLimit<ValueId>
GroupBy::getHashMapAggregationResults(
    IdTable* resultTable, const HashMapAggregationData& aggregationData,
    size_t dataIndex, size_t beginIndex, size_t endIndex) {
  sparqlExpression::VectorWithMemoryLimit<ValueId> aggregateResults(
      getExecutionContext()->getAllocator());
  aggregateResults.resize(endIndex - beginIndex);

  decltype(auto) groupValues = resultTable->getColumn(0);
  auto& aggregateDataVariant =
      aggregationData.getAggregationDataVariant(dataIndex);

  auto op = [&aggregationData, &aggregateDataVariant](Id val) {
    auto index = aggregationData.getIndex(val);

    auto visitor = [&index](auto& aggregateDataVariant) {
      return aggregateDataVariant.at(index).calculateResult();
    };

    return std::visit(visitor, aggregateDataVariant);
  };

  std::ranges::transform(groupValues.begin() + beginIndex,
                         groupValues.begin() + endIndex,
                         aggregateResults.begin(), op);

  return aggregateResults;
}

// _____________________________________________________________________________
void GroupBy::substituteGroupVariable(
    const std::vector<GroupBy::ParentAndChildIndex>& occurrences,
    IdTable* resultTable) const {
  decltype(auto) groupValues = resultTable->getColumn(0);

  for (const auto& occurrence : occurrences) {
    sparqlExpression::VectorWithMemoryLimit<ValueId> values(
        getExecutionContext()->getAllocator());
    values.resize(groupValues.size());
    std::ranges::copy(groupValues, values.begin());

    auto newExpression = std::make_unique<sparqlExpression::VectorIdExpression>(
        std::move(values));

    occurrence.parent_->replaceChild(occurrence.nThChild_,
                                     std::move(newExpression));
  }
}

// _____________________________________________________________________________
void GroupBy::substituteAllAggregates(
    std::vector<HashMapAggregateInformation>& info, size_t beginIndex,
    size_t endIndex, const HashMapAggregationData& aggregationData,
    IdTable* resultTable) {
  // Substitute in the results of all aggregates of `info`.
  for (auto& aggregate : info) {
    auto aggregateResults = getHashMapAggregationResults(
        resultTable, aggregationData, aggregate.aggregateDataIndex_, beginIndex,
        endIndex);

    // Substitute the resulting vector as a literal
    auto newExpression = std::make_unique<sparqlExpression::VectorIdExpression>(
        std::move(aggregateResults));

    AD_CONTRACT_CHECK(aggregate.parentAndIndex_.has_value());
    auto parentAndIndex = aggregate.parentAndIndex_.value();
    parentAndIndex.parent_->replaceChild(parentAndIndex.nThChild_,
                                         std::move(newExpression));
  }
}

// _____________________________________________________________________________
std::vector<size_t> GroupBy::HashMapAggregationData::getHashEntries(
    std::span<const Id> ids) {
  std::vector<size_t> hashEntries;
  hashEntries.reserve(ids.size());

  for (auto& val : ids) {
    auto [iterator, wasAdded] = map_.try_emplace(val, getNumberOfGroups());

    hashEntries.push_back(iterator->second);
  }

  for (auto& aggregation : aggregationData_)
    std::visit([this](auto& arg) { arg.resize(getNumberOfGroups()); },
               aggregation);

  return hashEntries;
}

// _____________________________________________________________________________
[[nodiscard]] std::vector<Id>
GroupBy::HashMapAggregationData::getSortedGroupColumn() const {
  std::vector<ValueId> sortedKeys;
  sortedKeys.reserve(map_.size());
  // TODO<C++23>: use ranges::to
  for (const auto& val : map_) {
    sortedKeys.push_back(val.first);
  }
  std::ranges::sort(sortedKeys);
  return sortedKeys;
}

// _____________________________________________________________________________
void GroupBy::evaluateAlias(
    HashMapAliasInformation& alias, IdTable* result,
    sparqlExpression::EvaluationContext& evaluationContext,
    const HashMapAggregationData& aggregationData, LocalVocab* localVocab) {
  auto& info = alias.aggregateInfo_;

  // Check if the grouped variable occurs in this expression
  auto groupByVariableSubstitutions =
      findGroupedVariable(alias.expr_.getPimpl());

  if (std::get_if<OccurenceAsRoot>(&groupByVariableSubstitutions)) {
    // If the aggregate is at the top of the alias, e.g. `SELECT (?a as ?x)
    // WHERE {...} GROUP BY ?a`, we can copy values directly from the column
    // of the grouped variable
    decltype(auto) groupValues = result->getColumn(0);
    decltype(auto) outValues = result->getColumn(alias.outCol_);
    std::ranges::copy(groupValues, outValues.begin());

    // We also need to store it for possible future use
    sparqlExpression::VectorWithMemoryLimit<ValueId> values(
        getExecutionContext()->getAllocator());
    values.resize(groupValues.size());
    std::ranges::copy(groupValues, values.begin());

    evaluationContext._previousResultsFromSameGroup.at(alias.outCol_) =
        sparqlExpression::copyExpressionResult(
            sparqlExpression::ExpressionResult{std::move(values)});
  } else if (info.size() == 1 && !info.at(0).parentAndIndex_.has_value()) {
    // Only one aggregate, and it is at the top of the alias, e.g.
    // `(AVG(?x) as ?y)`. The grouped by variable cannot occur inside
    // an aggregate, hence we don't need to substitute anything here
    auto& aggregate = info.at(0);

    // Get aggregate results
    auto aggregateResults = getHashMapAggregationResults(
        result, aggregationData, aggregate.aggregateDataIndex_,
        evaluationContext._beginIndex, evaluationContext._endIndex);

    // Copy to result table
    decltype(auto) outValues = result->getColumn(alias.outCol_);
    std::ranges::copy(aggregateResults,
                      outValues.begin() + evaluationContext._beginIndex);

    // Copy the result so that future aliases may reuse it
    evaluationContext._previousResultsFromSameGroup.at(alias.outCol_) =
        sparqlExpression::copyExpressionResult(
            sparqlExpression::ExpressionResult{std::move(aggregateResults)});
  } else {
    const auto& occurrences =
        get<std::vector<ParentAndChildIndex>>(groupByVariableSubstitutions);
    // Substitute in the values of the grouped variable
    substituteGroupVariable(occurrences, result);

    // Substitute in the results of all aggregates contained in the
    // expression of the current alias, if `info` is non-empty.
    substituteAllAggregates(info, evaluationContext._beginIndex,
                            evaluationContext._endIndex, aggregationData,
                            result);

    // Evaluate top-level alias expression
    sparqlExpression::ExpressionResult expressionResult =
        alias.expr_.getPimpl()->evaluate(&evaluationContext);

    // Copy the result so that future aliases may reuse it
    evaluationContext._previousResultsFromSameGroup.at(alias.outCol_) =
        sparqlExpression::copyExpressionResult(expressionResult);

    // Extract values
    extractValues(std::move(expressionResult), evaluationContext, result,
                  localVocab, alias.outCol_);
  }
}

// _____________________________________________________________________________
void GroupBy::createResultFromHashMap(
    IdTable* result, const HashMapAggregationData& aggregationData,
    std::vector<HashMapAliasInformation>& aggregateAliases,
    LocalVocab* localVocab) {
  // Create result table, filling in the group values, since they might be
  // required in evaluation
  auto sortedKeys = aggregationData.getSortedGroupColumn();
  size_t numberOfGroups = aggregationData.getNumberOfGroups();
  result->resize(numberOfGroups);

  std::ranges::copy(sortedKeys, result->getColumn(0).begin());

  // Initialize evaluation context
  sparqlExpression::EvaluationContext evaluationContext(
      *getExecutionContext(), _subtree->getVariableColumns(), *result,
      getExecutionContext()->getAllocator(), *localVocab);

  evaluationContext._groupedVariables = ad_utility::HashSet<Variable>{
      _groupByVariables.begin(), _groupByVariables.end()};
  evaluationContext._variableToColumnMapPreviousResults =
      getInternallyVisibleVariableColumns();
  evaluationContext._previousResultsFromSameGroup.resize(getResultWidth());
  evaluationContext._isPartOfGroupBy = true;

  size_t blockSize = 65536;

  for (size_t i = 0; i < numberOfGroups; i += blockSize) {
    evaluationContext._beginIndex = i;
    evaluationContext._endIndex = std::min(i + blockSize, numberOfGroups);

    for (auto& alias : aggregateAliases) {
      evaluateAlias(alias, result, evaluationContext, aggregationData,
                    localVocab);
    }
  }
}

// _____________________________________________________________________________
template <typename A>
concept SupportedAggregates =
    ad_utility::SameAsAnyTypeIn<A, GroupBy::Aggregations>;

// _____________________________________________________________________________
// Visitor function to extract values from the result of an evaluation of
// the child expression of an aggregate, and subsequently processing the values
// by calling the `increment` function of the corresponding aggregate.
static constexpr auto makeProcessGroupsVisitor =
    [](size_t blockSize,
       const sparqlExpression::EvaluationContext* evaluationContext,
       const std::vector<size_t>& hashEntries) {
      return [blockSize, evaluationContext,
              &hashEntries]<sparqlExpression::SingleExpressionResult T,
                            SupportedAggregates A>(T&& singleResult,
                                                   A& aggregationDataVector) {
        auto generator = sparqlExpression::detail::makeGenerator(
            std::forward<T>(singleResult), blockSize, evaluationContext);

        auto hashEntryIndex = 0;

        for (const auto& val : generator) {
          auto vectorOffset = hashEntries[hashEntryIndex];
          auto& aggregateData = aggregationDataVector.at(vectorOffset);

          aggregateData.increment(val, evaluationContext);

          ++hashEntryIndex;
        }
      };
    };

// _____________________________________________________________________________
void GroupBy::computeGroupByForHashMapOptimization(
    IdTable* result, std::vector<HashMapAliasInformation>& aggregateAliases,
    const IdTable& subresult, size_t columnIndex, LocalVocab* localVocab) {
  // Initialize aggregation data
  HashMapAggregationData aggregationData(getExecutionContext()->getAllocator(),
                                         aggregateAliases);

  // Initialize evaluation context
  sparqlExpression::EvaluationContext evaluationContext(
      *getExecutionContext(), _subtree->getVariableColumns(), subresult,
      getExecutionContext()->getAllocator(), *localVocab);

  evaluationContext._groupedVariables = ad_utility::HashSet<Variable>{
      _groupByVariables.begin(), _groupByVariables.end()};
  evaluationContext._isPartOfGroupBy = true;

  size_t blockSize = 65536;

  for (size_t i = 0; i < subresult.size(); i += blockSize) {
    evaluationContext._beginIndex = i;
    evaluationContext._endIndex = std::min(i + blockSize, subresult.size());

    auto currentBlockSize =
        evaluationContext._endIndex - evaluationContext._beginIndex;

    // Perform HashMap lookup once for all groups in current block
    auto groupValues =
        subresult.getColumn(columnIndex)
            .subspan(evaluationContext._beginIndex, currentBlockSize);
    auto hashEntries = aggregationData.getHashEntries(groupValues);

    for (auto& aggregateAlias : aggregateAliases) {
      for (auto& aggregate : aggregateAlias.aggregateInfo_) {
        // Evaluate child expression on block
        auto exprChildren = aggregate.expr_->children();
        sparqlExpression::ExpressionResult expressionResult =
            exprChildren[0]->evaluate(&evaluationContext);

        auto& aggregationDataVariant =
            aggregationData.getAggregationDataVariant(
                aggregate.aggregateDataIndex_);

        std::visit(makeProcessGroupsVisitor(currentBlockSize,
                                            &evaluationContext, hashEntries),
                   std::move(expressionResult), aggregationDataVariant);
      }
    }
  }

  createResultFromHashMap(result, aggregationData, aggregateAliases,
                          localVocab);
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
