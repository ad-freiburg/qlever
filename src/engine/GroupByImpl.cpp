// Copyright 2018 - 2025, University of Freiburg
// Chair of Algorithms and Data Structures
// Authors: Florian Kramer [2018 - 2020]
//          Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>
//
// Copyright 2025, Bayerische Motoren Werke Aktiengesellschaft (BMW AG)

#include "engine/GroupByImpl.h"

#include <absl/strings/str_join.h>

#include "backports/algorithm.h"
#include "engine/CallFixedSize.h"
#include "engine/ExistsJoin.h"
#include "engine/IndexScan.h"
#include "engine/Join.h"
#include "engine/LazyGroupBy.h"
#include "engine/Sort.h"
#include "engine/StripColumns.h"
#include "engine/sparqlExpressions/AggregateExpression.h"
#include "engine/sparqlExpressions/CountStarExpression.h"
#include "engine/sparqlExpressions/GroupConcatExpression.h"
#include "engine/sparqlExpressions/LiteralExpression.h"
#include "engine/sparqlExpressions/SampleExpression.h"
#include "engine/sparqlExpressions/SparqlExpression.h"
#include "engine/sparqlExpressions/SparqlExpressionGenerators.h"
#include "engine/sparqlExpressions/StdevExpression.h"
#include "global/RuntimeParameters.h"
#include "index/Index.h"
#include "index/IndexImpl.h"
#include "parser/Alias.h"
#include "util/HashSet.h"
#include "util/Timer.h"

namespace groupBy::detail {

template <typename T>
CPP_requires(HasResize, requires(T& val)(val.resize(std::declval<size_t>())));

template <size_t IN_WIDTH, size_t OUT_WIDTH>
class LazyGroupByRange
    : public ad_utility::InputRangeFromGet<Result::IdTableVocabPair> {
 private:
  using IdTableVocabPair = Result::IdTableVocabPair;
  // input arguments
  const GroupByImpl* parent_{nullptr};
  std::shared_ptr<const Result> subresult_;
  std::vector<GroupByImpl::Aggregate> aggregates_;
  std::vector<GroupByImpl::HashMapAliasInformation> aggregateAliases_;
  std::vector<size_t> groupByCols_;
  bool singleIdTable_{false};
  // runtime state
  size_t inWidth_;
  IdTable resultTable_;
  std::unique_ptr<LazyGroupBy> lazyGroupBy_;
  LocalVocab currentLocalVocab_;
  std::vector<LocalVocab> storedLocalVocabs_;
  GroupByImpl::GroupBlock currentGroupBlock_;
  bool groupSplitAcrossTables_{false};
  // range state
  bool isFinished_{false};
  std::optional<Result::LazyResult> range_;
  std::optional<Result::LazyResult::iterator> rangeIt_;

 public:
  LazyGroupByRange(
      const GroupByImpl* parent, std::shared_ptr<const Result> subresult,
      std::vector<GroupByImpl::Aggregate> aggregates,
      std::vector<GroupByImpl::HashMapAliasInformation> aggregateAliases,
      std::vector<size_t> groupByCols, bool singleIdTable,
      size_t subTreeResultWidth)
      : parent_(parent),
        subresult_(std::move(subresult)),
        aggregates_(std::move(aggregates)),
        aggregateAliases_(std::move(aggregateAliases)),
        groupByCols_(std::move(groupByCols)),
        singleIdTable_(singleIdTable),
        inWidth_(subTreeResultWidth),
        resultTable_(parent->getResultWidth(),
                     parent->getExecutionContext()->getAllocator()) {
    AD_CONTRACT_CHECK(inWidth_ == IN_WIDTH || IN_WIDTH == 0);
    AD_CONTRACT_CHECK(parent_ != nullptr);
  }

  void initialise() {
    lazyGroupBy_ = std::make_unique<LazyGroupBy>(
        currentLocalVocab_, std::move(aggregateAliases_),
        parent_->getExecutionContext()->getAllocator(), groupByCols_.size());
    range_ =
        Result::LazyResult(ad_utility::CachingContinuableTransformInputRange(
            subresult_->idTables(),
            [this](IdTableVocabPair& pair) { return process(pair); }));
    rangeIt_ = ql::ranges::begin(range_.value());
  }

  std::optional<IdTableVocabPair> get() override {
    if (isFinished_) {
      return std::nullopt;
    }

    if (!range_.has_value()) {
      initialise();
    } else {
      ++(rangeIt_.value());
    }

    // if the main range is empty, yield the final value.
    if (rangeIt_.value() == ql::ranges::end(range_.value())) {
      isFinished_ = true;
      return yieldFinalValue();
    }

    return std::optional(std::move(*(rangeIt_.value())));
  }

  // This method is forwarded to GroupByImpl::searchBlockBoundaries() during
  // process(). There is no reason to call this method outside of that use-case.
  void onBlockChange(size_t blockStart, size_t blockEnd,
                     sparqlExpression::EvaluationContext& evaluationContext) {
    if (groupSplitAcrossTables_) {
      lazyGroupBy_->processBlock(evaluationContext, blockStart, blockEnd);
      lazyGroupBy_->commitRow(resultTable_, evaluationContext,
                              currentGroupBlock_);
      groupSplitAcrossTables_ = false;
    } else {
      // This processes the whole block in batches if possible
      IdTableStatic<OUT_WIDTH> table =
          std::move(resultTable_).toStatic<OUT_WIDTH>();
      parent_->processBlock<OUT_WIDTH>(table, aggregates_, evaluationContext,
                                       blockStart, blockEnd,
                                       &currentLocalVocab_, groupByCols_);
      resultTable_ = std::move(table).toDynamic();
    }
  }

  // This method serves as the transformation for each IdTableVocabPair in the
  // subresult_->idTables(), input range. It us called by the range_ member.
  // There is no reason to call this method outside of that use-case.
  auto process(IdTableVocabPair& pair) {
    using LoopControl = ad_utility::LoopControl<Result::IdTableVocabPair>;
    auto& idTable = pair.idTable_;
    if (idTable.empty()) {
      return LoopControl::makeContinue();
    }

    AD_CORRECTNESS_CHECK(idTable.numColumns() == inWidth_);
    parent_->checkCancellation();
    storedLocalVocabs_.emplace_back(std::move(pair.localVocab_));

    if (currentGroupBlock_.empty()) {
      for (size_t col : groupByCols_) {
        currentGroupBlock_.emplace_back(col, idTable(0, col));
      }
    }

    sparqlExpression::EvaluationContext evaluationContext =
        parent_->createEvaluationContext(currentLocalVocab_, idTable);

    size_t lastBlockStart = parent_->searchBlockBoundaries(
        [this, &evaluationContext](size_t a, size_t b) {
          onBlockChange(a, b, evaluationContext);
        },
        idTable.asStaticView<IN_WIDTH>(), currentGroupBlock_);
    groupSplitAcrossTables_ = true;
    lazyGroupBy_->processBlock(evaluationContext, lastBlockStart,
                               idTable.size());
    if (!singleIdTable_ && !resultTable_.empty()) {
      currentLocalVocab_.mergeWith(storedLocalVocabs_);
      auto result = Result::IdTableVocabPair{std::move(resultTable_),
                                             std::move(currentLocalVocab_)};
      // Keep last local vocab for next commit, since we might write to
      // `currentLocalVocab_`, we need to clone it.
      currentLocalVocab_ = storedLocalVocabs_.back().clone();
      storedLocalVocabs_.clear();

      return LoopControl::yieldValue(std::move(result));
    }

    return LoopControl::makeContinue();
  }

  // After range_ is finished, this method is called to yield the final value,
  // which includes processing the last group if necessary.
  std::optional<Result::IdTableVocabPair> yieldFinalValue() {
    // No need for final commit when loop was never entered.
    if (!groupSplitAcrossTables_) {
      // If we have an implicit group by we need to produce one result row
      if (groupByCols_.empty()) {
        // If we have an implicit GROUP BY, where the entire input is a
        // single group, we need to produce one result row.
        parent_->processEmptyImplicitGroup<OUT_WIDTH>(resultTable_, aggregates_,
                                                      &currentLocalVocab_);
        return IdTableVocabPair{std::move(resultTable_),
                                std::move(currentLocalVocab_)};
      }
      if (singleIdTable_) {
        // Yield at least a single empty table if requested.
        return IdTableVocabPair{std::move(resultTable_),
                                std::move(currentLocalVocab_)};
      }
      return std::nullopt;
    }

    // Process remaining items in the last group.  For those we have already
    // called `lazyGroupBy.processBlock()` but the call to `commitRow` is
    // still missing. We have to setup a dummy input table and evaluation
    // context, that have the values of the `currentGroupBlock` in the
    // correct columns.
    IdTable idTable{inWidth_, ad_utility::makeAllocatorWithLimit<Id>(
                                  1_B * sizeof(Id) * inWidth_)};
    idTable.emplace_back();
    for (const auto& [colIdx, value] : currentGroupBlock_) {
      idTable.at(0, colIdx) = value;
    }

    sparqlExpression::EvaluationContext evaluationContext =
        parent_->createEvaluationContext(currentLocalVocab_, idTable);

    lazyGroupBy_->commitRow(resultTable_, evaluationContext,
                            currentGroupBlock_);
    currentLocalVocab_.mergeWith(storedLocalVocabs_);
    return IdTableVocabPair{std::move(resultTable_),
                            std::move(currentLocalVocab_)};
  }
};
}  // namespace groupBy::detail

using groupBy::detail::VectorOfAggregationData;

// _____________________________________________________________________________
GroupByImpl::GroupByImpl(QueryExecutionContext* qec,
                         vector<Variable> groupByVariables,
                         std::vector<Alias> aliases,
                         std::shared_ptr<QueryExecutionTree> subtree)
    : Operation{qec},
      _groupByVariables{std::move(groupByVariables)},
      _aliases{std::move(aliases)} {
  AD_CORRECTNESS_CHECK(subtree != nullptr);
  // Remove all undefined GROUP BY variables (according to the SPARQL standard
  // they are allowed, but have no effect on the result).
  ql::erase_if(_groupByVariables,
               [&map = subtree->getVariableColumns()](const auto& var) {
                 return !map.contains(var);
               });

  // The subtrees of a GROUP BY only need to compute columns that are grouped or
  // used in any of the aggregate aliases.
  if (getRuntimeParameter<&RuntimeParameters::stripColumns_>()) {
    std::set<Variable> usedVariables{_groupByVariables.begin(),
                                     _groupByVariables.end()};
    for (const auto& alias : _aliases) {
      for (const auto* var : alias._expression.containedVariables()) {
        usedVariables.insert(*var);
      }
    }
    subtree = QueryExecutionTree::makeTreeWithStrippedColumns(
        std::move(subtree), usedVariables);
  }

  // Sort `groupByVariables` to ensure that the cache key is order invariant.
  //
  // NOTE: It is tempting to do the same also for the aliases, but that would
  // break the case when an alias reuses a variable that was bound by a previous
  // alias.
  ql::ranges::sort(_groupByVariables, std::less<>{}, &Variable::name);

  // Aliases are like `BIND`s, which may contain `EXISTS` expressions.
  for (const auto& alias : _aliases) {
    subtree = ExistsJoin::addExistsJoinsToSubtree(
        alias._expression, std::move(subtree), getExecutionContext(),
        cancellationHandle_);
  }

  // The input of a GROUP BY has to be sorted. If possible, we get optimize out
  // the sort during the evaluation.
  auto sortColumns = computeSortColumns(subtree.get());
  _subtree =
      QueryExecutionTree::createSortedTree(std::move(subtree), sortColumns);
}

std::string GroupByImpl::getCacheKeyImpl() const {
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

std::string GroupByImpl::getDescriptor() const {
  if (_groupByVariables.empty()) {
    return "GroupBy (implicit)";
  }
  return "GroupBy on " +
         absl::StrJoin(_groupByVariables, " ", &Variable::AbslFormatter);
}

size_t GroupByImpl::getResultWidth() const {
  return getInternallyVisibleVariableColumns().size();
}

std::vector<ColumnIndex> GroupByImpl::resultSortedOn() const {
  auto varCols = getInternallyVisibleVariableColumns();
  vector<ColumnIndex> sortedOn;
  sortedOn.reserve(_groupByVariables.size());
  for (const auto& var : _groupByVariables) {
    sortedOn.push_back(varCols[var].columnIndex_);
  }
  return sortedOn;
}

std::vector<ColumnIndex> GroupByImpl::computeSortColumns(
    const QueryExecutionTree* subtree) {
  vector<ColumnIndex> cols;
  // If we have an implicit GROUP BY, where the entire input is a single group,
  // no sorting needs to be done.
  if (_groupByVariables.empty()) {
    return cols;
  }

  const auto& inVarColMap = subtree->getVariableColumns();

  std::unordered_set<ColumnIndex> sortColSet;

  for (const auto& var : _groupByVariables) {
    AD_CONTRACT_CHECK(inVarColMap.contains(var), "Variable ", var.name(),
                      " not found in subtree for GROUP BY");
    ColumnIndex col = inVarColMap.at(var).columnIndex_;
    // Avoid sorting by a column twice.
    if (sortColSet.find(col) == sortColSet.end()) {
      sortColSet.insert(col);
      cols.push_back(col);
    }
  }
  return cols;
}

// ____________________________________________________________________
VariableToColumnMap GroupByImpl::computeVariableToColumnMap() const {
  VariableToColumnMap result;
  // The returned columns are all groupByVariables followed by aggregates.
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

float GroupByImpl::getMultiplicity([[maybe_unused]] size_t col) {
  // Group by should currently not be used in the optimizer, unless
  // it is part of a subquery. In that case multiplicities may only be
  // taken from the actual result
  return 1;
}

uint64_t GroupByImpl::getSizeEstimateBeforeLimit() {
  if (_groupByVariables.empty()) {
    return 1;
  }
  // Assume that the total number of groups is the input size divided
  // by the minimal multiplicity of one of the grouped variables.
  auto varToMultiplicity = [this](const Variable& var) -> float {
    return _subtree->getMultiplicity(_subtree->getVariableColumn(var));
  };

  float minMultiplicity = ql::ranges::min(
      _groupByVariables | ql::views::transform(varToMultiplicity));
  return _subtree->getSizeEstimate() / minMultiplicity;
}

size_t GroupByImpl::getCostEstimate() {
  // TODO: add the cost of the actual group by operation to the cost.
  // Currently group by is only added to the optimizer as a terminal operation
  // and its cost should not affect the optimizers results.
  return _subtree->getCostEstimate();
}

template <size_t OUT_WIDTH>
void GroupByImpl::processGroup(
    const Aggregate& aggregate,
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

  auto visitor = CPP_template_lambda_mut(&)(typename T)(T && singleResult)(
      requires sparqlExpression::SingleExpressionResult<T>) {
    constexpr static bool isStrongId = std::is_same_v<T, Id>;
    if constexpr (isStrongId) {
      resultEntry = singleResult;
    } else if constexpr (sparqlExpression::isConstantResult<T>) {
      resultEntry = sparqlExpression::detail::constantExpressionResultToId(
          AD_FWD(singleResult), *localVocab);
    } else if constexpr (sparqlExpression::isVectorResult<T>) {
      AD_CORRECTNESS_CHECK(singleResult.size() == 1,
                           "An expression returned a vector expression result "
                           "that contained an unexpected amount of entries.");
      resultEntry = sparqlExpression::detail::constantExpressionResultToId(
          std::move(singleResult.at(0)), *localVocab);
    } else {
      // This should never happen since aggregates always return constants or
      // vectors.
      AD_THROW(absl::StrCat("An expression returned an invalid type ",
                            typeid(T).name(),
                            " as the result of an aggregation step."));
    }
  };

  std::visit(visitor, std::move(expressionResult));
}

// _____________________________________________________________________________
template <size_t IN_WIDTH, size_t OUT_WIDTH>
IdTable GroupByImpl::doGroupBy(const IdTable& inTable,
                               const vector<size_t>& groupByCols,
                               const vector<Aggregate>& aggregates,
                               LocalVocab* outLocalVocab) const {
  AD_LOG_DEBUG << "Group by input size " << inTable.size() << std::endl;
  IdTable dynResult{getResultWidth(), getExecutionContext()->getAllocator()};

  // If the input is empty, the result is also empty, except for an implicit
  // GROUP BY (`groupByCols.empty()`), which always has to produce one result
  // row (see the code further down).
  if (inTable.empty() && !groupByCols.empty()) {
    return dynResult;
  }

  const IdTableView<IN_WIDTH> input = inTable.asStaticView<IN_WIDTH>();
  IdTableStatic<OUT_WIDTH> result = std::move(dynResult).toStatic<OUT_WIDTH>();

  sparqlExpression::EvaluationContext evaluationContext =
      createEvaluationContext(*outLocalVocab, inTable);

  auto processNextBlock = [this, &result, &aggregates, &evaluationContext,
                           &outLocalVocab,
                           &groupByCols](size_t blockStart, size_t blockEnd) {
    processBlock<OUT_WIDTH>(result, aggregates, evaluationContext, blockStart,
                            blockEnd, outLocalVocab, groupByCols);
  };

  // Handle the implicit GROUP BY, where the entire input is a single group.
  if (groupByCols.empty()) {
    processNextBlock(0, input.size());
    return std::move(result).toDynamic();
  }

  // This stores the values of the group by numColumns for the current block. A
  // block ends when one of these values changes.
  GroupBlock currentGroupBlock;
  for (size_t col : groupByCols) {
    currentGroupBlock.push_back(std::pair<size_t, Id>(col, input(0, col)));
  }
  size_t lastBlockStart =
      searchBlockBoundaries(processNextBlock, input, currentGroupBlock);
  processNextBlock(lastBlockStart, input.size());
  return std::move(result).toDynamic();
}

// _____________________________________________________________________________
sparqlExpression::EvaluationContext GroupByImpl::createEvaluationContext(
    LocalVocab& localVocab, const IdTable& idTable) const {
  sparqlExpression::EvaluationContext evaluationContext{
      *getExecutionContext(),
      _subtree->getVariableColumns(),
      idTable,
      getExecutionContext()->getAllocator(),
      localVocab,
      cancellationHandle_,
      deadline_};

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
  return evaluationContext;
}

// _____________________________________________________________________________
Result GroupByImpl::computeResult(bool requestLaziness) {
  AD_LOG_DEBUG << "GroupBy result computation..." << std::endl;

  if (auto idTable = computeOptimizedGroupByIfPossible()) {
    // Note: The optimized group bys currently all include index scans and thus
    // can never produce local vocab entries. If this should ever change, then
    // we also have to take care of the local vocab here.
    return {std::move(idTable).value(), resultSortedOn(), LocalVocab{}};
  }

  std::vector<Aggregate> aggregates;
  aggregates.reserve(_aliases.size() + _groupByVariables.size());

  // parse the aggregate aliases
  const auto& varColMap = getInternallyVisibleVariableColumns();
  for (const Alias& alias : _aliases) {
    aggregates.push_back(
        {alias._expression, varColMap.at(alias._target).columnIndex_});
  }

  // Check if optimization for explicitly sorted child can be applied
  auto metadataForUnsequentialData =
      checkIfHashMapOptimizationPossible(aggregates);
  bool useHashMapOptimization = metadataForUnsequentialData.has_value();

  std::shared_ptr<const Result> subresult;
  if (useHashMapOptimization) {
    const auto* child = _subtree->getRootOperation()->getChildren().at(0);
    // Skip sorting
    subresult = child->getResult(true);
    // Update runtime information
    auto runTimeInfoChildren =
        child->getRootOperation()->getRuntimeInfoPointer();
    _subtree->getRootOperation()->updateRuntimeInformationWhenOptimizedOut(
        {runTimeInfoChildren});
  } else {
    // Always request child operation to provide a lazy result if the aggregate
    // expressions allow to compute the full result in chunks
    metadataForUnsequentialData =
        computeUnsequentialProcessingMetadata(aggregates, _groupByVariables);
    subresult = _subtree->getResult(metadataForUnsequentialData.has_value());
  }

  AD_LOG_DEBUG << "GroupBy subresult computation done" << std::endl;

  std::vector<size_t> groupByColumns;

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

  if (useHashMapOptimization) {
    // Helper lambda that calls `computeGroupByForHashMapOptimization` for the
    // given `subresults`.
    auto computeWithHashMap = [this, &metadataForUnsequentialData,
                               &groupByCols](auto&& subresults) {
      auto doCompute = [&](auto numCols) {
        return computeGroupByForHashMapOptimization<numCols>(
            metadataForUnsequentialData->aggregateAliases_, AD_FWD(subresults),
            groupByCols);
      };
      return ad_utility::callFixedSizeVi(groupByCols.size(), doCompute);
    };

    // Now call `computeWithHashMap` and return the result. It expects a range
    // of results, so if the result is fully materialized, we create an array
    // with a single element.
    if (subresult->isFullyMaterialized()) {
      return computeWithHashMap(
          std::array{std::pair{std::cref(subresult->idTable()),
                               std::cref(subresult->localVocab())}});
    } else {
      return computeWithHashMap(subresult->idTables());
    }
  }

  size_t inWidth = _subtree->getResultWidth();
  size_t outWidth = getResultWidth();

  if (!subresult->isFullyMaterialized()) {
    AD_CORRECTNESS_CHECK(metadataForUnsequentialData.has_value());

    Result::LazyResult generator = ad_utility::callFixedSizeVi(
        (std::array{inWidth, outWidth}),
        [&, self = this](auto inWidth, auto outWidth) {
          return self->computeResultLazily<inWidth, outWidth>(
              std::move(subresult), std::move(aggregates),
              std::move(metadataForUnsequentialData).value().aggregateAliases_,
              std::move(groupByCols), !requestLaziness);
        });

    return requestLaziness
               ? Result{std::move(generator), resultSortedOn()}
               : Result{ad_utility::getSingleElement(std::move(generator)),
                        resultSortedOn()};
  }

  AD_CORRECTNESS_CHECK(subresult->idTable().numColumns() == inWidth);

  // Make a copy of the local vocab. Note: the LocalVocab has reference
  // semantics via `shared_ptr`, so no actual strings are copied here.

  auto localVocab = subresult->getCopyOfLocalVocab();

  IdTable idTable = ad_utility::callFixedSizeVi(
      (std::array{inWidth, outWidth}),
      [&, self = this](auto inWidth, auto outWidth) {
        return self->doGroupBy<inWidth, outWidth>(
            subresult->idTable(), groupByCols, aggregates, &localVocab);
      });

  AD_LOG_DEBUG << "GroupBy result computation done." << std::endl;
  return {std::move(idTable), resultSortedOn(), std::move(localVocab)};
}

// _____________________________________________________________________________
template <int COLS, typename T>
QL_CONCEPT_OR_NOTHING(requires ranges::invocable<T, size_t, size_t>)
size_t GroupByImpl::searchBlockBoundaries(const T& onBlockChange,
                                          const IdTableView<COLS>& idTable,
                                          GroupBlock& currentGroupBlock) const {
  size_t blockStart = 0;

  for (size_t pos = 0; pos < idTable.size(); pos++) {
    checkCancellation();
    bool rowMatchesCurrentBlock =
        // TODO<joka921> ql::ranges has problems with the local lambda, find out
        // what's wrong.
        std::all_of(currentGroupBlock.begin(), currentGroupBlock.end(),
                    [&](const auto& colIdxAndValue) {
                      return idTable(pos, colIdxAndValue.first) ==
                             colIdxAndValue.second;
                    });
    if (!rowMatchesCurrentBlock) {
      onBlockChange(blockStart, pos);
      // setup for processing the next block
      blockStart = pos;
      for (auto& [colIdx, value] : currentGroupBlock) {
        value = idTable(pos, colIdx);
      }
    }
  }
  return blockStart;
}

// _____________________________________________________________________________
template <size_t OUT_WIDTH>
void GroupByImpl::processBlock(
    IdTableStatic<OUT_WIDTH>& output, const std::vector<Aggregate>& aggregates,
    sparqlExpression::EvaluationContext& evaluationContext, size_t blockStart,
    size_t blockEnd, LocalVocab* localVocab,
    const vector<size_t>& groupByCols) const {
  output.emplace_back();
  size_t rowIdx = output.size() - 1;
  for (size_t i = 0; i < groupByCols.size(); ++i) {
    output(rowIdx, i) =
        evaluationContext._inputTable(blockStart, groupByCols[i]);
  }
  for (const Aggregate& aggregate : aggregates) {
    processGroup<OUT_WIDTH>(aggregate, evaluationContext, blockStart, blockEnd,
                            &output, rowIdx, aggregate._outCol, localVocab);
  }
}

// _____________________________________________________________________________
template <size_t OUT_WIDTH>
void GroupByImpl::processEmptyImplicitGroup(
    IdTable& resultTable, const std::vector<Aggregate>& aggregates,
    LocalVocab* localVocab) const {
  size_t inWidth = _subtree->getResultWidth();
  IdTable idTable{inWidth, ad_utility::makeAllocatorWithLimit<Id>(0_B)};

  sparqlExpression::EvaluationContext evaluationContext =
      createEvaluationContext(*localVocab, idTable);
  resultTable.emplace_back();

  IdTableStatic<OUT_WIDTH> table = std::move(resultTable).toStatic<OUT_WIDTH>();
  for (const Aggregate& aggregate : aggregates) {
    processGroup<OUT_WIDTH>(aggregate, evaluationContext, 0, 0, &table, 0,
                            aggregate._outCol, localVocab);
  }
  resultTable = std::move(table).toDynamic();
}

// _____________________________________________________________________________
template <size_t IN_WIDTH, size_t OUT_WIDTH>
Result::LazyResult GroupByImpl::computeResultLazily(
    std::shared_ptr<const Result> subresult, std::vector<Aggregate> aggregates,
    std::vector<HashMapAliasInformation> aggregateAliases,
    std::vector<size_t> groupByCols, bool singleIdTable) const {
  return Result::LazyResult(
      groupBy::detail::LazyGroupByRange<IN_WIDTH, OUT_WIDTH>(
          this, std::move(subresult), std::move(aggregates),
          std::move(aggregateAliases), std::move(groupByCols), singleIdTable,
          _subtree->getResultWidth()));
}

// _____________________________________________________________________________
std::optional<IdTable> GroupByImpl::computeGroupByForSingleIndexScan() const {
  // The child must be an `IndexScan` for this optimization.
  auto indexScan =
      std::dynamic_pointer_cast<const IndexScan>(_subtree->getRootOperation());

  if (!indexScan) {
    return std::nullopt;
  }

  if (indexScan->numVariables() <= 1 ||
      !indexScan->graphsToFilter().areAllGraphsAllowed() ||
      !_groupByVariables.empty()) {
    return std::nullopt;
  }

  // Alias must be a single count of a variable
  auto varAndDistinctness = getVariableForCountOfSingleAlias();
  if (!varAndDistinctness.has_value()) {
    return std::nullopt;
  }

  // Distinct counts are only supported for triples with three variables without
  // a GRAPH variable and if no `LIMIT`/`OFFSET` clauses are present.
  bool countIsDistinct = varAndDistinctness.value().isDistinct_;
  if (countIsDistinct && (indexScan->numVariables() != 3 ||
                          !indexScan->additionalVariables().empty() ||
                          !indexScan->getLimitOffset().isUnconstrained())) {
    return std::nullopt;
  }

  IdTable table{1, getExecutionContext()->getAllocator()};
  table.emplace_back();
  const auto& var = varAndDistinctness.value().variable_;
  if (!isVariableBoundInSubtree(var)) {
    // The variable is never bound, so its count is zero.
    table(0, 0) = Id::makeFromInt(0);
  } else if (indexScan->numVariables() == 3) {
    if (countIsDistinct) {
      auto permutation =
          getPermutationForThreeVariableTriple(*_subtree, var, var);
      AD_CONTRACT_CHECK(permutation.has_value());
      table(0, 0) = Id::makeFromInt(
          getIndex().getImpl().numDistinctCol0(permutation.value()).normal);
    } else {
      const auto& limitOffset = indexScan->getLimitOffset();
      table(0, 0) = Id::makeFromInt(
          limitOffset.actualSize(getIndex().numTriples().normal));
    }
  } else {
    const auto& limitOffset = indexScan->getLimitOffset();
    table(0, 0) =
        Id::makeFromInt(limitOffset.actualSize(indexScan->getExactSize()));
  }
  return table;
}

// ____________________________________________________________________________
std::optional<IdTable> GroupByImpl::computeGroupByObjectWithCount() const {
  // The child must be an `IndexScan` with exactly two variables.
  auto indexScan =
      std::dynamic_pointer_cast<IndexScan>(_subtree->getRootOperation());
  if (!indexScan || !indexScan->graphsToFilter().areAllGraphsAllowed() ||
      indexScan->numVariables() != 2) {
    return std::nullopt;
  }
  const auto& permutedTriple = indexScan->getPermutedTriple();
  const auto& vocabulary = getIndex().getVocab();
  std::optional<Id> col0Id =
      permutedTriple[0]->toValueId(vocabulary, getIndex().encodedIriManager());
  if (!col0Id.has_value()) {
    return std::nullopt;
  }

  // There must be exactly one GROUP BY variable and the result of the index
  // scan must be sorted by it.
  if (_groupByVariables.size() != 1) {
    return std::nullopt;
  }
  const auto& groupByVariable = _groupByVariables.at(0);
  AD_CORRECTNESS_CHECK(
      *(permutedTriple[1]) == groupByVariable,
      "Result of index scan for GROUP BY must be sorted by the "
      "GROUP BY variable, this is a bug in the query planner",
      permutedTriple[1]->toString(), groupByVariable.name());

  // There must be exactly one alias, which is a non-distinct count of one of
  // the two variables of the index scan.
  auto countedVariable = getVariableForNonDistinctCountOfSingleAlias();
  bool countedVariableIsOneOfIndexScanVariables =
      countedVariable == *(permutedTriple[1]) ||
      countedVariable == *(permutedTriple[2]);
  if (!countedVariableIsOneOfIndexScanVariables) {
    return std::nullopt;
  }

  // Compute the result and update the runtime information (we don't actually
  // do the index scan, but something smarter).
  const auto& permutation = indexScan->permutation();
  auto result = permutation.getDistinctCol1IdsAndCounts(
      col0Id.value(), cancellationHandle_, locatedTriplesSnapshot(),
      indexScan->getLimitOffset());

  indexScan->updateRuntimeInformationWhenOptimizedOut({});

  return result;
}

// _____________________________________________________________________________
std::optional<IdTable> GroupByImpl::computeGroupByForFullIndexScan() const {
  if (_groupByVariables.size() != 1) {
    return std::nullopt;
  }
  const auto& groupByVariable = _groupByVariables.at(0);

  // The child must be an `IndexScan` with three variables that contains
  // the grouped variable.
  auto permutationEnum = getPermutationForThreeVariableTriple(
      *_subtree, groupByVariable, groupByVariable);

  if (!permutationEnum.has_value()) {
    return std::nullopt;
  }

  // Check that all the aliases are non-distinct counts. We currently support
  // only one or no such count. Redundant additional counts will lead to an
  // exception (it is easy to reformulate the query to trigger this
  // optimization). Also keep track of whether the counted variable is actually
  // bound by the index scan (else all counts will be 0).
  size_t numCounts = 0;
  bool variableIsBoundInSubtree = true;
  for (size_t i = 0; i < _aliases.size(); ++i) {
    const auto& alias = _aliases[i];
    if (auto count = alias._expression.getVariableForCount()) {
      if (count.value().isDistinct_) {
        return std::nullopt;
      }
      numCounts++;
      variableIsBoundInSubtree =
          isVariableBoundInSubtree(count.value().variable_);
    } else {
      return std::nullopt;
    }
  }

  if (numCounts > 1) {
    throw std::runtime_error{
        "This query contains two or more COUNT expressions in the same GROUP "
        "BY that would lead to identical values. This redundancy is currently "
        "not supported."};
  }

  const auto& indexScan = _subtree->getRootOperation();
  _subtree->getRootOperation()->updateRuntimeInformationWhenOptimizedOut();

  const auto& permutation =
      getExecutionContext()->getIndex().getPimpl().getPermutation(
          permutationEnum.value());
  auto table = permutation.getDistinctCol0IdsAndCounts(
      cancellationHandle_, locatedTriplesSnapshot(),
      indexScan->getLimitOffset());
  if (numCounts == 0) {
    table.setColumnSubset(std::array{ColumnIndex{0}});
  } else if (!variableIsBoundInSubtree) {
    // The variable inside the COUNT() is not part of the input, so it is always
    // unbound and has a count of 0 in each group.
    ql::ranges::fill(table.getColumn(1), Id::makeFromInt(0));
  }

  // TODO<joka921> This optimization should probably also apply if
  // the query is `SELECT DISTINCT ?s WHERE {?s ?p ?o} ` without a
  // GROUP BY, but that needs to be implemented in the `DISTINCT` operation.
  return table;
}

// ____________________________________________________________________________
std::optional<Permutation::Enum>
GroupByImpl::getPermutationForThreeVariableTriple(
    const QueryExecutionTree& tree, const Variable& variableByWhichToSort,
    const Variable& variableThatMustBeContained) {
  auto indexScan =
      std::dynamic_pointer_cast<const IndexScan>(tree.getRootOperation());

  if (!indexScan || !indexScan->graphsToFilter().areAllGraphsAllowed() ||
      indexScan->numVariables() != 3) {
    return std::nullopt;
  }
  {
    auto v = variableThatMustBeContained;
    if (v != indexScan->subject() && v != indexScan->predicate() &&
        v != indexScan->object()) {
      return std::nullopt;
    }
  }

  if (variableByWhichToSort == indexScan->subject()) {
    return Permutation::SPO;
  } else if (variableByWhichToSort == indexScan->predicate()) {
    return Permutation::POS;
  } else if (variableByWhichToSort == indexScan->object()) {
    return Permutation::OSP;
  } else {
    return std::nullopt;
  }
};

// ____________________________________________________________________________
std::optional<GroupByImpl::OptimizedGroupByData>
GroupByImpl::checkIfJoinWithFullScan(const Join& join) const {
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
  auto* child1 = static_cast<const Operation&>(join).getChildren().at(0);
  auto* child2 = static_cast<const Operation&>(join).getChildren().at(1);

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

// ____________________________________________________________________________
std::optional<IdTable> GroupByImpl::computeGroupByForJoinWithFullScan() const {
  auto join = std::dynamic_pointer_cast<Join>(_subtree->getRootOperation());
  if (!join || !join->getLimitOffset().isUnconstrained()) {
    return std::nullopt;
  }

  auto optimizedAggregateData = checkIfJoinWithFullScan(*join);
  if (!optimizedAggregateData.has_value()) {
    return std::nullopt;
  }
  const auto& [threeVarSubtree, subtree, permutation, columnIndex] =
      optimizedAggregateData.value();

  auto subresult = subtree.getResult();
  threeVarSubtree.getRootOperation()
      ->updateRuntimeInformationWhenOptimizedOut();

  join->updateRuntimeInformationWhenOptimizedOut(
      {subtree.getRootOperation()->getRuntimeInfoPointer(),
       threeVarSubtree.getRootOperation()->getRuntimeInfoPointer()});
  IdTable result{2, getExecutionContext()->getAllocator()};
  if (subresult->idTable().size() == 0) {
    return result;
  }

  auto idTable = std::move(result).toStatic<2>();
  const auto& index = getExecutionContext()->getIndex();

  // TODO<joka921, C++23> Simplify the following pattern by using
  // `ql::views::chunk_by` and implement a lazy version of this view for
  // input iterators.

  // Take care of duplicate values in the input.
  Id currentId = subresult->idTable()(0, columnIndex);
  size_t currentCount = 0;
  size_t currentCardinality =
      index.getCardinality(currentId, permutation, locatedTriplesSnapshot());

  auto pushRow = [&]() {
    // If the count is 0 this means that the element with the `currentId`
    // doesn't exist in the knowledge graph. Thus, the join with a three
    // variable triple would have filtered it out and we don't include it in
    // the final result.
    if (currentCount > 0) {
      // TODO<C++20, as soon as Clang supports it>: use `emplace_back(id1,
      // id2)` (requires parenthesized initialization of aggregates.
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
      currentCardinality =
          index.getCardinality(id, permutation, locatedTriplesSnapshot());
    }
    currentCount += currentCardinality;
  }
  pushRow();
  return std::move(idTable).toDynamic();
}

// _____________________________________________________________________________
std::optional<IdTable> GroupByImpl::computeOptimizedGroupByIfPossible() const {
  // TODO<C++23> Use `std::optional::or_else`.
  if (!getRuntimeParameter<
          &RuntimeParameters::groupByDisableIndexScanOptimizations_>()) {
    if (auto result = computeGroupByForSingleIndexScan()) {
      return result;
    }
    if (auto result = computeGroupByForFullIndexScan()) {
      return result;
    }
  }
  if (auto result = computeGroupByForJoinWithFullScan()) {
    return result;
  }
  if (auto result = computeGroupByObjectWithCount()) {
    return result;
  }
  if (auto result = computeCountStar()) {
    return result;
  }
  return std::nullopt;
}

// _____________________________________________________________________________
std::optional<GroupByImpl::HashMapOptimizationData>
GroupByImpl::computeUnsequentialProcessingMetadata(
    std::vector<Aggregate>& aliases,
    const std::vector<Variable>& groupByVariables) {
  // Get pointers to all aggregate expressions and their parents
  size_t numAggregates = 0;
  std::vector<HashMapAliasInformation> aliasesWithAggregateInfo;
  for (auto& alias : aliases) {
    auto expr = alias._expression.getPimpl();

    // Find all aggregates in the expression of the current alias.
    auto foundAggregates = findAggregates(expr);
    if (!foundAggregates.has_value()) return std::nullopt;

    for (auto& aggregate : foundAggregates.value()) {
      aggregate.aggregateDataIndex_ = numAggregates++;
    }

    // Find all grouped variables occurring in the alias expression
    std::vector<HashMapGroupedVariableInformation> groupedVariables;
    groupedVariables.reserve(groupByVariables.size());
    // TODO<C++23> use views::enumerate
    size_t i = 0;
    for (const auto& groupedVariable : groupByVariables) {
      groupedVariables.push_back(
          {groupedVariable, i, findGroupedVariable(expr, groupedVariable)});
      ++i;
    }

    aliasesWithAggregateInfo.push_back({alias._expression, alias._outCol,
                                        foundAggregates.value(),
                                        groupedVariables});
  }

  return HashMapOptimizationData{aliasesWithAggregateInfo};
}

// _____________________________________________________________________________
std::optional<GroupByImpl::HashMapOptimizationData>
GroupByImpl::checkIfHashMapOptimizationPossible(
    std::vector<Aggregate>& aliases) const {
  if (!getRuntimeParameter<&RuntimeParameters::groupByHashMapEnabled_>()) {
    return std::nullopt;
  }

  if (!std::dynamic_pointer_cast<const Sort>(_subtree->getRootOperation())) {
    return std::nullopt;
  }
  return computeUnsequentialProcessingMetadata(aliases, _groupByVariables);
}

// _____________________________________________________________________________
std::variant<std::vector<GroupByImpl::ParentAndChildIndex>,
             GroupByImpl::OccurAsRoot>
GroupByImpl::findGroupedVariable(sparqlExpression::SparqlExpression* expr,
                                 const Variable& groupedVariable) {
  std::variant<std::vector<ParentAndChildIndex>, OccurAsRoot> substitutions;
  findGroupedVariableImpl(expr, std::nullopt, substitutions, groupedVariable);
  return substitutions;
}

// _____________________________________________________________________________
void GroupByImpl::findGroupedVariableImpl(
    sparqlExpression::SparqlExpression* expr,
    std::optional<ParentAndChildIndex> parentAndChildIndex,
    std::variant<std::vector<ParentAndChildIndex>, OccurAsRoot>& substitutions,
    const Variable& groupedVariable) {
  AD_CORRECTNESS_CHECK(expr != nullptr);
  if (auto value = dynamic_cast<sparqlExpression::VariableExpression*>(expr)) {
    const auto& variable = value->value();
    if (variable != groupedVariable) return;
    if (parentAndChildIndex.has_value()) {
      auto vector =
          std::get_if<std::vector<ParentAndChildIndex>>(&substitutions);
      AD_CONTRACT_CHECK(vector != nullptr);
      vector->emplace_back(parentAndChildIndex.value());
    } else {
      substitutions = OccurAsRoot{};
      return;
    }
  }

  auto children = expr->children();

  // TODO<C++23> use views::enumerate
  size_t childIndex = 0;
  for (const auto& child : children) {
    ParentAndChildIndex parentAndChildIndexForChild{expr, childIndex++};
    findGroupedVariableImpl(child.get(), parentAndChildIndexForChild,
                            substitutions, groupedVariable);
  }
}

// _____________________________________________________________________________
std::optional<std::vector<GroupByImpl::HashMapAggregateInformation>>
GroupByImpl::findAggregates(sparqlExpression::SparqlExpression* expr) {
  std::vector<HashMapAggregateInformation> result;
  if (!findAggregatesImpl(expr, std::nullopt, result))
    return std::nullopt;
  else
    return result;
}

// _____________________________________________________________________________
std::optional<GroupByImpl::HashMapAggregateTypeWithData>
GroupByImpl::isSupportedAggregate(sparqlExpression::SparqlExpression* expr) {
  using enum HashMapAggregateType;
  using namespace sparqlExpression;

  // `expr` is not a distinct aggregate
  if (expr->isAggregate() !=
      SparqlExpression::AggregateStatus::NonDistinctAggregate)
    return std::nullopt;

  // `expr` is not a nested aggregated
  if (ql::ranges::any_of(expr->children(), [](const auto& ptr) {
        return ptr->containsAggregate();
      })) {
    return std::nullopt;
  }

  using H = HashMapAggregateTypeWithData;

  if (dynamic_cast<AvgExpression*>(expr)) return H{AVG};
  if (dynamic_cast<CountExpression*>(expr)) return H{COUNT};
  // We reuse the COUNT implementation which works, but leaves some optimization
  // potential on the table because `COUNT(*)` doesn't need to check for
  // undefined values.
  if (dynamic_cast<CountStarExpression*>(expr)) return H{COUNT};
  if (dynamic_cast<MinExpression*>(expr)) return H{MIN};
  if (dynamic_cast<MaxExpression*>(expr)) return H{MAX};
  if (dynamic_cast<SumExpression*>(expr)) return H{SUM};
  if (auto val = dynamic_cast<GroupConcatExpression*>(expr)) {
    return H{GROUP_CONCAT, val->getSeparator()};
  }
  // NOTE: The STDEV function is not suitable for lazy and hash map
  // optimizations.
  if (dynamic_cast<SampleExpression*>(expr)) return H{SAMPLE};

  // `expr` is an unsupported aggregate
  return std::nullopt;
}

// _____________________________________________________________________________
bool GroupByImpl::findAggregatesImpl(
    sparqlExpression::SparqlExpression* expr,
    std::optional<ParentAndChildIndex> parentAndChildIndex,
    std::vector<HashMapAggregateInformation>& info) {
  if (expr->isAggregate() !=
      sparqlExpression::SparqlExpression::AggregateStatus::NoAggregate) {
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
void GroupByImpl::extractValues(
    sparqlExpression::ExpressionResult&& expressionResult,
    sparqlExpression::EvaluationContext& evaluationContext,
    IdTable* resultTable, LocalVocab* localVocab, size_t outCol) {
  auto visitor = CPP_template_lambda_mut(&evaluationContext, &resultTable,
                                         &localVocab, &outCol)(typename T)(
      T && singleResult)(requires sparqlExpression::SingleExpressionResult<T>) {
    auto generator = sparqlExpression::detail::makeGenerator(
        AD_FWD(singleResult), evaluationContext.size(), &evaluationContext);

    auto targetIterator =
        resultTable->getColumn(outCol).begin() + evaluationContext._beginIndex;
    for (sparqlExpression::IdOrLiteralOrIri val : generator) {
      *targetIterator = sparqlExpression::detail::constantExpressionResultToId(
          std::move(val), *localVocab);
      ++targetIterator;
    }
  };

  std::visit(visitor, std::move(expressionResult));
}

// _____________________________________________________________________________
static constexpr auto resizeIfVector = [](auto& val, size_t size) {
  if constexpr (CPP_requires_ref(groupBy::detail::HasResize, decltype(val))) {
    val.resize(size);
  }
};

// _____________________________________________________________________________
template <size_t NUM_GROUP_COLUMNS>
sparqlExpression::VectorWithMemoryLimit<ValueId>
GroupByImpl::getHashMapAggregationResults(
    IdTable* resultTable,
    const HashMapAggregationData<NUM_GROUP_COLUMNS>& aggregationData,
    size_t dataIndex, size_t beginIndex, size_t endIndex,
    LocalVocab* localVocab, const Allocator& allocator) {
  sparqlExpression::VectorWithMemoryLimit<ValueId> aggregateResults(allocator);
  aggregateResults.resize(endIndex - beginIndex);

  auto& aggregateDataVariant =
      aggregationData.getAggregationDataVariant(dataIndex);

  using B = typename HashMapAggregationData<
      NUM_GROUP_COLUMNS>::template ArrayOrVector<Id>;
  for (size_t rowIdx = beginIndex; rowIdx < endIndex; ++rowIdx) {
    size_t vectorIdx;
    // Special case for lazy consumer where the hashmap is not used
    if (aggregationData.getNumberOfGroups() == 0) {
      vectorIdx = 0;
    } else {
      B mapKey;
      resizeIfVector(mapKey, aggregationData.numOfGroupedColumns_);

      for (size_t idx = 0; idx < mapKey.size(); ++idx) {
        mapKey.at(idx) = resultTable->getColumn(idx)[rowIdx];
      }
      vectorIdx = aggregationData.getIndex(mapKey);
    }

    auto visitor = [&aggregateResults, vectorIdx, rowIdx, beginIndex,
                    localVocab](auto& aggregateDataVariant) {
      aggregateResults[rowIdx - beginIndex] =
          aggregateDataVariant.at(vectorIdx).calculateResult(localVocab);
    };

    std::visit(visitor, aggregateDataVariant);
  }

  return aggregateResults;
}

// _____________________________________________________________________________
std::vector<std::unique_ptr<sparqlExpression::SparqlExpression>>
GroupByImpl::substituteGroupVariable(
    const std::vector<ParentAndChildIndex>& occurrences, IdTable* resultTable,
    size_t beginIndex, size_t count, size_t columnIndex,
    const Allocator& allocator) {
  decltype(auto) groupValues =
      resultTable->getColumn(columnIndex).subspan(beginIndex, count);

  std::vector<std::unique_ptr<sparqlExpression::SparqlExpression>>
      originalChildren;
  originalChildren.reserve(occurrences.size());
  for (const auto& occurrence : occurrences) {
    sparqlExpression::VectorWithMemoryLimit<ValueId> values(allocator);
    values.resize(groupValues.size());
    ql::ranges::copy(groupValues, values.begin());

    auto newExpression = std::make_unique<sparqlExpression::VectorIdExpression>(
        std::move(values));

    originalChildren.push_back(occurrence.parent_->replaceChild(
        occurrence.nThChild_, std::move(newExpression)));
  }
  return originalChildren;
}

// _____________________________________________________________________________
template <size_t NUM_GROUP_COLUMNS>
std::vector<std::unique_ptr<sparqlExpression::SparqlExpression>>
GroupByImpl::substituteAllAggregates(
    std::vector<HashMapAggregateInformation>& info, size_t beginIndex,
    size_t endIndex,
    const HashMapAggregationData<NUM_GROUP_COLUMNS>& aggregationData,
    IdTable* resultTable, LocalVocab* localVocab, const Allocator& allocator) {
  std::vector<std::unique_ptr<sparqlExpression::SparqlExpression>>
      originalChildren;
  originalChildren.reserve(info.size());
  // Substitute in the results of all aggregates of `info`.
  for (auto& aggregate : info) {
    auto aggregateResults = getHashMapAggregationResults(
        resultTable, aggregationData, aggregate.aggregateDataIndex_, beginIndex,
        endIndex, localVocab, allocator);

    // Substitute the resulting vector as a literal
    auto newExpression = std::make_unique<sparqlExpression::VectorIdExpression>(
        std::move(aggregateResults));

    AD_CONTRACT_CHECK(aggregate.parentAndIndex_.has_value());
    auto parentAndIndex = aggregate.parentAndIndex_.value();
    originalChildren.push_back(parentAndIndex.parent_->replaceChild(
        parentAndIndex.nThChild_, std::move(newExpression)));
  }
  return originalChildren;
}

// _____________________________________________________________________________
template <size_t NUM_GROUP_COLUMNS>
std::vector<size_t>
GroupByImpl::HashMapAggregationData<NUM_GROUP_COLUMNS>::getHashEntries(
    const ArrayOrVector<ql::span<const Id>>& groupByCols) {
  AD_CONTRACT_CHECK(groupByCols.size() > 0);

  std::vector<size_t> hashEntries;
  size_t numberOfEntries = groupByCols.at(0).size();
  hashEntries.reserve(numberOfEntries);

  // TODO: We pass the `Id`s column-wise into this function, and then handle
  //       them row-wise. Is there any advantage to this, or should we transform
  //       the data into a row-wise format before passing it?
  for (size_t i = 0; i < numberOfEntries; ++i) {
    ArrayOrVector<Id> row;
    resizeIfVector(row, numOfGroupedColumns_);

    // TODO<C++23> use views::enumerate
    auto idx = 0;
    for (const auto& val : groupByCols) {
      row[idx] = val[i];
      ++idx;
    }

    auto [iterator, wasAdded] = map_.try_emplace(row, getNumberOfGroups());
    hashEntries.push_back(iterator->second);
  }

  // CPP_template_lambda(capture)(typenames...)(arg)(requires ...)`
  auto resizeVectors = CPP_template_lambda()(typename T)(
      T & arg, size_t numberOfGroups,
      [[maybe_unused]] const HashMapAggregateTypeWithData& info)(
      requires true) {
    if constexpr (ql::concepts::same_as<typename T::value_type,
                                        GroupConcatAggregationData>) {
      arg.resize(numberOfGroups,
                 GroupConcatAggregationData{info.separator_.value()});
    } else {
      arg.resize(numberOfGroups);
    }
  };

  // TODO<C++23> use views::enumerate
  auto idx = 0;
  for (auto& aggregation : aggregationData_) {
    const auto& aggregationTypeWithData = aggregateTypeWithData_.at(idx);
    const auto numberOfGroups = getNumberOfGroups();

    std::visit(
        [&resizeVectors, &aggregationTypeWithData, numberOfGroups](auto& arg) {
          resizeVectors(arg, numberOfGroups, aggregationTypeWithData);
        },
        aggregation);
    ++idx;
  }

  return hashEntries;
}

// _____________________________________________________________________________
template <size_t NUM_GROUP_COLUMNS>
[[nodiscard]] GroupByImpl::HashMapAggregationData<
    NUM_GROUP_COLUMNS>::ArrayOrVector<std::vector<Id>>
GroupByImpl::HashMapAggregationData<NUM_GROUP_COLUMNS>::getSortedGroupColumns()
    const {
  // Get data in a row-wise manner.
  std::vector<ArrayOrVector<Id>> sortedKeys;
  for (const auto& val : map_) {
    sortedKeys.push_back(val.first);
  }

  // Sort data.
  ql::ranges::sort(sortedKeys.begin(), sortedKeys.end());

  // Get data in a column-wise manner.
  ArrayOrVector<std::vector<Id>> result;
  resizeIfVector(result, numOfGroupedColumns_);

  for (size_t idx = 0; idx < result.size(); ++idx)
    for (auto& val : sortedKeys) {
      result.at(idx).push_back(val.at(idx));
    }

  return result;
}

// _____________________________________________________________________________
template <size_t NUM_GROUP_COLUMNS>
void GroupByImpl::substituteAndEvaluate(
    HashMapAliasInformation& alias, IdTable* result,
    sparqlExpression::EvaluationContext& evaluationContext,
    const HashMapAggregationData<NUM_GROUP_COLUMNS>& aggregationData,
    LocalVocab* localVocab, const Allocator& allocator,
    std::vector<HashMapAggregateInformation>& info,
    const std::vector<HashMapGroupedVariableInformation>& substitutions) {
  // Store which SPARQL expressions of grouped variables have been substituted.
  std::vector<std::pair<
      const std::vector<ParentAndChildIndex>&,
      std::vector<std::unique_ptr<sparqlExpression::SparqlExpression>>>>
      originalChildrenForGroupVariable;
  originalChildrenForGroupVariable.reserve(substitutions.size());
  for (const auto& substitution : substitutions) {
    const auto& occurrences =
        std::get<std::vector<ParentAndChildIndex>>(substitution.occurrences_);
    // Substitute in the values of the grouped variable and store the original
    // expressions.
    originalChildrenForGroupVariable.emplace_back(
        occurrences, substituteGroupVariable(
                         occurrences, result, evaluationContext._beginIndex,
                         evaluationContext.size(),
                         substitution.resultColumnIndex_, allocator));
  }

  // Substitute in the results of all aggregates contained in the
  // expression of the current alias, if `info` is non-empty and keep the
  // original expressions.
  std::vector<std::unique_ptr<sparqlExpression::SparqlExpression>>
      originalChildren = substituteAllAggregates(
          info, evaluationContext._beginIndex, evaluationContext._endIndex,
          aggregationData, result, localVocab, allocator);

  // Evaluate top-level alias expression.
  sparqlExpression::ExpressionResult expressionResult =
      alias.expr_.getPimpl()->evaluate(&evaluationContext);

  // Restore original children. Only necessary when the expression will be
  // used in the future (not the case for the hash map optimization).
  auto restoreOriginalExpressions = [](auto&& range, auto& originalChildren) {
    for (auto&& [parentAndIndex, originalExpression] :
         ::ranges::views::zip(AD_FWD(range), originalChildren)) {
      parentAndIndex.parent_->replaceChild(parentAndIndex.nThChild_,
                                           std::move(originalExpression));
    }
  };

  // Restore grouped variable expressions.
  for (auto& [occurrences, children] : originalChildrenForGroupVariable) {
    restoreOriginalExpressions(occurrences, children);
  }

  // Restore aggregated variable expressions.
  restoreOriginalExpressions(
      info | ql::views::transform(
                 [](auto& aggregate) -> const ParentAndChildIndex& {
                   return aggregate.parentAndIndex_.value();
                 }),
      originalChildren);

  // Copy the result so that future aliases may reuse it.
  evaluationContext._previousResultsFromSameGroup.at(alias.outCol_) =
      sparqlExpression::copyExpressionResult(expressionResult);

  // Extract values.
  extractValues(std::move(expressionResult), evaluationContext, result,
                localVocab, alias.outCol_);
}
// _____________________________________________________________________________
template <size_t NUM_GROUP_COLUMNS>
void GroupByImpl::evaluateAlias(
    HashMapAliasInformation& alias, IdTable* result,
    sparqlExpression::EvaluationContext& evaluationContext,
    const HashMapAggregationData<NUM_GROUP_COLUMNS>& aggregationData,
    LocalVocab* localVocab, const Allocator& allocator) {
  auto& info = alias.aggregateInfo_;

  // Either:
  // - One of the variables occurs at the top. This can be copied as the result
  // - There is only one aggregate, and it appears at the top. No substitutions
  // necessary, can evaluate aggregate and copy results
  // - Possibly multiple aggregates and occurrences of grouped variables. All
  // have to be substituted away before evaluation

  const auto& substitutions = alias.groupedVariables_;
  auto topLevelGroupedVariable = ql::ranges::find_if(
      substitutions, [](const HashMapGroupedVariableInformation& val) {
        return std::holds_alternative<OccurAsRoot>(val.occurrences_);
      });

  if (topLevelGroupedVariable != substitutions.end()) {
    // If the aggregate is at the top of the alias, e.g. `SELECT (?a as ?x)
    // WHERE {...} GROUP BY ?a`, we can copy values directly from the column
    // of the grouped variable
    decltype(auto) groupValues =
        result->getColumn(topLevelGroupedVariable->resultColumnIndex_)
            .subspan(evaluationContext._beginIndex, evaluationContext.size());
    decltype(auto) outValues = result->getColumn(alias.outCol_);
    ql::ranges::copy(groupValues,
                     outValues.begin() + evaluationContext._beginIndex);

    // We also need to store it for possible future use
    sparqlExpression::VectorWithMemoryLimit<ValueId> values(allocator);
    values.resize(groupValues.size());
    ql::ranges::copy(groupValues, values.begin());

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
        evaluationContext._beginIndex, evaluationContext._endIndex, localVocab,
        allocator);

    // Copy to result table
    decltype(auto) outValues = result->getColumn(alias.outCol_);
    ql::ranges::copy(aggregateResults,
                     outValues.begin() + evaluationContext._beginIndex);

    // Copy the result so that future aliases may reuse it
    evaluationContext._previousResultsFromSameGroup.at(alias.outCol_) =
        sparqlExpression::copyExpressionResult(
            sparqlExpression::ExpressionResult{std::move(aggregateResults)});
  } else {
    substituteAndEvaluate<NUM_GROUP_COLUMNS>(alias, result, evaluationContext,
                                             aggregationData, localVocab,
                                             allocator, info, substitutions);
  }
}

// _____________________________________________________________________________
sparqlExpression::ExpressionResult
GroupByImpl::evaluateChildExpressionOfAggregateFunction(
    const HashMapAggregateInformation& aggregate,
    sparqlExpression::EvaluationContext& evaluationContext) {
  // The code below assumes that DISTINCT is not supported yet.
  AD_CORRECTNESS_CHECK(aggregate.expr_->isAggregate() ==
                       sparqlExpression::SparqlExpression::AggregateStatus::
                           NonDistinctAggregate);
  // Evaluate child expression on block
  auto exprChildren = aggregate.expr_->children();
  // `COUNT(*)` is the only expression without children, so we fake the
  // expression result in this case by providing an arbitrary, constant and
  // defined value. This value will be verified as non-undefined by the
  // `CountExpression` class and ignored afterward as long as `DISTINCT` is
  // not set (which is not supported yet).
  bool isCountStar =
      dynamic_cast<sparqlExpression::CountStarExpression*>(aggregate.expr_);
  AD_CORRECTNESS_CHECK(isCountStar || exprChildren.size() == 1);
  return isCountStar ? Id::makeFromBool(true)
                     : exprChildren[0]->evaluate(&evaluationContext);
}

// _____________________________________________________________________________
template <size_t NUM_GROUP_COLUMNS>
IdTable GroupByImpl::createResultFromHashMap(
    const HashMapAggregationData<NUM_GROUP_COLUMNS>& aggregationData,
    std::vector<HashMapAliasInformation>& aggregateAliases,
    LocalVocab* localVocab) const {
  // Create result table, filling in the group values, since they might be
  // required in evaluation
  ad_utility::Timer sortingTimer{ad_utility::Timer::Started};
  auto sortedKeys = aggregationData.getSortedGroupColumns();
  runtimeInfo().addDetail("timeResultSorting", sortingTimer.msecs());

  size_t numberOfGroups = aggregationData.getNumberOfGroups();
  IdTable result{getResultWidth(), getExecutionContext()->getAllocator()};
  result.resize(numberOfGroups);

  // Copy grouped by values
  for (size_t idx = 0; idx < aggregationData.numOfGroupedColumns_; ++idx) {
    ql::ranges::copy(sortedKeys.at(idx), result.getColumn(idx).begin());
  }

  // Initialize evaluation context
  sparqlExpression::EvaluationContext evaluationContext =
      createEvaluationContext(*localVocab, result);

  ad_utility::Timer evaluationAndResultsTimer{ad_utility::Timer::Started};
  for (size_t i = 0; i < numberOfGroups; i += GROUP_BY_HASH_MAP_BLOCK_SIZE) {
    checkCancellation();

    evaluationContext._beginIndex = i;
    evaluationContext._endIndex =
        std::min(i + GROUP_BY_HASH_MAP_BLOCK_SIZE, numberOfGroups);

    for (auto& alias : aggregateAliases) {
      evaluateAlias(alias, &result, evaluationContext, aggregationData,
                    localVocab, allocator());
    }
  }
  runtimeInfo().addDetail("timeEvaluationAndResults",
                          evaluationAndResultsTimer.msecs());
  return result;
}

// _____________________________________________________________________________
// Visitor function to extract values from the result of an evaluation of
// the child expression of an aggregate, and subsequently processing the
// values by calling the `addValue` function of the corresponding aggregate.
static constexpr auto makeProcessGroupsVisitor =
    [](size_t blockSize,
       const sparqlExpression::EvaluationContext* evaluationContext,
       const std::vector<size_t>& hashEntries) {
      return CPP_template_lambda(blockSize, evaluationContext, &hashEntries)(
          typename T, typename A)(T && singleResult, A & aggregationDataVector)(
          requires sparqlExpression::SingleExpressionResult<T> &&
          VectorOfAggregationData<A>) {
        auto generator = sparqlExpression::detail::makeGenerator(
            std::forward<T>(singleResult), blockSize, evaluationContext);

        auto hashEntryIndex = 0;

        for (const auto& val : generator) {
          auto vectorOffset = hashEntries[hashEntryIndex];
          auto& aggregateData = aggregationDataVector.at(vectorOffset);

          aggregateData.addValue(val, evaluationContext);

          ++hashEntryIndex;
        }
      };
    };

// _____________________________________________________________________________
template <size_t NUM_GROUP_COLUMNS, typename SubResults>
Result GroupByImpl::computeGroupByForHashMapOptimization(
    std::vector<HashMapAliasInformation>& aggregateAliases,
    SubResults subresults, const std::vector<size_t>& columnIndices) const {
  AD_CORRECTNESS_CHECK(columnIndices.size() == NUM_GROUP_COLUMNS ||
                       NUM_GROUP_COLUMNS == 0);
  LocalVocab localVocab;

  // Initialize the data for the aggregates of the GROUP BY operation.
  HashMapAggregationData<NUM_GROUP_COLUMNS> aggregationData(
      getExecutionContext()->getAllocator(), aggregateAliases,
      columnIndices.size());

  // Process the input blocks (pairs of `IdTable` and `LocalVocab`) one after
  // the other.
  ad_utility::Timer lookupTimer{ad_utility::Timer::Stopped};
  ad_utility::Timer aggregationTimer{ad_utility::Timer::Stopped};
  for (const auto& [inputTableRef, inputLocalVocabRef] : subresults) {
    const IdTable& inputTable = inputTableRef;
    const LocalVocab& inputLocalVocab = inputLocalVocabRef;

    // Merge the local vocab of each input block.
    //
    // NOTE: If the input blocks have very similar or even identical non-empty
    // local vocabs, no deduplication is performed.
    localVocab.mergeWith(inputLocalVocab);
    // Setup the `EvaluationContext` for this input block.
    sparqlExpression::EvaluationContext evaluationContext(
        *getExecutionContext(), _subtree->getVariableColumns(), inputTable,
        getExecutionContext()->getAllocator(), localVocab, cancellationHandle_,
        deadline_);
    evaluationContext._groupedVariables = ad_utility::HashSet<Variable>{
        _groupByVariables.begin(), _groupByVariables.end()};
    evaluationContext._isPartOfGroupBy = true;

    // Iterate of the rows of this input block. Process (up to)
    // `GROUP_BY_HASH_MAP_BLOCK_SIZE` rows at a time.
    for (size_t i = 0; i < inputTable.size();
         i += GROUP_BY_HASH_MAP_BLOCK_SIZE) {
      checkCancellation();

      evaluationContext._beginIndex = i;
      evaluationContext._endIndex =
          std::min(i + GROUP_BY_HASH_MAP_BLOCK_SIZE, inputTable.size());

      auto currentBlockSize = evaluationContext.size();

      // Perform HashMap lookup once for all groups in current block
      using U = typename HashMapAggregationData<
          NUM_GROUP_COLUMNS>::template ArrayOrVector<ql::span<const Id>>;
      U groupValues;
      resizeIfVector(groupValues, columnIndices.size());

      // TODO<C++23> use views::enumerate
      size_t j = 0;
      for (auto& idx : columnIndices) {
        groupValues[j] = inputTable.getColumn(idx).subspan(
            evaluationContext._beginIndex, currentBlockSize);
        ++j;
      }
      lookupTimer.cont();
      auto hashEntries = aggregationData.getHashEntries(groupValues);
      lookupTimer.stop();

      aggregationTimer.cont();
      for (auto& aggregateAlias : aggregateAliases) {
        for (auto& aggregate : aggregateAlias.aggregateInfo_) {
          sparqlExpression::ExpressionResult expressionResult =
              GroupByImpl::evaluateChildExpressionOfAggregateFunction(
                  aggregate, evaluationContext);

          auto& aggregationDataVariant =
              aggregationData.getAggregationDataVariant(
                  aggregate.aggregateDataIndex_);

          std::visit(makeProcessGroupsVisitor(currentBlockSize,
                                              &evaluationContext, hashEntries),
                     std::move(expressionResult), aggregationDataVariant);
        }
      }
      aggregationTimer.stop();
    }
  }

  runtimeInfo().addDetail("timeMapLookup", lookupTimer.msecs());
  runtimeInfo().addDetail("timeAggregation", aggregationTimer.msecs());
  IdTable resultTable =
      createResultFromHashMap(aggregationData, aggregateAliases, &localVocab);
  return {std::move(resultTable), resultSortedOn(), std::move(localVocab)};
}

// _____________________________________________________________________________
std::optional<Variable>
GroupByImpl::getVariableForNonDistinctCountOfSingleAlias() const {
  auto varAndDistinctness = getVariableForCountOfSingleAlias();
  if (!varAndDistinctness.has_value() ||
      varAndDistinctness.value().isDistinct_) {
    return std::nullopt;
  }
  return std::move(varAndDistinctness.value().variable_);
}

// _____________________________________________________________________________
std::optional<sparqlExpression::SparqlExpressionPimpl::VariableAndDistinctness>
GroupByImpl::getVariableForCountOfSingleAlias() const {
  return _aliases.size() == 1
             ? _aliases.front()._expression.getVariableForCount()
             : std::nullopt;
}

// _____________________________________________________________________________
bool GroupByImpl::isVariableBoundInSubtree(const Variable& variable) const {
  return _subtree->getVariableColumnOrNullopt(variable).has_value();
}

// _____________________________________________________________________________
std::unique_ptr<Operation> GroupByImpl::cloneImpl() const {
  return std::make_unique<GroupByImpl>(_executionContext, _groupByVariables,
                                       _aliases, _subtree->clone());
}

// _____________________________________________________________________________
std::optional<IdTable> GroupByImpl::computeCountStar() const {
  bool isSingleGlobalAggregateFunction =
      _groupByVariables.empty() && _aliases.size() == 1;
  if (!isSingleGlobalAggregateFunction) {
    return std::nullopt;
  }
  // We can't optimize `COUNT(DISTINCT *)`.
  const bool singleAggregateIsNonDistinctCountStar = [&]() {
    auto* countStar =
        dynamic_cast<const sparqlExpression::CountStarExpression*>(
            _aliases[0]._expression.getPimpl());
    return countStar && !countStar->isDistinct();
  }();
  if (!singleAggregateIsNonDistinctCountStar) {
    return std::nullopt;
  }

  auto childRes = _subtree->getResult(true);
  // Compute the result as a single `size_t`.
  auto res = [&input = *childRes]() -> size_t {
    if (input.isFullyMaterialized()) {
      return input.idTable().size();
    } else {
      auto gen = input.idTables();
      auto sz = gen | ql::views::transform([](const auto& pair) {
                  return pair.idTable_.numRows();
                }) |
                ql::views::common;
      return std::accumulate(sz.begin(), sz.end(), size_t{0});
    }
  }();

  // Wrap the result in an IdTable with a single row and column.
  IdTable result{1, getExecutionContext()->getAllocator()};
  result.push_back(std::array{Id::makeFromInt(res)});
  return result;
}
