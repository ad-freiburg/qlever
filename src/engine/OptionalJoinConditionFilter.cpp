// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Nino Frey, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "engine/OptionalJoinConditionFilter.h"

#include <numeric>

#include "engine/AddCombinedRowToTable.h"
#include "engine/sparqlExpressions/SparqlExpression.h"
#include "engine/sparqlExpressions/SparqlExpressionGenerators.h"
#include "engine/sparqlExpressions/SparqlExpressionValueGetters.h"
#include "index/IdTableUtils.h"

// _____________________________________________________________________________
OptionalJoin::ConditionFilter::ConditionFilter(
    const OptionalJoin& join, IdTableView<0> leftPermuted,
    ad_utility::AddCombinedRowToIdTable& rowAdder, size_t beginLeft,
    size_t endLeft)
    : join_{join},
      leftPermuted_{std::move(leftPermuted)},
      beginLeft_{beginLeft},
      internalColumns_{join.getInternalVariableColumns()} {
  AD_CONTRACT_CHECK(!join.condition_.empty());
  endLeft = std::min(endLeft, leftPermuted_.size());
  AD_CONTRACT_CHECK(beginLeft <= endLeft);
  stateOfLeftRow_.resize(endLeft - beginLeft, State::None);
  // The join algorithms on blocks flush the row adder very often, so wait
  // until enough rows have been written.
  rowAdder.trackLeftRowIndices(
      &leftRowOfResultRow_, [this](IdTable& result, LocalVocab&) {
        processNewRows(result, join_.conditionChunkSize_);
      });
}

// _____________________________________________________________________________
std::vector<char> OptionalJoin::ConditionFilter::evaluateCondition(
    const IdTable& result, size_t begin, size_t end) const {
  // Like for a `FILTER`, only rows with an effective boolean value of `true`
  // satisfy the condition (`false` and errors do not).
  std::vector<char> conditionHolds(end - begin, 1);
  LocalVocab localVocab;
  sparqlExpression::EvaluationContext context{
      *join_.getExecutionContext(),
      internalColumns_,
      result.asStaticView<0>(),
      join_.getExecutionContext()->getAllocator(),
      localVocab,
      join_.cancellationHandle()};
  context._beginIndex = begin;
  context._endIndex = end;
  using ValueGetter = sparqlExpression::detail::EffectiveBooleanValueGetter;
  auto markRowsWhereNotTrue = [&](auto&& expressionResult) {
    auto values = sparqlExpression::detail::makeGenerator(
        AD_FWD(expressionResult), context.size(), &context);
    size_t i = 0;
    for (auto&& value : values) {
      if (ValueGetter{}(value, &context) != ValueGetter::Result::True) {
        conditionHolds[i] = 0;
      }
      ++i;
    }
  };
  for (const auto& expression : join_.condition_) {
    std::visit(markRowsWhereNotTrue, expression.getPimpl()->evaluate(&context));
    join_.checkCancellation();
  }
  return conditionHolds;
}

// _____________________________________________________________________________
void OptionalJoin::ConditionFilter::removeRows(
    IdTable& result, size_t begin, const std::vector<char>& keep) const {
  AD_CORRECTNESS_CHECK(begin + keep.size() == result.size());
  const size_t numKept = begin + ql::ranges::count(keep, char{1});
  if (numKept == result.size()) {
    return;
  }
  for (size_t col = 0; col < result.numColumns(); ++col) {
    decltype(auto) column = result.getColumn(col);
    size_t target = begin;
    for (size_t i = 0; i < keep.size(); ++i) {
      if (keep[i]) {
        column[target++] = column[begin + i];
      }
    }
    join_.checkCancellation();
  }
  result.resize(numKept);
}

// _____________________________________________________________________________
void OptionalJoin::ConditionFilter::processNewRows(IdTable& result,
                                                   size_t minNumNewRows) {
  const size_t numRows = result.size();
  const size_t begin = numProcessedRows_;
  AD_CORRECTNESS_CHECK(leftRowOfResultRow_.size() == numRows &&
                       begin <= numRows);
  const size_t numNewRows = numRows - begin;
  if (numNewRows == 0 || numNewRows < minNumNewRows) {
    return;
  }
  // The rows that were added by `addOptionalRow` have no partner on the right,
  // so the condition doesn't apply to them, they are simply kept.
  auto isOptionalRow = [](uint64_t leftRow) {
    return (leftRow & ad_utility::AddCombinedRowToIdTable::OPTIONAL_ROW_FLAG) !=
           0;
  };
  const auto conditionHolds = evaluateCondition(result, begin, numRows);

  // Mark the left rows that have a partner among the new rows for which the
  // condition holds. This is done first, so that no candidate is stored for a
  // left row that has such a partner later in the same chunk.
  for (size_t i = 0; i < numNewRows; ++i) {
    uint64_t leftRow = leftRowOfResultRow_[begin + i];
    if (conditionHolds[i] && !isOptionalRow(leftRow)) {
      stateOfLeftRow_.at(leftRow - beginLeft_) = State::HasMatch;
    }
  }

  // A pair for which the condition does not hold is dropped, except for the
  // very first pair of a left row that has no match so far. That pair is the
  // candidate for the single row with UNDEF on the right, see `finish`.
  std::vector<char> keep(numNewRows);
  size_t numKept = begin;
  for (size_t i = 0; i < numNewRows; ++i) {
    uint64_t leftRow = leftRowOfResultRow_[begin + i];
    keep[i] = isOptionalRow(leftRow) || conditionHolds[i];
    if (!keep[i]) {
      auto& state = stateOfLeftRow_[leftRow - beginLeft_];
      if (state == State::None) {
        state = State::HasCandidate;
        candidates_.emplace_back(numKept, leftRow);
        keep[i] = 1;
      }
    }
    if (keep[i]) {
      leftRowOfResultRow_[numKept++] = leftRow;
    }
  }
  removeRows(result, begin, keep);
  leftRowOfResultRow_.resize(numKept);
  numProcessedRows_ = numKept;
  // Every left row keeps at least one row. This is important, because
  // `AddCombinedRowToIdTable` resets its local vocab if the callback leaves
  // the result empty.
  AD_CORRECTNESS_CHECK(!result.empty());
}

// _____________________________________________________________________________
bool OptionalJoin::ConditionFilter::finish(IdTable& result) {
  processNewRows(result, 0);

  // Each candidate is the first pair of its left row. If the left row found a
  // match in a later chunk, the candidate is removed. Otherwise it becomes the
  // row with UNDEF on the right. The columns of the result are [join columns,
  // non-join columns of the left, non-join columns of the right], see
  // `AddCombinedRowToIdTable`.
  const size_t numJoinColumns = join_._joinColumns.size();
  const size_t numKeptJoinColumns = join_.keepJoinColumns_ ? numJoinColumns : 0;
  const size_t firstRightColumn =
      numKeptJoinColumns + leftPermuted_.numColumns() - numJoinColumns;
  bool orderChanged = false;
  std::vector<char> keep(result.size(), 1);
  for (auto [i, leftRow] : candidates_) {
    if (stateOfLeftRow_[leftRow - beginLeft_] == State::HasMatch) {
      keep[i] = 0;
      continue;
    }
    // The value of a join column only changes if it is UNDEF in the left row,
    // then the order of the result might change.
    for (size_t col = 0; col < numKeptJoinColumns; ++col) {
      Id leftId = leftPermuted_(leftRow, col);
      if (result(i, col) != leftId) {
        result(i, col) = leftId;
        orderChanged = true;
      }
    }
    for (size_t col = firstRightColumn; col < result.numColumns(); ++col) {
      result(i, col) = Id::makeUndefined();
    }
  }
  join_.checkCancellation();
  removeRows(result, 0, keep);
  return orderChanged;
}

// _____________________________________________________________________________
void OptionalJoin::ConditionFilter::finishAndRestoreOrder(IdTable& result) {
  if (finish(result) && join_.keepJoinColumns_) {
    std::vector<ColumnIndex> cols(join_._joinColumns.size());
    std::iota(cols.begin(), cols.end(), ColumnIndex{0});
    IdTableUtils::sort(result, cols, join_.sortConfig());
  }
}
