// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Nino Frey, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "engine/OptionalJoinConditionFilter.h"

#include "engine/QueryExecutionContext.h"
#include "engine/sparqlExpressions/SparqlExpression.h"
#include "engine/sparqlExpressions/SparqlExpressionGenerators.h"
#include "engine/sparqlExpressions/SparqlExpressionValueGetters.h"

// _____________________________________________________________________________
OptionalJoinConditionFilter::OptionalJoinConditionFilter(
    const QueryExecutionContext& qec, const Condition& condition,
    VariableToColumnMap columns, size_t firstRightColumn,
    ad_utility::SharedCancellationHandle cancellationHandle,
    ad_utility::AllocatorWithLimit<Id> allocator)
    : qec_{qec},
      condition_{condition},
      columns_{std::move(columns)},
      firstRightColumn_{firstRightColumn},
      cancellationHandle_{std::move(cancellationHandle)},
      allocator_{std::move(allocator)},
      stateOfLeftRow_{allocator_},
      candidates_{firstRightColumn_, allocator_},
      candidatesVocab_{allocator_} {
  AD_CONTRACT_CHECK(!condition_.empty());
}

// _____________________________________________________________________________
std::vector<char> OptionalJoinConditionFilter::evaluateCondition(
    const IdTable& table, size_t begin, size_t end) const {
  std::vector<char> conditionHolds(end - begin, 1);
  // Strings that are created while the expressions are evaluated are not part
  // of the result.
  LocalVocab localVocab{allocator_};
  sparqlExpression::EvaluationContext context{
      qec_,       columns_,   table.asStaticView<0>(),
      allocator_, localVocab, cancellationHandle_};
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
  for (const auto& expression : condition_) {
    std::visit(markRowsWhereNotTrue, expression.getPimpl()->evaluate(&context));
    cancellationHandle_->throwIfCancelled();
  }
  return conditionHolds;
}

// _____________________________________________________________________________
void OptionalJoinConditionFilter::addCandidate(
    const IdTableView<0>& leftColumns, size_t row) {
  const size_t newRow = candidates_.size();
  candidates_.emplace_back();
  for (size_t col = 0; col < firstRightColumn_; ++col) {
    candidates_(newRow, col) = leftColumns(row, col);
  }
  isCandidateValid_.push_back(1);
}

// _____________________________________________________________________________
void OptionalJoinConditionFilter::removeRowsThatAreNotKept(
    IdTable& result, size_t begin, const std::vector<char>& isKept) const {
  const size_t numKept = ql::ranges::count(isKept, 1);
  // Nothing to remove.
  if (numKept == isKept.size()) {
    return;
  }
  // Per column, move each kept row forward to the next free position.
  for (size_t col = 0; col < result.numColumns(); ++col) {
    decltype(auto) column = result.getColumn(col);
    size_t target = begin;
    for (size_t i = 0; i < isKept.size(); ++i) {
      // Compact the kept rows.
      if (isKept[i]) {
        column[target] = column[begin + i];
        ++target;
      }
    }
    cancellationHandle_->throwIfCancelled();
  }
  // Cut off the rows that are left over at the end.
  result.resize(begin + numKept);
}

// _____________________________________________________________________________
void OptionalJoinConditionFilter::filterWrittenRows(
    IdTable& result, size_t begin,
    ql::span<const ad_utility::WrittenRow> writtenRows,
    const IdTableView<0>& leftColumns, const LocalVocab& localVocab) {
  AD_CORRECTNESS_CHECK(result.size() == begin + writtenRows.size());
  AD_CORRECTNESS_CHECK(leftColumns.numColumns() == firstRightColumn_ &&
                       firstRightColumn_ <= result.numColumns());
  // At first, `isKept[i]` says whether the condition holds for row `begin + i`.
  auto isKept = evaluateCondition(result, begin, result.size());
  const size_t numCandidatesBefore = candidates_.size();
  for (size_t i = 0; i < writtenRows.size(); ++i) {
    const auto& row = writtenRows[i];
    const std::pair leftRow{row.leftInput_, row.leftRowIndex_};
    if (!row.hasPartner_) {
      // A left row without a partner stays, with UNDEF on the right, whatever
      // the condition says.
      isKept[i] = 1;
    } else if (isKept[i]) {
      // The condition holds for this pair: mark its left row as matched.
      auto& state = stateOfLeftRow_.try_emplace(leftRow, MATCHED).first->second;
      // A new entry is already `MATCHED`. Otherwise, the state is the candidate
      // that an earlier pair of the left row has added, which is now invalid.
      if (state != MATCHED) {
        isCandidateValid_.at(state) = 0;
        state = MATCHED;
      }
    } else if (stateOfLeftRow_.try_emplace(leftRow, candidates_.size())
                   .second) {
      // The condition doesn't hold. Only the first pair of a left row adds a
      // candidate, and only if no pair of it has matched before.
      addCandidate(leftColumns, row.leftRowIndex_);
    }
  }
  if (candidates_.size() > numCandidatesBefore) {
    candidatesVocab_.mergeWith(localVocab);
  }
  removeRowsThatAreNotKept(result, begin, isKept);
}

// _____________________________________________________________________________
void OptionalJoinConditionFilter::appendValidCandidates(
    IdTable& result, LocalVocab& localVocab) {
  std::vector<size_t> validRows;
  for (size_t row = 0; row < isCandidateValid_.size(); ++row) {
    if (isCandidateValid_[row]) {
      validRows.push_back(row);
    }
  }
  if (validRows.empty()) {
    return;
  }
  const size_t oldSize = result.size();
  result.resize(oldSize + validRows.size());
  // The columns from the left are copied from the valid candidates.
  for (size_t col = 0; col < firstRightColumn_; ++col) {
    auto target = result.getColumn(col).subspan(oldSize);
    decltype(auto) candidates = candidates_.getColumn(col);
    for (size_t i = 0; i < validRows.size(); ++i) {
      target[i] = candidates[validRows[i]];
    }
  }
  // The candidates are UNDEF in the columns from the right.
  for (size_t col = firstRightColumn_; col < result.numColumns(); ++col) {
    ql::ranges::fill(result.getColumn(col).subspan(oldSize),
                     Id::makeUndefined());
  }
  localVocab.mergeWith(candidatesVocab_);
  numRowsWithoutMatch_ += validRows.size();
}

// _____________________________________________________________________________
void OptionalJoinConditionFilter::finishPreviousLeftRows(
    IdTable& result, LocalVocab& localVocab) {
  appendValidCandidates(result, localVocab);
  // The finished left rows get no more partners, so their state is no longer
  // needed.
  stateOfLeftRow_.clear();
  candidates_.clear();
  isCandidateValid_.clear();
  candidatesVocab_ = LocalVocab{allocator_};
}
