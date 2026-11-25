// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Nino Frey, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_ENGINE_OPTIONALJOINCONDITIONFILTER_H
#define QLEVER_SRC_ENGINE_OPTIONALJOINCONDITIONFILTER_H

#include <limits>
#include <utility>
#include <vector>

#include "engine/RowFilter.h"
#include "engine/VariableToColumnMap.h"
#include "engine/sparqlExpressions/SparqlExpressionPimpl.h"
#include "util/AllocatorWithLimit.h"
#include "util/CancellationHandle.h"
#include "util/HashMap.h"

class QueryExecutionContext;

// Applies the condition of an `OptionalJoin` (the FILTERs of the OPTIONAL group
// that use variables from the left, see `isPartOfOptionalJoinCondition`) to the
// rows that an `AddCombinedRowToIdTable` writes. By the SPARQL standard, a pair
// of a left and a right row is only part of the result if the condition holds
// for the joined row. A left row without such a pair is part of the result
// once, with UNDEF in all columns from the right.
//
// The pairs for which the condition doesn't hold are removed when they are
// written. For the first such pair of a left row, the left row is stored as a
// candidate for its row with UNDEF on the right. The candidate becomes invalid
// as soon as the condition holds for another pair of the same left row. The
// valid candidates are written when the left rows are finished and get no
// more partners (`finishPreviousLeftRows`), at the latest when the result is
// moved out of the `AddCombinedRowToIdTable`. These rows are appended, so the
// result is no longer sorted. A left row is identified by its input and its
// index, see `WrittenRow`.
//
// The filter is deliberately more general than its only user so far, the
// materialized `optionalJoin`, needs, so that a lazy `OptionalJoin` with a
// condition can build on it. The materialized join finishes all left rows at
// the end, when the left input is still complete, so a flag per left row would
// suffice there. The candidates (with their own local vocab), the address of
// the left input in `WrittenRow`, and `finishPreviousLeftRows` are only needed
// for the lazy join, which finishes the left rows per block, when the block may
// already be gone.
class OptionalJoinConditionFilter : public ad_utility::RowFilter {
 public:
  using Condition = std::vector<sparqlExpression::SparqlExpressionPimpl>;

 private:
  const QueryExecutionContext& qec_;
  // All expressions have to be true, like several FILTERs. It must stay alive
  // as long as this object.
  const Condition& condition_;
  // The variables of the rows that the `AddCombinedRowToIdTable` writes, in
  // the order of its columns.
  VariableToColumnMap columns_;
  // The columns from this one on come from the right input.
  size_t firstRightColumn_;
  ad_utility::SharedCancellationHandle cancellationHandle_;
  ad_utility::AllocatorWithLimit<Id> allocator_;

  // For each left row that has a partner and is not finished: `MATCHED` if the
  // condition holds for one of its pairs, else the row of its candidate in
  // `candidates_`.
  static constexpr size_t MATCHED = std::numeric_limits<size_t>::max();
  ad_utility::HashMapWithMemoryLimit<std::pair<const Id*, size_t>, size_t>
      stateOfLeftRow_;
  // The candidates, only the columns from the left (the columns from the right
  // are UNDEF). A candidate is invalid if its left row has found a match after
  // the candidate was added.
  IdTable candidates_;
  std::vector<char> isCandidateValid_;
  // Covers the Ids of the `candidates_`.
  LocalVocab candidatesVocab_;
  // See `numRowsWithoutMatch()`.
  size_t numRowsWithoutMatch_ = 0;

 public:
  OptionalJoinConditionFilter(
      const QueryExecutionContext& qec, const Condition& condition,
      VariableToColumnMap columns, size_t firstRightColumn,
      ad_utility::SharedCancellationHandle cancellationHandle,
      ad_utility::AllocatorWithLimit<Id> allocator);

  void filterWrittenRows(IdTable& result, size_t begin,
                         ql::span<const ad_utility::WrittenRow> writtenRows,
                         const IdTableView<0>& leftColumns,
                         const LocalVocab& localVocab) override;
  void finishPreviousLeftRows(IdTable& result, LocalVocab& localVocab) override;

  // The number of rows that `finishPreviousLeftRows` has appended so far, that
  // is, of left rows that have partners, but none for which the condition
  // holds.
  size_t numRowsWithoutMatch() const { return numRowsWithoutMatch_; }

 private:
  // Return for each row in `[begin, end)` of `table` whether the condition
  // holds for it. Like for a `FILTER`, this is the case iff the effective
  // boolean value of all expressions is `true` (not `false` or an error).
  std::vector<char> evaluateCondition(const IdTable& table, size_t begin,
                                      size_t end) const;

  // Store row `row` of `leftColumns` as a new candidate.
  void addCandidate(const IdTableView<0>& leftColumns, size_t row);

  // Keep only the rows `begin + i` of `result` for which `isKept[i]` is true.
  // The order of the kept rows stays the same.
  void removeRowsThatAreNotKept(IdTable& result, size_t begin,
                                const std::vector<char>& isKept) const;

  // Append a row with UNDEF on the right to `result` for each valid candidate.
  void appendValidCandidates(IdTable& result, LocalVocab& localVocab);
};

#endif  // QLEVER_SRC_ENGINE_OPTIONALJOINCONDITIONFILTER_H
