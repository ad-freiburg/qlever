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

#include "engine/OptionalJoin.h"

namespace ad_utility {
class AddCombinedRowToIdTable;
}

// Applies the condition of an `OptionalJoin` to the join result while it is
// being written by an `AddCombinedRowToIdTable`: after each block of new rows,
// the rows for which the condition doesn't hold are removed. This way, the
// (possibly very large) join result without condition is never fully
// materialized. Usage: Construct the object before the first row is added, and
// call `finish` on the result of the join. The object has to stay alive as
// long as rows are added to the row adder.
class OptionalJoin::ConditionFilter {
 public:
  // Let the `rowAdder` report the left row of each result row and replace its
  // blockwise callback by one that applies the condition. `leftPermuted` is
  // the left input of the `rowAdder` (join columns first). All rows that the
  // join reports have to be in `[beginLeft, endLeft)`.
  ConditionFilter(const OptionalJoin& join, IdTableView<0> leftPermuted,
                  ad_utility::AddCombinedRowToIdTable& rowAdder,
                  size_t beginLeft = 0,
                  size_t endLeft = std::numeric_limits<size_t>::max());
  ConditionFilter(const ConditionFilter&) = delete;
  ConditionFilter& operator=(const ConditionFilter&) = delete;

  // Apply the condition to the rows that have not been handled yet and decide
  // for each left row without a match which row becomes its single row with
  // UNDEF on the right. Return true iff the result is no longer sorted by the
  // join columns afterwards. Must be called exactly once.
  bool finish(IdTable& result);

  // Like `finish`, but sort the result by the join columns if the order was
  // changed.
  void finishAndRestoreOrder(IdTable& result);

 private:
  // Apply the condition to the rows of `result` that were added since the
  // last call, but only if there are at least `minNumNewRows` of them.
  void processNewRows(IdTable& result, size_t minNumNewRows);

  // Return for each row in `[begin, end)` of `result` whether the condition
  // holds for it.
  std::vector<char> evaluateCondition(const IdTable& result, size_t begin,
                                      size_t end) const;

  // Remove each row `begin + i` of `result` with `keep[i] == 0`, keeping the
  // order of the other rows. `keep` covers all rows from `begin` on.
  void removeRows(IdTable& result, size_t begin,
                  const std::vector<char>& keep) const;

  // `HasCandidate`: No pair of the left row fulfills the condition so far,
  // the first of its pairs is kept and stored in `candidates_`.
  enum struct State : char { None, HasCandidate, HasMatch };

  const OptionalJoin& join_;
  IdTableView<0> leftPermuted_;
  size_t beginLeft_;
  VariableToColumnMap internalColumns_;
  std::vector<State> stateOfLeftRow_;
  std::vector<uint64_t> leftRowOfResultRow_;
  // The row index in the result and the left row of each candidate, sorted by
  // the row index.
  std::vector<std::pair<size_t, uint64_t>> candidates_;
  // The number of rows at the beginning of the result that have already been
  // handled by `processNewRows`.
  size_t numProcessedRows_ = 0;
};

#endif  // QLEVER_SRC_ENGINE_OPTIONALJOINCONDITIONFILTER_H
