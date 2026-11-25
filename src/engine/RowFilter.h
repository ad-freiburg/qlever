// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Nino Frey, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_ENGINE_ROWFILTER_H
#define QLEVER_SRC_ENGINE_ROWFILTER_H

#include <cstddef>

#include "backports/span.h"
#include "engine/idTable/IdTable.h"
#include "global/Id.h"
#include "index/LocalVocab.h"

namespace ad_utility {

// Describes a row that an `AddCombinedRowToIdTable` has just written, see
// `RowFilter` below.
struct WrittenRow {
  // Together, `leftInput_` (the address of the first column of the left input)
  // and `leftRowIndex_` identify the left row of the written row.
  const Id* leftInput_;
  size_t leftRowIndex_;
  // False iff the row was added by `addOptionalRow`.
  bool hasPartner_;
};

// Changes the rows that an `AddCombinedRowToIdTable` writes, before they are
// passed to its `BlockwiseCallback`. This is used to apply the condition of an
// OPTIONAL join, see `OptionalJoinConditionFilter`.
class RowFilter {
 public:
  virtual ~RowFilter() = default;

  // The rows `[begin, result.size())` of `result` have just been written, the
  // `i`-th of them is described by `writtenRows[i]`. The filter may remove
  // some of these rows. `leftColumns` are the columns of the current left
  // input that are part of the result, they are the first columns of
  // `result`. Its row `leftRowIndex_` is what `addOptionalRow` writes to these
  // columns for that left row (unlike the written row, which takes the value
  // of a join column from the right if it is UNDEF on the left). The Ids of the
  // written rows and of the `leftColumns` are covered by `localVocab`.
  virtual void filterWrittenRows(IdTable& result, size_t begin,
                                 ql::span<const WrittenRow> writtenRows,
                                 const IdTableView<0>& leftColumns,
                                 const LocalVocab& localVocab) = 0;

  // The left rows of all rows written so far are finished, none of them gets
  // another partner. The filter may append rows to `result`, and then has to
  // add their local vocab to `localVocab`.
  virtual void finishPreviousLeftRows(IdTable& result,
                                      LocalVocab& localVocab) = 0;
};

}  // namespace ad_utility

#endif  // QLEVER_SRC_ENGINE_ROWFILTER_H
