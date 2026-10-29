// Copyright 2026, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Robin Textor-Falconi (textorr@cs.uni-freiburg.de)

#ifndef QLEVER_SRC_ENGINE_ORDERBYONSORTEDNUMERICINPUT_H
#define QLEVER_SRC_ENGINE_ORDERBYONSORTEDNUMERICINPUT_H

#include <optional>
#include <vector>

#include "engine/IndexScan.h"
#include "engine/idTable/IdTable.h"
#include "parser/data/LimitOffsetClause.h"
#include "util/AllocatorWithLimit.h"

// The fast path of `ORDER BY` for a single sort column, when the input is
// already sorted by that column in the internal order of the `Id`s (that is,
// by their bits), and the column contains only `Int`s or only `Double`s,
// possibly preceded by `Undefined` values.
//
// The internal order then deviates from the order of `ORDER BY` only in the
// placement of a few contiguous "runs" (see `NumericRun`), so the result is
// obtained by copying these runs in the right order and direction, in linear
// time and without comparisons. With a `LIMIT`, only the needed rows are
// copied, and an `IndexScan` below only needs to read the blocks that can
// contain them.
namespace orderByOnSortedNumericInput {

// The parts into which the internal order splits the `Int`s and the `Double`s
// (see `valueIdComparators::compareByBits`). The enumerators are in the
// internal order, and each run is sorted by value in the internal order,
// ascending except for `DoubleNegative`, which is sorted by descending value
// (the magnitude increases with the bits). `ORDER BY` puts all `NaN`s after
// all other doubles (see `OrderByComparator`). The sign bit of a `NaN` may be
// set, so there are two runs of `NaN`s.
enum class NumericRun {
  IntNonNegative,     // [0 .. max]
  IntNegative,        // [min .. -1]
  DoubleNonNegative,  // [0.0 .. +inf]
  DoublePositiveNan,
  DoubleNegative,  // [-0.0 .. -inf]
  DoubleNegativeNan,
};

// Return the run of the `id`, or `std::nullopt` if it is neither an `Int` nor
// a `Double`.
std::optional<NumericRun> getRunIfNumeric(Id id);

// If the `sortColumn` of the `input`, which must be sorted by that column in
// the internal order, qualifies for the fast path (see above), return the rows
// selected by the `limitOffset` in the order of `ORDER BY` on that column.
// Otherwise return `std::nullopt`.
std::optional<IdTable> computeOrderBy(
    const IdTableView<0>& input, ColumnIndex sortColumn, bool isDescending,
    const LimitOffsetClause& limitOffset,
    const ad_utility::AllocatorWithLimit<Id>& allocator);

// Return the indices (sorted) of the blocks of an `IndexScan` that contain the
// first `numRows` rows of an `ORDER BY` on the sorted variable of the scan,
// based on the metadata of the `blocks` alone. Return `std::nullopt` if all
// blocks are needed, or if the metadata does not guarantee that
// `computeOrderBy` takes the fast path on the result of the scan.
std::optional<std::vector<size_t>> selectBlocksForLimit(
    const IndexScan::BlocksOfSortedVariable& blocks, size_t numRows,
    bool isDescending);

}  // namespace orderByOnSortedNumericInput

#endif  // QLEVER_SRC_ENGINE_ORDERBYONSORTEDNUMERICINPUT_H
