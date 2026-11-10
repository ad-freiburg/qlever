// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Hannah Bast <bast@cs.uni-freiburg.de>, UFR
// 2026 Robin Textor-Falconi <textorr@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

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
// The internal order then deviates from the semantic order of `ORDER BY` only
// in the placement of a few contiguous runs (see `NumericRun`), so the result
// is obtained by copying these runs in the right order and direction, in
// linear time and without comparisons. With a `LIMIT`, only the needed rows
// are copied, and an `IndexScan` below only needs to read the blocks that can
// contain them.
//
// NOTE: This can be extended to other datatypes whose internal order deviates
// from the semantic order in a known way (in particular `VocabIndex`es), and
// to inputs with several datatypes (the datatypes are contiguous in the
// internal order, so this only requires ordering the datatypes, as long as
// `Int`s and `Double`s, which are compared by value, are not mixed).
namespace orderByOnSortedNumericInput {

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
//
// Only the blocks that lie within a single run can be skipped. A block that
// spans several runs (or sticks out of the scan) is always read completely; its
// rows are split into runs only afterwards, by `computeOrderBy` on the result
// of the scan, which works on rows and does not care about blocks.
std::optional<std::vector<size_t>> selectBlocksForLimit(
    const IndexScan::BlocksOfSortedVariable& blocks, size_t numRows,
    bool isDescending);

}  // namespace orderByOnSortedNumericInput

#endif  // QLEVER_SRC_ENGINE_ORDERBYONSORTEDNUMERICINPUT_H
