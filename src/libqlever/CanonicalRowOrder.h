// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_LIBQLEVER_CANONICALROWORDER_H
#define QLEVER_SRC_LIBQLEVER_CANONICALROWORDER_H

#include <cstddef>
#include <limits>
#include <vector>

#include "backports/span.h"
#include "engine/idTable/IdTable.h"
#include "global/Id.h"
#include "util/AllocatorWithLimit.h"

// The canonical row order of the tables that are written to a blob (see
// `NamedCachedQueryBlobManager::serialize`), and the helpers to establish and
// to exploit it. A table is in canonical order if its rows are sorted
// lexicographically by the columns `resultSortedOn` (in that order) first, and
// then by all remaining columns in increasing order of their column index. The
// `Id`s are compared via `ValueId::compareThreeWay`, and ties are broken by
// their raw bits (`Id::getBits()`). The tie-break is necessary because
// `compareThreeWay` can consider `Id`s equal that differ bitwise (two `Id`s of
// type `LocalVocabIndex` whose entries hold the same word).
// With it, two rows are equal in the canonical order iff they are bitwise
// identical, so the canonical order only depends on the multiset of rows, and
// not on the order in which a query plan happened to produce them. Rows that
// are equal keep their relative order when a table is brought into canonical
// order. Two tables in canonical order differ only by row insertions and
// deletions iff their contents differ only by these, which is what makes the
// diff of two blobs small (see `alignRows`).
namespace qlever::canonicalRowOrder {

// The value in the result of `alignRows` for a row without a counterpart.
inline constexpr size_t noMatchingRow = std::numeric_limits<size_t>::max();

// The columns of a table, which all have the same number of rows. The overloads
// of `isInCanonicalOrder` and `alignRows` for this type also work on tables
// that are not `IdTable`s, for example raw column data in a serialized blob.
using IdColumns = ql::span<const ConstIdColumnRef>;

// Return the permutation `oldRowOfNewRow` that brings the rows of `table` into
// canonical order, with respect to the `resultSortedOn` columns: the row at
// position `i` of the sorted table is the row `result[i]` of `table`. The
// result is the identity if `table` already is in canonical order.
std::vector<size_t> canonicalSortingPermutation(
    const IdTableView<0>& table, ql::span<const ColumnIndex> resultSortedOn);

// Return true iff `table` is in canonical order, with respect to the
// `resultSortedOn` columns. Return false (instead of failing) if
// `resultSortedOn` contains a column that does not exist.
bool isInCanonicalOrder(const IdTableView<0>& table,
                        ql::span<const ColumnIndex> resultSortedOn);

// Same as above, but for a table that is given by its `columns`.
bool isInCanonicalOrder(IdColumns columns,
                        ql::span<const ColumnIndex> resultSortedOn);

// Return a copy of `table` (allocated via `allocator`) whose row `i` is the
// row `oldRowOfNewRow[i]` of `table`. `oldRowOfNewRow` has to contain one
// valid row index per row of the result, which is not necessarily a
// permutation of all rows.
IdTable permuteRows(const IdTableView<0>& table,
                    ql::span<const size_t> oldRowOfNewRow,
                    const ad_utility::AllocatorWithLimit<Id>& allocator);

// Return the inverse of the permutation `permutation`, which has to contain
// each of the numbers `0, ..., permutation.size() - 1` exactly once. In
// particular, for the permutation `oldRowOfNewRow`, this is `newRowOfOldRow`.
std::vector<size_t> invertPermutation(ql::span<const size_t> permutation);

// Match the rows of the `target` table to the rows of the `base` table. Both
// tables have to be in canonical order with respect to the same
// `resultSortedOn` columns, and have to have the same number of columns
// (checked via `AD_CONTRACT_CHECK`; the order is not checked, see
// `isInCanonicalOrder`). Return a vector that contains for each row of `target`
// the row of `base` that is equal to it (bitwise identical in all columns, see
// above), or `noMatchingRow` if there is none.
// The matching is monotonic (the matched rows of `base` are increasing), and
// each row of `base` is matched at most once. If a row occurs several times,
// then the occurrences in `target` and `base` are matched pairwise in their
// order. The cost is linear in the sizes of the tables.
//
// NOTE: The `resultSortedOn` columns are needed because they determine in which
// order the columns are compared, and hence how the merge of the tables works.
std::vector<size_t> alignRows(const IdTableView<0>& base,
                              const IdTableView<0>& target,
                              ql::span<const ColumnIndex> resultSortedOn = {});

// Same as above, but for tables that are given by their columns (which have
// to have the same number of rows each).
std::vector<size_t> alignRows(IdColumns base, IdColumns target,
                              ql::span<const ColumnIndex> resultSortedOn = {});

}  // namespace qlever::canonicalRowOrder

#endif  // QLEVER_SRC_LIBQLEVER_CANONICALROWORDER_H
