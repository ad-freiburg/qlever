//  Copyright 2022, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#pragma once

#include "global/Id.h"
#include "parser/data/Variable.h"
#include "util/HashMap.h"
#include "util/TransparentFunctors.h"

// Store an index of a column together with additional information about that
// column which can be inferred from the `QueryExecutionTree` without actually
// computing the result.
struct ColumnIndexAndTypeInfo {
  // A strong enum for the status of a column. For some columns we know that
  // they will always be defined, while others might contain UNDEF values when
  // computing the result.
  enum struct UndefStatus { AlwaysDefined, PossiblyUndefined };
  // In GCC there is a bug in the `using enum` feature, so we manually export
  // the values
  static constexpr auto AlwaysDefined = UndefStatus::AlwaysDefined;
  static constexpr auto PossiblyUndefined = UndefStatus::PossiblyUndefined;

  // When explicitly cast to bool, then `true` means "This column might contain
  // undefined values".
  static_assert(static_cast<bool>(AlwaysDefined) == false);
  static_assert(static_cast<bool>(PossiblyUndefined) == true);

  // The column index.
  size_t columnIndex_;

  // The information whether this column *might* contain UNDEF values. The
  // naming of this member is consistent with casting to bool, so
  // `static_cast<bool>(mightContainUndef) == true` means that the column might
  // be undefined.
  UndefStatus mightContainUndef_;

  // Equality comparison, mostly used for testing.
  bool operator==(const ColumnIndexAndTypeInfo&) const = default;
};

// Return a `ColumnIndexAndType` info with the given `columnIndex` that is
// guaranteed to always be defined.
inline auto makeDefinedColumn =
    [](size_t columnIndex) -> ColumnIndexAndTypeInfo {
  return {columnIndex, ColumnIndexAndTypeInfo::UndefStatus::AlwaysDefined};
};

// Return a `ColumnIndexAndType` info with the given `columnIndex` that might
// contain UNDEF values.
inline auto makeUndefinedColumn =
    [](size_t columnIndex) -> ColumnIndexAndTypeInfo {
  return {columnIndex, ColumnIndexAndTypeInfo::UndefStatus::PossiblyUndefined};
};

// A hash map from variables to the column index of that variable in a table,
// used in several places (e.g. the `Operation` class, the `SparqlExpression`
// module, etc.).
using VariableToColumnMap = ad_utility::HashMap<Variable, size_t>;

// Similar to `VariableToColumnMap` but stores additional information together
// with the column index.
using VariableToColumnMapWithTypeInfo =
    ad_utility::HashMap<Variable, ColumnIndexAndTypeInfo>;

// Convert a `VariableToColumnMapWithTypeInfo` to a `VariableToColumnMap` by
// just storing the column indices without the additional information.
VariableToColumnMap removeTypeInfo(
    const VariableToColumnMapWithTypeInfo& varColMap);

// Return a vector that contains the contents of the `VariableToColumnMap` in
// ascending order of the column indices.
std::vector<std::pair<Variable, size_t>> copySortedByColumnIndex(
    VariableToColumnMap map);

// The same function, but works on `VariableToColumnMapWithTypeInfo`
std::vector<std::pair<Variable, ColumnIndexAndTypeInfo>>
copySortedByColumnIndex(VariableToColumnMapWithTypeInfo map);

// Compute the `VariableToColumnMapWithTypeInfo` for a binary JOIN operation.
// The order of the columns will be as follows: first the columns from the left
// operand (in the same order as in the input, including the join columns), then
// the columns of the right operand without the join columns. We additionally
// need the information whether the JOIN is optional, because then additional
// columns  might contain undefined values.
enum class BinOpType { Join, OptionalJoin };
VariableToColumnMapWithTypeInfo makeVarToColMapForJoinOperations(
    const VariableToColumnMapWithTypeInfo& leftVars,
    const VariableToColumnMapWithTypeInfo& rightVars,
    std::vector<std::array<ColumnIndex, 2>> joinColumns, BinOpType);
