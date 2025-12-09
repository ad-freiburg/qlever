//  Copyright 2022, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#ifndef QLEVER_SRC_ENGINE_VARIABLETOCOLUMNMAP_H
#define QLEVER_SRC_ENGINE_VARIABLETOCOLUMNMAP_H

#include "backports/three_way_comparison.h"
#include "global/Id.h"
#include "rdfTypes/Variable.h"
#include "util/HashMap.h"

// TODO<joka921> We have a cyclic dependency between `Id.h` and
// `VariableToColumnMap.h`.
using ColumnIndex = uint64_t;

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

  // When explicitly cast to bool, then `true` means `possibly undefined`.
  static_assert(static_cast<bool>(AlwaysDefined) == false);
  static_assert(static_cast<bool>(PossiblyUndefined) == true);

  // The column index.
  ColumnIndex columnIndex_;

  // The information whether this column *might* contain UNDEF values.
  UndefStatus mightContainUndef_;

  // Equality comparison, mostly used for testing.
  QL_DEFINE_DEFAULTED_EQUALITY_OPERATOR_LOCAL(ColumnIndexAndTypeInfo,
                                              columnIndex_, mightContainUndef_)
  // Serialization for ColumnIndexAndTypeInfo.
  AD_SERIALIZE_FRIEND_FUNCTION(ColumnIndexAndTypeInfo) {
    serializer | arg.columnIndex_;
    serializer | arg.mightContainUndef_;
  }
};

// Return a `ColumnIndexAndType` info with the given `columnIndex` that is
// guaranteed to always be defined.
inline auto makeAlwaysDefinedColumn =
    [](ColumnIndex columnIndex) -> ColumnIndexAndTypeInfo {
  return {columnIndex, ColumnIndexAndTypeInfo::UndefStatus::AlwaysDefined};
};

// Return a `ColumnIndexAndType` info with the given `columnIndex` that might
// contain UNDEF values.
inline auto makePossiblyUndefinedColumn =
    [](ColumnIndex columnIndex) -> ColumnIndexAndTypeInfo {
  return {columnIndex, ColumnIndexAndTypeInfo::UndefStatus::PossiblyUndefined};
};

// A hash map from variables to the column index of that variable in a table,
// used in several places (e.g. the `Operation` class, the `SparqlExpression`
// module, etc.).

// Similar to `VariableToColumnMap` but stores additional information together
// with the column index.
using VariableToColumnMap =
    ad_utility::HashMap<Variable, ColumnIndexAndTypeInfo>;

// The same function, but works on `VariableToColumnMapWithTypeInfo`
std::vector<std::pair<Variable, ColumnIndexAndTypeInfo>>
copySortedByColumnIndex(VariableToColumnMap map);

// Compute the `VariableToColumnMapWithTypeInfo` for a binary JOIN operation.
// The order of the columns will be as follows: first the columns from the left
// operand (in the same order as in the input, including the join columns), then
// the columns of the right operand without the join columns. We additionally
// need the information whether the JOIN is optional, because then additional
// columns might contain undefined values. We also need the total width of the
// left input, because there might be columns that are not represented in the
// `VariableToColumnMap` (e.g. because they are not visible because of subquery
// scoping). If `keepJoinColumns` is `false`, then the join columns will not be
// included in the result, and all columns that would have a column index `>=`
// any of the join columns are shifted to the left accordingly.
enum class BinOpType { Join, OptionalJoin };
VariableToColumnMap makeVarToColMapForJoinOperation(
    const VariableToColumnMap& leftVars, const VariableToColumnMap& rightVars,
    std::vector<std::array<ColumnIndex, 2>> joinColumns, BinOpType binOpType,
    size_t leftResultWidth, bool keepJoinColumns = true);

#endif  // QLEVER_SRC_ENGINE_VARIABLETOCOLUMNMAP_H
