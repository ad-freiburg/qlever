//  Copyright 2022, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include "engine/VariableToColumnMap.h"

#include "util/TransparentFunctors.h"

// _____________________________________________________________________________
std::vector<std::pair<Variable, ColumnIndexAndTypeInfo>>
copySortedByColumnIndex(VariableToColumnMap map) {
  std::vector<std::pair<Variable, ColumnIndexAndTypeInfo>> result{
      std::make_move_iterator(map.begin()), std::make_move_iterator(map.end())};
  ql::ranges::sort(result, std::less<>{},
                   [](const auto& pair) { return pair.second.columnIndex_; });
  return result;
}

// ______________________________________________________________________________
VariableToColumnMap makeVarToColMapForJoinOperation(
    const VariableToColumnMap& leftVars, const VariableToColumnMap& rightVars,
    std::vector<std::array<ColumnIndex, 2>> joinColumns, BinOpType binOpType,
    size_t leftResultWidth, bool keepJoinColumns) {
  // First come all the variables from the left input. Variables that only
  // appear in the left input always have the same definedness as in the input.
  // For join columns we might override it below.
  auto result = [&]() {
    if (keepJoinColumns) {
      return leftVars;
    }
    // Don't include the join columns, shift all variables that appear after
    // join columns to the left s.t. the result will be dense again.
    leftResultWidth -= joinColumns.size();
    VariableToColumnMap res;
    const auto& jcls = joinColumns | ql::views::transform(ad_utility::first);
    // We deliberately copy the entries from the child map, as we have to insert
    // them into the result later on anyway.
    for (auto el : leftVars) {
      auto& [variable, columnIndexWithType] = el;
      auto colIdx = columnIndexWithType.columnIndex_;
      if (ad_utility::contains(jcls, colIdx)) {
        // Nothing to do for join columns, simply skip them.
        continue;
      }
      // Find the number of join columns that appear beofore this column.
      size_t shift = ql::ranges::count_if(
          jcls, [col = columnIndexWithType.columnIndex_](ColumnIndex jcl) {
            return jcl < col;
          });
      columnIndexWithType.columnIndex_ -= shift;
      res.insert(std::move(el));
    }
    return res;
  }();
  bool isOptionalJoin = binOpType == BinOpType::OptionalJoin;

  // Add the variables from the right operand.
  size_t numJoinColumnsBefore = 0;
  for (const auto& [variable, columnIndexWithType] :
       copySortedByColumnIndex(rightVars)) {
    const auto& colIdxRight = columnIndexWithType.columnIndex_;
    // Figure out if the column (from the right operand) is a join column.
    auto joinColumnIt =
        ql::ranges::find(joinColumns, colIdxRight, ad_utility::second);
    if (joinColumnIt != joinColumns.end()) {
      if (keepJoinColumns) {
        // For non-optional joins, a join column is `AlwaysDefined` if it is
        // always defined in ANY of the inputs. For optional joins a join column
        // is `AlwaysDefined` if it is always defined in the left input.
        auto& undef = result.at(variable).mightContainUndef_;
        undef = static_cast<ColumnIndexAndTypeInfo::UndefStatus>(
            static_cast<bool>(undef) &&
            (isOptionalJoin ||
             static_cast<bool>(columnIndexWithType.mightContainUndef_)));
      }
      ++numJoinColumnsBefore;
    } else {
      // The column is not a join column. For non-optional joins it keeps its
      // definedness, but for optional joins, it is `PossiblyUndefined` if
      // there is a row in the left operand that has no match in the right
      // input.
      result[variable] = ColumnIndexAndTypeInfo{
          leftResultWidth + columnIndexWithType.columnIndex_ -
              numJoinColumnsBefore,
          static_cast<ColumnIndexAndTypeInfo::UndefStatus>(
              static_cast<bool>(columnIndexWithType.mightContainUndef_) ||
              isOptionalJoin)};
    }
  }
  return result;
}
