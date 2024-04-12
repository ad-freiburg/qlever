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
  std::ranges::sort(result, std::less<>{},
                    [](const auto& pair) { return pair.second.columnIndex_; });
  return result;
}

// ______________________________________________________________________________
VariableToColumnMap makeVarToColMapForJoinOperation(
    const VariableToColumnMap& leftVars, const VariableToColumnMap& rightVars,
    std::vector<std::array<ColumnIndex, 2>> joinColumns, BinOpType binOpType,
    size_t leftResultWidth) {
  // First come all the variables from the left input. Variables that only
  // appear in the left input always have the same definedness as in the input.
  // For join columns we might override it below.
  VariableToColumnMap result{leftVars};
  bool isOptionalJoin = binOpType == BinOpType::OptionalJoin;

  // Add the variables from the right operand.
  size_t numJoinColumnsBefore = 0;
  for (const auto& [variable, columnIndexWithType] :
       copySortedByColumnIndex(rightVars)) {
    const auto& colIdxRight = columnIndexWithType.columnIndex_;
    // Figure out if the column (from the right operand) is a join column.
    auto joinColumnIt =
        std::ranges::find(joinColumns, colIdxRight, ad_utility::second);
    if (joinColumnIt != joinColumns.end()) {
      // For non-optional joins, a join column is `AlwaysDefined` if it is
      // always defined in ANY of the inputs. For optional joins a join column
      // is `AlwaysDefined` if it is always defined in the left input.
      auto& undef = result.at(variable).mightContainUndef_;
      undef = static_cast<ColumnIndexAndTypeInfo::UndefStatus>(
          static_cast<bool>(undef) &&
          (isOptionalJoin ||
           static_cast<bool>(columnIndexWithType.mightContainUndef_)));
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
