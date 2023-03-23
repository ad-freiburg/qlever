//  Copyright 2022, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include "engine/VariableToColumnMap.h"

// _____________________________________________________________________________
std::vector<std::pair<Variable, ColumnIndexAndTypeInfo>>
copySortedByColumnIndex(VariableToColumnMapWithTypeInfo map) {
  std::vector<std::pair<Variable, ColumnIndexAndTypeInfo>> result{
      std::make_move_iterator(map.begin()), std::make_move_iterator(map.end())};
  std::ranges::sort(result, std::less<>{},
                    [](const auto& pair) { return pair.second.columnIndex_; });
  return result;
}

// _____________________________________________________________________________
std::vector<std::pair<Variable, size_t>> copySortedByColumnIndex(
    VariableToColumnMap map) {
  std::vector<std::pair<Variable, size_t>> result{
      std::make_move_iterator(map.begin()), std::make_move_iterator(map.end())};
  std::ranges::sort(result, std::less{}, ad_utility::second);
  return result;
}

// ___________________________________________________________________
VariableToColumnMap removeTypeInfo(
    const VariableToColumnMapWithTypeInfo& varColMap) {
  VariableToColumnMap result;
  for (const auto& [variable, colIdxAndTypeInfo] : varColMap) {
    result[variable] = colIdxAndTypeInfo.columnIndex_;
  }
  return result;
}

// ______________________________________________________________________________
VariableToColumnMapWithTypeInfo makeVarToColMapForJoinOperations(
    const VariableToColumnMapWithTypeInfo& leftVars,
    const VariableToColumnMapWithTypeInfo& rightVars,
    std::vector<std::array<ColumnIndex, 2>> joinColumns, BinOpType binOpType) {
  // First come all the variables from the left input. Variables that only
  // appear in the left input always have the same definedness as in the input.
  // For join columns we might override it below.
  VariableToColumnMapWithTypeInfo retVal{leftVars};
  bool isOptionalJoin = binOpType == BinOpType::OptionalJoin;
  size_t nextColumnIndex = retVal.size();

  // Add the variables from the right operand.
  for (const auto& [variable, columnIndexWithType] :
       copySortedByColumnIndex(rightVars)) {
    const auto& colIdxRight = columnIndexWithType.columnIndex_;
    // Figure out if the column (from the right operand) is a join column.
    auto joinColumnIt =
        std::ranges::find(joinColumns, colIdxRight, ad_utility::second);
    if (joinColumnIt != joinColumns.end()) {
      // For non-optional joins, a join column is always defined if it is always
      // defined in ANY of the inputs. For optional joins a join column is
      // always defined if it is always defined in the left input.
      auto& undef = retVal.at(variable).mightContainUndef_;
      undef = static_cast<ColumnIndexAndTypeInfo::UndefStatus>(
          static_cast<bool>(undef) &&
          (isOptionalJoin ||
           static_cast<bool>(columnIndexWithType.mightContainUndef_)));
    } else {
      // The column is not a join column. For non-optional joins it keeps its
      // definedness, but for optional joins, it might always be undefined if
      // there is a row in the left operand that has no match in the right
      // input.
      retVal[variable] = ColumnIndexAndTypeInfo{
          nextColumnIndex,
          static_cast<ColumnIndexAndTypeInfo::UndefStatus>(
              static_cast<bool>(columnIndexWithType.mightContainUndef_) ||
              isOptionalJoin)};
      nextColumnIndex++;
    }
  }
  return retVal;
}
