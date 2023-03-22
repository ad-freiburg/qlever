//  Copyright 2022, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include "./VariableToColumnMap.h"

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

// ______________________________________________________________________________
VariableToColumnMapWithTypeInfo makeVarToColMapForJoinOperations(
    const VariableToColumnMapWithTypeInfo& leftVars,
    const VariableToColumnMapWithTypeInfo& rightVars,
    std::vector<std::array<ColumnIndex, 2>> joinColumns, BinOpType binOpType) {
  VariableToColumnMapWithTypeInfo retVal(leftVars);
  bool isOptionalJoin = binOpType == BinOpType::OptionalJoin;
  size_t columnIndex = retVal.size();
  for (const auto& [variable, columnIndexWithType] :
       copySortedByColumnIndex(rightVars)) {
    const auto& colIdxRight = columnIndexWithType.columnIndex_;
    auto joinColumnIt =
        std::ranges::find(joinColumns, colIdxRight, ad_utility::second);

    if (joinColumnIt != joinColumns.end()) {
      auto& undef = retVal.at(variable).mightContainUndef_;
      undef = static_cast<ColumnIndexAndTypeInfo::UndefStatus>(
          static_cast<bool>(undef) &&
          (isOptionalJoin ||
           static_cast<bool>(columnIndexWithType.mightContainUndef_)));
    } else {
      retVal[variable] = ColumnIndexAndTypeInfo{
          columnIndex,
          static_cast<ColumnIndexAndTypeInfo::UndefStatus>(
              static_cast<bool>(columnIndexWithType.mightContainUndef_) ||
              isOptionalJoin)};
      columnIndex++;
    }
  }
  return retVal;
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
