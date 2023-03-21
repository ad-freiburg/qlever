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

// ______________________________________________________________________________
VariableToColumnMapWithTypeInfo makeVarToColMapForJoinOperations(
    const VariableToColumnMapWithTypeInfo& leftVars,
    const VariableToColumnMapWithTypeInfo& rightVars,
    std::vector<std::array<ColumnIndex, 2>> joinColumns, BinOpType binOpType) {
  VariableToColumnMapWithTypeInfo retVal(leftVars);
  // TODO<joka921> Does this work with the `No column` stuff in the Union.cpp?
  bool isOptionalJoin = binOpType == BinOpType::OptionalJoin;
  bool isUnion = binOpType == BinOpType::Union;
  size_t columnIndex = retVal.size();
  const auto variableColumnsRightSorted = copySortedByColumnIndex(rightVars);
  for (const auto& it : variableColumnsRightSorted) {
    const auto& colIdxRight = it.second.columnIndex_;
    auto joinColumnIt =
        std::ranges::find(joinColumns, colIdxRight, ad_utility::second);

    if (joinColumnIt != joinColumns.end()) {
      auto& undef = retVal.at(it.first).mightContainUndef_;
      if (isUnion) {
        undef = undef || it.second.mightContainUndef_;
      } else {
        undef = undef && (isOptionalJoin || it.second.mightContainUndef_);
      }
    } else {
      retVal[it.first] =
          ColumnIndexAndTypeInfo{columnIndex, it.second.mightContainUndef_ ||
                                                  isOptionalJoin || isUnion};
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
