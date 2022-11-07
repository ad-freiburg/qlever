//  Copyright 2022, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include "./VariableToColumnMap.h"

std::vector<std::pair<Variable, size_t>> sortedByColumnIndex(
    VariableToColumnMap map) {
  std::vector<std::pair<Variable, size_t>> result{
      std::make_move_iterator(map.begin()), std::make_move_iterator(map.end())};
  std::ranges::sort(result, {}, ad_utility::second);
  return result;
}
