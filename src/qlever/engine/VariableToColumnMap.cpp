//  Copyright 2022, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include "./VariableToColumnMap.h"

// _____________________________________________________________________________
std::vector<std::pair<Variable, size_t>> copySortedByColumnIndex(
    VariableToColumnMap map) {
  std::vector<std::pair<Variable, size_t>> result{
      std::make_move_iterator(map.begin()), std::make_move_iterator(map.end())};
  std::ranges::sort(result, std::less<>{}, ad_utility::second);
  return result;
}
