//  Copyright 2022, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#pragma once

#include "engine/VariableToColumnMap.h"

// __________________________________________________________________
void PrintTo(const ColumnIndexAndTypeInfo& colIdx, std::ostream* os) {
  auto& s = *os;
  s << "col: " << colIdx.columnIndex_
    << " might be undef: " << static_cast<bool>(colIdx.mightContainUndef_);
}
