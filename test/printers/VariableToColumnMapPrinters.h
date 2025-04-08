//  Copyright 2022, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#ifndef QLEVER_TEST_PRINTERS_VARIABLETOCOLUMNMAPPRINTERS_H
#define QLEVER_TEST_PRINTERS_VARIABLETOCOLUMNMAPPRINTERS_H

#include "engine/VariableToColumnMap.h"

// __________________________________________________________________
inline void PrintTo(const ColumnIndexAndTypeInfo& colIdx, std::ostream* os) {
  auto& s = *os;
  s << "col: " << colIdx.columnIndex_
    << " might be undef: " << static_cast<bool>(colIdx.mightContainUndef_);
}

#endif  // QLEVER_TEST_PRINTERS_VARIABLETOCOLUMNMAPPRINTERS_H
