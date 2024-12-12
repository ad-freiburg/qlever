//  Copyright 2024, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
// Author: Christoph Ullinger <ullingec@informatik.uni-freiburg.de>

#pragma once
#include "./VariablePrinters.h"
#include "parser/PayloadVariables.h"

// _____________________________________________________________
inline void PrintTo(const PayloadVariables& pv, std::ostream* os) {
  auto& s = *os;
  if (pv.isAll()) {
    s << std::string{"ALL"};
  } else {
    for (const auto& v : pv.getVariables()) {
      PrintTo(v, os);
      s << std::string{" "};
    }
  }
}
