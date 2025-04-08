//  Copyright 2022, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#ifndef QLEVER_TEST_PRINTERS_VARIABLEPRINTERS_H
#define QLEVER_TEST_PRINTERS_VARIABLEPRINTERS_H

#include "parser/data/Variable.h"

// _____________________________________________________________
inline void PrintTo(const Variable& var, std::ostream* os) {
  auto& s = *os;
  s << var.name();
}

#endif  // QLEVER_TEST_PRINTERS_VARIABLEPRINTERS_H
