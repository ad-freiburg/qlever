//  Copyright 2022, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#pragma once
#include "parser/data/Variable.h"

// _____________________________________________________________
inline void PrintTo(const Variable& var, std::ostream* os) {
  auto& s = *os;
  s << var.name();
}
