//  Copyright 2025, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
// Author: Christoph Ullinger <ullingec@informatik.uni-freiburg.de>

#ifndef QLEVER_TEST_PRINTERS_UNITOFMEASUREMENTPRINTERS_H
#define QLEVER_TEST_PRINTERS_UNITOFMEASUREMENTPRINTERS_H

#include "global/Constants.h"

// _____________________________________________________________
inline void PrintTo(const UnitOfMeasurement& unit, std::ostream* os) {
  auto& s = *os;
  if (unit == UnitOfMeasurement::KILOMETERS) {
    s << std::string{"km"};
  } else if (unit == UnitOfMeasurement::METERS) {
    s << std::string{"m"};
  } else if (unit == UnitOfMeasurement::MILES) {
    s << std::string{"mi"};
  } else {
    s << std::string{"unknown-unit"};
  }
}

#endif  // QLEVER_TEST_PRINTERS_UNITOFMEASUREMENTPRINTERS_H
