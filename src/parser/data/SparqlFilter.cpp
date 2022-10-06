// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)
// Author: Florian Kramer (flo.kramer@arcor.de)

#include "SparqlFilter.h"

#include <sstream>

string SparqlFilter::asString() const {
  std::ostringstream os;
  os << "FILTER(" << expression_.getDescriptor() << ")";
  return std::move(os).str();
}
