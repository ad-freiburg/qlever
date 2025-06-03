// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)
// Author: Florian Kramer (flo.kramer@arcor.de)

#include "parser/data/SparqlFilter.h"

#include <absl/strings/str_cat.h>
// _____________________________________________________________________________
string SparqlFilter::asString() const {
  return absl::StrCat("FILTER(", expression_.getDescriptor(), ")");
}
