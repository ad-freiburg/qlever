// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)
// Author: Florian Kramer (flo.kramer@arcor.de)
#pragma once

#include <string>
#include <vector>

#include "engine/sparqlExpressions/SparqlExpressionPimpl.h"

using std::string;
using std::vector;

class SparqlFilter {
 public:
  [[nodiscard]] string asString() const;

  sparqlExpression::SparqlExpressionPimpl expression_;
  // TODO<joka921> This comparison is only used for the unit tests, maybe it
  // should be defined there, because it is only approximate.
  bool operator==(const SparqlFilter& other) const {
    return expression_.getDescriptor() == other.expression_.getDescriptor();
  }
};
