// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)
// Author: Florian Kramer (flo.kramer@arcor.de)
#pragma once

#include <string>
#include <vector>

using std::string;
using std::vector;

class SparqlFilter {
 public:
  enum FilterType {
    EQ = 0,
    NE = 1,
    LT = 2,
    LE = 3,
    GT = 5,
    GE = 6,
    LANG_MATCHES = 7,
    REGEX = 8,
    PREFIX = 9
  };

  [[nodiscard]] string asString() const;

  FilterType _type;
  string _lhs;
  string _rhs;
  vector<string> _additionalLhs = {};
  vector<string> _additionalPrefixes = {};
  bool _regexIgnoreCase = false;
  // True if the str function was applied to the left side.
  bool _lhsAsString = false;

  bool operator==(const SparqlFilter&) const = default;
};
