// Copyright 2014, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)

#include <string>
#include <sstream>
#include <vector>

#include "ParsedQuery.h"

using std::string;
using std::vector;

// _____________________________________________________________________________
string ParsedQuery::asString() const {
  std::ostringstream os;

  // PREFIX
  os << "prefix: {";
  for (size_t i = 0; i < _prefixes.size(); ++i) {
    os << "\n\t" << _prefixes[i].asString();
    if (i + 1 < _prefixes.size()) { os << ','; }
  }
  os << "\n}";

  // SELECT
  os << "\nselect: {\n\t";
  for (size_t i = 0; i < _selectedVariables.size(); ++i) {
    os << _selectedVariables[i];
    if (i + 1 < _selectedVariables.size()) { os << ", "; }
  }
  os << "\n}";

  // WHERE
  os << "\nwhere: {";
  for (size_t i = 0; i < _whereClauseTriples.size(); ++i) {
    os << "\n\t" << _whereClauseTriples[i].asString();
    if (i + 1 < _whereClauseTriples.size()) { os << ','; }
  }
  os << "\n}";

  return os.str();
}

// _____________________________________________________________________________
string SparqlPrefix::asString() const {
  std::ostringstream os;
  os << "{" << _prefix << ": " << _uri << "}";
  return os.str();
}

// _____________________________________________________________________________
string SparqlTriple::asString() const {
  std::ostringstream os;
  os << "{s: " << _s << ",\tp: " << _p << ",\to: " << _o << "}";
  return os.str();
}
