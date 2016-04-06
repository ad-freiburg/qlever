// Copyright 2014, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)

#include <string>
#include <sstream>
#include <vector>
#include <unordered_map>

#include "../util/StringUtils.h"
#include "ParsedQuery.h"


using std::string;
using std::vector;

// _____________________________________________________________________________
string ParsedQuery::asString() const {
  std::ostringstream os;

  // PREFIX
  os << "PREFIX: {";
  for (size_t i = 0; i < _prefixes.size(); ++i) {
    os << "\n\t" << _prefixes[i].asString();
    if (i + 1 < _prefixes.size()) { os << ','; }
  }
  os << "\n}";

  // SELECT
  os << "\nSELECT: {\n\t";
  for (size_t i = 0; i < _selectedVariables.size(); ++i) {
    os << _selectedVariables[i];
    if (i + 1 < _selectedVariables.size()) { os << ", "; }
  }
  os << "\n}";

  // WHERE
  os << "\nWHERE: {";
  for (size_t i = 0; i < _whereClauseTriples.size(); ++i) {
    os << "\n\t" << _whereClauseTriples[i].asString();
    if (i + 1 < _whereClauseTriples.size()) { os << ','; }
  }
  os << "\n}";

  os << "\nLIMIT: " << (_limit.size() > 0 ? _limit : "no limit specified");
  os << "\nTEXTLIMIT: "
  << (_textLimit.size() > 0 ? _textLimit : "no limit specified");
  os << "\nOFFSET: " << (_offset.size() > 0 ? _offset : "no offset specified");
  os << "\nDISTINCT modifier is " << (_distinct ? "" : "not ") << "present.";
  os << "\nREDUCED modifier is " << (_reduced ? "" : "not ") << "present.";
  os << "\nORDER BY: ";
  if (_orderBy.size() == 0) {
    os << "not specified";
  } else {
    for (auto& key : _orderBy) {
      os << key._key << (key._desc ? " (DESC)" : " (ASC)") << "\t";
    }
  }
  os << "\n";
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
  os << "{s: " << _s << ", p: " << _p << ", o: " << _o << "}";
  return os.str();
}

// _____________________________________________________________________________
void ParsedQuery::expandPrefixes() {
  std::unordered_map<string, string> prefixMap;
  for (const auto& p: _prefixes) {
    prefixMap[p._prefix] = p._uri;
  }

  for (auto& trip: _whereClauseTriples) {
    expandPrefix(trip._s, prefixMap);
    expandPrefix(trip._p, prefixMap);
    expandPrefix(trip._o, prefixMap);
  }
}

// _____________________________________________________________________________
void ParsedQuery::expandPrefix(string& item, 
    const std::unordered_map<string, string>& prefixMap) {
  if (!ad_utility::startsWith(item, "?") &&
      !ad_utility::startsWith(item, "<")) {
    size_t i = item.find(':');
    if (i != string::npos && prefixMap.count(item.substr(0, i)) > 0) {
      string prefixUri = prefixMap.find(item.substr(0, i))->second;
      item = prefixUri.substr(0, prefixUri.size() - 1)
          + item.substr(i + 1) + '>';
    }
  }
}
