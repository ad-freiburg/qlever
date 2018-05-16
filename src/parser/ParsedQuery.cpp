// Copyright 2014, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)

#include <sstream>
#include <string>
#include <vector>

#include "../util/StringUtils.h"
#include "ParseException.h"
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
    if (i + 1 < _prefixes.size()) {
      os << ',';
    }
  }
  os << "\n}";

  // SELECT
  os << "\nSELECT: {\n\t";
  for (size_t i = 0; i < _selectedVariables.size(); ++i) {
    os << _selectedVariables[i];
    if (i + 1 < _selectedVariables.size()) {
      os << ", ";
    }
  }
  os << "\n}";

  // WHERE
  os << "\nWHERE: \n";
  _rootGraphPattern.toString(os, 1);

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
string SparqlFilter::asString() const {
  std::ostringstream os;
  os << "FILTER(" << _lhs;
  switch (_type) {
    case EQ:
      os << " < ";
      break;
    case NE:
      os << " != ";
      break;
    case LT:
      os << " < ";
      break;
    case LE:
      os << " <= ";
      break;
    case GT:
      os << " > ";
      break;
    case GE:
      os << " >= ";
      break;
  }
  os << _rhs << ")";
  return os.str();
}

// _____________________________________________________________________________
void ParsedQuery::expandPrefixes() {
  ad_utility::HashMap<string, string> prefixMap;
  prefixMap["ql"] = "<QLever-internal-function/>";
  for (const auto& p : _prefixes) {
    prefixMap[p._prefix] = p._uri;
  }

  vector<GraphPattern*> graphPatterns;
  graphPatterns.push_back(&_rootGraphPattern);
  // Traverse the graph pattern tree using dfs expanding the prefixes in every
  // pattern.
  while (!graphPatterns.empty()) {
    GraphPattern* pattern = graphPatterns.back();
    graphPatterns.pop_back();
    for (GraphPattern* p : pattern->_children) {
      graphPatterns.push_back(p);
    }

    for (auto& trip : pattern->_whereClauseTriples) {
      expandPrefix(trip._s, prefixMap);
      expandPrefix(trip._p, prefixMap);
      if (trip._p.find("in-context") != string::npos) {
        auto tokens = ad_utility::split(trip._o, ' ');
        trip._o = "";
        for (size_t i = 0; i < tokens.size(); ++i) {
          expandPrefix(tokens[i], prefixMap);
          trip._o += tokens[i];
          if (i + 1 < tokens.size()) {
            trip._o += " ";
          }
        }
      } else {
        expandPrefix(trip._o, prefixMap);
      }
    }
    for (auto& f : pattern->_filters) {
      expandPrefix(f._lhs, prefixMap);
      expandPrefix(f._rhs, prefixMap);
    }
  }
}

// _____________________________________________________________________________
void ParsedQuery::expandPrefix(
    string& item, const ad_utility::HashMap<string, string>& prefixMap) {
  if (!ad_utility::startsWith(item, "?") &&
      !ad_utility::startsWith(item, "<")) {
    size_t i = item.find(':');
    size_t from = item.find("^^");
    if (from == string::npos) {
      from = 0;
    } else {
      from += 2;
    }
    if (i != string::npos && i >= from &&
        prefixMap.count(item.substr(from, i - from)) > 0) {
      string prefixUri = prefixMap.find(item.substr(from, i - from))->second;
      if (from == 0) {
        item = prefixUri.substr(0, prefixUri.size() - 1) + item.substr(i + 1) +
               '>';
      } else {
        item = item.substr(0, from) +
               prefixUri.substr(0, prefixUri.size() - 1) + item.substr(i + 1) +
               '>';
      }
    }
  }
}

void ParsedQuery::parseAliases() {
  for (size_t i = 0; i < _selectedVariables.size(); i++) {
    const std::string& var = _selectedVariables[i];
    if (var[0] == '(') {
      std::string inner = var.substr(1, var.size() - 2);
      if (ad_utility::startsWith(inner, "COUNT") ||
          ad_utility::startsWith(inner, "GROUP_CONCAT") ||
          ad_utility::startsWith(inner, "FIRST") ||
          ad_utility::startsWith(inner, "LAST") ||
          ad_utility::startsWith(inner, "SAMPLE") ||
          ad_utility::startsWith(inner, "MIN") ||
          ad_utility::startsWith(inner, "MAX") ||
          ad_utility::startsWith(inner, "SUM") ||
          ad_utility::startsWith(inner, "AVG")) {
        Alias a;
        a._isAggregate = true;
        size_t pos = inner.find("as");
        if (pos == std::string::npos) {
          throw ParseException("Alias " + var + " is malformed.");
        }
        std::string newVarName = inner.substr(pos + 2, var.size() - pos - 2);
        newVarName = ad_utility::strip(newVarName, " \t\n");
        a._outVarName = newVarName;
        a._function = inner;

        // find the second opening bracket
        pos = inner.find('(', 1);
        pos++;
        while (pos < inner.size() &&
               ::std::isspace(static_cast<unsigned char>(inner[pos]))) {
          pos++;
        }
        size_t start = pos;
        while (pos < inner.size() &&
               !::std::isspace(static_cast<unsigned char>(inner[pos]))) {
          pos++;
        }
        if (pos == start || pos >= inner.size()) {
          throw ParseException("Alias " + var + " is malformed.");
        }

        a._inVarName = inner.substr(start, pos - start - 1);
        _aliases.push_back(a);
        // Replace the variable in the selected variables array with the aliased
        // name.
        _selectedVariables[i] = newVarName;
      } else {
        throw ParseException("Unknown or malformed alias: " + var);
      }
    }
  }
}

// _____________________________________________________________________________
ParsedQuery::GraphPattern::~GraphPattern() {
  for (GraphPattern* child : _children) {
    delete child;
  }
}

// _____________________________________________________________________________
ParsedQuery::GraphPattern::GraphPattern(GraphPattern&& other)
    : _whereClauseTriples(std::move(other._whereClauseTriples)),
      _filters(std::move(other._filters)),
      _optional(other._optional),
      _children(std::move(other._children)) {
  other._children.clear();
}

// _____________________________________________________________________________
ParsedQuery::GraphPattern::GraphPattern(const GraphPattern& other)
    : _whereClauseTriples(other._whereClauseTriples),
      _filters(other._filters),
      _optional(other._optional) {
  _children.reserve(other._children.size());
  for (const GraphPattern* g : other._children) {
    _children.push_back(new GraphPattern(*g));
  }
}

// _____________________________________________________________________________
ParsedQuery::GraphPattern& ParsedQuery::GraphPattern::operator=(
    const ParsedQuery::GraphPattern& other) {
  _whereClauseTriples = std::vector<SparqlTriple>(other._whereClauseTriples);
  _filters = std::vector<SparqlFilter>(other._filters);
  _optional = other._optional;
  _children.clear();
  _children.reserve(other._children.size());
  for (const GraphPattern* g : other._children) {
    _children.push_back(new GraphPattern(*g));
  }
  return *this;
}

// _____________________________________________________________________________
void ParsedQuery::GraphPattern::toString(std::ostringstream& os,
                                         int indentation) const {
  for (int j = 1; j < indentation; ++j) os << "  ";
  if (_optional) {
    os << "OPTIONAL ";
  }
  os << "{";
  for (size_t i = 0; i + 1 < _whereClauseTriples.size(); ++i) {
    os << "\n";
    for (int j = 0; j < indentation; ++j) os << "  ";
    os << _whereClauseTriples[i].asString() << ',';
  }
  if (_whereClauseTriples.size() > 0) {
    os << "\n";
    for (int j = 0; j < indentation; ++j) os << "  ";
    os << _whereClauseTriples.back().asString();
  }
  for (size_t i = 0; i + 1 < _filters.size(); ++i) {
    os << "\n";
    for (int j = 0; j < indentation; ++j) os << "  ";
    os << _filters[i].asString() << ',';
  }
  if (_filters.size() > 0) {
    os << "\n";
    for (int j = 0; j < indentation; ++j) os << "  ";
    os << _filters.back().asString();
  }
  for (GraphPattern* child : _children) {
    os << "\n";
    child->toString(os, indentation + 1);
  }
  os << "\n";
  for (int j = 1; j < indentation; ++j) os << "  ";
  os << "}";
}
