// Copyright 2014, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)


#include "../util/StringUtils.h"
#include "./SparqlParser.h"
#include "./ParseException.h"
#include "../util/Exception.h"
#include "../global/Constants.h"

// _____________________________________________________________________________
ParsedQuery SparqlParser::parse(const string& query) {
  ParsedQuery result;
  result._originalString = query;

  // Split prologue
  size_t i = query.find("SELECT");
  if (i == string::npos) {
    throw ParseException("Missing keyword \"SELECT\", "
                             "currently only select queries are supported.");
  }

  size_t j = query.find("WHERE");
  if (j == string::npos) {
    throw ParseException("Missing keyword \"WHERE\", "
                             "currently only select queries are supported.");
  }

  if (i >= j) {
    throw ParseException("Keyword \"WHERE\" "
                             "found after keyword \"SELECT\". Invalid query.");
  }

  size_t k = query.find("}", j);
  if (k == string::npos) {
    throw ParseException("Missing \"}\" symbol after \"WHERE\".");
  }

  parsePrologue(ad_utility::strip(query.substr(0, i), " \n\t"), result);
  parseSelect(ad_utility::strip(query.substr(i, j - (i + 1)), " \n\t"), result);
  parseWhere(ad_utility::strip(query.substr(j, k - j), " \n\t"), result);

  parseSolutionModifiers(ad_utility::strip(query.substr(k + 1), " \n\t"),
                         result);
  return result;
}

// _____________________________________________________________________________
void SparqlParser::parsePrologue(string str, ParsedQuery& query) {
  if (str.find("BASE") != string::npos) {
    throw ParseException("Bases are not supported, yet.");
  }

  size_t i = str.find("PREFIX");
  while (i != string::npos) {
    size_t j = str.find("PREFIX", i + 1);
    addPrefix(str.substr(i, j - i), query);
    i = j;
  }
}

// _____________________________________________________________________________
void SparqlParser::addPrefix(const string& str, ParsedQuery& query) {
  auto parts = ad_utility::split(ad_utility::strip(str, ' '), ' ');
  if (parts.size() != 3) {
    throw ParseException(string("Invalid PREFIX statement: ") + str);
  }
  string uri = ad_utility::strip(parts[2], " \t\n");
  if (uri.size() == 0 || uri[0] != '<' || uri[uri.size() - 1] != '>') {
    throw ParseException(string("Invalid URI in PREFIX: ") + uri);
  }
  SparqlPrefix p{ad_utility::strip(parts[1], " :\t\n"), uri};
  query._prefixes.emplace_back(p);
};

// _____________________________________________________________________________
void SparqlParser::parseSelect(const string& str, ParsedQuery& query) {
  assert(ad_utility::startsWith(str, "SELECT"));
  auto vars = ad_utility::split(str, ' ');
  size_t i = 1;
  if (vars.size() > i && vars[i] == "DISTINCT") {
    ++i;
    query._distinct = true;
  }
  if (vars.size() > i && vars[i] == "REDUCED") {
    ++i;
    query._reduced = true;
  }
  for (; i < vars.size(); ++i) {
    if (ad_utility::startsWith(vars[i], "?")) {
      query._selectedVariables.push_back(vars[i]);
    } else {
      throw ParseException(string("Invalid variable in select clause: \"") +
                           vars[i] + "\"");
    }
  }
}

// _____________________________________________________________________________
void SparqlParser::parseWhere(const string& str, ParsedQuery& query) {
  size_t i = str.find('{');
  size_t j = str.find('}', i);
  if (i == string::npos) {
    throw ParseException("Need curly braces in where clause.");
  }
  string inner = ad_utility::strip(str.substr(i + 1, j - i - 1), "\n\t ");

  // Split where clauses. Cannot simply split at dots, because they may occur
  // in URIs, stuff with namespaces or literals.
  vector<string> clauses;
  vector<string> filters;
  size_t start = 0;
  bool insideUri = false;
  bool insideNsThing = false;
  bool insideLiteral = false;
  while (start < inner.size()) {
    size_t k = start;
    while (inner[k] == ' ' || inner[k] == '\t') { ++k; }
    if (inner[k] == 'F') {
      if (inner.substr(k, 6) == "FILTER") {
        size_t end = inner.find(')', k);
        if (end == string::npos) {
          AD_THROW(ad_semsearch::Exception::BAD_QUERY,
                   "Filter without closing paramthesis.");
        }
        filters.push_back(inner.substr(k, end - k + 1));
        size_t posOfDot = inner.find('.', end);
        start = (posOfDot == string::npos ? end + 1 : posOfDot + 1);
        continue;
      }
    }
    while (k < inner.size()) {
      if (!insideUri && !insideLiteral && !insideNsThing) {
        if (inner[k] == '.') {
          clauses.emplace_back(inner.substr(start, k - start));
          break;
        }
        if (inner[k] == '<') { insideUri = true; }
        if (inner[k] == '\"') { insideLiteral = true; }
        if (inner[k] == ':') { insideNsThing = true; }
      } else {
        if (insideUri && inner[k] == '>') { insideUri = false; }
        if (insideLiteral && inner[k] == '\"') { insideUri = false; }
        if (insideNsThing && (inner[k] == ' ' || inner[k] == '\t')) {
          insideNsThing = false;
        }
      }
      ++k;
    }
    if (k == inner.size()) { clauses.emplace_back(inner.substr(start)); }
    start = k + 1;
  }
  for (const string& clause: clauses) {
    string c = ad_utility::strip(clause, ' ');
    if (c.size() > 0) {
      addWhereTriple(c, query);
    }
  }
  for (const string& filter: filters) {
    addFilter(filter, query);
  }
}

// _____________________________________________________________________________
void SparqlParser::addWhereTriple(const string& str, ParsedQuery& query) {
  size_t i = 0;
  while (i < str.size() &&
         (str[i] == ' ' || str[i] == '\t' || str[i] == '\n')) { ++i; }
  if (i == str.size()) {
    AD_THROW(ad_semsearch::Exception::BAD_QUERY, "Illegal triple: " + str);
  }
  size_t j = i + 1;
  while (j < str.size() && str[j] != '\t' && str[j] != ' ' &&
         str[j] != '\n') { ++j; }
  if (j == str.size()) {
    AD_THROW(ad_semsearch::Exception::BAD_QUERY, "Illegal triple: " + str);
  }

  string s = str.substr(i, j - i);
  i = j;
  while (i < str.size() &&
         (str[i] == ' ' || str[i] == '\t' || str[i] == '\n')) { ++i; }
  if (i == str.size()) {
    AD_THROW(ad_semsearch::Exception::BAD_QUERY, "Illegal triple: " + str);
  }
  j = i + 1;
  while (j < str.size() && str[j] != '\t' && str[j] != ' ' &&
         str[j] != '\n') { ++j; }
  string p = str.substr(i, j - i);
  if (p == OCCURS_WITH_RELATION) {
    string o = ad_utility::strip(str.substr(j), " \t\n");
    query._owTriples.push_back(SparqlTriple(s, p, o));
  } else {
    i = j;
    while (i < str.size() &&
           (str[i] == ' ' || str[i] == '\t' || str[i] == '\n')) { ++i; }
    if (i == str.size()) {
      AD_THROW(ad_semsearch::Exception::BAD_QUERY, "Illegal triple: " + str);
    }
    if (str[i] == '<') {
      // URI
      j = str.find('>', i + 1);
      if (j == string::npos) {
        AD_THROW(ad_semsearch::Exception::BAD_QUERY,
                 "Illegal object in : " + str);
      }
      ++j;
    } else {
      if (str[i] == '\"') {
        // Literal
        j = str.find('\"', i + 1);
        if (j == string::npos) {
          AD_THROW(ad_semsearch::Exception::BAD_QUERY,
                   "Illegal literal in : " + str);
        }
        ++j;
      } else {
        j = i + 1;
      }
      while (j < str.size() && str[j] != ' ' && str[j] != '\t' &&
                                                str[j] != '\n') { ++j; }
    }
    string o = str.substr(i, j - i);
    query._whereClauseTriples.push_back(SparqlTriple(s, p, o));
  }
}

// _____________________________________________________________________________
void SparqlParser::parseSolutionModifiers(const string& str,
                                          ParsedQuery& query) {
  auto tokens = ad_utility::splitAny(str, " \n\t");
  for (size_t i = 0; i < tokens.size(); ++i) {
    if (tokens[i] == "ORDER"
        && i < tokens.size() - 2
        && tokens[i + 1] == "BY") {
      i += 1;
      while (i + 1 < tokens.size()
             && tokens[i + 1] != "LIMIT"
             && tokens[i + 1] != "OFFSET") {
        query._orderBy.emplace_back(OrderKey(tokens[i + 1]));
        ++i;
      }
    }
    if (tokens[i] == "LIMIT" && i < tokens.size() - 1) {
      query._limit = tokens[i + 1];
      ++i;
    }
    if (tokens[i] == "OFFSET" && i < tokens.size() - 1) {
      query._offset = tokens[i + 1];
      ++i;
    }
  }
}

// _____________________________________________________________________________
void SparqlParser::addFilter(const string& str, ParsedQuery& query) {
  size_t i = str.find('(');
  AD_CHECK(i != string::npos);
  size_t j = str.find(')', i + 1);
  AD_CHECK(j != string::npos);
  string filter = str.substr(i + 1, j - i - 1);
  auto tokens = ad_utility::split(filter, ' ');
  if (tokens.size() != 3) {
    AD_THROW(ad_semsearch::Exception::BAD_QUERY,
             "Unknown syntax for filter: " + filter);
  }
  if (tokens[0].size() == 0 || tokens[0][0] != '?' || tokens[2].size() == 0 ||
      tokens[2][0] != '?') {
    AD_THROW(ad_semsearch::Exception::NOT_YET_IMPLEMENTED,
             "Filter not supported yet: " + filter);
  }
  SparqlFilter f;
  f._lhs = tokens[0];
  f._rhs = tokens[2];

  if (tokens[1] == "=" || tokens[1] == "==" ) {
    f._type = SparqlFilter::EQ;
  } else if (tokens[1] == "!=") {
    f._type = SparqlFilter::NE;
  } else if (tokens[1] == "<") {
    f._type = SparqlFilter::LT;
  } else if (tokens[1] == "<=") {
    f._type = SparqlFilter::LE;
  } else if (tokens[1] == "<") {
    f._type = SparqlFilter::GT;
  } else if (tokens[1] == ">=") {
    f._type = SparqlFilter::GE;
  } else {
    AD_THROW(ad_semsearch::Exception::NOT_YET_IMPLEMENTED,
             "Filter not supported yet: " + filter);
  }
  query._filters.emplace_back(f);
}
