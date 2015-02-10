// Copyright 2014, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)

#include <bits/algorithmfwd.h>
#include "./SparqlParser.h"
#include "./ParseException.h"
#include "../util/StringUtils.h"

// _____________________________________________________________________________
ParsedQuery SparqlParser::parse(const string& query) {
  ParsedQuery result;

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

  parsePrologue(ad_utility::strip(query.substr(0, i), " \n\t"), result);
  parseSelect(ad_utility::strip(query.substr(i, j - (i + 1)), " \n\t"), result);
  parseWhere(ad_utility::strip(query.substr(j), " \n\t"), result);

  return result;
}

// _____________________________________________________________________________
void SparqlParser::parsePrologue(string str, ParsedQuery& query) {
  if (str.find("BASE") != string::npos) {
    throw ParseException("Bases are not supported, yet.");
  }

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
  for (size_t i = 1; i < vars.size(); ++i) {
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
  // in URIs.
  vector<string> clauses;
  size_t start = 0;
  bool insideUri = false;
  while (start < inner.size()) {
    size_t k = start;
    while (k < inner.size()) {
      if (!insideUri) {
        if (inner[k] == '.') {
          clauses.emplace_back(inner.substr(start, k - start));
          break;
        }
        if (inner[k] == '<') {insideUri = true;}
      } else {
        if (inner[k] == '>') {insideUri = false;}
      }
      ++k;
    }
    if (k == inner.size()) {clauses.emplace_back(inner.substr(start));}
    start = k + 1;
  }
  for (const string& clause: clauses) {
    string c = ad_utility::strip(clause, ' ');
    if (c.size() > 0) {
      addWhereTriple(c, query);
    }
  }
}

// _____________________________________________________________________________
void SparqlParser::addWhereTriple(const string& str, ParsedQuery& query) {
  auto spo = ad_utility::split(str, ' ');
  if (spo.size() != 3) {
    throw ParseException(string("Invalid triple, expected format: \"s p o\". "
        "Triple was: ") + str);
  }
  query._whereClauseTriples.emplace_back(SparqlTriple{
      ad_utility::strip(spo[0], "\n\t "),
      ad_utility::strip(spo[1], "\n\t "),
      ad_utility::strip(spo[2], "\n\t ")
  });
}
