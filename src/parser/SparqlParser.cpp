// Copyright 2014, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)


#include "../util/StringUtils.h"
#include "./SparqlParser.h"
#include "./ParseException.h"
#include "../util/Exception.h"
#include "../global/Constants.h"
#include "../util/Log.h"

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
  parseSelect(ad_utility::strip(query.substr(i, j - i), " \n\t"), result);
  parseWhere(ad_utility::strip(query.substr(j, k - j + 1), " \n\t"), result);

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
  auto parts = ad_utility::splitWs(ad_utility::strip(str, ' '));
  if (parts.size() != 3) {
    throw ParseException(string("Invalid PREFIX statement: ") + str);
  }
  string uri = ad_utility::strip(parts[2], " \t\n");
  if (uri.size() == 0 || uri[0] != '<' || uri[uri.size() - 1] != '>') {
    throw ParseException(string("Invalid URI in PREFIX: ") + uri);
  }
  SparqlPrefix p{ad_utility::strip(parts[1], " :\t\n"), uri};
  query._prefixes.emplace_back(p);
}

// _____________________________________________________________________________
void SparqlParser::parseSelect(const string& str, ParsedQuery& query) {
  assert(ad_utility::startsWith(str, "SELECT"));
  auto vars = ad_utility::splitWs(str);
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
    if (ad_utility::startsWith(vars[i], "?") ||
        ad_utility::startsWith(vars[i], "SCORE(") ||
        ad_utility::startsWith(vars[i], "TEXT(")) {
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
  assert(j != string::npos);
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
    while (inner[k] == ' ' || inner[k] == '\t' || inner[k] == '\n') { ++k; }
    if (inner[k] == 'F') {
      if (inner.substr(k, 6) == "FILTER" || inner.substr(k, 6) == "filter") {
        size_t end = inner.find(')', k);
        if (end == string::npos) {
          AD_THROW(ad_semsearch::Exception::BAD_QUERY,
                   "Filter without closing parenthesis.");
        }
        filters.push_back(inner.substr(k, end - k + 1));
        size_t posOfDelim = inner.find_first_of(". \t\n", end + 1);
        start = (posOfDelim == string::npos ? end + 1 : posOfDelim + 1);
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
        if (insideLiteral && inner[k] == '\"') { insideLiteral = false; }
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
  if (p == CONTAINS_WORD_PREDICATE ||
      p == CONTAINS_WORD_PREDICATE_NS) {
    o = stripAndLowercaseKeywordLiteral(o);
  }
  SparqlTriple triple(s, p, o);
  // Quadratic in number of triples in query.
  // Shouldn't be a problem here, though.
  // Could use a (hash)-set instead of vector.
  if (std::find(query._whereClauseTriples.begin(),
                query._whereClauseTriples.end(), triple) !=
      query._whereClauseTriples.end()) {
    LOG(INFO) << "Ignoring duplicate triple: " << str << std::endl;
  } else {
    query._whereClauseTriples.push_back(triple);
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
             && tokens[i + 1] != "OFFSET"
             && tokens[i + 1] != "TEXTLIMIT") {
        query._orderBy.emplace_back(OrderKey(tokens[i + 1]));
        ++i;
      }
    }
    if (tokens[i] == "LIMIT" && i < tokens.size() - 1) {
      query._limit = tokens[i + 1];
      ++i;
    }
    if (tokens[i] == "TEXTLIMIT" && i < tokens.size() - 1) {
      query._textLimit = tokens[i + 1];
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


  // Handle filters with prefix predicates (langMatches, prefix, regex, etc)
  if (i >= 1 && str[i - 1] != ' ') {
    auto s = str.rfind(' ', i);
    if (s != string::npos) {
      s++;
      auto pred = str.substr(s, i - s);
      auto parts = ad_utility::split(str.substr(i + 1, j - i - 1), ',');
      AD_CHECK(parts.size() == 2);
      auto lhs = ad_utility::strip(parts[0], ' ');
      auto rhs = ad_utility::strip(parts[1], ' ');
      if (pred == "langMatches") {
        AD_THROW(ad_semsearch::Exception::NOT_YET_IMPLEMENTED,
                 "No filters with langMatches supported, yet.")
      }
      if (pred == "regex") {
        AD_THROW(ad_semsearch::Exception::NOT_YET_IMPLEMENTED,
                 "No filters with regex supported, yet. "
                     "Try prefix(...) or comparisons with == instead"
                     " if that satisfies your need.")
      }
      if (pred == "prefix") {
        // Rewrite this filter into two ones that use >= and <.
        // First parse and try to get both function arguments.
        AD_CHECK(lhs.size() > 0 && lhs[0] == '?');
        // Rhs has to be a literal.
        AD_CHECK(rhs.size() >= 2 && rhs[0] == '"');
        SparqlFilter f1;
        f1._lhs = lhs;
        f1._rhs = rhs.substr(0, rhs.size() - 1);
        f1._rhs += " ";
        f1._type = SparqlFilter::GE;
        SparqlFilter f2;
        f2._lhs = lhs;
        f2._type = SparqlFilter::LT;
        string upper = rhs.substr(0, rhs.size() - 2);
        upper += rhs[rhs.size() - 2] + 1;
        upper += rhs.back();
        f2._rhs = upper;
        query._filters.emplace_back(f1);
        query._filters.emplace_back(f2);
        return;
      }
    }
  }

  // Handle filters with infix predicate (== != <= etc).
  string filter = str.substr(i + 1, j - i - 1);
  vector<string> tokens;
  size_t startP2 = 0;
  for (size_t si = 0; si < filter.size(); ++si) {
    if (filter[si] == '=' || filter[si] == '!' || filter[si] == '<'
        || filter[si] == '>') {
      if (startP2 == 0) {
        tokens.push_back(ad_utility::strip(filter.substr(0, si), " \t"));
        startP2 = si;
      }
    } else {
      if (startP2 > 0) {
        tokens.push_back(ad_utility::strip(filter.substr(startP2, si - startP2),
                                           " \t"));
        tokens.push_back(ad_utility::strip(filter.substr(si), " \t"));
        break;
      }
    }
  }

  if (tokens.size() != 3) {
    AD_THROW(ad_semsearch::Exception::BAD_QUERY,
             "Unknown syntax for filter: " + filter);
  }
  if (tokens[0].size() == 0 || tokens[0][0] != '?' || tokens[2].size() == 0) {
    AD_THROW(ad_semsearch::Exception::NOT_YET_IMPLEMENTED,
             "Filter not supported yet: " + filter);
  }
  SparqlFilter f;
  f._lhs = tokens[0];
  f._rhs = tokens[2];
  if (f._rhs[0] == '\'') {
    size_t closing = f._rhs.rfind('\'');
    if (closing != 0) {
      f._rhs[0] = '\"';
      f._rhs[closing] = '\"';
    }
  }

  if (tokens[1] == "=" || tokens[1] == "==") {
    f._type = SparqlFilter::EQ;
  } else if (tokens[1] == "!=") {
    f._type = SparqlFilter::NE;
  } else if (tokens[1] == "<") {
    f._type = SparqlFilter::LT;
  } else if (tokens[1] == "<=") {
    f._type = SparqlFilter::LE;
  } else if (tokens[1] == ">") {
    f._type = SparqlFilter::GT;
  } else if (tokens[1] == ">=") {
    f._type = SparqlFilter::GE;
  } else {
    AD_THROW(ad_semsearch::Exception::NOT_YET_IMPLEMENTED,
             "Filter not supported yet: " + filter);
  }
  query._filters.emplace_back(f);
}

// _____________________________________________________________________________
string SparqlParser::stripAndLowercaseKeywordLiteral(const string& lit) {
  if (lit.size() > 2 && lit[0] == '"' && lit.back() == '"') {
    string stripped = ad_utility::strip(lit, '"');
    //stripped.erase(std::remove(stripped.begin(), stripped.end(), '\''),
    //               stripped.end());
    return ad_utility::getLowercaseUtf8(stripped);
  }
  return lit;
}
