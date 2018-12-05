// Copyright 2014, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)

#include "./SparqlParser.h"

#include <unordered_set>

#include "../global/Constants.h"
#include "../util/Conversions.h"
#include "../util/Exception.h"
#include "../util/Log.h"
#include "../util/StringUtils.h"
#include "./ParseException.h"

// _____________________________________________________________________________
ParsedQuery SparqlParser::parse(const string& query) {
  ParsedQuery result;
  result._originalString = query;

  // Split prologue
  size_t i = query.find("SELECT");
  if (i == string::npos) {
    throw ParseException(
        "Missing keyword \"SELECT\", "
        "currently only select queries are supported.");
  }

  size_t j = query.find("WHERE");
  if (j == string::npos) {
    throw ParseException(
        "Missing keyword \"WHERE\", "
        "currently only select queries are supported.");
  }

  if (i >= j) {
    throw ParseException(
        "Keyword \"WHERE\" "
        "found after keyword \"SELECT\". Invalid query.");
  }

  size_t k = query.rfind("}");
  if (k == string::npos || k < i) {
    throw ParseException("Missing \"}\" symbol after \"WHERE\".");
  }

  parsePrologue(ad_utility::strip(query.substr(0, i), " \n\t"), result);
  parseSelect(ad_utility::strip(query.substr(i, j - i), " \n\t"), result);
  parseWhere(ad_utility::strip(query.substr(j, k - j + 1), " \n\t"), result);

  parseSolutionModifiers(ad_utility::strip(query.substr(k + 1), " \n\t"),
                         result);

  if (result._groupByVariables.size() > 0) {
    // Check if all selected variables are either aggregated or
    // part of the group by statement.
    for (const string& var : result._selectedVariables) {
      if (var[0] == '?') {
        if (std::find(result._groupByVariables.begin(),
                      result._groupByVariables.end(),
                      var) == result._groupByVariables.end()) {
          throw ParseException("Variable " + var +
                               " is selected but not "
                               "aggregated despite the result not being"
                               "grouped by " +
                               var + ".");
        }
      }
    }
  }
  result.parseAliases();
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
  auto parts = ad_utility::splitWs(str);
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
  // split the string on whitespace but leave aliases intact
  std::vector<string> vars;
  size_t start = 0;
  size_t pos = 0;
  bool inAlias = false;
  int bracketDepth = 0;
  while (pos < str.size()) {
    if (!inAlias && ::isspace(static_cast<unsigned char>(str[pos]))) {
      if (start != pos) {
        vars.emplace_back(str.substr(start, pos - start));
      }
      // skip any whitespace
      while (pos < str.size() &&
             ::isspace(static_cast<unsigned char>(str[pos]))) {
        pos++;
      }
      start = pos;
    }
    if (str[pos] == '(') {
      bracketDepth++;
      inAlias = true;
    } else if (str[pos] == ')') {
      bracketDepth--;
      if (bracketDepth == 0) {
        inAlias = false;
      }
    }
    pos++;
  }
  // avoid adding whitespace at the back of the string
  if (start != str.size()) {
    vars.emplace_back(str.substr(start));
  }

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
        ad_utility::startsWith(vars[i], "TEXT(") || vars[i][0] == '(') {
      query._selectedVariables.push_back(vars[i]);
    } else {
      throw ParseException(string("Invalid variable in select clause: \"") +
                           vars[i] + "\"");
    }
  }
}

// _____________________________________________________________________________
void SparqlParser::parseWhere(const string& str, ParsedQuery& query,
                              ParsedQuery::GraphPattern* currentPattern) {
  if (currentPattern == nullptr) {
    currentPattern = &query._rootGraphPattern;
    query._rootGraphPattern._id = 0;
  }
  size_t i = str.find('{');
  size_t j = str.rfind('}');
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
    while (inner[k] == ' ' || inner[k] == '\t' || inner[k] == '\n') {
      ++k;
    }
    if (inner[k] == 'O' || inner[k] == 'o') {
      if (inner.substr(k, 8) == "OPTIONAL" ||
          inner.substr(k, 8) == "optional") {
        // find opening and closing brackets of optional part
        size_t ob = inner.find('{', k);
        size_t cb = ad_utility::findClosingBracket(inner, ob, '{', '}');
        currentPattern->_children.push_back(
            new ParsedQuery::GraphPatternOperation(
                ParsedQuery::GraphPatternOperation::Type::OPTIONAL,
                {new ParsedQuery::GraphPattern()}));
        currentPattern->_children.back()->_childGraphPatterns[0]->_optional =
            true;
        currentPattern->_children.back()->_childGraphPatterns[0]->_id =
            query._numGraphPatterns;
        query._numGraphPatterns++;
        // Recursively call parseWhere to parse the optional part.
        parseWhere(inner.substr(ob, cb - ob + 1), query,
                   currentPattern->_children.back()->_childGraphPatterns[0]);

        // set start to the end of the optional part
        start = cb + 1;
        continue;
      }
    } else if (inner[k] == '{') {
      // look for a subquery
      size_t h = k + 1;
      while (std::isspace(inner[h])) {
        h++;
      }
      if (ad_utility::getLowercase(inner.substr(h, 6)) == "select") {
        size_t selectPos = h;
        size_t endBracket = ad_utility::findClosingBracket(inner, k, '{', '}');
        if (endBracket == size_t(-1)) {
          throw ParseException(
              "SELECT keyword found at " + std::to_string(selectPos) + " (" +
              inner.substr(selectPos, 15) +
              ") without a closing bracket for the outer brackets.");
        }
        // look for a subquery
        size_t openingBracket = inner.find('{', selectPos);
        if (openingBracket == std::string::npos) {
          throw ParseException("SELECT keyword found at " +
                               std::to_string(selectPos) + " (" +
                               inner.substr(selectPos, 15) +
                               ") without an accompanying opening bracket.");
        }
        size_t closing_bracket =
            ad_utility::findClosingBracket(inner, openingBracket, '{', '}');
        if (closing_bracket == size_t(-1)) {
          throw ParseException("The subquery at " + std::to_string(selectPos) +
                               "(" + inner.substr(selectPos, 15) +
                               ") is missing a closing bracket.");
        }
        std::string subquery_string =
            inner.substr(selectPos, endBracket - selectPos);
        LOG(DEBUG) << "Found subquery: " << subquery_string << std::endl;

        // create the subquery operation
        ParsedQuery::GraphPatternOperation* u =
            new ParsedQuery::GraphPatternOperation(
                ParsedQuery::GraphPatternOperation::Type::SUBQUERY);
        u->_subquery = new ParsedQuery(parse(subquery_string));
        // Remove all manual ordering from the subquery as it would be changed
        // by the parent query.
        u->_subquery->_orderBy.clear();
        currentPattern->_children.push_back(u);

        // continue parsing after the union statement
        start = endBracket + 1;
        continue;
      }

      // look for a UNION
      // find the opening and closing bracket of the left side
      size_t leftOpeningBracket = k;
      size_t leftClosingBracket =
          ad_utility::findClosingBracket(inner, k, '{', '}');
      k = leftClosingBracket + 1;
      while (inner[k] == ' ' || inner[k] == '\t' || inner[k] == '\n') {
        ++k;
      }
      // look for the union keyword
      if (ad_utility::getLowercase(inner.substr(k, 5)) != "union") {
        throw ParseException("Found an unexpected pair of brackets in " +
                             inner + " at positions " +
                             std::to_string(leftOpeningBracket) + " and " +
                             std::to_string(leftClosingBracket));
      }
      k += 5;
      while (inner[k] == ' ' || inner[k] == '\t' || inner[k] == '\n') {
        ++k;
      }
      // find the opening and closing brackets of the right part
      size_t rightOpeningBracket = k;
      size_t rightClosingBracket =
          ad_utility::findClosingBracket(inner, k, '{', '}');

      // create the union operation
      ParsedQuery::GraphPatternOperation* u =
          new ParsedQuery::GraphPatternOperation(
              ParsedQuery::GraphPatternOperation::Type::UNION,
              {new ParsedQuery::GraphPattern(),
               new ParsedQuery::GraphPattern()});
      u->_childGraphPatterns[0]->_optional = false;
      u->_childGraphPatterns[1]->_optional = false;
      u->_childGraphPatterns[0]->_id = query._numGraphPatterns;
      u->_childGraphPatterns[1]->_id = query._numGraphPatterns + 1;
      query._numGraphPatterns += 2;
      currentPattern->_children.push_back(u);

      // parse the left and right bracket
      parseWhere(inner.substr(leftOpeningBracket,
                              leftClosingBracket - leftOpeningBracket + 1),
                 query, u->_childGraphPatterns[0]);
      parseWhere(inner.substr(rightOpeningBracket,
                              rightClosingBracket - rightOpeningBracket + 1),
                 query, u->_childGraphPatterns[1]);

      // continue parsing after the union statement
      start = rightClosingBracket + 1;
      continue;
    } else if (inner[k] == 'F' || inner[k] == 'f') {
      if (inner.substr(k, 6) == "FILTER" || inner.substr(k, 6) == "filter") {
        // find the final closing bracket
        size_t end = k;
        size_t bracketDepth = 0;
        while (end < inner.size()) {
          if (inner[end] == '(') {
            bracketDepth++;
          } else if (inner[end] == ')') {
            bracketDepth--;
            if (bracketDepth == 0) {
              break;
            }
          }
          end++;
        }
        if (end == string::npos || end == inner.size()) {
          AD_THROW(ad_semsearch::Exception::BAD_QUERY,
                   "Filter without closing parenthesis.");
        }
        filters.push_back(inner.substr(k, end - k + 1));
        size_t posOfDelim = inner.find_first_of(". \t\n", end + 1);
        start = (posOfDelim == string::npos ? end + 1 : posOfDelim + 1);
        continue;
      }
    } else if (inner[k] == 'S' || inner[k] == 's') {
    }
    while (k < inner.size()) {
      if (!insideUri && !insideLiteral && !insideNsThing) {
        if (inner[k] == '.') {
          clauses.emplace_back(inner.substr(start, k - start));
          break;
        }
        if (inner[k] == '<') {
          insideUri = true;
        }
        if (inner[k] == '\"') {
          insideLiteral = true;
        }
        if (inner[k] == ':') {
          insideNsThing = true;
        }
      } else {
        if (insideUri && inner[k] == '>') {
          insideUri = false;
        }
        if (insideLiteral && inner[k] == '\"') {
          insideLiteral = false;
        }
        if (insideNsThing && (inner[k] == ' ' || inner[k] == '\t')) {
          insideNsThing = false;
        }
      }
      ++k;
    }
    if (k == inner.size()) {
      clauses.emplace_back(inner.substr(start));
    }
    start = k + 1;
  }
  for (const string& clause : clauses) {
    string c = ad_utility::strip(clause, ' ');
    if (c.size() > 0) {
      addWhereTriple(c, currentPattern);
    }
  }
  for (const string& filter : filters) {
    // We might add a language filter, those need the GraphPattern
    addFilter(filter, &currentPattern->_filters, currentPattern);
  }
}

// _____________________________________________________________________________
void SparqlParser::addWhereTriple(const string& str,
                                  ParsedQuery::GraphPattern* pattern) {
  size_t i = 0;
  while (i < str.size() &&
         (str[i] == ' ' || str[i] == '\t' || str[i] == '\n')) {
    ++i;
  }
  if (i == str.size()) {
    AD_THROW(ad_semsearch::Exception::BAD_QUERY, "Illegal triple: " + str);
  }
  size_t j = i + 1;
  while (j < str.size() && str[j] != '\t' && str[j] != ' ' && str[j] != '\n') {
    ++j;
  }
  if (j == str.size()) {
    AD_THROW(ad_semsearch::Exception::BAD_QUERY, "Illegal triple: " + str);
  }

  string s = str.substr(i, j - i);
  i = j;
  while (i < str.size() &&
         (str[i] == ' ' || str[i] == '\t' || str[i] == '\n')) {
    ++i;
  }
  if (i == str.size()) {
    AD_THROW(ad_semsearch::Exception::BAD_QUERY, "Illegal triple: " + str);
  }
  j = i + 1;
  while (j < str.size() && str[j] != '\t' && str[j] != ' ' && str[j] != '\n') {
    ++j;
  }
  string p = str.substr(i, j - i);

  i = j;
  while (i < str.size() &&
         (str[i] == ' ' || str[i] == '\t' || str[i] == '\n')) {
    ++i;
  }
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
           str[j] != '\n') {
      ++j;
    }
  }
  string o = str.substr(i, j - i);
  if (p == CONTAINS_WORD_PREDICATE || p == CONTAINS_WORD_PREDICATE_NS) {
    o = stripAndLowercaseKeywordLiteral(o);
  }

  SparqlTriple triple(s, p, o);
  // Quadratic in number of triples in query.
  // Shouldn't be a problem here, though.
  // Could use a (hash)-set instead of vector.
  if (std::find(pattern->_whereClauseTriples.begin(),
                pattern->_whereClauseTriples.end(),
                triple) != pattern->_whereClauseTriples.end()) {
    LOG(INFO) << "Ignoring duplicate triple: " << str << std::endl;
  } else {
    pattern->_whereClauseTriples.push_back(triple);
  }
}

// _____________________________________________________________________________
void SparqlParser::parseSolutionModifiers(const string& str,
                                          ParsedQuery& query) {
  // Split the string at any whitespace but ignore whitespace inside brackets
  // to allow for alias parsing.
  auto tokens = ad_utility::splitWsWithEscape(str, '(', ')');
  for (size_t i = 0; i < tokens.size(); ++i) {
    if (tokens[i] == "ORDER" && i < tokens.size() - 2 &&
        tokens[i + 1] == "BY") {
      i += 1;
      while (i + 1 < tokens.size() && tokens[i + 1] != "LIMIT" &&
             tokens[i + 1] != "OFFSET" && tokens[i + 1] != "TEXTLIMIT" &&
             tokens[i + 1] != "GROUP" && tokens[i + 1] != "HAVING") {
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
    if (tokens[i] == "GROUP" && i < tokens.size() - 2 &&
        tokens[i + 1] == "BY") {
      i++;
      while (i + 1 < tokens.size() && tokens[i + 1][0] == '?') {
        query._groupByVariables.push_back(tokens[i + 1]);
        i++;
      }
    }
    if (tokens[i] == "HAVING" && i < tokens.size() - 1) {
      while (i + 1 < tokens.size() && tokens[i + 1] != "LIMIT" &&
             tokens[i + 1] != "OFFSET" && tokens[i + 1] != "TEXTLIMIT" &&
             tokens[i + 1] != "GROUP" && tokens[i + 1] != "ORDER") {
        addFilter(tokens[i + 1], &query._havingClauses);
        i++;
      }
    }
  }
}

// _____________________________________________________________________________
void SparqlParser::addFilter(const string& str, vector<SparqlFilter>* _filters,
                             ParsedQuery::GraphPattern* pattern) {
  size_t i = str.find('(');
  AD_CHECK(i != string::npos);
  size_t j = str.rfind(')');
  AD_CHECK(j != string::npos);

  // Handle filters with prefix predicates (langMatches, prefix, regex, etc)
  if (i >= 1 && str[i - 1] != ' ') {
    // find the last whitespace after the 'FILTER' keyword (as in e.g.
    // 'FILTER regex...'. If none is found assume this is a filter from
    // the having clause where the pred would start at the beginning of the
    // string.
    auto s = str.rfind(' ', i);
    if (s != string::npos || str.substr(0, 6) != "FILTER") {
      if (s != string::npos) {
        s++;
      } else {
        s = 0;
      }
      auto pred = str.substr(s, i - s);
      auto parts = ad_utility::split(str.substr(i + 1, j - i - 1), ',');
      AD_CHECK(parts.size() == 2 || (parts.size() == 3 && pred == "regex"));
      std::string lhs = ad_utility::strip(parts[0], ' ');
      std::string rhs = ad_utility::strip(parts[1], ' ');
      if (pred == "langMatches") {
        if (!pattern) {
          AD_THROW(
              ad_semsearch::Exception::BAD_QUERY,
              "Invalid position for language filter. Probable cause: language "
              "filters are currently not supported in HAVING clauses.");
        }
        if (!ad_utility::startsWith(lhs, "lang(")) {
          AD_THROW(ad_semsearch::Exception::BAD_QUERY,
                   "langMatches filters"
                   "are only supported"
                   "when used with the "
                   "lang function.");
        }
        std::string lvar = lhs.substr(5, lhs.size() - 6);
        std::cout << "lvar:" << lvar << std::endl;

        auto langTag = rhs.substr(1, rhs.size() - 2);
        // first find a predicate for the given variable
        auto it =
            std::find_if(pattern->_whereClauseTriples.begin(),
                         pattern->_whereClauseTriples.end(),
                         [&lvar](const auto& tr) { return tr._o == lvar; });
        while (it != pattern->_whereClauseTriples.end() &&
               ad_utility::startsWith(it->_p, "?")) {
          it = std::find_if(it + 1, pattern->_whereClauseTriples.end(),
                            [&lvar](const auto& tr) { return tr._o == lvar; });
        }
        if (it == pattern->_whereClauseTriples.end()) {
          LOG(INFO)
              << "language filter variable " + rhs +
                     "that did not appear as object in any suitable triple. "
                     "using special literal-to-language triple instead.\n";
          auto langEntity = ad_utility::convertLangtagToEntityUri(langTag);
          SparqlTriple triple(lvar, LANGUAGE_PREDICATE, langEntity);
          // Quadratic in number of triples in query.
          // Shouldn't be a problem here, though.
          // Could use a (hash)-set instead of vector.
          if (std::find(pattern->_whereClauseTriples.begin(),
                        pattern->_whereClauseTriples.end(),
                        triple) != pattern->_whereClauseTriples.end()) {
            LOG(INFO) << "Ignoring duplicate triple: " << str << std::endl;
          } else {
            pattern->_whereClauseTriples.push_back(triple);
          }
        } else {
          // replace the triple
          string taggedPredicate = '@' + langTag + '@' + it->_p;
          *it = SparqlTriple(it->_s, taggedPredicate, it->_o);
        }

        // Convert the language filter to a special triple
        // to make it more efficient (no need for string resolution)
        return;
      }
      if (pred == "regex") {
        SparqlFilter f;
        f._type = SparqlFilter::REGEX;
        f._lhs = lhs;
        f._rhs = rhs.substr(1, rhs.size() - 2);
        if (f._rhs[0] == '^') {
          // Check if we can use the more efficient prefix filter instead of
          // an expensive regex filter.
          bool isSimple = true;
          bool escaped = false;

          // Check if the regex is only a prefix regex or also does
          // anything else.
          const static string regexControlChars = "[]^$.|?*+()";
          for (size_t i = 1; isSimple && i < f._rhs.size(); i++) {
            if (f._rhs[i] == '\\') {
              escaped = true;
              continue;
            }
            if (!escaped) {
              char c = f._rhs[i];
              if (regexControlChars.find(c) != string::npos) {
                isSimple = false;
              }
            }
            escaped = false;
          }
          if (isSimple) {
            // There are no regex special chars apart from the leading '^' so
            // we can use a prefix filter.
            f._type = SparqlFilter::PREFIX;
          }
        }
        f._regexIgnoreCase =
            (parts.size() == 3 && ad_utility::strip(parts[2], ' ') == "\"i\"");
        _filters->emplace_back(f);
        return;
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
        _filters->emplace_back(f1);
        _filters->emplace_back(f2);
        return;
      }
    }
  }

  // Handle filters with infix predicate (== != <= etc).
  string filter = str.substr(i + 1, j - i - 1);
  vector<string> tokens;
  size_t startP2 = 0;
  for (size_t si = 0; si < filter.size(); ++si) {
    if (filter[si] == '=' || filter[si] == '!' || filter[si] == '<' ||
        filter[si] == '>') {
      if (startP2 == 0) {
        tokens.push_back(ad_utility::strip(filter.substr(0, si), " \t"));
        startP2 = si;
      }
    } else {
      if (startP2 > 0) {
        tokens.push_back(
            ad_utility::strip(filter.substr(startP2, si - startP2), " \t"));
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
  _filters->emplace_back(f);
}

// _____________________________________________________________________________
string SparqlParser::stripAndLowercaseKeywordLiteral(const string& lit) {
  if (lit.size() > 2 && lit[0] == '"' && lit.back() == '"') {
    string stripped = ad_utility::strip(lit, '"');
    // stripped.erase(std::remove(stripped.begin(), stripped.end(), '\''),
    //               stripped.end());
    return ad_utility::getLowercaseUtf8(stripped);
  }
  return lit;
}
