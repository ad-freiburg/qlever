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
#include "PropertyPathParser.h"

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
void SparqlParser::parseWhere(
    const string& str, ParsedQuery& query,
    std::shared_ptr<ParsedQuery::GraphPattern> currentPattern) {
  if (currentPattern == nullptr) {
    // Make the shared pointer point to the root graphpattern without deleting
    // it.
    currentPattern = query._rootGraphPattern;
    query._rootGraphPattern->_id = 0;
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

  // If these are not empty the last subject and / or predicate is reused
  std::string lastSubject;
  std::string lastPredicate;
  while (start < inner.size()) {
    size_t k = start;
    while (k < inner.size() && std::isspace(inner[k])) {
      ++k;
    }
    if (k == inner.size()) {
      break;
    }
    if (inner[k] == 'O' || inner[k] == 'o') {
      if (inner.substr(k, 8) == "OPTIONAL" ||
          inner.substr(k, 8) == "optional") {
        // find opening and closing brackets of optional part
        size_t ob = inner.find('{', k);
        size_t cb = ad_utility::findClosingBracket(inner, ob, '{', '}');
        currentPattern->_children.push_back(std::make_shared<
                                            ParsedQuery::GraphPatternOperation>(
            ParsedQuery::GraphPatternOperation::Type::OPTIONAL,
            std::initializer_list<std::shared_ptr<ParsedQuery::GraphPattern>>{
                std::make_shared<ParsedQuery::GraphPattern>()}));
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
        LOG(DEBUG) << "Found subquery:\n" << subquery_string << std::endl;

        // create the subquery operation
        std::shared_ptr<ParsedQuery::GraphPatternOperation> u =
            std::make_shared<ParsedQuery::GraphPatternOperation>(
                ParsedQuery::GraphPatternOperation::Type::SUBQUERY);
        u->_subquery = std::make_shared<ParsedQuery>(parse(subquery_string));
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
      std::shared_ptr<ParsedQuery::GraphPatternOperation> u =
          std::make_shared<ParsedQuery::GraphPatternOperation>(
              ParsedQuery::GraphPatternOperation::Type::UNION,
              std::initializer_list<std::shared_ptr<ParsedQuery::GraphPattern>>{
                  std::make_shared<ParsedQuery::GraphPattern>(),
                  std::make_shared<ParsedQuery::GraphPattern>()});
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
    } else if (inner[k] == 'V' || inner[k] == 'v') {
      if (inner.substr(k, 6) == "VALUES" || inner.substr(k, 6) == "values") {
        size_t valuesStatementStart = k;
        k += 6;
        while (k < inner.size() && inner[k] != '(' && inner[k] != '?') {
          k++;
        }
        if (k == inner.size()) {
          throw ParseException(
              "Expected a variable name or a set of variable names after "
              "VALUES, but got " +
              inner.substr(valuesStatementStart, 16));
        }
        SparqlValues values;
        if (inner[k] == '(') {
          // The values statement has several variables
          k++;
          // find the end of the variables
          size_t varStart = k;
          while (k < inner.size() && inner[k] != ')') {
            k++;
          }
          if (k >= inner.size()) {
            throw ParseException(
                "Unterminated variable list for VALUES statement: " +
                inner.substr(valuesStatementStart, 256));
          }
          values._variables =
              ad_utility::splitWs(inner.substr(varStart, k - varStart));

          // find the beginning of the data
          while (k < inner.size() && inner[k] != '{') {
            k++;
          }
          // Skip the opening brace
          k++;
          if (k >= inner.size()) {
            throw ParseException(
                "Missing opening bracket '{' for values statement: " +
                inner.substr(valuesStatementStart, 256));
          }

          while (k < inner.size() && inner[k] != '}') {
            while (k < inner.size() && inner[k] != '(' && inner[k] != '}') {
              if (!std::isspace(inner[k])) {
                throw ParseException(
                    "Expected another row of values in values statement: " +
                    inner.substr(valuesStatementStart, 256));
              }
              k++;
            }
            if (inner[k] == '}') {
              break;
            }
            if (k >= inner.size()) {
              throw ParseException(
                  "Expected an opening bracket '(' for the values of: " +
                  inner.substr(valuesStatementStart, 256));
            }
            size_t valuesStart = k + 1;
            while (k < inner.size() && inner[k] != ')') {
              k++;
            }
            k++;
            if (k >= inner.size()) {
              throw ParseException(
                  "Missing closing bracket ')' for the values of: " +
                  inner.substr(valuesStatementStart, 256));
            }
            values._values.emplace_back(values._variables.size());
            for (size_t i = 0; i < values._variables.size(); i++) {
              while (valuesStart < inner.size() &&
                     std::isspace(inner[valuesStart])) {
                valuesStart++;
              }
              values._values.back()[i] = readTriplePart(inner, &valuesStart);
              if (valuesStart >= inner.size() || valuesStart >= k) {
                throw ParseException(
                    "Expected " + std::to_string(values._variables.size()) +
                    " values in every row of the VALUES block of " +
                    inner.substr(valuesStatementStart, 256));
              }
            }
          }
          if (k == inner.size()) {
            throw ParseException("A values block is not properly terminated: " +
                                 inner.substr(valuesStatementStart, 256));
          }

        } else {
          // The values statement has only one variable
          size_t var_start = k;
          while (k < inner.size() && !std::isspace(inner[k]) &&
                 inner[k] != '{') {
            k++;
          }
          if (k == inner.size()) {
            throw ParseException(
                "Expected values after the variable definition of the VALUES "
                "block, but reached the end of the input instead.");
          }
          values._variables.emplace_back(
              inner.substr(var_start, k - var_start));
          while (k < inner.size() && inner[k] != '{') {
            k++;
          }
          // Skip the opening brace
          k++;
          if (k == inner.size()) {
            throw ParseException(
                "Expected values after the variable definition of the VALUES "
                "block, but reached the end of the input instead.");
          }
          while (k < inner.size() && inner[k] != '}') {
            while (k < inner.size() && std::isspace(inner[k])) {
              ++k;
            }
            if (k == inner.size()) {
              throw ParseException(
                  "Expected a closing bracket but got no input for values "
                  "statement " +
                  inner.substr(valuesStatementStart, 256));
            }
            values._values.emplace_back();
            values._values.back().push_back(
                std::string(readTriplePart(inner, &k)));
          }
          if (k == inner.size()) {
            throw ParseException("A values block is not properly terminated: " +
                                 inner.substr(valuesStatementStart, 256));
          }
        }
        currentPattern->_inlineValues.emplace_back(values);
        start = k + 1;
        continue;
      }
    }

    std::string subject;
    if (lastSubject.empty()) {
      subject = readTriplePart(inner, &k);
      while (k < inner.size() && std::isspace(inner[k])) {
        ++k;
      }
      if (k == inner.size()) {
        throw ParseException(
            "Expected a subject but reached the end of input.");
      }
    } else {
      subject = lastSubject;
      lastSubject.clear();
    }
    std::string predicate;
    if (lastPredicate.empty()) {
      predicate = readTriplePart(inner, &k);
      while (k < inner.size() && std::isspace(inner[k])) {
        ++k;
      }
      if (k == inner.size()) {
        throw ParseException(
            "Expected a predicate but reached the end of input after subject " +
            subject);
      }
    } else {
      predicate = lastPredicate;
      lastPredicate.clear();
    }
    std::string clause =
        subject + " " + predicate + " " + readTriplePart(inner, &k);
    clauses.emplace_back(clause);

    // In case there is whitespace in front of the next separator skip it
    while (k < inner.size() && inner[k] != '.' && inner[k] != ';' &&
           inner[k] != ',') {
      if (!std::isspace(inner[k])) {
        throw ParseException(
            "Expected either '.', ',' or ';' after the triple " + clause);
      }
      k++;
    }

    if (k < inner.size()) {
      // Repeat the subject or the subject and predicate
      if (inner[k] == ';') {
        lastSubject = subject;
      } else if (inner[k] == ',') {
        lastSubject = subject;
        lastPredicate = predicate;
      }
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

std::string_view SparqlParser::readTriplePart(const std::string& s,
                                              size_t* pos) {
  size_t start = *pos;
  bool insideUri = false;
  bool insidePrefixed = false;
  bool insideLiteral = false;
  while (*pos < s.size()) {
    if (!insideUri && !insideLiteral && !insidePrefixed) {
      if (s[*pos] == '.' || std::isspace(static_cast<unsigned char>(s[*pos])) ||
          s[*pos] == ';' || s[*pos] == ',' || s[*pos] == '}' ||
          s[*pos] == ')') {
        return std::string_view(s.data() + start, (*pos) - start);
      }
      if (s[*pos] == '<') {
        insideUri = true;
      }
      if (s[*pos] == '\"') {
        insideLiteral = true;
      }
      if (s[*pos] == ':') {
        insidePrefixed = true;
      }
    } else if (insidePrefixed) {
      if (std::isspace(static_cast<unsigned char>(s[*pos])) || s[*pos] == '}') {
        return std::string_view(s.data() + start, (*pos) - start);
      } else if (s[*pos] == '.' || s[*pos] == ';' || s[*pos] == ',') {
        if ((*pos) + 1 >= s.size() ||
            (s[(*pos) + 1] == '?' || s[(*pos) + 1] == '<' ||
             s[(*pos) + 1] == '\"' ||
             std::isspace(static_cast<unsigned char>(s[(*pos) + 1])))) {
          insidePrefixed = false;
          // Need to reevaluate the dot as a separator
          (*pos)--;
        }
      }
    } else {
      if (insideUri && s[*pos] == '>') {
        insideUri = false;
      }
      if (insideLiteral && s[*pos] == '\"') {
        insideLiteral = false;
      }
    }
    (*pos)++;
  }

  return std::string_view(s.data() + start, (*pos) - start);
}

// _____________________________________________________________________________
void SparqlParser::addWhereTriple(
    const string& str, std::shared_ptr<ParsedQuery::GraphPattern> pattern) {
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
  if (o[0] == '"') {
    o = parseLiteral(o, true);
  }
  if (p == CONTAINS_WORD_PREDICATE || p == CONTAINS_WORD_PREDICATE_NS) {
    o = stripAndLowercaseKeywordLiteral(o);
  }
  SparqlTriple triple(s, PropertyPathParser(p).parse(), o);
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
  // Split the string at any whitespace but ignore whitespace inside
  // brackets to allow for alias parsing.
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
void SparqlParser::addFilter(
    const string& str, vector<SparqlFilter>* _filters,
    std::shared_ptr<ParsedQuery::GraphPattern> pattern) {
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
      if (rhs[0] == '"') {
        // Parse the rhs as a literal, throw an exception if it is malformed
        // or not a literal.
        rhs = parseLiteral(rhs, true);
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
            // There are no regex special chars apart from the leading '^'
            // so we can use a prefix filter.
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
      if (pred == "langMatches") {
        if (!pattern) {
          AD_THROW(ad_semsearch::Exception::BAD_QUERY,
                   "Invalid position for language filter. Probable cause: "
                   "language "
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

        auto langTag = rhs.substr(1, rhs.size() - 2);
        // first find a predicate for the given variable
        auto it =
            std::find_if(pattern->_whereClauseTriples.begin(),
                         pattern->_whereClauseTriples.end(),
                         [&lvar](const auto& tr) { return tr._o == lvar; });
        while (it != pattern->_whereClauseTriples.end() &&
               it->_p._operation == PropertyPath::Operation::IRI &&
               ad_utility::startsWith(it->_p._iri, "?")) {
          it = std::find_if(it + 1, pattern->_whereClauseTriples.end(),
                            [&lvar](const auto& tr) { return tr._o == lvar; });
        }
        if (it == pattern->_whereClauseTriples.end()) {
          LOG(INFO)
              << "language filter variable " + rhs +
                     "that did not appear as object in any suitable "
                     "triple. "
                     "using special literal-to-language triple instead.\n";
          auto langEntity = ad_utility::convertLangtagToEntityUri(langTag);
          PropertyPath taggedPredicate(PropertyPath::Operation::IRI);
          taggedPredicate._iri = LANGUAGE_PREDICATE;
          SparqlTriple triple(lvar, taggedPredicate, langEntity);
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
          PropertyPath taggedPredicate(PropertyPath::Operation::IRI);
          taggedPredicate._iri = '@' + langTag + '@' + it->_p._iri;
          *it = SparqlTriple(it->_s, taggedPredicate, it->_o);
        }

        // Convert the language filter to a special triple
        // to make it more efficient (no need for string resolution)
        return;
      }
      AD_THROW(ad_semsearch::Exception::BAD_QUERY,
               "Unknown keyword at beginning of filter: " + str);
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

  // Check if we filter on the language of the left side of the
  // filter.
  if (ad_utility::startsWith(tokens[0], "lang") && tokens[1] == "=") {
    if (!pattern) {
      AD_THROW(ad_semsearch::Exception::BAD_QUERY,
               "Invalid position for language filter. Probable cause: language "
               "filters are currently not supported in HAVING clauses.");
    }
    std::string lhs = tokens[0];
    std::string rhs = tokens[2];
    std::string lvar = lhs.substr(5, lhs.size() - 6);

    auto langTag = rhs.substr(1, rhs.size() - 2);
    // first find a predicate for the given variable
    auto it = std::find_if(pattern->_whereClauseTriples.begin(),
                           pattern->_whereClauseTriples.end(),
                           [&lvar](const auto& tr) { return tr._o == lvar; });
    while (it != pattern->_whereClauseTriples.end() &&
           it->_p._operation == PropertyPath::Operation::IRI &&
           ad_utility::startsWith(it->_p._iri, "?")) {
      it = std::find_if(it + 1, pattern->_whereClauseTriples.end(),
                        [&lvar](const auto& tr) { return tr._o == lvar; });
    }
    if (it == pattern->_whereClauseTriples.end()) {
      LOG(INFO) << "language filter variable " + rhs +
                       "that did not appear as object in any suitable triple. "
                       "using special literal-to-language triple instead.\n";
      auto langEntity = ad_utility::convertLangtagToEntityUri(langTag);
      PropertyPath taggedPredicate(PropertyPath::Operation::IRI);
      taggedPredicate._iri = LANGUAGE_PREDICATE;
      SparqlTriple triple(lvar, taggedPredicate, langEntity);
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
      PropertyPath taggedPredicate(PropertyPath::Operation::IRI);
      taggedPredicate._iri = '@' + langTag + '@' + it->_p._iri;

      *it = SparqlTriple(it->_s, taggedPredicate, it->_o);
    }

    // Convert the language filter to a special triple
    // to make it more efficient (no need for string resolution)
    return;
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

// _____________________________________________________________________________
string SparqlParser::parseLiteral(const string& literal, bool isEntireString,
                                  size_t off /*defaults to 0*/) {
  std::stringstream out;
  size_t pos = off;
  if (isEntireString) {
    // check for a leading qutation mark
    while (pos < literal.size() &&
           std::isspace(static_cast<unsigned char>(literal[pos]))) {
      pos++;
    }
    if (pos == literal.size() || literal[pos] != '"') {
      throw ParseException("The literal: " + literal +
                           " does not begin with a quotation mark.");
    }
  }
  while (pos < literal.size() && literal[pos] != '"') {
    pos++;
  }
  if (pos == literal.size()) {
    // the string does not contain a literal
    return "";
  }
  out << '"';
  pos++;
  bool escaped = false;
  while (pos < literal.size() && (escaped || literal[pos] != '"')) {
    escaped = false;
    if (literal[pos] == '\\' && pos + 1 < literal.size() &&
        literal[pos + 1] == '"') {
      // Allow for escaping " using \ but do not change any other form of
      // escaping.
      escaped = true;
    } else {
      out << literal[pos];
    }
    pos++;
  }
  out << '"';
  pos++;
  if (pos < literal.size() && literal[pos] == '@') {
    out << literal[pos];
    pos++;
    // add the language tag
    // allow for ascii based language tags (no current language tag should
    // contain non ascii letters).
    while (pos < literal.size() &&
           std::isalpha(static_cast<unsigned char>(literal[pos]))) {
      out << literal[pos];
      pos++;
    }
  }
  if (pos + 1 < literal.size() && literal[pos] == '^' &&
      literal[pos + 1] == '^') {
    // add the xsd type
    while (pos < literal.size() &&
           !std::isspace(static_cast<unsigned char>(literal[pos]))) {
      out << literal[pos];
      pos++;
    }
  }
  if (isEntireString && pos < literal.size()) {
    // check for trailing non whitespace characters
    while (pos < literal.size() &&
           std::isspace(static_cast<unsigned char>(literal[pos]))) {
      pos++;
    }
    if (pos < literal.size()) {
      throw ParseException("The literal: " + literal +
                           " was not terminated properly.");
    }
  }
  return out.str();
}
