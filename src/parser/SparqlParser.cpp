// Copyright 2014, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)

#include "./SparqlParser.h"

#include <unordered_set>

#include "../global/Constants.h"
#include "../util/Conversions.h"
#include "../util/Exception.h"
#include "../util/HashSet.h"
#include "../util/Log.h"
#include "../util/StringUtils.h"
#include "./ParseException.h"
#include "PropertyPathParser.h"

SparqlParser::SparqlParser(const string& query) : _lexer(query), _query(query) {
  LOG(DEBUG) << "Parsing " << query << std::endl;
}

// _____________________________________________________________________________
ParsedQuery SparqlParser::parse() {
  ParsedQuery result;
  result._originalString = _query;
  parsePrologue(&result);
  _lexer.expect("select");
  parseQuery(&result);
  _lexer.expectEmpty();

  return result;
}

// _____________________________________________________________________________
void SparqlParser::parseQuery(ParsedQuery* query) {
  parseSelect(query);

  _lexer.expect("{");
  parseWhere(query);

  parseSolutionModifiers(query);

  if (query->_groupByVariables.size() > 0) {
    // Check if all selected variables are either aggregated or
    // part of the group by statement.
    for (const string& var : query->_selectedVariables) {
      if (var[0] == '?') {
        bool is_alias = false;
        for (const ParsedQuery::Alias& a : query->_aliases) {
          if (a._outVarName == var) {
            is_alias = true;
            break;
          }
        }
        if (is_alias) {
          continue;
        }
        if (std::find(query->_groupByVariables.begin(),
                      query->_groupByVariables.end(),
                      var) == query->_groupByVariables.end()) {
          throw ParseException("Variable " + var +
                               " is selected but not "
                               "aggregated despite the query not being "
                               "grouped by " +
                               var + ".\n" + _lexer.input());
        }
      }
    }
  }

  ad_utility::HashMap<std::string, size_t> variable_counts;
  for (const std::string& s : query->_selectedVariables) {
    variable_counts[s]++;
  }
  for (const ParsedQuery::Alias& a : query->_aliases) {
    // The variable was already added to the selected variables while
    // parsing the alias, thus it should appear exactly once
    if (variable_counts[a._outVarName] > 1) {
      throw ParseException("The variable name " + a._outVarName +
                           " used in "
                           "an alias was already selected on.\n" +
                           _lexer.input());
    }
    variable_counts[a._outVarName];
  }
}

// _____________________________________________________________________________
void SparqlParser::parsePrologue(ParsedQuery* query) {
  while (_lexer.accept("prefix")) {
    _lexer.expect(SparqlToken::Type::IRI);
    string key = _lexer.current().raw;
    _lexer.expect(SparqlToken::Type::IRI);
    string value = _lexer.current().raw;
    addPrefix(key, value, query);
  }
}

// _____________________________________________________________________________
void SparqlParser::addPrefix(const string& key, const string& value,
                             ParsedQuery* query) {
  // Remove the trailing : from the key
  SparqlPrefix p{key.substr(0, key.size() - 1), value};
  query->_prefixes.emplace_back(p);
}

// _____________________________________________________________________________
void SparqlParser::parseSelect(ParsedQuery* query) {
  if (_lexer.accept("distinct")) {
    query->_distinct = true;
  }
  if (_lexer.accept("reduced")) {
    query->_reduced = true;
  }
  while (!_lexer.accept("where")) {
    if (_lexer.accept(SparqlToken::Type::VARIABLE)) {
      query->_selectedVariables.push_back(_lexer.current().raw);
    } else if (_lexer.accept("text")) {
      _lexer.expect("(");
      std::ostringstream s;
      s << "TEXT(";
      _lexer.expect(SparqlToken::Type::VARIABLE);
      s << _lexer.current().raw;
      _lexer.expect(")");
      s << ")";
      query->_selectedVariables.push_back(s.str());
    } else if (_lexer.accept("score")) {
      _lexer.expect("(");
      std::ostringstream s;
      s << "SCORE(";
      _lexer.expect(SparqlToken::Type::VARIABLE);
      s << _lexer.current().raw;
      _lexer.expect(")");
      s << ")";
      query->_selectedVariables.push_back(s.str());
    } else if (_lexer.accept("(")) {
      // expect an alias
      ParsedQuery::Alias a = parseAlias();
      query->_aliases.push_back(a);
      query->_selectedVariables.emplace_back(a._outVarName);
      _lexer.expect(")");
    } else {
      _lexer.accept();
      throw ParseException("Error in SELECT: unexpected token: " +
                           _lexer.current().raw);
    }
    if (_lexer.empty()) {
      throw ParseException("Keyword WHERE expected after SELECT.");
    }
  }
}

// _____________________________________________________________________________
OrderKey SparqlParser::parseOrderKey(const std::string& order,
                                     ParsedQuery* query) {
  _lexer.expect("(");
  std::ostringstream s;
  s << order << "(";
  if (_lexer.accept("score")) {
    _lexer.expect("(");
    s << "SCORE(";
    _lexer.expect(SparqlToken::Type::VARIABLE);
    s << _lexer.current().raw;
    _lexer.expect(")");
    s << ")";
  } else if (_lexer.accept("(")) {
    ParsedQuery::Alias a = parseAlias();
    for (const std::string& s : query->_selectedVariables) {
      if (s == a._outVarName) {
        throw ParseException("A variable with name " + s +
                             " is already used, but the order by with alias " +
                             a._function + " tries to use it again.");
      }
    }
    _lexer.expect(")");
    s << a._outVarName;
    query->_aliases.emplace_back(a);
  } else {
    _lexer.expect(SparqlToken::Type::VARIABLE);
    s << _lexer.current().raw;
  }
  _lexer.expect(")");
  s << ")";
  return OrderKey(s.str());
}

// _____________________________________________________________________________
ParsedQuery::Alias SparqlParser::parseAlias() {
  _lexer.expect(SparqlToken::Type::AGGREGATE);
  std::ostringstream func;
  func << ad_utility::getUppercaseUtf8(_lexer.current().raw);
  const std::string agg = _lexer.current().raw;
  ParsedQuery::Alias a;
  if (agg == "count") {
    a._type = ParsedQuery::AggregateType::COUNT;
  } else if (agg == "sample") {
    a._type = ParsedQuery::AggregateType::SAMPLE;
  } else if (agg == "min") {
    a._type = ParsedQuery::AggregateType::MIN;
  } else if (agg == "max") {
    a._type = ParsedQuery::AggregateType::MAX;
  } else if (agg == "sum") {
    a._type = ParsedQuery::AggregateType::SUM;
  } else if (agg == "avg") {
    a._type = ParsedQuery::AggregateType::AVG;
  } else if (agg == "group_concat") {
    a._type = ParsedQuery::AggregateType::GROUP_CONCAT;
  } else {
    throw ParseException("Unknown aggregate " + agg);
  }
  a._isAggregate = true;

  _lexer.expect("(");
  if (_lexer.accept("distinct")) {
    func << "DISTINCT ";
    a._isDistinct = true;
  }
  _lexer.expect(SparqlToken::Type::VARIABLE);
  a._inVarName = _lexer.current().raw;
  func << "(" << a._inVarName;
  if (_lexer.accept(";")) {
    if (agg != "group_concat") {
      throw ParseException(
          "Only GROUP_CONCAT may have additional arguments passed to it.");
    }
    _lexer.expect("separator");
    _lexer.expect("=");
    _lexer.expect(SparqlToken::Type::RDFLITERAL);
    func << ";SEPARATOR=" << _lexer.current().raw;
    a._delimiter = _lexer.current().raw;
    // Remove the enclosing quotation marks
    a._delimiter = a._delimiter.substr(1, a._delimiter.size() - 2);
  }
  _lexer.expect(")");
  _lexer.expect("as");
  _lexer.expect(SparqlToken::Type::VARIABLE);
  a._outVarName = _lexer.current().raw;
  func << ") as " << a._outVarName;

  a._function = func.str();
  return a;
}

// _____________________________________________________________________________
void SparqlParser::parseWhere(
    ParsedQuery* query,
    std::shared_ptr<ParsedQuery::GraphPattern> currentPattern) {
  if (currentPattern == nullptr) {
    // Make the shared pointer point to the root graphpattern without deleting
    // it.
    currentPattern = query->_rootGraphPattern;
    query->_rootGraphPattern->_id = 0;
  }

  // If these are not empty the last subject and / or predicate is reused
  std::string lastSubject;
  std::string lastPredicate;
  while (!_lexer.accept("}")) {
    if (_lexer.empty()) {
      throw ParseException(
          "Expected a closing bracket for WHERE but reached "
          "the end of the input.");
    }
    if (_lexer.accept("optional")) {
      currentPattern->_children.push_back(
          std::make_shared<ParsedQuery::GraphPatternOperation>(
              ParsedQuery::GraphPatternOperation::Type::OPTIONAL,
              std::initializer_list<std::shared_ptr<ParsedQuery::GraphPattern>>{
                  std::make_shared<ParsedQuery::GraphPattern>()}));
      currentPattern->_children.back()->_childGraphPatterns[0]->_optional =
          true;
      currentPattern->_children.back()->_childGraphPatterns[0]->_id =
          query->_numGraphPatterns;
      query->_numGraphPatterns++;
      _lexer.expect("{");
      // Recursively call parseWhere to parse the optional part.
      parseWhere(query,
                 currentPattern->_children.back()->_childGraphPatterns[0]);
      _lexer.accept(".");
    } else if (_lexer.accept("{")) {
      // Subquery or union
      if (_lexer.accept("select")) {
        // subquery
        // create the subquery operation
        std::shared_ptr<ParsedQuery::GraphPatternOperation> u =
            std::make_shared<ParsedQuery::GraphPatternOperation>(
                ParsedQuery::GraphPatternOperation::Type::SUBQUERY);
        u->_subquery = std::make_shared<ParsedQuery>();
        parseQuery(u->_subquery.get());
        // Remove all manual ordering from the subquery as it would be changed
        // by the parent query.
        u->_subquery->_orderBy.clear();
        currentPattern->_children.push_back(u);
        // The closing bracked } is consumed by the subquery
        _lexer.accept(".");
      } else {
        // union
        // create the union operation
        std::shared_ptr<ParsedQuery::GraphPatternOperation> u =
            std::make_shared<ParsedQuery::GraphPatternOperation>(
                ParsedQuery::GraphPatternOperation::Type::UNION,
                std::initializer_list<
                    std::shared_ptr<ParsedQuery::GraphPattern>>{
                    std::make_shared<ParsedQuery::GraphPattern>(),
                    std::make_shared<ParsedQuery::GraphPattern>()});
        u->_childGraphPatterns[0]->_optional = false;
        u->_childGraphPatterns[1]->_optional = false;
        u->_childGraphPatterns[0]->_id = query->_numGraphPatterns;
        u->_childGraphPatterns[1]->_id = query->_numGraphPatterns + 1;
        query->_numGraphPatterns += 2;
        currentPattern->_children.push_back(u);

        // parse the left and right bracket
        parseWhere(query, u->_childGraphPatterns[0]);
        _lexer.expect("union");
        _lexer.expect("{");
        parseWhere(query, u->_childGraphPatterns[1]);
        _lexer.accept(".");
      }
    } else if (_lexer.accept("filter")) {
      parseFilter(&currentPattern->_filters, true, currentPattern);
      // A filter may have an optional dot after it
      _lexer.accept(".");
    } else if (_lexer.accept("values")) {
      SparqlValues values;
      if (_lexer.accept("(")) {
        // values with several variables
        while (_lexer.accept(SparqlToken::Type::VARIABLE)) {
          values._variables.push_back(_lexer.current().raw);
        }
        _lexer.expect(")");
        _lexer.expect("{");
        while (_lexer.accept("(")) {
          values._values.emplace_back(values._variables.size());
          for (size_t i = 0; i < values._variables.size(); i++) {
            if (!_lexer.accept(SparqlToken::Type::RDFLITERAL)) {
              _lexer.expect(SparqlToken::Type::IRI);
            }
            values._values.back()[i] = _lexer.current().raw;
          }
          _lexer.expect(")");
        }
        _lexer.expect("}");
      } else if (_lexer.accept(SparqlToken::Type::VARIABLE)) {
        // values with a single variable
        values._variables.push_back(_lexer.current().raw);
        _lexer.expect("{");
        while (_lexer.accept(SparqlToken::Type::IRI) ||
               _lexer.accept(SparqlToken::Type::RDFLITERAL)) {
          values._values.emplace_back(1);
          values._values.back()[0] = _lexer.current().raw;
        }
        _lexer.expect("}");
      } else {
        throw ParseException(
            "Expected either a single or a set of variables "
            "after VALUES");
      }
      currentPattern->_inlineValues.emplace_back(values);
      _lexer.accept(".");
    } else {
      std::string subject;
      if (lastSubject.empty()) {
        if (_lexer.accept(SparqlToken::Type::VARIABLE)) {
          subject = _lexer.current().raw;
        } else if (_lexer.accept(SparqlToken::Type::RDFLITERAL)) {
          subject = parseLiteral(_lexer.current().raw, true);
        } else {
          _lexer.expect(SparqlToken::Type::IRI);
          subject = _lexer.current().raw;
        }
      } else {
        subject = lastSubject;
        lastSubject.clear();
      }

      std::string predicate;
      if (lastPredicate.empty()) {
        if (_lexer.accept(SparqlToken::Type::VARIABLE)) {
          predicate = _lexer.current().raw;
        } else if (_lexer.accept(SparqlToken::Type::RDFLITERAL)) {
          predicate = parseLiteral(_lexer.current().raw, true);
        } else {
          // Assume the token is a predicate path. This will be verified
          // separately later.
          _lexer.expandNextUntilWhitespace();
          _lexer.accept();
          predicate = _lexer.current().raw;
        }
      } else {
        predicate = lastPredicate;
        lastPredicate.clear();
      }

      std::string object;
      if (_lexer.accept(SparqlToken::Type::VARIABLE)) {
        object = _lexer.current().raw;
      } else if (_lexer.accept(SparqlToken::Type::RDFLITERAL)) {
        object = parseLiteral(_lexer.current().raw, true);
      } else {
        _lexer.expect(SparqlToken::Type::IRI);
        object = _lexer.current().raw;
      }

      if (predicate == CONTAINS_WORD_PREDICATE ||
          predicate == CONTAINS_WORD_PREDICATE_NS) {
        object = stripAndLowercaseKeywordLiteral(object);
      }

      SparqlTriple triple(subject, PropertyPathParser(predicate).parse(),
                          object);
      if (std::find(currentPattern->_whereClauseTriples.begin(),
                    currentPattern->_whereClauseTriples.end(),
                    triple) != currentPattern->_whereClauseTriples.end()) {
        LOG(INFO) << "Ignoring duplicate triple: " << subject << ' '
                  << predicate << ' ' << object << std::endl;
      } else {
        currentPattern->_whereClauseTriples.push_back(triple);
      }

      if (_lexer.accept(";")) {
        lastSubject = subject;
      } else if (_lexer.accept(",")) {
        lastSubject = subject;
        lastPredicate = predicate;
      } else if (_lexer.accept("}")) {
        break;
      } else {
        _lexer.expect(".");
      }
    }
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
void SparqlParser::parseSolutionModifiers(ParsedQuery* query) {
  while (!_lexer.empty() && !_lexer.accept("}")) {
    if (_lexer.accept("order")) {
      _lexer.expect("by");
      bool reached_end = false;
      while (!reached_end) {
        if (_lexer.accept(SparqlToken::Type::VARIABLE)) {
          query->_orderBy.emplace_back(OrderKey(_lexer.current().raw));
        } else if (_lexer.accept("asc")) {
          query->_orderBy.emplace_back(parseOrderKey("ASC", query));
        } else if (_lexer.accept("desc")) {
          query->_orderBy.emplace_back(parseOrderKey("DESC", query));
        } else {
          reached_end = true;
          if (query->_orderBy.empty()) {
            // Need at least one statement after the order by
            throw ParseException(
                "Expected either a variable or ASC/DESC after "
                "ORDER BY.");
          }
        }
      }
    } else if (_lexer.accept("limit")) {
      _lexer.expect(SparqlToken::Type::INTEGER);
      query->_limit = _lexer.current().raw;
    } else if (_lexer.accept("textlimit")) {
      _lexer.expect(SparqlToken::Type::INTEGER);
      query->_textLimit = _lexer.current().raw;
    } else if (_lexer.accept("offset")) {
      _lexer.expect(SparqlToken::Type::INTEGER);
      query->_offset = _lexer.current().raw;
    } else if (_lexer.accept("group")) {
      _lexer.expect("by");
      _lexer.expect(SparqlToken::Type::VARIABLE);
      query->_groupByVariables.emplace_back(_lexer.current().raw);
      while (_lexer.accept(SparqlToken::Type::VARIABLE)) {
        query->_groupByVariables.emplace_back(_lexer.current().raw);
      }
    } else if (_lexer.accept("having")) {
      parseFilter(&query->_havingClauses, true, query->_rootGraphPattern);
      while (parseFilter(&query->_havingClauses, false,
                         query->_rootGraphPattern)) {
      }
    } else if (_lexer.accept("textlimit")) {
      _lexer.expect(SparqlToken::Type::INTEGER);
      query->_textLimit = std::stoi(_lexer.current().raw);
    } else {
      _lexer.accept();
      throw ParseException("Expected a solution modifier but got " +
                           _lexer.current().raw);
    }
  }
}

// _____________________________________________________________________________
bool SparqlParser::parseFilter(
    vector<SparqlFilter>* _filters, bool failOnNoFilter,
    std::shared_ptr<ParsedQuery::GraphPattern> pattern) {
  if (_lexer.accept("(")) {
    if (_lexer.accept("lang")) {
      _lexer.expect("(");
      _lexer.expect(SparqlToken::Type::VARIABLE);
      std::string lhs = _lexer.current().raw;
      _lexer.expect(")");
      _lexer.expect("=");
      _lexer.expect(SparqlToken::Type::RDFLITERAL);
      std::string rhs = _lexer.current().raw;
      _lexer.expect(")");
      addLangFilter(lhs, rhs, pattern);
      return true;
    }
    SparqlFilter f;
    if (_lexer.accept("str")) {
      _lexer.expect("(");
      f._lhsAsString = true;
    }
    // LHS
    if (_lexer.accept(SparqlToken::Type::IRI)) {
      f._lhs = _lexer.current().raw;
    } else if (_lexer.accept(SparqlToken::Type::VARIABLE)) {
      f._lhs = _lexer.current().raw;
    } else if (_lexer.accept(SparqlToken::Type::RDFLITERAL)) {
      f._lhs = _lexer.current().raw;
    } else if (_lexer.accept(SparqlToken::Type::INTEGER)) {
      f._lhs = _lexer.current().raw;
    } else if (_lexer.accept(SparqlToken::Type::FLOAT)) {
      f._lhs = _lexer.current().raw;
    } else {
      _lexer.accept();
      throw ParseException(_lexer.current().raw +
                           " is not a valid left hand side for a filter.");
    }
    if (f._lhsAsString) {
      _lexer.expect(")");
    }
    // TYPE
    if (_lexer.accept("=")) {
      f._type = SparqlFilter::EQ;
    } else if (_lexer.accept("!")) {
      _lexer.expect("=");
      f._type = SparqlFilter::NE;
    } else if (_lexer.accept("<")) {
      if (_lexer.accept("=")) {
        f._type = SparqlFilter::LE;
      } else {
        f._type = SparqlFilter::LT;
      }
    } else if (_lexer.accept(">")) {
      if (_lexer.accept("=")) {
        f._type = SparqlFilter::GE;
      } else {
        f._type = SparqlFilter::GT;
      }
    } else {
      _lexer.accept();
      throw ParseException(_lexer.current().raw +
                           " is not a valid relation for a filter.");
    }
    // RHS
    if (_lexer.accept(SparqlToken::Type::IRI)) {
      f._rhs = _lexer.current().raw;
    } else if (_lexer.accept(SparqlToken::Type::VARIABLE)) {
      f._rhs = _lexer.current().raw;
    } else if (_lexer.accept(SparqlToken::Type::RDFLITERAL)) {
      // Resolve escaped characters
      f._rhs = parseLiteral(_lexer.current().raw, true);
    } else if (_lexer.accept(SparqlToken::Type::INTEGER)) {
      f._rhs = _lexer.current().raw;
    } else if (_lexer.accept(SparqlToken::Type::FLOAT)) {
      f._rhs = _lexer.current().raw;
    } else {
      _lexer.accept();
      throw ParseException(_lexer.current().raw +
                           " is not a valid right hand side for a filter.");
    }
    _lexer.expect(")");
    _filters->emplace_back(f);
    return true;
  } else if (_lexer.accept("langmatches")) {
    _lexer.expect("(");
    _lexer.expect("lang");
    _lexer.expect("(");
    _lexer.expect(SparqlToken::Type::VARIABLE);
    std::string lhs = _lexer.current().raw;
    _lexer.expect(")");
    _lexer.expect(",");
    _lexer.expect(SparqlToken::Type::RDFLITERAL);
    std::string rhs = parseLiteral(_lexer.current().raw, true);
    _lexer.expect(")");
    addLangFilter(lhs, rhs, pattern);
    return true;
  } else if (_lexer.accept("regex")) {
    SparqlFilter f;
    f._type = SparqlFilter::REGEX;
    _lexer.expect("(");
    if (_lexer.accept("str")) {
      _lexer.expect("(");
      f._lhsAsString = true;
    }
    _lexer.expect(SparqlToken::Type::VARIABLE);
    f._lhs = _lexer.current().raw;
    if (f._lhsAsString) {
      _lexer.expect(")");
    }
    _lexer.expect(",");
    _lexer.expect(SparqlToken::Type::RDFLITERAL);
    f._rhs = parseLiteral(_lexer.current().raw, true);
    // Remove the enlcosing quotation marks
    f._rhs = f._rhs.substr(1, f._rhs.size() - 2);
    if (_lexer.accept(",")) {
      _lexer.expect("\"i\"");
      f._regexIgnoreCase = true;
    }
    _lexer.expect(")");
    if (f._rhs[0] == '^' && !f._regexIgnoreCase) {
      // Check if we can use the more efficient prefix filter instead
      // of an expensive regex filter.
      bool isSimple = true;
      bool escaped = false;
      std::vector<size_t> escapePositions;  // position of backslashes that are used for escaping within the regex
      // these have to be removed if the regex is simply a prefix filter.

      // Check if the regex is only a prefix regex or also does
      // anything else.
      const static string regexControlChars = "[]^$.|?*+()";
      for (size_t i = 1; isSimple && i < f._rhs.size(); i++) {
        if (f._rhs[i] == '\\') {
          if (!escaped) {
            escapePositions.push_back(i);
          }
          escaped = !escaped;  // correctly deal with consecutive backslashes
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

        // we have to remove the escaping backslashes
        for (auto it = escapePositions.rbegin(); it != escapePositions.rend(); ++it) {
          f._rhs.erase(f._rhs.begin() + *it);
        }
      }
    }
    _filters->emplace_back(f);
    return true;
  } else if (_lexer.accept("prefix")) {
    _lexer.expect("(");
    SparqlFilter f1;
    SparqlFilter f2;
    f1._type = SparqlFilter::GE;
    f2._type = SparqlFilter::LT;
    // Do prefix filtering by using two filters (one testing >=, the other =)
    _lexer.expect(SparqlToken::Type::VARIABLE);
    f1._lhs = _lexer.current().raw;
    f2._lhs = f1._lhs;
    _lexer.expect(",");
    _lexer.expect(SparqlToken::Type::RDFLITERAL);
    f1._rhs = _lexer.current().raw;
    f2._rhs = f1._lhs;
    f1._rhs = f1._rhs.substr(0, f1._rhs.size() - 1) + " ";
    f2._rhs = f2._rhs.substr(0, f2._rhs.size() - 2);
    f2._rhs += f1._rhs[f1._rhs.size() - 2] + 1;
    f2._rhs += f1._rhs[f1._rhs.size() - 1];
    _filters->emplace_back(f1);
    _filters->emplace_back(f2);
    _lexer.expect(")");
    return true;
  } else if (failOnNoFilter) {
    _lexer.accept();
    throw ParseException("Expected a filter but got " + _lexer.current().raw);
  }
  return false;
}

void SparqlParser::addLangFilter(
    const std::string& lhs, const std::string& rhs,
    std::shared_ptr<ParsedQuery::GraphPattern> pattern) {
  auto langTag = rhs.substr(1, rhs.size() - 2);
  // First find a suitable triple for the given variable. It
  // must use a predicate that is not a variable or complex
  // predicate path
  auto it =
      std::find_if(pattern->_whereClauseTriples.begin(),
                   pattern->_whereClauseTriples.end(), [&lhs](const auto& tr) {
                     return tr._o == lhs &&
                            (tr._p._operation == PropertyPath::Operation::IRI &&
                             !isVariable(tr._p));
                   });
  if (it == pattern->_whereClauseTriples.end()) {
    LOG(DEBUG) << "language filter variable " + lhs +
                      " did not appear as object in any suitable "
                      "triple. "
                      "Using literal-to-language predicate instead.\n";
    auto langEntity = ad_utility::convertLangtagToEntityUri(langTag);
    PropertyPath taggedPredicate(PropertyPath::Operation::IRI);
    taggedPredicate._iri = LANGUAGE_PREDICATE;
    SparqlTriple triple(lhs, taggedPredicate, langEntity);
    // Quadratic in number of triples in query.
    // Shouldn't be a problem here, though.
    // Could use a (hash)-set instead of vector.
    if (std::find(pattern->_whereClauseTriples.begin(),
                  pattern->_whereClauseTriples.end(),
                  triple) != pattern->_whereClauseTriples.end()) {
      LOG(DEBUG) << "Ignoring duplicate triple: lang(" << lhs << ") = " << rhs
                 << std::endl;
    } else {
      pattern->_whereClauseTriples.push_back(triple);
    }
  } else {
    // replace the triple
    PropertyPath taggedPredicate(PropertyPath::Operation::IRI);
    taggedPredicate._iri = '@' + langTag + '@' + it->_p._iri;
    SparqlTriple taggedTriple(it->_s, taggedPredicate, it->_o);
    LOG(DEBUG) << "replacing predicate " << it->_p.asString() << " with "
               << taggedTriple._p.asString() << std::endl;
    *it = taggedTriple;
  }
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
  // The delimiter of the string. Either ' or "
  char delimiter = '"';
  if (isEntireString) {
    // check for a leading qutation mark
    while (pos < literal.size() &&
           std::isspace(static_cast<unsigned char>(literal[pos]))) {
      pos++;
    }
    if (pos == literal.size() ||
        (literal[pos] != '"' && literal[pos] != '\'')) {
      throw ParseException("The literal: " + literal +
                           " does not begin with a quotation mark.");
    }
  }
  while (pos < literal.size() && literal[pos] != '"' && literal[pos] != '\'') {
    pos++;
  }
  if (pos == literal.size()) {
    // the string does not contain a literal
    return "";
  }
  delimiter = literal[pos];
  out << '"';
  pos++;
  bool escaped = false;
  while (pos < literal.size() && (escaped || literal[pos] != delimiter)) {
    escaped = false;
    if (literal[pos] == '\\' && pos + 1 < literal.size() &&
        literal[pos + 1] == delimiter) {
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
