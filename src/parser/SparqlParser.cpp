// Copyright 2014, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)

#include "./SparqlParser.h"

#include <unordered_set>
#include <variant>

#include "./SparqlParserHelpers.h"
#include "PropertyPathParser.h"

using namespace std::literals::string_literals;

SparqlParser::SparqlParser(const string& query) : _lexer(query), _query(query) {
  LOG(DEBUG) << "Parsing " << query << std::endl;
}

// _____________________________________________________________________________
ParsedQuery SparqlParser::parse() {
  ParsedQuery result;
  result._originalString = _query;
  parsePrologue(&result);
  if (_lexer.accept("construct")) {
    parseQuery(&result, CONSTRUCT_QUERY);
  } else {
    _lexer.expect("select");
    parseQuery(&result, SELECT_QUERY);
  }
  _lexer.expectEmpty();

  return result;
}

// _____________________________________________________________________________
void SparqlParser::parseQuery(ParsedQuery* query, QueryType queryType) {
  if (queryType == CONSTRUCT_QUERY) {
    auto str = _lexer.getUnconsumedInput();
    SparqlQleverVisitor::PrefixMap prefixes;
    for (const auto& prefix : query->_prefixes) {
      prefixes[prefix._prefix] = prefix._uri;
    }
    auto parseResult =
        sparqlParserHelpers::parseConstructTemplate(str, std::move(prefixes));
    query->_clause = std::move(parseResult._resultOfParse);
    _lexer.reset(std::move(parseResult._remainingText));
    _lexer.expect("where");
  } else if (queryType == SELECT_QUERY) {
    parseSelect(query);
  } else {
    // Unsupported query type
    AD_CHECK(false);
  }

  _lexer.expect("{");
  parseWhere(query);

  parseSolutionModifiers(query);

  if (!query->_groupByVariables.empty()) {
    if (query->hasSelectClause() &&
        query->selectClause()._varsOrAsterisk.isManuallySelectedVariables()) {
      const auto& selectClause = query->selectClause();
      // Check if all selected variables are either aggregated or
      for (const string& var :
           selectClause._varsOrAsterisk.getSelectedVariables()) {
        if (var[0] == '?') {
          bool is_alias = false;
          for (const ParsedQuery::Alias& a : selectClause._aliases) {
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
    } else if (query->hasSelectClause() &&
               query->selectClause()._varsOrAsterisk.isAllVariablesSelected()) {
      throw ParseException(
          "GROUP BY is not allowed when all variables are selected via SELECT "
          "*");
    } else if (query->hasConstructClause()) {
      auto& constructClause = query->constructClause();
      for (const auto& triple : constructClause) {
        for (const auto& varOrTerm : triple) {
          if (auto variable = std::get_if<Variable>(&varOrTerm)) {
            const auto& var = variable->name();
            if (std::find(query->_groupByVariables.begin(),
                          query->_groupByVariables.end(),
                          var) == query->_groupByVariables.end()) {
              throw ParseException("Variable " + var +
                                   " is used but not "
                                   "aggregated despite the query not being "
                                   "grouped by " +
                                   var + ".\n" + _lexer.input());
            }
          }
        }
      }
    } else {
      // Invalid clause type
      AD_CHECK(false);
    }
  }
  if (!query->hasSelectClause()) {
    return;
  }
  const auto& selectClause = query->selectClause();

  ad_utility::HashMap<std::string, size_t> variable_counts;
  if (selectClause._varsOrAsterisk.isManuallySelectedVariables()) {
    for (const std::string& s :
         selectClause._varsOrAsterisk.getSelectedVariables()) {
      variable_counts[s]++;
    }
  }

  for (const ParsedQuery::Alias& a : selectClause._aliases) {
    // The variable was already added to the selected variables while
    // parsing the alias, thus it should appear exactly once
    if (variable_counts[a._outVarName] > 1) {
      throw ParseException("The variable name " + a._outVarName +
                           " used in "
                           "an alias was already selected on.\n" +
                           _lexer.input());
    }
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
  auto& selectClause = query->selectClause();
  if (_lexer.accept("distinct")) {
    selectClause._distinct = true;
  }
  if (_lexer.accept("reduced")) {
    selectClause._reduced = true;
  }
  if (_lexer.accept("*")) {
    selectClause._varsOrAsterisk.setAllVariablesSelected();
  }
  std::vector<std::string> manuallySelectedVariables;
  while (!_lexer.accept("where")) {
    if (selectClause._varsOrAsterisk.isAllVariablesSelected()) {
      throw ParseException("Keyword WHERE expected after SELECT '*'");
    }
    if (_lexer.accept(SparqlToken::Type::VARIABLE)) {
      manuallySelectedVariables.push_back(_lexer.current().raw);
    } else if (_lexer.accept("text")) {
      _lexer.expect("(");
      std::ostringstream s;
      s << "TEXT(";
      _lexer.expect(SparqlToken::Type::VARIABLE);
      s << _lexer.current().raw;
      _lexer.expect(")");
      s << ")";
      manuallySelectedVariables.push_back(std::move(s).str());
    } else if (_lexer.accept("score")) {
      _lexer.expect("(");
      std::ostringstream s;
      s << "SCORE(";
      _lexer.expect(SparqlToken::Type::VARIABLE);
      s << _lexer.current().raw;
      _lexer.expect(")");
      s << ")";
      manuallySelectedVariables.push_back(std::move(s).str());
    } else if (_lexer.accept("(")) {
      // Expect an alias.
      ParsedQuery::Alias a = parseAliasWithAntlr(*query);
      selectClause._aliases.push_back(a);
      manuallySelectedVariables.push_back(a._outVarName);
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
  if (selectClause._varsOrAsterisk.isManuallySelectedVariables()) {
    selectClause._varsOrAsterisk.setManuallySelected(manuallySelectedVariables);
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
  } else if (query->hasSelectClause() &&
             query->selectClause()
                 ._varsOrAsterisk.isManuallySelectedVariables() &&
             _lexer.accept("(")) {
    // TODO This assumes that aliases can stand in the ORDER BY
    // This is not true, only expression may stand there
    ParsedQuery::Alias a = parseAliasWithAntlr(*query);
    auto& selectClause = query->selectClause();

    for (const auto& selectedVariable :
         selectClause._varsOrAsterisk.getSelectedVariables()) {
      if (selectedVariable == a._outVarName) {
        throw ParseException("A variable with name " + selectedVariable +
                             " is already used, but the ORDER BY with alias " +
                             a._expression.getDescriptor() +
                             " tries to use it again.");
      }
    }
    _lexer.expect(")");
    s << a._outVarName;
    selectClause._aliases.emplace_back(a);
  } else {
    _lexer.expect(SparqlToken::Type::VARIABLE);
    s << _lexer.current().raw;
  }
  _lexer.expect(")");
  s << ")";
  return OrderKey(std::move(s).str());
}

// _____________________________________________________________________________
void SparqlParser::parseWhere(ParsedQuery* query,
                              ParsedQuery::GraphPattern* currentPattern) {
  if (currentPattern == nullptr) {
    // Make the shared pointer point to the root graphpattern without deleting
    // it.
    currentPattern = &query->_rootGraphPattern;
    query->_rootGraphPattern._id = 0;
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
      currentPattern->_children.emplace_back(
          GraphPatternOperation::Optional{ParsedQuery::GraphPattern()});
      auto& opt = currentPattern->_children.back()
                      .get<GraphPatternOperation::Optional>();
      auto& child = opt._child;
      child._optional = true;
      child._id = query->_numGraphPatterns;
      query->_numGraphPatterns++;
      _lexer.expect("{");
      // Recursively call parseWhere to parse the optional part.
      parseWhere(query, &child);
      _lexer.accept(".");
    } else if (_lexer.accept("bind")) {
      _lexer.expect("(");
      GraphPatternOperation::Bind bind{parseExpressionWithAntlr(*query), ""s};
      _lexer.expect("as");
      _lexer.expect(SparqlToken::Type::VARIABLE);
      query->registerVariableVisibleInQueryBody(_lexer.current().raw);
      bind._target = _lexer.current().raw;
      _lexer.expect(")");
      currentPattern->_children.emplace_back(std::move(bind));
      // The dot after a BIND is optional.
      _lexer.accept(".");
    } else if (_lexer.accept("minus")) {
      currentPattern->_children.emplace_back(
          GraphPatternOperation::Minus{ParsedQuery::GraphPattern()});
      auto& opt =
          currentPattern->_children.back().get<GraphPatternOperation::Minus>();
      auto& child = opt._child;
      child._optional = false;
      child._id = query->_numGraphPatterns;
      query->_numGraphPatterns++;
      _lexer.expect("{");
      // Recursively call parseWhere to parse the subtrahend.
      parseWhere(query, &child);
      _lexer.accept(".");
    } else if (_lexer.accept("{")) {
      // Subquery or union
      if (_lexer.accept("select")) {
        // subquery
        // create the subquery operation
        GraphPatternOperation::Subquery subq;
        subq._subquery._prefixes = query->_prefixes;
        parseQuery(&subq._subquery, SELECT_QUERY);

        // Add the variables from the subquery that are visible to the outside
        // (because they were selected, or because of a SELECT *) to the outer
        // query.
        ParsedQuery::SelectedVarsOrAsterisk varsOrAsteriskFromSubquery =
            subq._subquery.selectClause()._varsOrAsterisk;
        auto selectedVariablesFromSubquery =
            varsOrAsteriskFromSubquery.getSelectedVariables();
        for (const auto& variable : selectedVariablesFromSubquery) {
          query->registerVariableVisibleInQueryBody(variable);
        }

        currentPattern->_children.emplace_back(std::move(subq));
        // The closing bracked } is consumed by the subquery
        _lexer.accept(".");
      } else {
        // union
        // create the union operation
        auto un = GraphPatternOperation::Union{ParsedQuery::GraphPattern{},
                                               ParsedQuery::GraphPattern{}};
        un._child1._optional = false;
        un._child2._optional = false;
        un._child1._id = query->_numGraphPatterns;
        un._child2._id = query->_numGraphPatterns + 1;
        query->_numGraphPatterns += 2;

        // parse the left and right bracket
        parseWhere(query, &un._child1);
        _lexer.expect("union");
        _lexer.expect("{");
        parseWhere(query, &un._child2);
        _lexer.accept(".");
        currentPattern->_children.emplace_back(std::move(un));
      }
    } else if (_lexer.accept("filter")) {
      // append to the global filters of the pattern.
      parseFilter(&currentPattern->_filters, true, currentPattern);
      // A filter may have an optional dot after it
      _lexer.accept(".");
    } else if (_lexer.accept("values")) {
      SparqlValues values;
      if (_lexer.accept("(")) {
        // values with several variables
        while (_lexer.accept(SparqlToken::Type::VARIABLE)) {
          values._variables.push_back(_lexer.current().raw);
          query->registerVariableVisibleInQueryBody(_lexer.current().raw);
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
        query->registerVariableVisibleInQueryBody(_lexer.current().raw);
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
      currentPattern->_children.emplace_back(
          GraphPatternOperation::Values{std::move(values)});
      _lexer.accept(".");
    } else {
      std::string subject;
      if (lastSubject.empty()) {
        if (_lexer.accept(SparqlToken::Type::VARIABLE)) {
          subject = _lexer.current().raw;
          query->registerVariableVisibleInQueryBody(_lexer.current().raw);
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
          query->registerVariableVisibleInQueryBody(_lexer.current().raw);
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
        query->registerVariableVisibleInQueryBody(_lexer.current().raw);
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
      auto& v = lastBasicPattern(currentPattern)._whereClauseTriples;
      if (std::find(v.begin(), v.end(), triple) != v.end()) {
        LOG(INFO) << "Ignoring duplicate triple: " << subject << ' '
                  << predicate << ' ' << object << std::endl;
      } else {
        v.push_back(triple);
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
void SparqlParser::parseSolutionModifiers(ParsedQuery* query) {
  while (!_lexer.empty() && !_lexer.accept("}")) {
    if (_lexer.accept(SparqlToken::Type::ORDER_BY)) {
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
      query->_limit = std::stoul(_lexer.current().raw);
    } else if (_lexer.accept("textlimit")) {
      _lexer.expect(SparqlToken::Type::INTEGER);
      query->_textLimit = _lexer.current().raw;
    } else if (_lexer.accept("offset")) {
      _lexer.expect(SparqlToken::Type::INTEGER);
      query->_offset = std::stoul(_lexer.current().raw);
    } else if (_lexer.accept(SparqlToken::Type::GROUP_BY)) {
      _lexer.expect(SparqlToken::Type::VARIABLE);
      query->_groupByVariables.emplace_back(_lexer.current().raw);
      while (_lexer.accept(SparqlToken::Type::VARIABLE)) {
        query->_groupByVariables.emplace_back(_lexer.current().raw);
      }
    } else if (_lexer.accept("having")) {
      parseFilter(&query->_havingClauses, true, &query->_rootGraphPattern);
      while (parseFilter(&query->_havingClauses, false,
                         &query->_rootGraphPattern)) {
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
bool SparqlParser::parseFilter(vector<SparqlFilter>* _filters,
                               bool failOnNoFilter,
                               ParsedQuery::GraphPattern* pattern) {
  size_t numParentheses = 0;
  while (_lexer.accept("(")) {
    numParentheses++;
  }
  auto expectClose = [numParentheses, this]() mutable {
    while (numParentheses) {
      _lexer.expect(")");
      numParentheses--;
    }
  };
  if (_lexer.accept("lang") && numParentheses) {
    _lexer.expect("(");
    _lexer.expect(SparqlToken::Type::VARIABLE);
    std::string lhs = _lexer.current().raw;
    _lexer.expect(")");
    _lexer.expect("=");
    _lexer.expect(SparqlToken::Type::RDFLITERAL);
    std::string rhs = _lexer.current().raw;
    expectClose();
    addLangFilter(lhs, rhs, pattern);
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
    std::string rhs = _lexer.current().raw;
    expectClose();
    addLangFilter(lhs, rhs, pattern);
    return true;
  } else if (_lexer.accept("regex")) {
    std::vector<SparqlFilter> v;
    v.push_back(parseRegexFilter(false));
    if (numParentheses) {
      while (_lexer.accept(SparqlToken::Type::LOGICAL_OR)) {
        v.push_back(parseRegexFilter(true));
      }
    }
    if (!(v.size() == 1 || std::all_of(v.begin(), v.end(), [](const auto& f) {
            return f._type == SparqlFilter::PREFIX;
          }))) {
      throw ParseException(
          "Multiple regex filters concatenated via || must currently all be "
          "PREFIX filters");
    }
    // merge the prefix filters (does nothing in case of a single regex filter
    for (auto it = v.begin() + 1; it < v.end(); ++it) {
      v[0]._additionalLhs.push_back(std::move(it->_lhs));
      v[0]._additionalPrefixes.push_back(std::move(it->_rhs));
    }
    _filters->push_back(v[0]);
    expectClose();
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
    expectClose();
    return true;
  } else if (numParentheses) {
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
      f._rhs = _lexer.current().raw;
    } else if (_lexer.accept(SparqlToken::Type::INTEGER)) {
      f._rhs = _lexer.current().raw;
    } else if (_lexer.accept(SparqlToken::Type::FLOAT)) {
      f._rhs = _lexer.current().raw;
    } else {
      _lexer.accept();
      throw ParseException(_lexer.current().raw +
                           " is not a valid right hand side for a filter.");
    }
    expectClose();
    _filters->emplace_back(f);
    return true;

  } else if (failOnNoFilter) {
    _lexer.accept();
    throw ParseException("Expected a filter but got " + _lexer.current().raw);
  }
  expectClose();
  return false;
}

void SparqlParser::addLangFilter(const std::string& lhs, const std::string& rhs,
                                 ParsedQuery::GraphPattern* pattern) {
  auto langTag = rhs.substr(1, rhs.size() - 2);
  // First find a suitable triple for the given variable. It
  // must use a predicate that is not a variable or complex
  // predicate path
  auto& t = lastBasicPattern(pattern)._whereClauseTriples;
  auto it = std::find_if(t.begin(), t.end(), [&lhs](const auto& tr) {
    return tr._o == lhs && (tr._p._operation == PropertyPath::Operation::IRI &&
                            !isVariable(tr._p));
  });
  if (it == t.end()) {
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
    if (std::find(t.begin(), t.end(), triple) != t.end()) {
      LOG(DEBUG) << "Ignoring duplicate triple: lang(" << lhs << ") = " << rhs
                 << std::endl;
    } else {
      t.push_back(triple);
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
string SparqlParser::stripAndLowercaseKeywordLiteral(std::string_view lit) {
  if (lit.size() > 2 && lit[0] == '"' && lit.back() == '"') {
    auto stripped = lit.substr(1, lit.size() - 2);
    return ad_utility::getLowercaseUtf8(stripped);
  }
  return std::string{lit};
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
  return std::move(out).str();
}
SparqlFilter SparqlParser::parseRegexFilter(bool expectKeyword) {
  if (expectKeyword) {
    _lexer.expect("regex");
  }
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
  f._rhs = _lexer.current().raw;
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
    std::vector<size_t>
        escapePositions;  // position of backslashes that are used for
    // escaping within the regex
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
      char c = f._rhs[i];
      bool isControlChar = regexControlChars.find(c) != string::npos;
      if (!escaped && isControlChar) {
        isSimple = false;
      } else if (escaped && !isControlChar) {
        const std::string error =
            "Escaping the character "s + c +
            " is not allowed in QLever's regex filters. (Regex was " + f._rhs +
            ") Please note that "
            "there are two levels of Escaping in place here: One for Sparql "
            "and one for the regex engine";
        throw ParseException(error);
      }
      escaped = false;
    }
    if (isSimple) {
      // There are no regex special chars apart from the leading '^'
      // so we can use a prefix filter.
      f._type = SparqlFilter::PREFIX;

      // we have to remove the escaping backslashes
      for (auto it = escapePositions.rbegin(); it != escapePositions.rend();
           ++it) {
        f._rhs.erase(f._rhs.begin() + *it);
      }
    }
  }
  return f;
}

GraphPatternOperation::BasicGraphPattern& SparqlParser::lastBasicPattern(
    ParsedQuery::GraphPattern* ptr) const {
  auto& c = ptr->_children;
  if (c.empty() || !c.back().is<GraphPatternOperation::BasicGraphPattern>()) {
    c.emplace_back(GraphPatternOperation::BasicGraphPattern{});
  }
  return c.back().get<GraphPatternOperation::BasicGraphPattern>();
}

// Helper function that converts the prefix map from `parsedQuery` (a vector of
// pairs of prefix and IRI) to the prefix map we need for the
// `SparqlQleverVisitor` (a hash map from prefixes to IRIs).
namespace {
SparqlQleverVisitor::PrefixMap getPrefixMap(const ParsedQuery& parsedQuery) {
  SparqlQleverVisitor::PrefixMap prefixMap;
  for (const auto& prefixDef : parsedQuery._prefixes) {
    prefixMap[prefixDef._prefix] = prefixDef._uri;
  }
  return prefixMap;
}
}  // namespace

// ________________________________________________________________________
sparqlExpression::SparqlExpressionPimpl SparqlParser::parseExpressionWithAntlr(
    const ParsedQuery& parsedQuery) {
  auto str = _lexer.getUnconsumedInput();
  auto resultOfParseAndRemainingText =
      sparqlParserHelpers::parseExpression(str, getPrefixMap(parsedQuery));
  _lexer.reset(std::move(resultOfParseAndRemainingText._remainingText));
  return std::move(resultOfParseAndRemainingText._resultOfParse);
}

// ________________________________________________________________________
ParsedQuery::Alias SparqlParser::parseAliasWithAntlr(
    const ParsedQuery& parsedQuery) {
  auto str = _lexer.getUnconsumedInput();
  auto resultOfParseAndRemainingText =
      sparqlParserHelpers::parseAlias(str, getPrefixMap(parsedQuery));
  _lexer.reset(std::move(resultOfParseAndRemainingText._remainingText));
  return std::move(resultOfParseAndRemainingText._resultOfParse);
}
