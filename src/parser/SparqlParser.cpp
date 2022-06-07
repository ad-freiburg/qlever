// Copyright 2014, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)

#include "./SparqlParser.h"

#include <unordered_set>
#include <variant>

#include "../util/Algorithm.h"
#include "../util/OverloadCallOperator.h"
#include "./SparqlParserHelpers.h"
#include "PropertyPathParser.h"
#include "sparqlParser/SparqlQleverVisitor.h"

using namespace std::literals::string_literals;

SparqlParser::SparqlParser(const string& query) : lexer_(query), query_(query) {
  LOG(DEBUG) << "Parsing " << query << std::endl;
}

// _____________________________________________________________________________
ParsedQuery SparqlParser::parse() {
  ParsedQuery result;
  result._originalString = query_;
  parsePrologue(&result);
  if (lexer_.accept("construct")) {
    parseQuery(&result, CONSTRUCT_QUERY);
  } else {
    lexer_.expect("select");
    parseQuery(&result, SELECT_QUERY);
  }
  lexer_.expectEmpty();

  return result;
}

// _____________________________________________________________________________
void SparqlParser::parseQuery(ParsedQuery* query, QueryType queryType) {
  if (queryType == CONSTRUCT_QUERY) {
    auto str = lexer_.getUnconsumedInput();
    SparqlQleverVisitor::PrefixMap prefixes;
    for (const auto& prefix : query->_prefixes) {
      prefixes[prefix._prefix] = prefix._uri;
    }
    auto parseResult =
        sparqlParserHelpers::parseConstructTemplate(str, std::move(prefixes));
    query->_clause = std::move(parseResult.resultOfParse_);
    lexer_.reset(std::move(parseResult.remainingText_));
    lexer_.expect("where");
  } else if (queryType == SELECT_QUERY) {
    parseSelect(query);
  } else {
    // Unsupported query type
    AD_CHECK(false);
  }

  lexer_.expect("{");
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
                                 var + ".\n" + lexer_.input());
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
                                   var + ".\n" + lexer_.input());
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
                           lexer_.input());
    }
  }
}

// _____________________________________________________________________________
void SparqlParser::parsePrologue(ParsedQuery* query) {
  while (lexer_.accept("prefix")) {
    lexer_.expect(SparqlToken::Type::IRI);
    string key = lexer_.current().raw;
    lexer_.expect(SparqlToken::Type::IRI);
    string value = lexer_.current().raw;
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
  if (lexer_.accept("distinct")) {
    selectClause._distinct = true;
  }
  if (lexer_.accept("reduced")) {
    selectClause._reduced = true;
  }
  if (lexer_.accept("*")) {
    selectClause._varsOrAsterisk.setAllVariablesSelected();
  }
  std::vector<std::string> manuallySelectedVariables;
  while (!lexer_.accept("where")) {
    if (selectClause._varsOrAsterisk.isAllVariablesSelected()) {
      throw ParseException("Keyword WHERE expected after SELECT '*'");
    }
    if (lexer_.accept(SparqlToken::Type::VARIABLE)) {
      manuallySelectedVariables.push_back(lexer_.current().raw);
    } else if (lexer_.accept("text")) {
      lexer_.expect("(");
      std::ostringstream s;
      s << "TEXT(";
      lexer_.expect(SparqlToken::Type::VARIABLE);
      s << lexer_.current().raw;
      lexer_.expect(")");
      s << ")";
      manuallySelectedVariables.push_back(std::move(s).str());
    } else if (lexer_.accept("score")) {
      lexer_.expect("(");
      std::ostringstream s;
      s << "SCORE(";
      lexer_.expect(SparqlToken::Type::VARIABLE);
      s << lexer_.current().raw;
      lexer_.expect(")");
      s << ")";
      manuallySelectedVariables.push_back(std::move(s).str());
    } else if (lexer_.accept("(")) {
      // Expect an alias.
      ParsedQuery::Alias a =
          parseWithAntlr(sparqlParserHelpers::parseAlias, *query);
      selectClause._aliases.push_back(a);
      manuallySelectedVariables.push_back(a._outVarName);
      lexer_.expect(")");
    } else {
      lexer_.accept();
      throw ParseException("Error in SELECT: unexpected token: " +
                           lexer_.current().raw);
    }
    if (lexer_.empty()) {
      throw ParseException("Keyword WHERE expected after SELECT.");
    }
  }
  if (selectClause._varsOrAsterisk.isManuallySelectedVariables()) {
    selectClause._varsOrAsterisk.setManuallySelected(manuallySelectedVariables);
  }
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
  while (!lexer_.accept("}")) {
    if (lexer_.empty()) {
      throw ParseException(
          "Expected a closing bracket for WHERE but reached "
          "the end of the input.");
    }
    if (lexer_.accept("optional")) {
      currentPattern->_children.emplace_back(
          GraphPatternOperation::Optional{ParsedQuery::GraphPattern()});
      auto& opt = currentPattern->_children.back()
                      .get<GraphPatternOperation::Optional>();
      auto& child = opt._child;
      child._optional = true;
      child._id = query->_numGraphPatterns;
      query->_numGraphPatterns++;
      lexer_.expect("{");
      // Recursively call parseWhere to parse the optional part.
      parseWhere(query, &child);
      lexer_.accept(".");
    } else if (lexer_.peek("bind")) {
      GraphPatternOperation::Bind bind =
          parseWithAntlr(sparqlParserHelpers::parseBind, *query);
      query->registerVariableVisibleInQueryBody(bind._target);
      currentPattern->_children.emplace_back(std::move(bind));
      // The dot after a BIND is optional.
      lexer_.accept(".");
    } else if (lexer_.accept("minus")) {
      currentPattern->_children.emplace_back(
          GraphPatternOperation::Minus{ParsedQuery::GraphPattern()});
      auto& opt =
          currentPattern->_children.back().get<GraphPatternOperation::Minus>();
      auto& child = opt._child;
      child._optional = false;
      child._id = query->_numGraphPatterns;
      query->_numGraphPatterns++;
      lexer_.expect("{");
      // Recursively call parseWhere to parse the subtrahend.
      parseWhere(query, &child);
      lexer_.accept(".");
    } else if (lexer_.accept("{")) {
      // Subquery or union
      if (lexer_.accept("select")) {
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
        lexer_.accept(".");
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
        lexer_.expect("union");
        lexer_.expect("{");
        parseWhere(query, &un._child2);
        lexer_.accept(".");
        currentPattern->_children.emplace_back(std::move(un));
      }
    } else if (lexer_.accept("filter")) {
      // append to the global filters of the pattern.
      parseFilter(&currentPattern->_filters, true, currentPattern);
      // A filter may have an optional dot after it
      lexer_.accept(".");
    } else if (lexer_.accept("values")) {
      SparqlValues values;
      if (lexer_.accept("(")) {
        // values with several variables
        while (lexer_.accept(SparqlToken::Type::VARIABLE)) {
          values._variables.push_back(lexer_.current().raw);
          query->registerVariableVisibleInQueryBody(lexer_.current().raw);
        }
        lexer_.expect(")");
        lexer_.expect("{");
        while (lexer_.accept("(")) {
          values._values.emplace_back(values._variables.size());
          for (size_t i = 0; i < values._variables.size(); i++) {
            if (!lexer_.accept(SparqlToken::Type::RDFLITERAL)) {
              lexer_.expect(SparqlToken::Type::IRI);
            }
            values._values.back()[i] = lexer_.current().raw;
          }
          lexer_.expect(")");
        }
        lexer_.expect("}");
      } else if (lexer_.accept(SparqlToken::Type::VARIABLE)) {
        // values with a single variable
        values._variables.push_back(lexer_.current().raw);
        query->registerVariableVisibleInQueryBody(lexer_.current().raw);
        lexer_.expect("{");
        while (lexer_.accept(SparqlToken::Type::IRI) ||
               lexer_.accept(SparqlToken::Type::RDFLITERAL)) {
          values._values.emplace_back(1);
          values._values.back()[0] = lexer_.current().raw;
        }
        lexer_.expect("}");
      } else {
        throw ParseException(
            "Expected either a single or a set of variables "
            "after VALUES");
      }
      currentPattern->_children.emplace_back(
          GraphPatternOperation::Values{std::move(values)});
      lexer_.accept(".");
    } else {
      std::string subject;
      if (lastSubject.empty()) {
        if (lexer_.accept(SparqlToken::Type::VARIABLE)) {
          subject = lexer_.current().raw;
          query->registerVariableVisibleInQueryBody(lexer_.current().raw);
        } else if (lexer_.accept(SparqlToken::Type::RDFLITERAL)) {
          subject = parseLiteral(lexer_.current().raw, true);
        } else {
          lexer_.expect(SparqlToken::Type::IRI);
          subject = lexer_.current().raw;
        }
      } else {
        subject = lastSubject;
        lastSubject.clear();
      }

      std::string predicate;
      if (lastPredicate.empty()) {
        if (lexer_.accept(SparqlToken::Type::VARIABLE)) {
          predicate = lexer_.current().raw;
          query->registerVariableVisibleInQueryBody(lexer_.current().raw);
        } else if (lexer_.accept(SparqlToken::Type::RDFLITERAL)) {
          predicate = parseLiteral(lexer_.current().raw, true);
        } else if (lexer_.accept(SparqlToken::Type::A_RDF_TYPE_ALIAS)) {
          predicate = "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>";
        } else {
          // Assume the token is a predicate path. This will be verified
          // separately later.
          lexer_.expandNextUntilWhitespace();
          lexer_.accept();
          predicate = lexer_.current().raw;
        }
      } else {
        predicate = lastPredicate;
        lastPredicate.clear();
      }

      std::string object;
      if (lexer_.accept(SparqlToken::Type::VARIABLE)) {
        object = lexer_.current().raw;
        query->registerVariableVisibleInQueryBody(lexer_.current().raw);
      } else if (lexer_.accept(SparqlToken::Type::RDFLITERAL)) {
        object = parseLiteral(lexer_.current().raw, true);
      } else {
        lexer_.expect(SparqlToken::Type::IRI);
        object = lexer_.current().raw;
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

      if (lexer_.accept(";")) {
        lastSubject = subject;
      } else if (lexer_.accept(",")) {
        lastSubject = subject;
        lastPredicate = predicate;
      } else if (lexer_.accept("}")) {
        break;
      } else {
        lexer_.expect(".");
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
  while (!lexer_.empty() && !lexer_.accept("}")) {
    if (lexer_.peek(SparqlToken::Type::ORDER_BY)) {
      auto order_keys =
          parseWithAntlr(sparqlParserHelpers::parseOrderClause, *query);

      auto processVariableOrderKey = [&query](VariableOrderKey orderKey) {
        // Check whether grouping is done and the variable being ordered by
        // is neither grouped nor the result of an alias in the select
        const vector<std::string>& groupByVariables = query->_groupByVariables;
        if (!groupByVariables.empty() &&
            !ad_utility::contains(groupByVariables, orderKey.variable_) &&
            !ad_utility::contains_if(
                query->selectClause()._aliases,
                [&orderKey](const ParsedQuery::Alias& alias) {
                  return alias._outVarName == orderKey.variable_;
                })) {
          throw ParseException(
              "Variable " + orderKey.variable_ +
              " was used in an ORDER BY "
              "clause, but is neither grouped, nor created as an alias in the "
              "SELECT clause.");
        }

        query->_orderBy.push_back(std::move(orderKey));
      };

      // QLever currently only supports ordering by variables. To allow
      // all `orderConditions`, the corresponding expression is bound to a new
      // internal variable. Ordering is then done by this variable.
      auto processExpressionOrderKey = [&query,
                                        this](ExpressionOrderKey orderKey) {
        if (!query->_groupByVariables.empty())
          // TODO<qup42> Implement this by adding a hidden alias in the
          //  SELECT clause.
          throw ParseException(
              "Ordering by an expression while grouping is not supported by "
              "QLever. (The expression is \"" +
              orderKey.expression_.getDescriptor() +
              "\"). Please assign this expression to a "
              "new variable in the SELECT clause and then order by this "
              "variable.");
        // Internal variable name to which the result of the helper bind is
        // assigned.
        std::string helperBindTargetVar =
            SOLUTION_MODIFIER_HELPER_BIND_PREFIX +
            std::to_string(numAdditionalVariables_);
        numAdditionalVariables_++;
        GraphPatternOperation::Bind helperBind{std::move(orderKey.expression_),
                                               helperBindTargetVar};
        // Don't register the helperBindTargetVar as visible because it is used
        // internally and should not be selected by SELECT *.
        // TODO<qup42, joka921> Implement "internal" variables, that can't be
        //  selected at all and can never interfere with variables from the
        //  query.
        query->_rootGraphPattern._children.emplace_back(std::move(helperBind));
        query->_orderBy.emplace_back(helperBindTargetVar,
                                     orderKey.isDescending_);
      };

      for (auto& orderKey : order_keys) {
        std::visit(ad_utility::OverloadCallOperator{processVariableOrderKey,
                                                    processExpressionOrderKey},
                   std::move(orderKey));
      }
    } else if (lexer_.peek("limit") || lexer_.peek("textlimit") ||
               lexer_.peek("offset")) {
      query->_limitOffset =
          parseWithAntlr(sparqlParserHelpers::parseLimitOffsetClause, *query);
    } else if (lexer_.accept(SparqlToken::Type::GROUP_BY)) {
      lexer_.expect(SparqlToken::Type::VARIABLE);
      query->_groupByVariables.emplace_back(lexer_.current().raw);
      while (lexer_.accept(SparqlToken::Type::VARIABLE)) {
        query->_groupByVariables.emplace_back(lexer_.current().raw);
      }
    } else if (lexer_.accept("having")) {
      parseFilter(&query->_havingClauses, true, &query->_rootGraphPattern);
      while (parseFilter(&query->_havingClauses, false,
                         &query->_rootGraphPattern)) {
      }
    } else {
      lexer_.accept();
      throw ParseException("Expected a solution modifier but got " +
                           lexer_.current().raw);
    }
  }
}

// _____________________________________________________________________________
bool SparqlParser::parseFilter(vector<SparqlFilter>* _filters,
                               bool failOnNoFilter,
                               ParsedQuery::GraphPattern* pattern) {
  size_t numParentheses = 0;
  while (lexer_.accept("(")) {
    numParentheses++;
  }
  auto expectClose = [numParentheses, this]() mutable {
    while (numParentheses) {
      lexer_.expect(")");
      numParentheses--;
    }
  };
  if (lexer_.accept("lang") && numParentheses) {
    lexer_.expect("(");
    lexer_.expect(SparqlToken::Type::VARIABLE);
    std::string lhs = lexer_.current().raw;
    lexer_.expect(")");
    lexer_.expect("=");
    lexer_.expect(SparqlToken::Type::RDFLITERAL);
    std::string rhs = lexer_.current().raw;
    expectClose();
    addLangFilter(lhs, rhs, pattern);
    return true;
  } else if (lexer_.accept("langmatches")) {
    lexer_.expect("(");
    lexer_.expect("lang");
    lexer_.expect("(");
    lexer_.expect(SparqlToken::Type::VARIABLE);
    std::string lhs = lexer_.current().raw;
    lexer_.expect(")");
    lexer_.expect(",");
    lexer_.expect(SparqlToken::Type::RDFLITERAL);
    std::string rhs = lexer_.current().raw;
    expectClose();
    addLangFilter(lhs, rhs, pattern);
    return true;
  } else if (lexer_.accept("regex")) {
    std::vector<SparqlFilter> v;
    v.push_back(parseRegexFilter(false));
    if (numParentheses) {
      while (lexer_.accept(SparqlToken::Type::LOGICAL_OR)) {
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
  } else if (lexer_.accept("prefix")) {
    lexer_.expect("(");
    SparqlFilter f1;
    SparqlFilter f2;
    f1._type = SparqlFilter::GE;
    f2._type = SparqlFilter::LT;
    // Do prefix filtering by using two filters (one testing >=, the other =)
    lexer_.expect(SparqlToken::Type::VARIABLE);
    f1._lhs = lexer_.current().raw;
    f2._lhs = f1._lhs;
    lexer_.expect(",");
    lexer_.expect(SparqlToken::Type::RDFLITERAL);
    f1._rhs = lexer_.current().raw;
    f2._rhs = f1._lhs;
    f1._rhs = f1._rhs.substr(0, f1._rhs.size() - 1) + " ";
    f2._rhs = f2._rhs.substr(0, f2._rhs.size() - 2);
    f2._rhs += f1._rhs[f1._rhs.size() - 2] + 1;
    f2._rhs += f1._rhs[f1._rhs.size() - 1];
    _filters->emplace_back(f1);
    _filters->emplace_back(f2);
    lexer_.expect(")");
    expectClose();
    return true;
  } else if (numParentheses) {
    SparqlFilter f;
    if (lexer_.accept("str")) {
      lexer_.expect("(");
      f._lhsAsString = true;
    }
    // LHS
    if (lexer_.accept(SparqlToken::Type::IRI)) {
      f._lhs = lexer_.current().raw;
    } else if (lexer_.accept(SparqlToken::Type::VARIABLE)) {
      f._lhs = lexer_.current().raw;
    } else if (lexer_.accept(SparqlToken::Type::RDFLITERAL)) {
      f._lhs = lexer_.current().raw;
    } else if (lexer_.accept(SparqlToken::Type::INTEGER)) {
      f._lhs = lexer_.current().raw;
    } else if (lexer_.accept(SparqlToken::Type::FLOAT)) {
      f._lhs = lexer_.current().raw;
    } else {
      lexer_.accept();
      throw ParseException(lexer_.current().raw +
                           " is not a valid left hand side for a filter.");
    }
    if (f._lhsAsString) {
      lexer_.expect(")");
    }
    // TYPE
    if (lexer_.accept("=")) {
      f._type = SparqlFilter::EQ;
    } else if (lexer_.accept("!")) {
      lexer_.expect("=");
      f._type = SparqlFilter::NE;
    } else if (lexer_.accept("<")) {
      if (lexer_.accept("=")) {
        f._type = SparqlFilter::LE;
      } else {
        f._type = SparqlFilter::LT;
      }
    } else if (lexer_.accept(">")) {
      if (lexer_.accept("=")) {
        f._type = SparqlFilter::GE;
      } else {
        f._type = SparqlFilter::GT;
      }
    } else {
      lexer_.accept();
      throw ParseException(lexer_.current().raw +
                           " is not a valid relation for a filter.");
    }
    // RHS
    if (lexer_.accept(SparqlToken::Type::IRI)) {
      f._rhs = lexer_.current().raw;
    } else if (lexer_.accept(SparqlToken::Type::VARIABLE)) {
      f._rhs = lexer_.current().raw;
    } else if (lexer_.accept(SparqlToken::Type::RDFLITERAL)) {
      // Resolve escaped characters
      f._rhs = lexer_.current().raw;
    } else if (lexer_.accept(SparqlToken::Type::INTEGER)) {
      f._rhs = lexer_.current().raw;
    } else if (lexer_.accept(SparqlToken::Type::FLOAT)) {
      f._rhs = lexer_.current().raw;
    } else {
      lexer_.accept();
      throw ParseException(lexer_.current().raw +
                           " is not a valid right hand side for a filter.");
    }
    expectClose();
    _filters->emplace_back(f);
    return true;

  } else if (failOnNoFilter) {
    lexer_.accept();
    throw ParseException("Expected a filter but got " + lexer_.current().raw);
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
    lexer_.expect("regex");
  }
  SparqlFilter f;
  f._type = SparqlFilter::REGEX;
  lexer_.expect("(");
  if (lexer_.accept("str")) {
    lexer_.expect("(");
    f._lhsAsString = true;
  }
  lexer_.expect(SparqlToken::Type::VARIABLE);
  f._lhs = lexer_.current().raw;
  if (f._lhsAsString) {
    lexer_.expect(")");
  }
  lexer_.expect(",");
  lexer_.expect(SparqlToken::Type::RDFLITERAL);
  f._rhs = lexer_.current().raw;
  // Remove the enlcosing quotation marks
  f._rhs = f._rhs.substr(1, f._rhs.size() - 2);
  if (lexer_.accept(",")) {
    lexer_.expect("\"i\"");
    f._regexIgnoreCase = true;
  }
  lexer_.expect(")");
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
template <typename F>
auto SparqlParser::parseWithAntlr(F f, const ParsedQuery& parsedQuery)
    -> decltype(f(std::declval<const string&>(),
                  std::declval<SparqlQleverVisitor::PrefixMap>())
                    .resultOfParse_) {
  auto resultOfParseAndRemainingText =
      f(lexer_.getUnconsumedInput(), getPrefixMap(parsedQuery));
  lexer_.reset(std::move(resultOfParseAndRemainingText.remainingText_));
  return std::move(resultOfParseAndRemainingText.resultOfParse_);
}
