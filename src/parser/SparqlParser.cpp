// Copyright 2014, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)

#include "./SparqlParser.h"

#include <unordered_set>
#include <variant>

#include "../util/Algorithm.h"
#include "../util/OverloadCallOperator.h"
#include "./SparqlParserHelpers.h"
#include "Alias.h"
#include "data/Types.h"
#include "sparqlParser/SparqlQleverVisitor.h"

using namespace std::literals::string_literals;

SparqlParser::SparqlParser(const string& query) : lexer_(query), query_(query) {
  LOG(DEBUG) << "Parsing " << query << std::endl;
}

namespace {
// Converts the PrefixMap to the legacy data format used by ParsedQuery
vector<SparqlPrefix> convertPrefixMap(
    const SparqlQleverVisitor::PrefixMap& map) {
  vector<SparqlPrefix> prefixes;
  for (auto const& [label, iri] : map) {
    prefixes.emplace_back(label, iri);
  }
  return prefixes;
}
}  // namespace

// _____________________________________________________________________________
ParsedQuery SparqlParser::parse() {
  ParsedQuery result;
  result._originalString = query_;
  // parsePrologue parses all the prefixes which are stored in a member
  // PrefixMap. This member is returned on parse.
  result._prefixes = convertPrefixMap(parseWithAntlr(
      sparqlParserHelpers::parsePrologue,
      {{INTERNAL_PREDICATE_PREFIX_NAME, INTERNAL_PREDICATE_PREFIX_IRI}}));

  if (lexer_.accept("construct")) {
    parseQuery(&result, CONSTRUCT_QUERY);
  } else if (lexer_.peek("select")) {
    parseQuery(&result, SELECT_QUERY);
  } else {
    throw ParseException("Query must either be a SELECT or CONSTRUCT.");
  }
  lexer_.expectEmpty();

  return result;
}

// _____________________________________________________________________________
void SparqlParser::parseQuery(ParsedQuery* query, QueryType queryType) {
  if (queryType == CONSTRUCT_QUERY) {
    SparqlQleverVisitor::PrefixMap prefixes;
    for (const auto& prefix : query->_prefixes) {
      prefixes[prefix._prefix] = prefix._uri;
    }
    query->_clause =
        parseWithAntlr(sparqlParserHelpers::parseConstructTemplate, prefixes)
            .value_or(ad_utility::sparql_types::Triples{});
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
    if (query->hasSelectClause() && !query->selectClause().isAsterisk()) {
      const auto& selectClause = query->selectClause();
      // Check if all selected variables are either aggregated or
      for (const string& var : selectClause.getSelectedVariablesAsStrings()) {
        if (var[0] == '?') {
          if (ad_utility::contains_if(selectClause.getAliases(),
                                      [&var](const Alias& alias) {
                                        return alias._outVarName == var;
                                      })) {
            continue;
          }
          if (!ad_utility::contains_if(query->_groupByVariables,
                                       [&var](const Variable& grouping) {
                                         return var == grouping.name();
                                       })) {
            throw ParseException("Variable " + var +
                                 " is selected but not "
                                 "aggregated despite the query not being "
                                 "grouped by " +
                                 var + ".\n" + lexer_.input());
          }
        }
      }
    } else if (query->hasSelectClause() && query->selectClause().isAsterisk()) {
      throw ParseException(
          "GROUP BY is not allowed when all variables are selected via SELECT "
          "*");
    } else if (query->hasConstructClause()) {
      auto& constructClause = query->constructClause();
      for (const auto& triple : constructClause) {
        for (const auto& varOrTerm : triple) {
          if (auto variable = std::get_if<Variable>(&varOrTerm)) {
            const auto& var = variable->name();
            if (!ad_utility::contains_if(query->_groupByVariables,
                                         [&var](const Variable& grouping) {
                                           return var == grouping.name();
                                         })) {
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

  // TODO<joka921> The following check should be directly in the
  // `SelectClause` class (in the `setSelected` member).

  // TODO<joka921> Is this even the correct way to check this? we should also
  // verify that the variable is new (not even visible in the query body).
  ad_utility::HashMap<std::string, size_t> variable_counts;

  for (const std::string& s : selectClause.getSelectedVariablesAsStrings()) {
    variable_counts[s]++;
  }

  for (const Alias& a : selectClause.getAliases()) {
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
void SparqlParser::parseSelect(ParsedQuery* query) {
  query->_clause =
      parseWithAntlr(sparqlParserHelpers::parseSelectClause, *query);
  lexer_.expect("where");  // Still parsed with old parser. Expects WHERE
                           // keyword to be consumed.
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
      query->registerVariableVisibleInQueryBody(Variable{bind._target});
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
      if (lexer_.peek("select")) {
        // subquery
        // create the subquery operation
        GraphPatternOperation::Subquery subq;
        subq._subquery._prefixes = query->_prefixes;
        parseQuery(&subq._subquery, SELECT_QUERY);

        // Add the variables from the subquery that are visible to the outside
        // (because they were selected, or because of a SELECT *) to the outer
        // query.
        const auto& selectedVariablesFromSubquery =
            subq._subquery.selectClause().getSelectedVariables();
        for (const auto& variable : selectedVariablesFromSubquery) {
          query->registerVariableVisibleInQueryBody(variable);
        }

        currentPattern->_children.emplace_back(std::move(subq));
        // The closing bracket } is consumed by the subquery
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
      SparqlFilter filter = parseFilter(true).value();
      if (filter._type == SparqlFilter::LANG_MATCHES) {
        currentPattern->addLanguageFilter(filter._lhs, filter._rhs);
      } else {
        currentPattern->_filters.push_back(std::move(filter));
      }
      // A filter may have an optional dot after it
      lexer_.accept(".");
    } else if (lexer_.peek("values")) {
      auto values =
          parseWithAntlr(sparqlParserHelpers::parseInlineData, *query);
      for (const auto& variable : values._inlineValues._variables) {
        query->registerVariableVisibleInQueryBody(Variable{variable});
      }
      currentPattern->_children.emplace_back(std::move(values));
      lexer_.accept(".");
    } else {
      // TODO: Make TripleComponent constructible from these types.
      auto iri = [](const Iri& iri) { return TripleComponent{iri.toSparql()}; };
      auto blankNode = [](const BlankNode& blankNode) -> TripleComponent {
        return blankNode.toSparql();
      };
      auto literal = [](const Literal& literal) {
        // Problem: ql:contains-word causes the " to be stripped.
        // TODO: Move stripAndLowercaseKeywordLiteral out to this point or
        //  rewrite the Turtle Parser s.t. this code can be integrated into the
        //  visitor. In this case the turtle parser should output the
        //  corresponding modell class.
        try {
          return TurtleStringParser<TokenizerCtre>::parseTripleObject(
              literal.toSparql());
        } catch (const TurtleStringParser<TokenizerCtre>::ParseException&) {
          return TripleComponent{literal.toSparql()};
        }
      };
      auto graphTerm = [&iri, &blankNode, &literal](const GraphTerm& term) {
        return term.visit(
            ad_utility::OverloadCallOperator{iri, blankNode, literal});
      };
      auto varTriple = [](const Variable& var) {
        return TripleComponent{var.name()};
      };
      auto varOrTerm = [&varTriple, &graphTerm](VarOrTerm varOrTerm) {
        return varOrTerm.visit(
            ad_utility::OverloadCallOperator{varTriple, graphTerm});
      };

      auto varPath = [](const Variable& var) {
        return PropertyPath::fromVariable(var);
      };
      auto path = [](const PropertyPath& path) { return path; };
      auto varOrPath =
          [&varPath,
           &path](const ad_utility::sparql_types::VarOrPath& varOrPath) {
            return std::visit(ad_utility::OverloadCallOperator{varPath, path},
                              varOrPath);
          };

      auto registerIfVariable = [&query](auto* variant) {
        if (Variable* variable = std::get_if<Variable>(variant)) {
          query->registerVariableVisibleInQueryBody(*variable);
        }
      };

      vector<ad_utility::sparql_types::TripleWithPropertyPath> triples =
          parseWithAntlr(sparqlParserHelpers::parseTriplesSameSubjectPath,
                         *query);
      auto& v = lastBasicPattern(currentPattern)._whereClauseTriples;
      for (auto& triple : triples) {
        registerIfVariable(&triple.subject_);
        registerIfVariable(&triple.predicate_);
        registerIfVariable(&triple.object_);
        // TODO SparqlTriple and TripleWithPropertyPath should be merged into
        //  one type.
        SparqlTriple sparqlTriple = {varOrTerm(triple.subject_),
                                     varOrPath(triple.predicate_),
                                     varOrTerm(triple.object_)};
        if (std::find(v.begin(), v.end(), sparqlTriple) != v.end()) {
          LOG(INFO) << "Ignoring duplicate triple: " << sparqlTriple._s << ' '
                    << sparqlTriple._p << ' ' << sparqlTriple._o << std::endl;
        } else {
          v.emplace_back(std::move(sparqlTriple));
        }
      }

      if (lexer_.accept("}")) {
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
        // Check whether grouping is done. The variable being ordered by
        // must then be either grouped or the result of an alias in the select.
        const vector<Variable>& groupByVariables = query->_groupByVariables;
        if (!groupByVariables.empty() &&
            !ad_utility::contains_if(groupByVariables,
                                     [&orderKey](const Variable& var) {
                                       return orderKey.variable_ == var.name();
                                     }) &&
            !ad_utility::contains_if(query->selectClause().getAliases(),
                                     [&orderKey](const Alias& alias) {
                                       return alias._outVarName ==
                                              orderKey.variable_;
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
        auto additionalVariable =
            addInternalBind(query, std::move(orderKey.expression_));
        query->_orderBy.emplace_back(additionalVariable.name(),
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
    } else if (lexer_.peek(SparqlToken::Type::GROUP_BY)) {
      auto group_keys =
          parseWithAntlr(sparqlParserHelpers::parseGroupClause, *query);

      auto processVariable = [&query](const Variable& groupKey) {
        query->_groupByVariables.emplace_back(groupKey.name());
      };
      auto processExpression =
          [&query, this](sparqlExpression::SparqlExpressionPimpl groupKey) {
            auto helperTarget = addInternalBind(query, std::move(groupKey));
            query->_groupByVariables.emplace_back(helperTarget.name());
          };
      auto processAlias = [&query](Alias groupKey) {
        GraphPatternOperation::Bind helperBind{std::move(groupKey._expression),
                                               groupKey._outVarName};
        query->_rootGraphPattern._children.emplace_back(std::move(helperBind));
        query->registerVariableVisibleInQueryBody(
            Variable{groupKey._outVarName});
        query->_groupByVariables.emplace_back(groupKey._outVarName);
      };

      for (auto& orderKey : group_keys) {
        std::visit(
            ad_utility::OverloadCallOperator{processVariable, processExpression,
                                             processAlias},
            std::move(orderKey));
      }
    } else if (lexer_.accept("having")) {
      auto addHavingFilter = [&](bool failOnNoFilter) {
        auto filterOpt = parseFilter(failOnNoFilter);
        if (filterOpt.has_value()) {
          auto& filter = filterOpt.value();
          if (filter._type == SparqlFilter::LANG_MATCHES) {
            throw ParseException(
                "Language filter in HAVING clause currently not "
                "supported by QLever");
          }
          query->_havingClauses.push_back(std::move(filter));
          return true;
        } else {
          AD_CHECK(!failOnNoFilter);
          return false;
        }
      };
      addHavingFilter(true);
      // Add remaining filters.
      while (addHavingFilter(false)) {
      }
    } else {
      lexer_.accept();
      throw ParseException("Expected a solution modifier but got " +
                           lexer_.current().raw);
    }
  }
}

// _____________________________________________________________________________
std::optional<SparqlFilter> SparqlParser::parseFilter(bool failOnNoFilter) {
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
    return SparqlFilter{SparqlFilter::LANG_MATCHES, lhs, rhs};
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
    return SparqlFilter{SparqlFilter::LANG_MATCHES, lhs, rhs};
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
    expectClose();
    return v[0];
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
    return f;

  } else if (failOnNoFilter) {
    lexer_.accept();
    throw ParseException("Expected a filter but got " + lexer_.current().raw);
  }
  expectClose();
  return std::nullopt;
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
TripleComponent SparqlParser::parseLiteral(const ParsedQuery& pq,
                                           const string& literal,
                                           bool isEntireString,
                                           size_t off /* defaults to 0 */) {
  auto parseLiteralAsString = [&]() -> std::string {
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
    while (pos < literal.size() && literal[pos] != '"' &&
           literal[pos] != '\'') {
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
  };
  auto resultAsString = parseLiteralAsString();
  // Convert the Literals to ints or doubles if they have the appropriate
  // types.
  ParsedQuery::expandPrefix(resultAsString, getPrefixMap(pq));
  return TurtleStringParser<TokenizerCtre>::parseTripleObject(resultAsString);
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

// ________________________________________________________________________
template <typename F>
auto SparqlParser::parseWithAntlr(F f, const ParsedQuery& parsedQuery)
    -> decltype(f(std::declval<const string&>(),
                  std::declval<SparqlQleverVisitor::PrefixMap>())
                    .resultOfParse_) {
  return parseWithAntlr(f, getPrefixMap(parsedQuery));
}

// ________________________________________________________________________
template <typename F>
auto SparqlParser::parseWithAntlr(F f, SparqlQleverVisitor::PrefixMap prefixMap)
    -> decltype(f(std::declval<const string&>(),
                  std::declval<SparqlQleverVisitor::PrefixMap>())
                    .resultOfParse_) {
  auto resultOfParseAndRemainingText =
      f(lexer_.getUnconsumedInput(), std::move(prefixMap));
  lexer_.reset(std::move(resultOfParseAndRemainingText.remainingText_));
  return std::move(resultOfParseAndRemainingText.resultOfParse_);
}

// ________________________________________________________________________
template <typename F>
auto SparqlParser::parseWithAntlr(
    F f, const std::string& input,
    const SparqlQleverVisitor::PrefixMap& prefixMap)
    -> decltype(f(std::declval<const string&>(),
                  std::declval<SparqlQleverVisitor::PrefixMap>())
                    .resultOfParse_) {
  auto resultOfParseAndRemainingText = f(input, prefixMap);
  return std::move(resultOfParseAndRemainingText.resultOfParse_);
}

// ________________________________________________________________________
Variable SparqlParser::addInternalBind(
    ParsedQuery* query, sparqlExpression::SparqlExpressionPimpl expression) {
  // Internal variable name to which the result of the helper bind is
  // assigned.
  std::string targetVariable =
      INTERNAL_VARIABLE_PREFIX + std::to_string(numInternalVariables_);
  numInternalVariables_++;
  GraphPatternOperation::Bind bind{std::move(expression), targetVariable};
  query->_rootGraphPattern._children.emplace_back(std::move(bind));
  // Don't register the targetVariable as visible because it is used
  // internally and should not be selected by SELECT *.
  // TODO<qup42, joka921> Implement "internal" variables, that can't be
  //  selected at all and can never interfere with variables from the
  //  query.
  return Variable{std::move(targetVariable)};
}

// _____________________________________________________________________________
SparqlFilter SparqlParser::parseFilterExpression(const string& filterContent) {
  SparqlParser parser(filterContent);
  return parser.parseFilter(true).value();
}
