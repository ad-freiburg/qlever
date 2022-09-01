// Copyright 2014, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)

#include "./SparqlParser.h"

#include <unordered_set>
#include <variant>

#include "Alias.h"
#include "data/Types.h"
#include "sparqlParser/SparqlQleverVisitor.h"

using namespace std::literals::string_literals;
using AntlrParser = SparqlAutomaticParser;

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
      &AntlrParser::prologue,
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
    query->_clause = parseWithAntlr(&AntlrParser::constructTemplate, *query)
                         .value_or(ad_utility::sparql_types::Triples{});
  } else if (queryType == SELECT_QUERY) {
    parseSelect(query);
  } else {
    // Unsupported query type
    AD_FAIL();
  }

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
      AD_FAIL();
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
  query->_clause = parseWithAntlr(&AntlrParser::selectClause, *query);
}

// _____________________________________________________________________________
SparqlQleverVisitor::PrefixMap SparqlParser::getPrefixMap(
    const ParsedQuery& parsedQuery) {
  SparqlQleverVisitor::PrefixMap prefixMap;
  for (const auto& prefixDef : parsedQuery._prefixes) {
    prefixMap[prefixDef._prefix] = prefixDef._uri;
  }
  return prefixMap;
}

// _____________________________________________________________________________
void SparqlParser::parseWhere(ParsedQuery* query) {
  auto [pattern, visibleVariables] =
      parseWithAntlr(&AntlrParser::whereClause, *query);
  query->_rootGraphPattern = std::move(pattern);
  for (const auto& var : visibleVariables) {
    query->registerVariableVisibleInQueryBody(var);
  }
}

// _____________________________________________________________________________
void SparqlParser::parseSolutionModifiers(ParsedQuery* query) {
  query->addSolutionModifiers(
      parseWithAntlr(&AntlrParser::solutionModifier, *query));

  lexer_.accept("}");
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

// ________________________________________________________________________
template <typename ContextType>
auto SparqlParser::parseWithAntlr(
    ContextType* (SparqlAutomaticParser::*F)(void),
    const ParsedQuery& parsedQuery)
    -> decltype((std::declval<sparqlParserHelpers::ParserAndVisitor>())
                    .parseTypesafe(F)
                    .resultOfParse_) {
  return parseWithAntlr(F, getPrefixMap(parsedQuery));
}

// ________________________________________________________________________
template <typename ContextType>
auto SparqlParser::parseWithAntlr(
    ContextType* (SparqlAutomaticParser::*F)(void),
    SparqlQleverVisitor::PrefixMap prefixMap)
    -> decltype((std::declval<sparqlParserHelpers::ParserAndVisitor>())
                    .parseTypesafe(F)
                    .resultOfParse_) {
  sparqlParserHelpers::ParserAndVisitor p{lexer_.getUnconsumedInput(),
                                          std::move(prefixMap)};
  auto resultOfParseAndRemainingText = p.parseTypesafe(F);
  lexer_.reset(std::move(resultOfParseAndRemainingText.remainingText_));
  return std::move(resultOfParseAndRemainingText.resultOfParse_);
}

// _____________________________________________________________________________
SparqlFilter SparqlParser::parseFilterExpression(const string& filterContent) {
  SparqlParser parser(filterContent);
  return parser.parseFilter(true).value();
}
