// Copyright 2021, University of Freiburg,
// Chair of Algorithms and Data Structures
// Authors:
//   Hannah Bast <bast@cs.uni-freiburg.de>
//   2022 Julian Mundhahs <mundhahj@tf.uni-freiburg.de>

#include "parser/sparqlParser/SparqlQleverVisitor.h"

#include <string>
#include <vector>

#include "parser/SparqlParser.h"
#include "parser/TokenizerCtre.h"
#include "parser/TurtleParser.h"

using namespace ad_utility::sparql_types;
using ExpressionPtr = sparqlExpression::SparqlExpression::Ptr;
using SparqlExpressionPimpl = sparqlExpression::SparqlExpressionPimpl;
using SelectClause = parsedQuery::SelectClause;
using GraphPattern = parsedQuery::GraphPattern;
using Bind = parsedQuery::Bind;
using Values = parsedQuery::Values;
using BasicGraphPattern = parsedQuery::BasicGraphPattern;
using GraphPatternOperation = parsedQuery::GraphPatternOperation;
using SparqlValues = parsedQuery::SparqlValues;

using Visitor = SparqlQleverVisitor;
using Parser = SparqlAutomaticParser;

// ___________________________________________________________________________
ExpressionPtr Visitor::processIriFunctionCall(
    const std::string& iri, std::vector<ExpressionPtr> argList) {
  // Lambda that checks the number of arguments and throws an error if it's
  // not right.
  auto checkNumArgs = [&argList](const std::string_view prefix,
                                 const std::string_view functionName,
                                 size_t numArgs) {
    static std::array<std::string, 6> wordForNumArgs = {
        "no", "one", "two", "three", "four", "five"};
    if (argList.size() != numArgs) {
      throw ParseException{absl::StrCat(
          "Function ", prefix, functionName, " takes ",
          numArgs < 5 ? wordForNumArgs[numArgs] : std::to_string(numArgs),
          numArgs == 1 ? " argument" : " arguments")};
    }
  };

  constexpr static std::string_view geofPrefix =
      "<http://www.opengis.net/def/function/geosparql/";
  std::string_view iriView = iri;
  if (iriView.starts_with(geofPrefix)) {
    iriView.remove_prefix(geofPrefix.size());
    AD_CHECK(iriView.ends_with('>'));
    iriView.remove_suffix(1);
    if (iriView == "distance") {
      checkNumArgs("geof:", iriView, 2);
      return createExpression<sparqlExpression::DistExpression>(
          std::move(argList[0]), std::move(argList[1]));
    } else if (iriView == "longitude") {
      checkNumArgs("geof:", iriView, 1);
      return createExpression<sparqlExpression::LongitudeExpression>(
          std::move(argList[0]));
    } else if (iriView == "latitude") {
      checkNumArgs("geof:", iriView, 1);
      return createExpression<sparqlExpression::LatitudeExpression>(
          std::move(argList[0]));
    }
  }
  throw ParseException{"Function \"" + iri + "\" not supported"};
}

void Visitor::addVisibleVariable(string var) {
  addVisibleVariable(Variable{std::move(var)});
}

void Visitor::addVisibleVariable(Variable var) {
  visibleVariables_.back().emplace_back(std::move(var));
}

// ___________________________________________________________________________
namespace {
string stripAndLowercaseKeywordLiteral(std::string_view lit) {
  if (lit.size() > 2 && lit[0] == '"' && lit.back() == '"') {
    auto stripped = lit.substr(1, lit.size() - 2);
    return ad_utility::getLowercaseUtf8(stripped);
  }
  return std::string{lit};
}

// ___________________________________________________________________________
template <typename Current, typename... Others>
constexpr const ad_utility::Last<Current, Others...>* unwrapVariant(
    const auto& arg) {
  if constexpr (sizeof...(Others) > 0) {
    if constexpr (ad_utility::isSimilar<decltype(arg), Current>) {
      if (const auto ptr = std::get_if<ad_utility::First<Others...>>(&arg)) {
        return unwrapVariant<Others...>(*ptr);
      }
      return nullptr;
    } else {
      return unwrapVariant<Others...>(arg);
    }
  } else {
    return &arg;
  }
}
}  // namespace

// ___________________________________________________________________________
PathTuples joinPredicateAndObject(VarOrPath predicate, ObjectList objectList) {
  PathTuples tuples;
  for (auto& object : objectList.first) {
    // TODO The fulltext index should perform the splitting of its keywords,
    //  and not the SparqlParser.
    if (PropertyPath* path = std::get_if<PropertyPath>(&predicate)) {
      if (path->asString() == CONTAINS_WORD_PREDICATE ||
          // TODO _NS no longer needed?
          path->asString() == CONTAINS_WORD_PREDICATE_NS) {
        if (const Literal* literal =
                unwrapVariant<VarOrTerm, GraphTerm, Literal>(object)) {
          object = Literal{stripAndLowercaseKeywordLiteral(literal->literal())};
        }
      }
    }
    tuples.push_back({predicate, std::move(object)});
  }
  return tuples;
}

// ____________________________________________________________________________________
std::variant<ParsedQuery, Triples> Visitor::visitTypesafe(
    Parser::QueryContext* ctx) {
  // The prologue (BASE and PREFIX declarations)  only affects the internal
  // state of the visitor.
  visitTypesafe(ctx->prologue());
  if (ctx->selectQuery()) {
    return visitTypesafe(ctx->selectQuery());
  }
  if (ctx->constructQuery()) {
    return visitTypesafe(ctx->constructQuery());
  }
  reportError(ctx, "QLever only supports select and construct queries");
}

// ____________________________________________________________________________________
SelectClause Visitor::visitTypesafe(Parser::SelectClauseContext* ctx) {
  SelectClause select;

  select.distinct_ = static_cast<bool>(ctx->DISTINCT());
  select.reduced_ = static_cast<bool>(ctx->REDUCED());

  if (ctx->asterisk) {
    select.setAsterisk();
  } else {
    select.setSelected(visitVector(ctx->varOrAlias()));
  }
  return select;
}

// ____________________________________________________________________________________
VarOrAlias Visitor::visitTypesafe(Parser::VarOrAliasContext* ctx) {
  return visitAlternative<VarOrAlias>(ctx->var(), ctx->alias());
}

// ____________________________________________________________________________________
Alias Visitor::visitTypesafe(Parser::AliasContext* ctx) {
  // A SPARQL alias has only one child, namely the contents within
  // parentheses.
  return visitTypesafe(ctx->aliasWithoutBrackets());
}

// ____________________________________________________________________________________
Alias Visitor::visitTypesafe(Parser::AliasWithoutBracketsContext* ctx) {
  return {{visitTypesafe(ctx->expression())}, ctx->var()->getText()};
}

// ____________________________________________________________________________________
Triples Visitor::visitTypesafe(Parser::ConstructQueryContext* ctx) {
  if (!ctx->datasetClause().empty()) {
    reportError(ctx, "Datasets are not supported");
  }
  // TODO: once where clause is supported also process whereClause and
  // solutionModifiers
  if (ctx->constructTemplate()) {
    return visitTypesafe(ctx->constructTemplate()).value_or(Triples{});
  } else {
    return {};
  }
}

// ____________________________________________________________________________________
Variable Visitor::visitTypesafe(Parser::VarContext* ctx) {
  return Variable{ctx->getText()};
}

// ____________________________________________________________________________________
GraphPatternOperation Visitor::visitTypesafe(Parser::BindContext* ctx) {
  addVisibleVariable(ctx->var()->getText());
  return GraphPatternOperation{
      Bind{{visitTypesafe(ctx->expression())}, ctx->var()->getText()}};
}

// ____________________________________________________________________________________
GraphPatternOperation Visitor::visitTypesafe(Parser::InlineDataContext* ctx) {
  Values values = visitTypesafe(ctx->dataBlock());
  for (const auto& variable : values._inlineValues._variables) {
    addVisibleVariable(variable);
  }
  return GraphPatternOperation{std::move(values)};
}

// ____________________________________________________________________________________
Values Visitor::visitTypesafe(Parser::DataBlockContext* ctx) {
  return visitAlternative<Values>(ctx->inlineDataOneVar(),
                                  ctx->inlineDataFull());
}

// ____________________________________________________________________________________
std::optional<Values> Visitor::visitTypesafe(Parser::ValuesClauseContext* ctx) {
  return visitOptional(ctx->dataBlock());
}

// ____________________________________________________________________________________
GraphPattern Visitor::visitTypesafe(Parser::GroupGraphPatternContext* ctx) {
  GraphPattern pattern;
  pattern._id = numGraphPatterns_++;
  if (ctx->subSelect()) {
    auto [subquery, valuesOpt] = visitTypesafe(ctx->subSelect());
    pattern._graphPatterns.emplace_back(std::move(subquery));
    if (valuesOpt.has_value()) {
      pattern._graphPatterns.emplace_back(std::move(valuesOpt.value()));
    }
    return pattern;
  } else if (ctx->groupGraphPatternSub()) {
    auto [subOps, filters] = visitTypesafe(ctx->groupGraphPatternSub());
    pattern._graphPatterns = std::move(subOps);
    for (auto& filter : filters) {
      if (filter._type == SparqlFilter::LANG_MATCHES) {
        pattern.addLanguageFilter(filter._lhs, filter._rhs);
      } else {
        pattern._filters.push_back(std::move(filter));
      }
    }
    return pattern;
  } else {
    AD_FAIL();  // Unreachable
  }
}

Visitor::OperationsAndFilters Visitor::visitTypesafe(
    Parser::GroupGraphPatternSubContext* ctx) {
  vector<GraphPatternOperation> ops;
  vector<SparqlFilter> filters;

  auto filter = [&filters](SparqlFilter filter) {
    filters.emplace_back(std::move(filter));
  };
  auto op = [&ops](GraphPatternOperation op) {
    ops.emplace_back(std::move(op));
  };

  if (ctx->triplesBlock()) {
    ops.emplace_back(visitTypesafe(ctx->triplesBlock()));
  }
  auto others = visitVector(ctx->graphPatternNotTriplesAndMaybeTriples());
  for (auto& [graphPattern, triples] : others) {
    std::visit(ad_utility::OverloadCallOperator{filter, op},
               std::move(graphPattern));

    // TODO<C++23>: use `optional.transform` for this pattern.
    if (!triples.has_value()) {
      continue;
    }
    if (ops.empty() || !std::holds_alternative<BasicGraphPattern>(ops.back())) {
      ops.emplace_back(BasicGraphPattern{});
    }
    std::get<BasicGraphPattern>(ops.back())
        .appendTriples(std::move(triples.value()));
  }
  return {std::move(ops), std::move(filters)};
}

Visitor::OperationOrFilterAndMaybeTriples Visitor::visitTypesafe(
    Parser::GraphPatternNotTriplesAndMaybeTriplesContext* ctx) {
  return {visitTypesafe(ctx->graphPatternNotTriples()),
          visitOptional(ctx->triplesBlock())};
}

// ____________________________________________________________________________________
BasicGraphPattern Visitor::visitTypesafe(Parser::TriplesBlockContext* ctx) {
  auto iri = [](const Iri& iri) -> TripleComponent { return iri.toSparql(); };
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
  auto varOrPath = [&varPath,
                    &path](ad_utility::sparql_types::VarOrPath varOrPath) {
    return std::visit(ad_utility::OverloadCallOperator{varPath, path},
                      std::move(varOrPath));
  };

  auto registerIfVariable = [this](const auto& variant) {
    if (holds_alternative<Variable>(variant)) {
      addVisibleVariable(std::get<Variable>(variant));
    }
  };

  auto convertAndRegisterTriple =
      [&varOrTerm, &varOrPath,
       &registerIfVariable](TripleWithPropertyPath&& triple) -> SparqlTriple {
    registerIfVariable(triple.subject_);
    registerIfVariable(triple.predicate_);
    registerIfVariable(triple.object_);

    return {varOrTerm(std::move(triple.subject_)),
            varOrPath(std::move(triple.predicate_)),
            varOrTerm(std::move(triple.object_))};
  };

  BasicGraphPattern triples = {ad_utility::transform(
      visitTypesafe(ctx->triplesSameSubjectPath()), convertAndRegisterTriple)};
  if (ctx->triplesBlock()) {
    triples.appendTriples(visitTypesafe(ctx->triplesBlock()));
  }
  return triples;
}

// ____________________________________________________________________________________
Visitor::OperationOrFilter Visitor::visitTypesafe(
    Parser::GraphPatternNotTriplesContext* ctx) {
  if (ctx->graphGraphPattern() || ctx->serviceGraphPattern()) {
    reportError(ctx,
                "GraphGraphPattern or ServiceGraphPattern are not supported.");
  } else {
    return visitAlternative<std::variant<GraphPatternOperation, SparqlFilter>>(
        ctx->filterR(), ctx->optionalGraphPattern(), ctx->minusGraphPattern(),
        ctx->bind(), ctx->inlineData(), ctx->groupOrUnionGraphPattern());
  }
}

// ____________________________________________________________________________________
GraphPatternOperation Visitor::visitTypesafe(
    Parser::OptionalGraphPatternContext* ctx) {
  auto pattern = visitTypesafe(ctx->groupGraphPattern());
  return GraphPatternOperation{parsedQuery::Optional{std::move(pattern)}};
}

// ____________________________________________________________________________________
sparqlExpression::SparqlExpression::Ptr Visitor::visitTypesafe(
    Parser::ExpressionContext* ctx) {
  return visitTypesafe(ctx->conditionalOrExpression());
}

// ____________________________________________________________________________________
Visitor::PatternAndVisibleVariables Visitor::visitTypesafe(
    Parser::WhereClauseContext* ctx) {
  visibleVariables_.emplace_back();
  auto pattern = visitTypesafe(ctx->groupGraphPattern());
  auto visible = std::move(visibleVariables_.back());
  visibleVariables_.pop_back();
  return {std::move(pattern), std::move(visible)};
}

// ____________________________________________________________________________________
SolutionModifiers Visitor::visitTypesafe(Parser::SolutionModifierContext* ctx) {
  SolutionModifiers modifiers;
  visitIf(&modifiers.groupByVariables_, ctx->groupClause());
  visitIf(&modifiers.havingClauses_, ctx->havingClause());
  visitIf(&modifiers.orderBy_, ctx->orderClause());
  visitIf(&modifiers.limitOffset_, ctx->limitOffsetClauses());
  return modifiers;
}

// ____________________________________________________________________________________
LimitOffsetClause Visitor::visitTypesafe(
    Parser::LimitOffsetClausesContext* ctx) {
  LimitOffsetClause clause{};
  visitIf(&clause._limit, ctx->limitClause());
  visitIf(&clause._offset, ctx->offsetClause());
  visitIf(&clause._textLimit, ctx->textLimitClause());
  return clause;
}

// ____________________________________________________________________________________
vector<SparqlFilter> Visitor::visitTypesafe(Parser::HavingClauseContext* ctx) {
  return visitVector(ctx->havingCondition());
}

namespace {
SparqlFilter parseFilter(auto* ctx) {
  try {
    return SparqlParser::parseFilterExpression(ctx->getText());
  } catch (const std::bad_optional_access& error) {
    throw ParseException("The expression " + ctx->getText() +
                         " is currently not supported by Qlever inside a "
                         "FILTER or HAVING clause.");
  } catch (const ParseException& error) {
    throw ParseException("The expression " + ctx->getText() +
                         " is currently not supported by Qlever inside a "
                         "FILTER or HAVING clause. Details: " +
                         error.what());
  }
}
}  // namespace

// ____________________________________________________________________________________
SparqlFilter Visitor::visitTypesafe(Parser::HavingConditionContext* ctx) {
  SparqlFilter filter = parseFilter(ctx);
  if (filter._type == SparqlFilter::LANG_MATCHES) {
    throw ParseException(
        "Language filter in HAVING clause currently not "
        "supported by QLever. Got: " +
        ctx->getText());
  } else {
    return filter;
  }
}

// ____________________________________________________________________________________
vector<OrderKey> Visitor::visitTypesafe(Parser::OrderClauseContext* ctx) {
  return visitVector(ctx->orderCondition());
}

// ____________________________________________________________________________________
vector<GroupKey> Visitor::visitTypesafe(Parser::GroupClauseContext* ctx) {
  return visitVector(ctx->groupCondition());
}

// ____________________________________________________________________________________
std::optional<Triples> Visitor::visitTypesafe(
    Parser::ConstructTemplateContext* ctx) {
  return visitOptional(ctx->constructTriples());
}

// ____________________________________________________________________________________
string Visitor::visitTypesafe(Parser::StringContext* ctx) {
  // TODO: The string rule also allow triple quoted strings with different
  //  escaping rules. These are currently not handled. They should be parsed
  //  into a typesafe format with a unique representation.
  return ctx->getText();
}

// ____________________________________________________________________________________
string Visitor::visitTypesafe(Parser::IriContext* ctx) {
  // TODO return an IRI, not a std::string.
  string langtag =
      ctx->PREFIX_LANGTAG() ? ctx->PREFIX_LANGTAG()->getText() : "";
  if (ctx->iriref()) {
    return langtag + visitTypesafe(ctx->iriref());
  } else {
    AD_CHECK(ctx->prefixedName())
    return langtag + visitTypesafe(ctx->prefixedName());
  }
}

// ____________________________________________________________________________________
string Visitor::visitTypesafe(Parser::IrirefContext* ctx) {
  return RdfEscaping::unescapeIriref(ctx->getText());
}

// ____________________________________________________________________________________
string Visitor::visitTypesafe(Parser::PrefixedNameContext* ctx) {
  return visitAlternative<std::string>(ctx->pnameLn(), ctx->pnameNs());
}

// ____________________________________________________________________________________
string Visitor::visitTypesafe(Parser::PnameLnContext* ctx) {
  string text = ctx->getText();
  auto pos = text.find(':');
  auto pnameNS = text.substr(0, pos);
  auto pnLocal = text.substr(pos + 1);
  if (!_prefixMap.contains(pnameNS)) {
    // TODO<joka921> : proper name
    reportError(ctx, "Prefix " + pnameNS +
                         " was not registered using a PREFIX declaration");
  }
  auto inner = _prefixMap[pnameNS];
  // strip the trailing ">"
  inner = inner.substr(0, inner.size() - 1);
  return inner + RdfEscaping::unescapePrefixedIri(pnLocal) + ">";
}

// ____________________________________________________________________________________
string Visitor::visitTypesafe(Parser::PnameNsContext* ctx) {
  auto text = ctx->getText();
  auto prefix = text.substr(0, text.length() - 1);
  if (!_prefixMap.contains(prefix)) {
    // TODO<joka921> : proper name
    reportError(ctx, "Prefix " + prefix +
                         " was not registered using a PREFIX declaration");
  }
  return _prefixMap[prefix];
}

// ____________________________________________________________________________________
SparqlQleverVisitor::PrefixMap SparqlQleverVisitor::visitTypesafe(
    SparqlAutomaticParser::PrologueContext* ctx) {
  if (!ctx->baseDecl().empty()) {
    reportError(ctx->baseDecl(0), "BaseDecl is not supported.");
  }
  for (const auto& prefix : ctx->prefixDecl()) {
    visitTypesafe(prefix);
  }
  // TODO: we return a part of our internal state here. This will go away when
  //  queries can be parsed completely with ANTLR.
  return _prefixMap;
}

// ____________________________________________________________________________________
SparqlPrefix SparqlQleverVisitor::visitTypesafe(
    SparqlAutomaticParser::BaseDeclContext* ctx) {
  reportError(ctx, "BaseDecl is not supported.");
}

// ____________________________________________________________________________________
SparqlPrefix SparqlQleverVisitor::visitTypesafe(
    SparqlAutomaticParser::PrefixDeclContext* ctx) {
  auto text = ctx->PNAME_NS()->getText();
  // Remove the ':' at the end of the PNAME_NS
  auto prefixLabel = text.substr(0, text.length() - 1);
  auto prefixIri = visitTypesafe(ctx->iriref());
  _prefixMap[prefixLabel] = prefixIri;
  return {prefixLabel, prefixIri};
}

// ____________________________________________________________________________________
ParsedQuery Visitor::visitTypesafe(Parser::SelectQueryContext* ctx) {
  reportError(ctx, "SelectQuery is not yet supported.");
}

// ____________________________________________________________________________________
Visitor::SubQueryAndMaybeValues Visitor::visitTypesafe(
    Parser::SubSelectContext* ctx) {
  ParsedQuery query;
  query._clause = visitTypesafe(ctx->selectClause());
  auto [pattern, visibleVariables] = visitTypesafe(ctx->whereClause());
  query._rootGraphPattern = std::move(pattern);
  query.setNumInternalVariables(numInternalVariables_);
  query.addSolutionModifiers(visitTypesafe(ctx->solutionModifier()));
  numInternalVariables_ = query.getNumInternalVariables();
  auto values = visitTypesafe(ctx->valuesClause());
  for (const auto& variable : visibleVariables) {
    query.registerVariableVisibleInQueryBody(variable);
  }
  // Variables that are selected in this query are visible in the parent query.
  for (const auto& variable : query.selectClause().getSelectedVariables()) {
    addVisibleVariable(variable);
  }
  query._numGraphPatterns = numGraphPatterns_++;
  return {parsedQuery::Subquery{std::move(query)}, std::move(values)};
}

// ____________________________________________________________________________________
GroupKey Visitor::visitTypesafe(Parser::GroupConditionContext* ctx) {
  // TODO<qup42> Deploy an abstraction `visitExpressionPimpl(someContext*)` that
  // performs exactly those two steps and is also used in all the other places
  // where we currently call
  // `SparqlExpressionPimpl(visitTypesafe(ctx->something()))` manually.
  auto makeExpression = [&ctx, this](auto* subCtx) -> GroupKey {
    auto expr = SparqlExpressionPimpl{visitTypesafe(subCtx)};
    expr.setDescriptor(ctx->getText());
    return expr;
  };
  if (ctx->var() && !ctx->expression()) {
    return Variable{ctx->var()->getText()};
  } else if (ctx->builtInCall() || ctx->functionCall()) {
    // builtInCall and functionCall are both also an Expression
    return (ctx->builtInCall() ? makeExpression(ctx->builtInCall())
                               : makeExpression(ctx->functionCall()));
  } else if (ctx->expression()) {
    auto expr = SparqlExpressionPimpl{visitTypesafe(ctx->expression())};
    if (ctx->AS() && ctx->var()) {
      return Alias{std::move(expr), ctx->var()->getText()};
    } else {
      return expr;
    }
  }
  AD_FAIL();  // Should be unreachable.
}

// ____________________________________________________________________________________
OrderKey Visitor::visitTypesafe(Parser::OrderConditionContext* ctx) {
  auto visitExprOrderKey = [this](bool isDescending,
                                  auto* context) -> OrderKey {
    auto expr = SparqlExpressionPimpl{visitTypesafe(context)};
    if (auto exprIsVariable = expr.getVariableOrNullopt();
        exprIsVariable.has_value()) {
      return VariableOrderKey{exprIsVariable.value(), isDescending};
    } else {
      return ExpressionOrderKey{std::move(expr), isDescending};
    }
  };

  if (ctx->var()) {
    return VariableOrderKey(ctx->var()->getText());
  } else if (ctx->constraint()) {
    return visitExprOrderKey(false, ctx->constraint());
  } else if (ctx->brackettedExpression()) {
    return visitExprOrderKey(ctx->DESC() != nullptr,
                             ctx->brackettedExpression());
  }
  AD_FAIL();  // Should be unreachable.
}

// ____________________________________________________________________________________
unsigned long long int Visitor::visitTypesafe(Parser::LimitClauseContext* ctx) {
  return visitTypesafe(ctx->integer());
}

// ____________________________________________________________________________________
unsigned long long int Visitor::visitTypesafe(
    Parser::OffsetClauseContext* ctx) {
  return visitTypesafe(ctx->integer());
}

// ____________________________________________________________________________________
unsigned long long int Visitor::visitTypesafe(
    Parser::TextLimitClauseContext* ctx) {
  return visitTypesafe(ctx->integer());
}

// ____________________________________________________________________________________
SparqlValues Visitor::visitTypesafe(Parser::InlineDataOneVarContext* ctx) {
  SparqlValues values;
  auto var = visitTypesafe(ctx->var());
  values._variables.push_back(var.name());
  if (ctx->dataBlockValue().empty())
    reportError(ctx,
                "No values were specified in Values "
                "clause. This is not supported by QLever.");
  for (auto& dataBlockValue : ctx->dataBlockValue()) {
    values._values.push_back({visitTypesafe(dataBlockValue)});
  }
  return values;
}

// ____________________________________________________________________________________
SparqlValues Visitor::visitTypesafe(Parser::InlineDataFullContext* ctx) {
  SparqlValues values;
  if (ctx->dataBlockSingle().empty())
    reportError(ctx,
                "No values were specified in Values "
                "clause. This is not supported by QLever.");
  if (ctx->NIL())
    reportError(ctx,
                "No variables were specified in Values "
                "clause. This is not supported by QLever.");
  for (auto& var : ctx->var()) {
    values._variables.push_back(visitTypesafe(var).name());
  }
  values._values = visitVector(ctx->dataBlockSingle());
  if (std::any_of(values._values.begin(), values._values.end(),
                  [numVars = values._variables.size()](const auto& inner) {
                    return inner.size() != numVars;
                  })) {
    reportError(ctx,
                "The number of values in every data block must "
                "match the number of variables in a values clause.");
  }
  return values;
}

// ____________________________________________________________________________________
vector<std::string> Visitor::visitTypesafe(
    Parser::DataBlockSingleContext* ctx) {
  if (ctx->NIL())
    reportError(ctx,
                "No values were specified in DataBlock."
                "This is not supported by QLever.");
  return visitVector(ctx->dataBlockValue());
}

// ____________________________________________________________________________________
std::string Visitor::visitTypesafe(Parser::DataBlockValueContext* ctx) {
  // Return a string
  if (ctx->iri()) {
    return visitTypesafe(ctx->iri());
  } else if (ctx->rdfLiteral()) {
    return visitTypesafe(ctx->rdfLiteral());
  } else if (ctx->numericLiteral()) {
    // TODO implement
    reportError(ctx, "Numbers in values clauses are not supported.");
  } else if (ctx->booleanLiteral()) {
    // TODO implement
    reportError(ctx, "Booleans in values clauses are not supported.");
  } else if (ctx->UNDEF()) {
    // TODO implement
    reportError(ctx, "UNDEF in values clauses is not supported.");
  }
  AD_FAIL()  // Should be unreachable.
}

// ____________________________________________________________________________________
GraphPatternOperation Visitor::visitTypesafe(
    Parser::MinusGraphPatternContext* ctx) {
  return GraphPatternOperation{
      parsedQuery::Minus{visitTypesafe(ctx->groupGraphPattern())}};
}

// ____________________________________________________________________________________
namespace {
GraphPattern wrap(GraphPatternOperation op) {
  auto pattern = GraphPattern();
  pattern._graphPatterns.emplace_back(std::move(op));
  return pattern;
}
}  // namespace

// ____________________________________________________________________________________
GraphPatternOperation Visitor::visitTypesafe(
    Parser::GroupOrUnionGraphPatternContext* ctx) {
  auto children = visitVector(ctx->groupGraphPattern());
  if (children.size() > 1) {
    // https://en.cppreference.com/w/cpp/algorithm/accumulate
    // a similar thing is done in QueryPlaner::uniteGraphPatterns
    auto foldOp = [](GraphPatternOperation op1, GraphPattern op2) {
      return GraphPatternOperation{
          parsedQuery::Union{wrap(std::move(op1)), std::move(op2)}};
    };
    // TODO<joka921> QLever should support Nary UNIONs directly.
    return std::accumulate(std::next(children.begin(), 2), children.end(),
                           GraphPatternOperation{parsedQuery::Union{
                               std::move(children[0]), std::move(children[1])}},
                           foldOp);
  } else {
    return GraphPatternOperation{
        parsedQuery::GroupGraphPattern{std::move(children[0])}};
  }
}

// ____________________________________________________________________________________
SparqlFilter Visitor::visitTypesafe(Parser::FilterRContext* ctx) {
  return parseFilter(ctx->constraint());
}

// ____________________________________________________________________________________
ExpressionPtr Visitor::visitTypesafe(Parser::ConstraintContext* ctx) {
  return visitAlternative<ExpressionPtr>(
      ctx->brackettedExpression(), ctx->builtInCall(), ctx->functionCall());
}

// ____________________________________________________________________________________
ExpressionPtr Visitor::visitTypesafe(Parser::FunctionCallContext* ctx) {
  return processIriFunctionCall(visitTypesafe(ctx->iri()),
                                visitTypesafe(ctx->argList()));
}

// ____________________________________________________________________________________
vector<Visitor::ExpressionPtr> Visitor::visitTypesafe(
    Parser::ArgListContext* ctx) {
  // If no arguments, return empty expression vector.
  if (ctx->NIL()) {
    return std::vector<ExpressionPtr>{};
  }
  // The grammar allows an optional DISTICT before the argument list (the
  // whole list, not the individual arguments), but we currently don't support
  // it.
  if (ctx->DISTINCT()) {
    reportError(
        ctx, "DISTINCT for argument lists of IRI functions are not supported");
  }
  // Visit the expression of each argument.
  return visitVector(ctx->expression());
}

// ____________________________________________________________________________________
Triples Visitor::visitTypesafe(Parser::ConstructTriplesContext* ctx) {
  auto result = visitTypesafe(ctx->triplesSameSubject());
  if (ctx->constructTriples()) {
    auto newTriples = visitTypesafe(ctx->constructTriples());
    ad_utility::appendVector(result, std::move(newTriples));
  }
  return result;
}

// ____________________________________________________________________________________
Triples Visitor::visitTypesafe(Parser::TriplesSameSubjectContext* ctx) {
  Triples triples;
  if (ctx->varOrTerm()) {
    VarOrTerm subject = visitTypesafe(ctx->varOrTerm());
    AD_CHECK(ctx->propertyListNotEmpty());
    auto propertyList = visitTypesafe(ctx->propertyListNotEmpty());
    for (auto& tuple : propertyList.first) {
      triples.push_back({subject, std::move(tuple[0]), std::move(tuple[1])});
    }
    ad_utility::appendVector(triples, std::move(propertyList.second));
  } else if (ctx->triplesNode()) {
    auto tripleNodes = visitTriplesNode(ctx->triplesNode()).as<Node>();
    ad_utility::appendVector(triples, std::move(tripleNodes.second));
    AD_CHECK(ctx->propertyList());
    auto propertyList = visitTypesafe(ctx->propertyList());
    for (auto& tuple : propertyList.first) {
      triples.push_back(
          {tripleNodes.first, std::move(tuple[0]), std::move(tuple[1])});
    }
    ad_utility::appendVector(triples, std::move(propertyList.second));
  } else {
    // Invalid grammar
    AD_FAIL();
  }
  return triples;
}

// ____________________________________________________________________________________
PropertyList Visitor::visitTypesafe(Parser::PropertyListContext* ctx) {
  return ctx->propertyListNotEmpty()
             ? visitTypesafe(ctx->propertyListNotEmpty())
             : PropertyList{Tuples{}, Triples{}};
}

// ____________________________________________________________________________________
PropertyList Visitor::visitTypesafe(Parser::PropertyListNotEmptyContext* ctx) {
  Tuples triplesWithoutSubject;
  Triples additionalTriples;
  auto verbs = ctx->verb();
  auto objectLists = ctx->objectList();
  for (size_t i = 0; i < verbs.size(); i++) {
    // TODO use zip-style approach once C++ supports ranges
    auto objectList = visitTypesafe(objectLists.at(i));
    auto verb = visitTypesafe(verbs.at(i));
    for (auto& object : objectList.first) {
      triplesWithoutSubject.push_back({verb, std::move(object)});
    }
    ad_utility::appendVector(additionalTriples, std::move(objectList.second));
  }
  return {std::move(triplesWithoutSubject), std::move(additionalTriples)};
}

// ____________________________________________________________________________________
VarOrTerm Visitor::visitTypesafe(Parser::VerbContext* ctx) {
  // TODO<qup42, joka921> Is there a way to make this visitAlternative in the
  // presence of the a case?
  if (ctx->varOrIri()) {
    return visitTypesafe(ctx->varOrIri());
  } else if (ctx->getText() == "a") {
    // Special keyword 'a'
    return GraphTerm{Iri{"<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>"}};
  } else {
    AD_FAIL()  // Should be unreachable.
  }
}

// ____________________________________________________________________________________
ObjectList Visitor::visitTypesafe(Parser::ObjectListContext* ctx) {
  Objects objects;
  Triples additionalTriples;
  auto objectContexts = ctx->objectR();
  for (auto& objectContext : objectContexts) {
    auto graphNode = visitTypesafe(objectContext);
    ad_utility::appendVector(additionalTriples, std::move(graphNode.second));
    objects.push_back(std::move(graphNode.first));
  }
  return {std::move(objects), std::move(additionalTriples)};
}

// ____________________________________________________________________________________
Node Visitor::visitTypesafe(Parser::ObjectRContext* ctx) {
  return visitTypesafe(ctx->graphNode());
}

// ___________________________________________________________________________
vector<TripleWithPropertyPath> SparqlQleverVisitor::visitTypesafe(
    SparqlAutomaticParser::TriplesSameSubjectPathContext* ctx) {
  if (ctx->varOrTerm()) {
    vector<TripleWithPropertyPath> triples;
    auto subject = visitTypesafe(ctx->varOrTerm());
    auto tuples = visitTypesafe(ctx->propertyListPathNotEmpty());
    for (auto& [predicate, object] : tuples) {
      // TODO<clang,c++20> clang does not yet support emplace_back for
      // aggregates.
      triples.push_back(TripleWithPropertyPath{subject, std::move(predicate),
                                               std::move(object)});
    }
    return triples;
  } else if (ctx->triplesNodePath()) {
    throwCollectionsAndBlankNodePathsNotSupported(ctx->triplesNodePath());
  } else {
    AD_FAIL()  // Should be unreachable.
  }
}

// ___________________________________________________________________________
std::optional<PathTuples> SparqlQleverVisitor::visitTypesafe(
    SparqlAutomaticParser::PropertyListPathContext* ctx) {
  return visitOptional(ctx->propertyListPathNotEmpty());
}

// ___________________________________________________________________________
PathTuples SparqlQleverVisitor::visitTypesafe(
    SparqlAutomaticParser::PropertyListPathNotEmptyContext* ctx) {
  PathTuples tuples = visitTypesafe(ctx->tupleWithPath());
  vector<PathTuples> tuplesWithoutPaths = visitVector(ctx->tupleWithoutPath());
  for (auto& tuplesWithoutPath : tuplesWithoutPaths) {
    tuples.insert(tuples.end(), tuplesWithoutPath.begin(),
                  tuplesWithoutPath.end());
  }
  return tuples;
}

// ____________________________________________________________________________________
PropertyPath Visitor::visitTypesafe(Parser::VerbPathContext* ctx) {
  PropertyPath p = visitTypesafe(ctx->path());
  // TODO move computeCanBeNull into PropertyPath constructor.
  p.computeCanBeNull();
  return p;
}

// ____________________________________________________________________________________
Variable Visitor::visitTypesafe(Parser::VerbSimpleContext* ctx) {
  return visitTypesafe(ctx->var());
}

// ____________________________________________________________________________________
PathTuples SparqlQleverVisitor::visitTypesafe(
    SparqlAutomaticParser::TupleWithoutPathContext* ctx) {
  VarOrPath predicate = visitTypesafe(ctx->verbPathOrSimple());
  ObjectList objectList = visitTypesafe(ctx->objectList());
  return joinPredicateAndObject(predicate, objectList);
}

// ____________________________________________________________________________________
PathTuples SparqlQleverVisitor::visitTypesafe(
    SparqlAutomaticParser::TupleWithPathContext* ctx) {
  VarOrPath predicate = visitTypesafe(ctx->verbPathOrSimple());
  ObjectList objectList = visitTypesafe(ctx->objectListPath());
  return joinPredicateAndObject(predicate, objectList);
}

// ____________________________________________________________________________________
VarOrPath Visitor::visitTypesafe(Parser::VerbPathOrSimpleContext* ctx) {
  return visitAlternative<ad_utility::sparql_types::VarOrPath>(
      ctx->verbPath(), ctx->verbSimple());
}

// ___________________________________________________________________________
ObjectList SparqlQleverVisitor::visitTypesafe(
    SparqlAutomaticParser::ObjectListPathContext* ctx) {
  // The second parameter is empty because collections and blank not paths,
  // which might add additional triples, are currently not supported.
  // When this is implemented they will be returned by visit(ObjectPathContext).
  return {visitVector(ctx->objectPath()), {}};
}

// ____________________________________________________________________________________
VarOrTerm Visitor::visitTypesafe(Parser::ObjectPathContext* ctx) {
  return visitTypesafe(ctx->graphNodePath());
}

// ____________________________________________________________________________________
PropertyPath Visitor::visitTypesafe(Parser::PathContext* ctx) {
  return visitTypesafe(ctx->pathAlternative());
}

// ____________________________________________________________________________________
PropertyPath Visitor::visitTypesafe(Parser::PathAlternativeContext* ctx) {
  return PropertyPath::makeAlternative(visitVector(ctx->pathSequence()));
}

// ____________________________________________________________________________________
PropertyPath Visitor::visitTypesafe(Parser::PathSequenceContext* ctx) {
  return PropertyPath::makeSequence(visitVector(ctx->pathEltOrInverse()));
}

// ____________________________________________________________________________________
PropertyPath Visitor::visitTypesafe(Parser::PathEltContext* ctx) {
  PropertyPath p = visitTypesafe(ctx->pathPrimary());

  if (ctx->pathMod()) {
    // TODO move case distinction +/*/? into PropertyPath.
    if (ctx->pathMod()->getText() == "+") {
      p = PropertyPath::makeTransitiveMin(p, 1);
    } else if (ctx->pathMod()->getText() == "?") {
      p = PropertyPath::makeTransitiveMax(p, 1);
    } else if (ctx->pathMod()->getText() == "*") {
      p = PropertyPath::makeTransitive(p);
    } else {
      AD_FAIL()  // Should be unreachable.
    }
  }

  return p;
}

// ____________________________________________________________________________________
PropertyPath Visitor::visitTypesafe(Parser::PathEltOrInverseContext* ctx) {
  PropertyPath p = visitTypesafe(ctx->pathElt());

  if (ctx->negationOperator) {
    p = PropertyPath::makeInverse(std::move(p));
  }

  return p;
}

// ____________________________________________________________________________________
PropertyPath Visitor::visitTypesafe(Parser::PathPrimaryContext* ctx) {
  // TODO<qup42, joka921> Is there a way to make this visitAlternative in the
  // presence of the a case?
  if (ctx->iri()) {
    return PropertyPath::fromIri(visitTypesafe(ctx->iri()));
  } else if (ctx->path()) {
    return visitTypesafe(ctx->path());
  } else if (ctx->pathNegatedPropertySet()) {
    return visitTypesafe(ctx->pathNegatedPropertySet());
  } else if (ctx->getText() == "a") {
    // Special keyword 'a'
    return PropertyPath::fromIri(
        "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>");
  }
  AD_FAIL()  // Should be unreachable.
}

// ____________________________________________________________________________________
PropertyPath Visitor::visitTypesafe(
    Parser::PathNegatedPropertySetContext* ctx) {
  reportError(ctx, "\"!\" inside a property path is not supported by QLever.");
}

// ____________________________________________________________________________________
unsigned long long int Visitor::visitTypesafe(Parser::IntegerContext* ctx) {
  try {
    return std::stoull(ctx->getText());
  } catch (const std::out_of_range&) {
    reportError(
        ctx,
        "Integer " + ctx->getText() +
            " does not fit into 64 bits. This is not supported by QLever.");
  }
}

// ____________________________________________________________________________________
Node Visitor::visitTypesafe(Parser::TriplesNodeContext* ctx) {
  return visitAlternative<Node>(ctx->collection(),
                                ctx->blankNodePropertyList());
}

// ____________________________________________________________________________________
Node Visitor::visitTypesafe(Parser::BlankNodePropertyListContext* ctx) {
  VarOrTerm var{GraphTerm{newBlankNode()}};
  Triples triples;
  auto propertyList = visitTypesafe(ctx->propertyListNotEmpty());
  for (auto& tuple : propertyList.first) {
    triples.push_back({var, std::move(tuple[0]), std::move(tuple[1])});
  }
  ad_utility::appendVector(triples, std::move(propertyList.second));
  return {std::move(var), std::move(triples)};
}

// ____________________________________________________________________________________
Node Visitor::visitTypesafe(Parser::CollectionContext* ctx) {
  Triples triples;
  VarOrTerm nextElement{
      GraphTerm{Iri{"<http://www.w3.org/1999/02/22-rdf-syntax-ns#nil>"}}};
  auto nodes = ctx->graphNode();
  for (auto context : Reversed{nodes}) {
    VarOrTerm currentVar{GraphTerm{newBlankNode()}};
    auto graphNode = visitTypesafe(context);

    triples.push_back(
        {currentVar,
         VarOrTerm{GraphTerm{
             Iri{"<http://www.w3.org/1999/02/22-rdf-syntax-ns#first>"}}},
         std::move(graphNode.first)});
    triples.push_back(
        {currentVar,
         VarOrTerm{GraphTerm{
             Iri{"<http://www.w3.org/1999/02/22-rdf-syntax-ns#rest>"}}},
         std::move(nextElement)});
    nextElement = std::move(currentVar);

    ad_utility::appendVector(triples, std::move(graphNode.second));
  }
  return {std::move(nextElement), std::move(triples)};
}

// ____________________________________________________________________________________
Node Visitor::visitTypesafe(Parser::GraphNodeContext* ctx) {
  if (ctx->varOrTerm()) {
    return {visitTypesafe(ctx->varOrTerm()), Triples{}};
  } else if (ctx->triplesNode()) {
    return visitTriplesNode(ctx->triplesNode()).as<Node>();
  } else {
    AD_FAIL();
  }
}

// ____________________________________________________________________________________
VarOrTerm SparqlQleverVisitor::visitTypesafe(
    SparqlAutomaticParser::GraphNodePathContext* ctx) {
  if (ctx->varOrTerm()) {
    return visitTypesafe(ctx->varOrTerm());
  } else if (ctx->triplesNodePath()) {
    throwCollectionsAndBlankNodePathsNotSupported(ctx->triplesNodePath());
  } else {
    AD_FAIL()  // Should be unreachable.
  }
}

// ____________________________________________________________________________________
VarOrTerm Visitor::visitTypesafe(Parser::VarOrTermContext* ctx) {
  return visitAlternative<VarOrTerm>(ctx->var(), ctx->graphTerm());
}

// ____________________________________________________________________________________
VarOrTerm Visitor::visitTypesafe(Parser::VarOrIriContext* ctx) {
  if (ctx->var()) {
    return visitTypesafe(ctx->var());
  } else if (ctx->iri()) {
    // TODO<qup42> If `visitTypesafe` returns an `Iri` and `VarOrTerm` can be
    // constructed from an `Iri`, this whole function becomes
    // `visitAlternative`.
    return GraphTerm{Iri{visitTypesafe(ctx->iri())}};
  } else {
    AD_FAIL()  // Should be unreachable.
  }
}

// ____________________________________________________________________________________
GraphTerm Visitor::visitTypesafe(Parser::GraphTermContext* ctx) {
  if (ctx->blankNode()) {
    return visitTypesafe(ctx->blankNode());
  } else if (ctx->iri()) {
    return Iri{visitTypesafe(ctx->iri())};
  } else if (ctx->NIL()) {
    return Iri{"<http://www.w3.org/1999/02/22-rdf-syntax-ns#nil>"};
  } else {
    return visitAlternative<Literal>(ctx->numericLiteral(),
                                     ctx->booleanLiteral(), ctx->rdfLiteral());
  }
}

// ____________________________________________________________________________________
ExpressionPtr Visitor::visitTypesafe(
    Parser::ConditionalOrExpressionContext* ctx) {
  auto childContexts = ctx->conditionalAndExpression();
  auto children = visitVector(ctx->conditionalAndExpression());
  AD_CHECK(!children.empty());
  auto result = std::move(children.front());
  using C = sparqlExpression::OrExpression::Children;
  std::for_each(children.begin() + 1, children.end(),
                [&result](ExpressionPtr& ptr) {
                  result = std::make_unique<sparqlExpression::OrExpression>(
                      C{std::move(result), std::move(ptr)});
                });
  result->descriptor() = ctx->getText();
  return result;
}

// ____________________________________________________________________________________
ExpressionPtr Visitor::visitTypesafe(
    Parser::ConditionalAndExpressionContext* ctx) {
  auto children = visitVector(ctx->valueLogical());
  AD_CHECK(!children.empty());
  auto result = std::move(children.front());
  using C = sparqlExpression::AndExpression::Children;
  std::for_each(children.begin() + 1, children.end(),
                [&result](ExpressionPtr& ptr) {
                  result = std::make_unique<sparqlExpression::AndExpression>(
                      C{std::move(result), std::move(ptr)});
                });
  result->descriptor() = ctx->getText();
  return result;
}

// ____________________________________________________________________________________
ExpressionPtr Visitor::visitTypesafe(Parser::ValueLogicalContext* ctx) {
  return visitTypesafe(ctx->relationalExpression());
}

// ____________________________________________________________________________________
ExpressionPtr Visitor::visitTypesafe(Parser::RelationalExpressionContext* ctx) {
  auto childContexts = ctx->numericExpression();

  if (childContexts.size() == 1) {
    return visitTypesafe(childContexts[0]);
  }
  if (false) {
    // TODO<joka921> Once we have reviewed and merged the EqualsExpression,
    // this can be uncommented.
    /*
   if (ctx->children[1]->getText() == "=") {
     auto leftChild = std::move(
         visitNumericExpression(childContexts[0]).as<ExpressionPtr>());
     auto rightChild = std::move(
         visitNumericExpression(childContexts[1]).as<ExpressionPtr>());

     return
   ExpressionPtr{std::make_unique<sparqlExpression::EqualsExpression>(
         std::move(leftChild), std::move(rightChild))};

     */
  } else {
    reportError(
        ctx,
        "This parser does not yet support relational expressions = < etc.");
  }
}

// ____________________________________________________________________________________
ExpressionPtr Visitor::visitTypesafe(Parser::NumericExpressionContext* ctx) {
  return visitTypesafe(ctx->additiveExpression());
}

// ____________________________________________________________________________________
ExpressionPtr Visitor::visitTypesafe(Parser::AdditiveExpressionContext* ctx) {
  auto children = visitVector(ctx->multiplicativeExpression());
  auto opTypes = visitOperationTags(ctx->children, {"+", "-"});

  if (!ctx->strangeMultiplicativeSubexprOfAdditive().empty()) {
    reportError(ctx,
                "You currently have to put a space between a +/- and the "
                "number after it.");
  }

  AD_CHECK(!children.empty());
  AD_CHECK(children.size() == opTypes.size() + 1);

  auto result = std::move(children.front());
  auto childIt = children.begin() + 1;
  auto opIt = opTypes.begin();
  while (childIt != children.end()) {
    if (*opIt == "+") {
      result = createExpression<sparqlExpression::AddExpression>(
          std::move(result), std::move(*childIt));
    } else if (*opIt == "-") {
      result = createExpression<sparqlExpression::SubtractExpression>(
          std::move(result), std::move(*childIt));
    } else {
      AD_FAIL();
    }
    ++childIt;
    ++opIt;
  }
  return result;
}

// ____________________________________________________________________________________
ExpressionPtr Visitor::visitTypesafe(
    Parser::MultiplicativeExpressionContext* ctx) {
  auto children = visitVector(ctx->unaryExpression());
  auto opTypes = visitOperationTags(ctx->children, {"*", "/"});

  AD_CHECK(!children.empty());
  AD_CHECK(children.size() == opTypes.size() + 1);

  auto result = std::move(children.front());
  auto childIt = children.begin() + 1;
  auto opIt = opTypes.begin();
  while (childIt != children.end()) {
    if (*opIt == "*") {
      result = createExpression<sparqlExpression::MultiplyExpression>(
          std::move(result), std::move(*childIt));
    } else if (*opIt == "/") {
      result = createExpression<sparqlExpression::DivideExpression>(
          std::move(result), std::move(*childIt));
    } else {
      AD_FAIL();
    }
    ++childIt;
    ++opIt;
  }
  return result;
}

// ____________________________________________________________________________________
ExpressionPtr Visitor::visitTypesafe(Parser::UnaryExpressionContext* ctx) {
  auto child = visitTypesafe(ctx->primaryExpression());
  if (ctx->children[0]->getText() == "-") {
    return createExpression<sparqlExpression::UnaryMinusExpression>(
        std::move(child));
  } else if (ctx->getText() == "!") {
    return createExpression<sparqlExpression::UnaryNegateExpression>(
        std::move(child));
  } else {
    // no sign or an explicit '+'
    return child;
  }
}

// ____________________________________________________________________________________
ExpressionPtr Visitor::visitTypesafe(Parser::PrimaryExpressionContext* ctx) {
  using std::make_unique;
  using namespace sparqlExpression;

  if (ctx->rdfLiteral()) {
    // TODO<joka921> : handle strings with value datatype that are
    // not in the knowledge base correctly.
    return make_unique<StringOrIriExpression>(ctx->rdfLiteral()->getText());
  } else if (ctx->numericLiteral()) {
    auto integralWrapper = [](int64_t x) {
      return ExpressionPtr{make_unique<IntExpression>(x)};
    };
    auto doubleWrapper = [](double x) {
      return ExpressionPtr{make_unique<DoubleExpression>(x)};
    };
    return std::visit(
        ad_utility::OverloadCallOperator{integralWrapper, doubleWrapper},
        visitTypesafe(ctx->numericLiteral()));
  } else if (ctx->booleanLiteral()) {
    return make_unique<BoolExpression>(visitTypesafe(ctx->booleanLiteral()));
  } else if (ctx->var()) {
    return make_unique<VariableExpression>(
        sparqlExpression::Variable{ctx->var()->getText()});
  } else {
    return visitAlternative<ExpressionPtr>(
        ctx->builtInCall(), ctx->iriOrFunction(), ctx->brackettedExpression());
  }
}

// ____________________________________________________________________________________
ExpressionPtr Visitor::visitTypesafe(Parser::BrackettedExpressionContext* ctx) {
  return visitTypesafe(ctx->expression());
}

// ____________________________________________________________________________________
ExpressionPtr Visitor::visitTypesafe(
    [[maybe_unused]] Parser::BuiltInCallContext* ctx) {
  if (ctx->aggregate()) {
    return visitTypesafe(ctx->aggregate());
    // TODO: Implement built-in calls according to the following examples.
    //
    // } else if (ad_utility::getLowercase(ctx->children[0]->getText()) ==
    //            "sqr") {
    //   if (ctx->expression().size() != 1) {
    //     throw ParseException{"SQR needs one argument"};
    //   }
    //   auto children = visitVector<ExpressionPtr>(ctx->expression());
    //   return createExpression<sparqlExpression::SquareExpression>(
    //       std::move(children[0]));
    // } else if (ad_utility::getLowercase(ctx->children[0]->getText()) ==
    //            "dist") {
    //   if (ctx->expression().size() != 2) {
    //     throw ParseException{"DIST needs two arguments"};
    //   }
    //   auto children = visitVector<ExpressionPtr>(ctx->expression());
    //   return createExpression<sparqlExpression::DistExpression>(
    //       std::move(children[0]), std::move(children[1]));
  } else {
    reportError(ctx,
                "Built-in function not yet implemented (only aggregates like "
                "COUNT so far)");
  }
}

// ____________________________________________________________________________________
ExpressionPtr Visitor::visitTypesafe(Parser::AggregateContext* ctx) {
  using namespace sparqlExpression;
  // the only case that there is no child expression is COUNT(*), so we can
  // check this outside the if below.
  if (!ctx->expression()) {
    reportError(ctx,
                "This parser currently doesn't support COUNT(*), please "
                "specify an explicit expression for the COUNT");
  }
  auto childExpression = visitTypesafe(ctx->expression());
  auto children = ctx->children;
  bool distinct = false;
  for (const auto& child : children) {
    if (ad_utility::getLowercase(child->getText()) == "distinct") {
      distinct = true;
    }
  }
  auto makePtr = [&]<typename ExpressionType>(auto&&... additionalArgs) {
    ExpressionPtr result{std::make_unique<ExpressionType>(
        distinct, std::move(childExpression), AD_FWD(additionalArgs)...)};
    result->descriptor() = ctx->getText();
    return result;
  };

  std::string functionName = ad_utility::getLowercase(children[0]->getText());

  if (functionName == "count") {
    return makePtr.operator()<CountExpression>();
  } else if (functionName == "sum") {
    return makePtr.operator()<SumExpression>();
  } else if (functionName == "max") {
    return makePtr.operator()<MaxExpression>();
  } else if (functionName == "min") {
    return makePtr.operator()<MinExpression>();
  } else if (functionName == "avg") {
    return makePtr.operator()<AvgExpression>();
  } else if (functionName == "group_concat") {
    // Use a space as a default separator

    std::string separator;
    if (ctx->string()) {
      // TODO: The string rule also allow triple quoted strings with different
      //  escaping rules. These are currently not handled. They should be parsed
      //  into a typesafe format with a unique representation.
      separator = visitTypesafe(ctx->string());
      // If there was a separator, we have to strip the quotation marks
      AD_CHECK(separator.size() >= 2);
      separator = separator.substr(1, separator.size() - 2);
    } else {
      separator = " "s;
    }

    return makePtr.operator()<GroupConcatExpression>(std::move(separator));
  } else if (functionName == "sample") {
    return makePtr.operator()<SampleExpression>();
  }
  AD_FAIL()  // Should be unreachable.
}

// ____________________________________________________________________________________
ExpressionPtr Visitor::visitTypesafe(Parser::IriOrFunctionContext* ctx) {
  // Case 1: Just an IRI.
  if (ctx->argList() == nullptr) {
    return std::make_unique<sparqlExpression::StringOrIriExpression>(
        ctx->getText());
  }
  // Case 2: Function call, where the function name is an IRI.
  return processIriFunctionCall(visitTypesafe(ctx->iri()),
                                visitTypesafe(ctx->argList()));
}

// ____________________________________________________________________________________
std::string Visitor::visitTypesafe(Parser::RdfLiteralContext* ctx) {
  // TODO: This should really be an RdfLiteral class that stores a unified
  //  version of the string, and the langtag/datatype separately.
  string ret = visitTypesafe(ctx->string());
  if (ctx->LANGTAG()) {
    ret += ctx->LANGTAG()->getText();
  } else if (ctx->iri()) {
    ret += ("^^" + visitTypesafe(ctx->iri()));
  }
  return ret;
}

// ____________________________________________________________________________________
std::variant<int64_t, double> Visitor::visitTypesafe(
    Parser::NumericLiteralContext* ctx) {
  return visitAlternative<std::variant<int64_t, double>>(
      ctx->numericLiteralUnsigned(), ctx->numericLiteralPositive(),
      ctx->numericLiteralNegative());
}

// ____________________________________________________________________________________
namespace {
template <typename Ctx>
std::variant<int64_t, double> parseNumericLiteral(Ctx* ctx, bool parseAsInt) {
  try {
    if (parseAsInt) {
      return std::stoll(ctx->getText());
    } else {
      return std::stod(ctx->getText());
    }
  } catch (const std::out_of_range& range) {
    throw ParseException("Could not parse Numeric Literal \"" + ctx->getText() +
                         "\". It is out of range. Reason: " + range.what());
  }
}
}  // namespace

// ____________________________________________________________________________________
std::variant<int64_t, double> Visitor::visitTypesafe(
    Parser::NumericLiteralUnsignedContext* ctx) {
  return parseNumericLiteral(ctx, ctx->INTEGER());
}

// ____________________________________________________________________________________
std::variant<int64_t, double> Visitor::visitTypesafe(
    Parser::NumericLiteralPositiveContext* ctx) {
  return parseNumericLiteral(ctx, ctx->INTEGER_POSITIVE());
}

// ____________________________________________________________________________________
std::variant<int64_t, double> Visitor::visitTypesafe(
    Parser::NumericLiteralNegativeContext* ctx) {
  return parseNumericLiteral(ctx, ctx->INTEGER_NEGATIVE());
}

// ____________________________________________________________________________________
bool Visitor::visitTypesafe(Parser::BooleanLiteralContext* ctx) {
  return ctx->getText() == "true";
}

// ____________________________________________________________________________________
BlankNode Visitor::visitTypesafe(Parser::BlankNodeContext* ctx) {
  if (ctx->ANON()) {
    return newBlankNode();
  }
  if (ctx->BLANK_NODE_LABEL()) {
    // strip _: prefix from string
    constexpr size_t length = std::string_view{"_:"}.length();
    const string label = ctx->BLANK_NODE_LABEL()->getText().substr(length);
    // false means the query explicitly contains a blank node label
    return {false, label};
  }
  // invalid grammar
  AD_FAIL();
}

// ____________________________________________________________________________________

template <typename Ctx>
auto Visitor::visitVector(const std::vector<Ctx*>& childContexts)
    -> std::vector<decltype(visitTypesafe(childContexts[0]))> {
  std::vector<decltype(visitTypesafe(childContexts[0]))> children;
  for (const auto& child : childContexts) {
    children.emplace_back(std::move(visitTypesafe(child)));
  }
  return children;
}

// ____________________________________________________________________________________

template <typename Out, typename... Contexts>
Out Visitor::visitAlternative(Contexts*... ctxs) {
  // Check that exactly one of the `ctxs` is not `nullptr`.
  AD_CHECK(1u == (... + static_cast<bool>(ctxs)));
  std::optional<Out> out;
  // Visit the one `context` which is not null and write the result to `out`.
  (..., visitIf<std::optional<Out>, Out>(&out, ctxs));
  return std::move(out.value());
}

// ____________________________________________________________________________________
template <typename Ctx>
auto Visitor::visitOptional(Ctx* ctx)
    -> std::optional<decltype(visitTypesafe(ctx))> {
  if (ctx) {
    return visitTypesafe(ctx);
  } else {
    return std::nullopt;
  }
}

// ____________________________________________________________________________________
template <typename Target, typename Intermediate, typename Ctx>
void Visitor::visitIf(Target* target, Ctx* ctx) {
  if (ctx) {
    *target = Intermediate{visitTypesafe(ctx)};
  }
}

// ____________________________________________________________________________________
void Visitor::reportError(antlr4::ParserRuleContext* ctx,
                          const std::string& msg) {
  throw ParseException{
      absl::StrCat("Clause \"", ctx->getText(), "\" at line ",
                   ctx->getStart()->getLine(), ":",
                   ctx->getStart()->getCharPositionInLine(), " ", msg),
      generateMetadata(ctx)};
}
