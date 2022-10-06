// Copyright 2021, University of Freiburg,
// Chair of Algorithms and Data Structures
// Authors:
//   Hannah Bast <bast@cs.uni-freiburg.de>
//   2022 Julian Mundhahs <mundhahj@tf.uni-freiburg.de>

#include "parser/sparqlParser/SparqlQleverVisitor.h"

#include <string>
#include <vector>

#include "engine/sparqlExpressions/LangExpression.h"
#include "engine/sparqlExpressions/RegexExpression.h"
#include "engine/sparqlExpressions/RelationalExpressions.h"
#include "parser/SparqlParser.h"
#include "parser/TokenizerCtre.h"
#include "parser/TurtleParser.h"

using namespace ad_utility::sparql_types;
using namespace sparqlExpression;
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
      if (path->asString() == CONTAINS_WORD_PREDICATE) {
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

// ___________________________________________________________________________
SparqlExpressionPimpl Visitor::visitExpressionPimpl(auto* ctx) {
  return SparqlExpressionPimpl{visit(ctx), std::move(ctx->getText())};
}

// ____________________________________________________________________________________
ParsedQuery Visitor::visit(Parser::QueryContext* ctx) {
  // The prologue (BASE and PREFIX declarations)  only affects the internal
  // state of the visitor.
  visit(ctx->prologue());
  auto query =
      visitAlternative<ParsedQuery>(ctx->selectQuery(), ctx->constructQuery(),
                                    ctx->describeQuery(), ctx->askQuery());

  query._originalString = ctx->getStart()->getInputStream()->toString();

  return query;
}

// ____________________________________________________________________________________
SelectClause Visitor::visit(Parser::SelectClauseContext* ctx) {
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
VarOrAlias Visitor::visit(Parser::VarOrAliasContext* ctx) {
  return visitAlternative<VarOrAlias>(ctx->var(), ctx->alias());
}

// ____________________________________________________________________________________
Alias Visitor::visit(Parser::AliasContext* ctx) {
  // A SPARQL alias has only one child, namely the contents within
  // parentheses.
  return visit(ctx->aliasWithoutBrackets());
}

// ____________________________________________________________________________________
Alias Visitor::visit(Parser::AliasWithoutBracketsContext* ctx) {
  return {visitExpressionPimpl(ctx->expression()), visit(ctx->var())};
}

// ____________________________________________________________________________________
ParsedQuery Visitor::visit(Parser::ConstructQueryContext* ctx) {
  visitVector(ctx->datasetClause());
  ParsedQuery query;
  if (ctx->constructTemplate()) {
    query._clause = visit(ctx->constructTemplate())
                        .value_or(parsedQuery::ConstructClause{});
    auto [pattern, visibleVariables] = visit(ctx->whereClause());
    query._rootGraphPattern = std::move(pattern);
    query.registerVariablesVisibleInQueryBody(visibleVariables);
  } else {
    query._clause = parsedQuery::ConstructClause{
        visitOptional(ctx->triplesTemplate()).value_or(Triples{})};
  }
  query.addSolutionModifiers(visit(ctx->solutionModifier()));

  return query;
}

// ____________________________________________________________________________________
ParsedQuery Visitor::visit(Parser::DescribeQueryContext* ctx) {
  reportNotSupported(ctx, "DESCRIBE queries are");
}

// ____________________________________________________________________________________
ParsedQuery Visitor::visit(Parser::AskQueryContext* ctx) {
  reportNotSupported(ctx, "ASK queries are");
}

// ____________________________________________________________________________________
void Visitor::visit(Parser::DatasetClauseContext* ctx) {
  reportNotSupported(ctx, "FROM clauses are");
}

// ____________________________________________________________________________________
void Visitor::visit(Parser::DefaultGraphClauseContext*) {
  // This rule is only used by the `DatasetClause` rule which also is not
  // supported and should already have thrown an exception.
  AD_FAIL();
}

// ____________________________________________________________________________________
void Visitor::visit(Parser::NamedGraphClauseContext*) {
  // This rule is only used by the `DatasetClause` rule which also is not
  // supported and should already have thrown an exception.
  AD_FAIL();
}

// ____________________________________________________________________________________
void Visitor::visit(Parser::SourceSelectorContext*) {
  // This rule is only indirectly used by the `DatasetClause` rule which also is
  // not supported and should already have thrown an exception.
  AD_FAIL();
}

// ____________________________________________________________________________________
Variable Visitor::visit(Parser::VarContext* ctx) {
  return Variable{ctx->getText()};
}

// ____________________________________________________________________________________
GraphPatternOperation Visitor::visit(Parser::BindContext* ctx) {
  addVisibleVariable(ctx->var()->getText());
  return GraphPatternOperation{
      Bind{visitExpressionPimpl(ctx->expression()), visit(ctx->var())}};
}

// ____________________________________________________________________________________
GraphPatternOperation Visitor::visit(Parser::InlineDataContext* ctx) {
  Values values = visit(ctx->dataBlock());
  for (const auto& variable : values._inlineValues._variables) {
    addVisibleVariable(variable);
  }
  return GraphPatternOperation{std::move(values)};
}

// ____________________________________________________________________________________
Values Visitor::visit(Parser::DataBlockContext* ctx) {
  return visitAlternative<Values>(ctx->inlineDataOneVar(),
                                  ctx->inlineDataFull());
}

// ____________________________________________________________________________________
std::optional<Values> Visitor::visit(Parser::ValuesClauseContext* ctx) {
  return visitOptional(ctx->dataBlock());
}

// ____________________________________________________________________________________
GraphPattern Visitor::visit(Parser::GroupGraphPatternContext* ctx) {
  GraphPattern pattern;
  pattern._id = numGraphPatterns_++;
  if (ctx->subSelect()) {
    auto [subquery, valuesOpt] = visit(ctx->subSelect());
    pattern._graphPatterns.emplace_back(std::move(subquery));
    if (valuesOpt.has_value()) {
      pattern._graphPatterns.emplace_back(std::move(valuesOpt.value()));
    }
    return pattern;
  } else if (ctx->groupGraphPatternSub()) {
    auto [subOps, filters] = visit(ctx->groupGraphPatternSub());

    if (subOps.empty()) {
      reportError(ctx,
                  "QLever currently doesn't support empty GroupGraphPatterns "
                  "and WHERE clauses");
    }

    pattern._graphPatterns = std::move(subOps);
    for (auto& filter : filters) {
      // TODO<joka921> Add the checks that there are no LANG() expressions left
      // after this adding.
      if (auto langFilterData =
              filter.expression_.getLanguageFilterExpression();
          langFilterData.has_value()) {
        const auto& [variable, language] = langFilterData.value();
        pattern.addLanguageFilter(variable.name(), language);
      } else {
        pattern._filters.push_back(std::move(filter));
      }
    }
    return pattern;
  } else {
    AD_FAIL();  // Unreachable
  }
}

Visitor::OperationsAndFilters Visitor::visit(
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
    ops.emplace_back(visit(ctx->triplesBlock()));
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

Visitor::OperationOrFilterAndMaybeTriples Visitor::visit(
    Parser::GraphPatternNotTriplesAndMaybeTriplesContext* ctx) {
  return {visit(ctx->graphPatternNotTriples()),
          visitOptional(ctx->triplesBlock())};
}

// ____________________________________________________________________________________
BasicGraphPattern Visitor::visit(Parser::TriplesBlockContext* ctx) {
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
      visit(ctx->triplesSameSubjectPath()), convertAndRegisterTriple)};
  if (ctx->triplesBlock()) {
    triples.appendTriples(visit(ctx->triplesBlock()));
  }
  return triples;
}

// ____________________________________________________________________________________
Visitor::OperationOrFilter Visitor::visit(
    Parser::GraphPatternNotTriplesContext* ctx) {
  return visitAlternative<std::variant<GraphPatternOperation, SparqlFilter>>(
      ctx->filterR(), ctx->optionalGraphPattern(), ctx->minusGraphPattern(),
      ctx->bind(), ctx->inlineData(), ctx->groupOrUnionGraphPattern(),
      ctx->graphGraphPattern(), ctx->serviceGraphPattern());
}

// ____________________________________________________________________________________
GraphPatternOperation Visitor::visit(Parser::OptionalGraphPatternContext* ctx) {
  auto pattern = visit(ctx->groupGraphPattern());
  return GraphPatternOperation{parsedQuery::Optional{std::move(pattern)}};
}

// ____________________________________________________________________________________
parsedQuery::GraphPatternOperation Visitor::visit(
    Parser::GraphGraphPatternContext* ctx) {
  reportNotSupported(ctx, "Named Graphs (FROM, GRAPH) are");
}

// ____________________________________________________________________________________
parsedQuery::GraphPatternOperation Visitor::visit(
    Parser::ServiceGraphPatternContext* ctx) {
  reportNotSupported(ctx, "Federated queries (SERVICE) are");
}

// ____________________________________________________________________________________
sparqlExpression::SparqlExpression::Ptr Visitor::visit(
    Parser::ExpressionContext* ctx) {
  return visit(ctx->conditionalOrExpression());
}

// ____________________________________________________________________________________
Visitor::PatternAndVisibleVariables Visitor::visit(
    Parser::WhereClauseContext* ctx) {
  visibleVariables_.emplace_back();
  auto pattern = visit(ctx->groupGraphPattern());
  auto visible = std::move(visibleVariables_.back());
  visibleVariables_.pop_back();
  return {std::move(pattern), std::move(visible)};
}

// ____________________________________________________________________________________
SolutionModifiers Visitor::visit(Parser::SolutionModifierContext* ctx) {
  SolutionModifiers modifiers;
  visitIf(&modifiers.groupByVariables_, ctx->groupClause());
  visitIf(&modifiers.havingClauses_, ctx->havingClause());
  visitIf(&modifiers.orderBy_, ctx->orderClause());
  visitIf(&modifiers.limitOffset_, ctx->limitOffsetClauses());
  return modifiers;
}

// ____________________________________________________________________________________
LimitOffsetClause Visitor::visit(Parser::LimitOffsetClausesContext* ctx) {
  LimitOffsetClause clause{};
  visitIf(&clause._limit, ctx->limitClause());
  visitIf(&clause._offset, ctx->offsetClause());
  visitIf(&clause._textLimit, ctx->textLimitClause());
  return clause;
}

// ____________________________________________________________________________________
vector<SparqlFilter> Visitor::visit(Parser::HavingClauseContext* ctx) {
  // TODO<joka921> we should check for the illegal language filters here,
  // not in the downstream path.
  auto expressions = visitVector(ctx->havingCondition());
  return ad_utility::transform(std::move(expressions), [](auto&& expression) {
    return SparqlFilter{std::move(expression)};
  });
}

// ____________________________________________________________________________________
SparqlFilter Visitor::visit(Parser::HavingConditionContext* ctx) {
  auto expression = visitExpressionPimpl(ctx->constraint());
  if (auto langFilterData = expression.getLanguageFilterExpression();
      langFilterData.has_value()) {
    reportNotSupported(ctx, "Language filters in HAVING clauses are");
  }
  return {std::move(expression)};
}

// ____________________________________________________________________________________
vector<OrderKey> Visitor::visit(Parser::OrderClauseContext* ctx) {
  return visitVector(ctx->orderCondition());
}

// ____________________________________________________________________________________
vector<GroupKey> Visitor::visit(Parser::GroupClauseContext* ctx) {
  return visitVector(ctx->groupCondition());
}

// ____________________________________________________________________________________
std::optional<parsedQuery::ConstructClause> Visitor::visit(
    Parser::ConstructTemplateContext* ctx) {
  if (ctx->constructTriples()) {
    return parsedQuery::ConstructClause{visit(ctx->constructTriples())};
  } else {
    return std::nullopt;
  }
}

// ____________________________________________________________________________________
string Visitor::visit(Parser::StringContext* ctx) {
  return RdfEscaping::normalizeRDFLiteral(ctx->getText());
}

// ____________________________________________________________________________________
string Visitor::visit(Parser::IriContext* ctx) {
  // TODO return an IRI, not a std::string.
  string langtag =
      ctx->PREFIX_LANGTAG() ? ctx->PREFIX_LANGTAG()->getText() : "";
  return langtag + visitAlternative<string>(ctx->iriref(), ctx->prefixedName());
}

// ____________________________________________________________________________________
string Visitor::visit(Parser::IrirefContext* ctx) {
  return RdfEscaping::unescapeIriref(ctx->getText());
}

// ____________________________________________________________________________________
string Visitor::visit(Parser::PrefixedNameContext* ctx) {
  return visitAlternative<std::string>(ctx->pnameLn(), ctx->pnameNs());
}

// ____________________________________________________________________________________
string Visitor::visit(Parser::PnameLnContext* ctx) {
  string text = ctx->getText();
  auto pos = text.find(':');
  auto pnameNS = text.substr(0, pos);
  auto pnLocal = text.substr(pos + 1);
  if (!prefixMap_.contains(pnameNS)) {
    // TODO<joka921> : proper name
    reportError(ctx, "Prefix " + pnameNS +
                         " was not registered using a PREFIX declaration");
  }
  auto inner = prefixMap_[pnameNS];
  // strip the trailing ">"
  inner = inner.substr(0, inner.size() - 1);
  return inner + RdfEscaping::unescapePrefixedIri(pnLocal) + ">";
}

// ____________________________________________________________________________________
string Visitor::visit(Parser::PnameNsContext* ctx) {
  auto text = ctx->getText();
  auto prefix = text.substr(0, text.length() - 1);
  if (!prefixMap_.contains(prefix)) {
    // TODO<joka921> : proper name
    reportError(ctx, "Prefix " + prefix +
                         " was not registered using a PREFIX declaration");
  }
  return prefixMap_[prefix];
}

// ____________________________________________________________________________________
void Visitor::visit(Parser::PrologueContext* ctx) {
  visitVector(ctx->baseDecl());
  visitVector(ctx->prefixDecl());
}

// ____________________________________________________________________________________
void Visitor::visit(Parser::BaseDeclContext* ctx) {
  reportNotSupported(ctx, "BASE declarations are");
}

// ____________________________________________________________________________________
void Visitor::visit(Parser::PrefixDeclContext* ctx) {
  auto text = ctx->PNAME_NS()->getText();
  // Remove the ':' at the end of the PNAME_NS
  auto prefixLabel = text.substr(0, text.length() - 1);
  auto prefixIri = visit(ctx->iriref());
  prefixMap_[prefixLabel] = prefixIri;
}

// ____________________________________________________________________________________
ParsedQuery Visitor::visit(Parser::SelectQueryContext* ctx) {
  ParsedQuery query;
  query._clause = visit(ctx->selectClause());
  visitVector(ctx->datasetClause());
  auto [pattern, visibleVariables] = visit(ctx->whereClause());
  query._rootGraphPattern = std::move(pattern);
  query.registerVariablesVisibleInQueryBody(visibleVariables);
  query.addSolutionModifiers(visit(ctx->solutionModifier()));

  return query;
}

// ____________________________________________________________________________________
Visitor::SubQueryAndMaybeValues Visitor::visit(Parser::SubSelectContext* ctx) {
  ParsedQuery query;
  query._clause = visit(ctx->selectClause());
  auto [pattern, visibleVariables] = visit(ctx->whereClause());
  query._rootGraphPattern = std::move(pattern);
  query.setNumInternalVariables(numInternalVariables_);
  query.registerVariablesVisibleInQueryBody(visibleVariables);
  query.addSolutionModifiers(visit(ctx->solutionModifier()));
  numInternalVariables_ = query.getNumInternalVariables();
  auto values = visit(ctx->valuesClause());
  // Variables that are selected in this query are visible in the parent query.
  for (const auto& variable : query.selectClause().getSelectedVariables()) {
    addVisibleVariable(variable);
  }
  query._numGraphPatterns = numGraphPatterns_++;
  return {parsedQuery::Subquery{std::move(query)}, std::move(values)};
}

// ____________________________________________________________________________________
GroupKey Visitor::visit(Parser::GroupConditionContext* ctx) {
  if (ctx->var() && !ctx->expression()) {
    return Variable{ctx->var()->getText()};
  } else if (ctx->builtInCall() || ctx->functionCall()) {
    // builtInCall and functionCall are both also an Expression
    return (ctx->builtInCall() ? visitExpressionPimpl(ctx->builtInCall())
                               : visitExpressionPimpl(ctx->functionCall()));
  } else if (ctx->expression()) {
    auto expr = visitExpressionPimpl(ctx->expression());
    if (ctx->AS() && ctx->var()) {
      return Alias{std::move(expr), visit(ctx->var())};
    } else {
      return expr;
    }
  }
  AD_FAIL();  // Should be unreachable.
}

// ____________________________________________________________________________________
OrderKey Visitor::visit(Parser::OrderConditionContext* ctx) {
  auto visitExprOrderKey = [this](bool isDescending,
                                  auto* context) -> OrderKey {
    auto expr = visitExpressionPimpl(context);
    if (auto exprIsVariable = expr.getVariableOrNullopt();
        exprIsVariable.has_value()) {
      return VariableOrderKey{exprIsVariable.value(), isDescending};
    } else {
      return ExpressionOrderKey{std::move(expr), isDescending};
    }
  };

  if (ctx->var()) {
    return VariableOrderKey(visit(ctx->var()));
  } else if (ctx->constraint()) {
    return visitExprOrderKey(false, ctx->constraint());
  } else if (ctx->brackettedExpression()) {
    return visitExprOrderKey(ctx->DESC() != nullptr,
                             ctx->brackettedExpression());
  }
  AD_FAIL();  // Should be unreachable.
}

// ____________________________________________________________________________________
uint64_t Visitor::visit(Parser::LimitClauseContext* ctx) {
  return visit(ctx->integer());
}

// ____________________________________________________________________________________
uint64_t Visitor::visit(Parser::OffsetClauseContext* ctx) {
  return visit(ctx->integer());
}

// ____________________________________________________________________________________
uint64_t Visitor::visit(Parser::TextLimitClauseContext* ctx) {
  return visit(ctx->integer());
}

// ____________________________________________________________________________________
SparqlValues Visitor::visit(Parser::InlineDataOneVarContext* ctx) {
  SparqlValues values;
  auto var = visit(ctx->var());
  values._variables.push_back(var.name());
  if (ctx->dataBlockValue().empty())
    reportError(ctx,
                "No values were specified in Values "
                "clause. This is not supported by QLever.");
  for (auto& dataBlockValue : ctx->dataBlockValue()) {
    values._values.push_back({visit(dataBlockValue)});
  }
  return values;
}

// ____________________________________________________________________________________
SparqlValues Visitor::visit(Parser::InlineDataFullContext* ctx) {
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
    values._variables.push_back(visit(var).name());
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
vector<std::string> Visitor::visit(Parser::DataBlockSingleContext* ctx) {
  if (ctx->NIL())
    reportError(ctx,
                "No values were specified in DataBlock."
                "This is not supported by QLever.");
  return visitVector(ctx->dataBlockValue());
}

// ____________________________________________________________________________________
std::string Visitor::visit(Parser::DataBlockValueContext* ctx) {
  // Return a string
  if (ctx->iri()) {
    return visit(ctx->iri());
  } else if (ctx->rdfLiteral()) {
    return visit(ctx->rdfLiteral());
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
GraphPatternOperation Visitor::visit(Parser::MinusGraphPatternContext* ctx) {
  return GraphPatternOperation{
      parsedQuery::Minus{visit(ctx->groupGraphPattern())}};
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
GraphPatternOperation Visitor::visit(
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
SparqlFilter Visitor::visit(Parser::FilterRContext* ctx) {
  return SparqlFilter{visitExpressionPimpl(ctx->constraint())};
}

// ____________________________________________________________________________________
ExpressionPtr Visitor::visit(Parser::ConstraintContext* ctx) {
  return visitAlternative<ExpressionPtr>(
      ctx->brackettedExpression(), ctx->builtInCall(), ctx->functionCall());
}

// ____________________________________________________________________________________
ExpressionPtr Visitor::visit(Parser::FunctionCallContext* ctx) {
  return processIriFunctionCall(visit(ctx->iri()), visit(ctx->argList()));
}

// ____________________________________________________________________________________
vector<Visitor::ExpressionPtr> Visitor::visit(Parser::ArgListContext* ctx) {
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
void Visitor::visit(Parser::ExpressionListContext*) {
  // This rule is only used by the `RelationExpression` and `BuiltInCall` rules
  // which also are not supported and should already have thrown an exception.
  AD_FAIL();
}

// ____________________________________________________________________________________
template <typename Context>
Triples Visitor::parseTriplesConstruction(Context* ctx) {
  auto result = visit(ctx->triplesSameSubject());
  Context* subContext = [](Context* ctx) -> Context* {
    if constexpr (std::is_same_v<Context, Parser::ConstructTriplesContext>) {
      return ctx->constructTriples();
    } else if constexpr (std::is_same_v<Context,
                                        Parser::TriplesTemplateContext>) {
      return ctx->triplesTemplate();
    }
  }(ctx);
  if (subContext) {
    auto newTriples = visit(subContext);
    ad_utility::appendVector(result, std::move(newTriples));
  }
  return result;
}

// ____________________________________________________________________________________
Triples Visitor::visit(Parser::ConstructTriplesContext* ctx) {
  return parseTriplesConstruction(ctx);
}

// ____________________________________________________________________________________
Triples Visitor::visit(Parser::TriplesTemplateContext* ctx) {
  return parseTriplesConstruction(ctx);
}

// ____________________________________________________________________________________
Triples Visitor::visit(Parser::TriplesSameSubjectContext* ctx) {
  Triples triples;
  if (ctx->varOrTerm()) {
    VarOrTerm subject = visit(ctx->varOrTerm());
    AD_CHECK(ctx->propertyListNotEmpty());
    auto propertyList = visit(ctx->propertyListNotEmpty());
    for (auto& tuple : propertyList.first) {
      triples.push_back({subject, std::move(tuple[0]), std::move(tuple[1])});
    }
    ad_utility::appendVector(triples, std::move(propertyList.second));
  } else if (ctx->triplesNode()) {
    auto tripleNodes = visit(ctx->triplesNode());
    ad_utility::appendVector(triples, std::move(tripleNodes.second));
    AD_CHECK(ctx->propertyList());
    auto propertyList = visit(ctx->propertyList());
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
PropertyList Visitor::visit(Parser::PropertyListContext* ctx) {
  return ctx->propertyListNotEmpty() ? visit(ctx->propertyListNotEmpty())
                                     : PropertyList{Tuples{}, Triples{}};
}

// ____________________________________________________________________________________
PropertyList Visitor::visit(Parser::PropertyListNotEmptyContext* ctx) {
  Tuples triplesWithoutSubject;
  Triples additionalTriples;
  auto verbs = ctx->verb();
  auto objectLists = ctx->objectList();
  for (size_t i = 0; i < verbs.size(); i++) {
    // TODO use zip-style approach once C++ supports ranges
    auto objectList = visit(objectLists.at(i));
    auto verb = visit(verbs.at(i));
    for (auto& object : objectList.first) {
      triplesWithoutSubject.push_back({verb, std::move(object)});
    }
    ad_utility::appendVector(additionalTriples, std::move(objectList.second));
  }
  return {std::move(triplesWithoutSubject), std::move(additionalTriples)};
}

// ____________________________________________________________________________________
VarOrTerm Visitor::visit(Parser::VerbContext* ctx) {
  if (ctx->varOrIri()) {
    return visit(ctx->varOrIri());
  } else if (ctx->getText() == "a") {
    // Special keyword 'a'
    return GraphTerm{Iri{"<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>"}};
  } else {
    AD_FAIL()  // Should be unreachable.
  }
}

// ____________________________________________________________________________________
ObjectList Visitor::visit(Parser::ObjectListContext* ctx) {
  Objects objects;
  Triples additionalTriples;
  auto objectContexts = ctx->objectR();
  for (auto& objectContext : objectContexts) {
    auto graphNode = visit(objectContext);
    ad_utility::appendVector(additionalTriples, std::move(graphNode.second));
    objects.push_back(std::move(graphNode.first));
  }
  return {std::move(objects), std::move(additionalTriples)};
}

// ____________________________________________________________________________________
Node Visitor::visit(Parser::ObjectRContext* ctx) {
  return visit(ctx->graphNode());
}

// ___________________________________________________________________________
vector<TripleWithPropertyPath> Visitor::visit(
    Parser::TriplesSameSubjectPathContext* ctx) {
  // If a triple `?var ql:contains-word "words"` or `?var ql:contains-entity
  // <entity>` is contained in the query, then the variable `?ql_textscore_var`
  // is implicitly created and visible in the query body.
  auto setTextscoreVisibleIfPresent = [this](VarOrTerm& subject,
                                             VarOrPath& predicate) {
    if (auto* var = std::get_if<Variable>(&subject)) {
      if (auto* propertyPath = std::get_if<PropertyPath>(&predicate)) {
        if (propertyPath->asString() == CONTAINS_ENTITY_PREDICATE ||
            propertyPath->asString() == CONTAINS_WORD_PREDICATE) {
          addVisibleVariable(
              absl::StrCat(TEXTSCORE_VARIABLE_PREFIX, var->name().substr(1)));
        }
      }
    }
  };

  if (ctx->varOrTerm()) {
    vector<TripleWithPropertyPath> triples;
    auto subject = visit(ctx->varOrTerm());
    auto tuples = visit(ctx->propertyListPathNotEmpty());
    for (auto& [predicate, object] : tuples) {
      // TODO<clang,c++20> clang does not yet support emplace_back for
      // aggregates.
      setTextscoreVisibleIfPresent(subject, predicate);
      triples.push_back(TripleWithPropertyPath{subject, std::move(predicate),
                                               std::move(object)});
    }
    return triples;
  } else if (ctx->triplesNodePath()) {
    visit(ctx->triplesNodePath());
  } else {
    AD_FAIL()  // Should be unreachable.
  }
}

// ___________________________________________________________________________
std::optional<PathTuples> Visitor::visit(Parser::PropertyListPathContext* ctx) {
  return visitOptional(ctx->propertyListPathNotEmpty());
}

// ___________________________________________________________________________
PathTuples Visitor::visit(Parser::PropertyListPathNotEmptyContext* ctx) {
  PathTuples tuples = visit(ctx->tupleWithPath());
  vector<PathTuples> tuplesWithoutPaths = visitVector(ctx->tupleWithoutPath());
  for (auto& tuplesWithoutPath : tuplesWithoutPaths) {
    tuples.insert(tuples.end(), tuplesWithoutPath.begin(),
                  tuplesWithoutPath.end());
  }
  return tuples;
}

// ____________________________________________________________________________________
PropertyPath Visitor::visit(Parser::VerbPathContext* ctx) {
  PropertyPath p = visit(ctx->path());
  // TODO move computeCanBeNull into PropertyPath constructor.
  p.computeCanBeNull();
  return p;
}

// ____________________________________________________________________________________
Variable Visitor::visit(Parser::VerbSimpleContext* ctx) {
  return visit(ctx->var());
}

// ____________________________________________________________________________________
PathTuples Visitor::visit(Parser::TupleWithoutPathContext* ctx) {
  VarOrPath predicate = visit(ctx->verbPathOrSimple());
  ObjectList objectList = visit(ctx->objectList());
  return joinPredicateAndObject(predicate, objectList);
}

// ____________________________________________________________________________________
PathTuples Visitor::visit(Parser::TupleWithPathContext* ctx) {
  VarOrPath predicate = visit(ctx->verbPathOrSimple());
  ObjectList objectList = visit(ctx->objectListPath());
  return joinPredicateAndObject(predicate, objectList);
}

// ____________________________________________________________________________________
VarOrPath Visitor::visit(Parser::VerbPathOrSimpleContext* ctx) {
  return visitAlternative<ad_utility::sparql_types::VarOrPath>(
      ctx->verbPath(), ctx->verbSimple());
}

// ___________________________________________________________________________
ObjectList Visitor::visit(Parser::ObjectListPathContext* ctx) {
  // The second parameter is empty because collections and blank not paths,
  // which might add additional triples, are currently not supported.
  // When this is implemented they will be returned by visit(ObjectPathContext).
  return {visitVector(ctx->objectPath()), {}};
}

// ____________________________________________________________________________________
VarOrTerm Visitor::visit(Parser::ObjectPathContext* ctx) {
  return visit(ctx->graphNodePath());
}

// ____________________________________________________________________________________
PropertyPath Visitor::visit(Parser::PathContext* ctx) {
  return visit(ctx->pathAlternative());
}

// ____________________________________________________________________________________
PropertyPath Visitor::visit(Parser::PathAlternativeContext* ctx) {
  return PropertyPath::makeAlternative(visitVector(ctx->pathSequence()));
}

// ____________________________________________________________________________________
PropertyPath Visitor::visit(Parser::PathSequenceContext* ctx) {
  return PropertyPath::makeSequence(visitVector(ctx->pathEltOrInverse()));
}

// ____________________________________________________________________________________
PropertyPath Visitor::visit(Parser::PathEltContext* ctx) {
  PropertyPath p = visit(ctx->pathPrimary());

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
PropertyPath Visitor::visit(Parser::PathEltOrInverseContext* ctx) {
  PropertyPath p = visit(ctx->pathElt());

  if (ctx->negationOperator) {
    p = PropertyPath::makeInverse(std::move(p));
  }

  return p;
}

// ____________________________________________________________________________________
void Visitor::visit(Parser::PathModContext*) {
  // This rule is only used by the `PathElt` rule which should have handled the
  // content of this rule.
  AD_FAIL();
}

// ____________________________________________________________________________________
PropertyPath Visitor::visit(Parser::PathPrimaryContext* ctx) {
  // TODO: implement a strong Iri type, s.t. the ctx->iri() case can become a
  //  simple `return visit(...)`. Then the three cases which are not the
  //  `special a` case can be merged into a `visitAlternative(...)`.
  if (ctx->iri()) {
    return PropertyPath::fromIri(visit(ctx->iri()));
  } else if (ctx->path()) {
    return visit(ctx->path());
  } else if (ctx->pathNegatedPropertySet()) {
    return visit(ctx->pathNegatedPropertySet());
  } else if (ctx->getText() == "a") {
    // Special keyword 'a'
    return PropertyPath::fromIri(
        "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>");
  }
  AD_FAIL()  // Should be unreachable.
}

// ____________________________________________________________________________________
PropertyPath Visitor::visit(Parser::PathNegatedPropertySetContext* ctx) {
  reportError(ctx, "\"!\" inside a property path is not supported by QLever.");
}

// ____________________________________________________________________________________
PropertyPath Visitor::visit(Parser::PathOneInPropertySetContext*) {
  // This rule is only used by the `PathNegatedPropertySet` rule which also is
  // not supported and should already have thrown an exception.
  AD_FAIL();
}

// ____________________________________________________________________________________
uint64_t Visitor::visit(Parser::IntegerContext* ctx) {
  try {
    // unsigned long long int might be larger than 8 bytes as per the standard.
    // If that were the case this could lead to overflows.
    // TODO<joka921> Use `std::from_chars` but first check for the compiler
    //  support.
    static_assert(sizeof(unsigned long long int) == sizeof(uint64_t));
    return std::stoull(ctx->getText());
  } catch (const std::out_of_range&) {
    reportError(
        ctx,
        "Integer " + ctx->getText() +
            " does not fit into 64 bits. This is not supported by QLever.");
  }
}

// ____________________________________________________________________________________
Node Visitor::visit(Parser::TriplesNodeContext* ctx) {
  return visitAlternative<Node>(ctx->collection(),
                                ctx->blankNodePropertyList());
}

// ____________________________________________________________________________________
Node Visitor::visit(Parser::BlankNodePropertyListContext* ctx) {
  VarOrTerm var{GraphTerm{newBlankNode()}};
  Triples triples;
  auto propertyList = visit(ctx->propertyListNotEmpty());
  for (auto& tuple : propertyList.first) {
    triples.push_back({var, std::move(tuple[0]), std::move(tuple[1])});
  }
  ad_utility::appendVector(triples, std::move(propertyList.second));
  return {std::move(var), std::move(triples)};
}

// ____________________________________________________________________________________
void Visitor::visit(Parser::TriplesNodePathContext* ctx) {
  visitAlternative<void>(ctx->blankNodePropertyListPath(),
                         ctx->collectionPath());
  AD_FAIL();
}

// ____________________________________________________________________________________
void Visitor::visit(Parser::BlankNodePropertyListPathContext* ctx) {
  throwCollectionsAndBlankNodePathsNotSupported(ctx);
}

// ____________________________________________________________________________________
Node Visitor::visit(Parser::CollectionContext* ctx) {
  Triples triples;
  VarOrTerm nextElement{
      GraphTerm{Iri{"<http://www.w3.org/1999/02/22-rdf-syntax-ns#nil>"}}};
  auto nodes = ctx->graphNode();
  for (auto context : Reversed{nodes}) {
    VarOrTerm currentVar{GraphTerm{newBlankNode()}};
    auto graphNode = visit(context);

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
void Visitor::visit(Parser::CollectionPathContext* ctx) {
  throwCollectionsAndBlankNodePathsNotSupported(ctx);
}

// ____________________________________________________________________________________
Node Visitor::visit(Parser::GraphNodeContext* ctx) {
  if (ctx->varOrTerm()) {
    return {visit(ctx->varOrTerm()), Triples{}};
  } else if (ctx->triplesNode()) {
    return visit(ctx->triplesNode());
  } else {
    AD_FAIL();
  }
}

// ____________________________________________________________________________________
VarOrTerm Visitor::visit(Parser::GraphNodePathContext* ctx) {
  if (ctx->varOrTerm()) {
    return visit(ctx->varOrTerm());
  } else if (ctx->triplesNodePath()) {
    visit(ctx->triplesNodePath());
  } else {
    AD_FAIL()  // Should be unreachable.
  }
}

// ____________________________________________________________________________________
VarOrTerm Visitor::visit(Parser::VarOrTermContext* ctx) {
  return visitAlternative<VarOrTerm>(ctx->var(), ctx->graphTerm());
}

// ____________________________________________________________________________________
VarOrTerm Visitor::visit(Parser::VarOrIriContext* ctx) {
  if (ctx->var()) {
    return visit(ctx->var());
  } else if (ctx->iri()) {
    // TODO<qup42> If `visit` returns an `Iri` and `VarOrTerm` can be
    // constructed from an `Iri`, this whole function becomes
    // `visitAlternative`.
    return GraphTerm{Iri{visit(ctx->iri())}};
  } else {
    AD_FAIL()  // Should be unreachable.
  }
}

// ____________________________________________________________________________________
GraphTerm Visitor::visit(Parser::GraphTermContext* ctx) {
  if (ctx->blankNode()) {
    return visit(ctx->blankNode());
  } else if (ctx->iri()) {
    return Iri{visit(ctx->iri())};
  } else if (ctx->NIL()) {
    return Iri{"<http://www.w3.org/1999/02/22-rdf-syntax-ns#nil>"};
  } else {
    return visitAlternative<Literal>(ctx->numericLiteral(),
                                     ctx->booleanLiteral(), ctx->rdfLiteral());
  }
}

// ____________________________________________________________________________________
ExpressionPtr Visitor::visit(Parser::ConditionalOrExpressionContext* ctx) {
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
ExpressionPtr Visitor::visit(Parser::ConditionalAndExpressionContext* ctx) {
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
ExpressionPtr Visitor::visit(Parser::ValueLogicalContext* ctx) {
  return visit(ctx->relationalExpression());
}

// ___________________________________________________________________________
ExpressionPtr Visitor::visit(Parser::RelationalExpressionContext* ctx) {
  auto children = visitVector(ctx->numericExpression());

  if (ctx->expressionList()) {
    reportError(
        ctx,
        "IN/ NOT IN in expressions are currently not supported by QLever.");
  }
  AD_CHECK(children.size() == 1 || children.size() == 2);
  if (children.size() == 1) {
    return std::move(children[0]);
  }

  auto make = [&]<typename Expr>() {
    return createExpression<Expr>(std::move(children[0]),
                                  std::move(children[1]));
  };
  std::string relation = ctx->children[1]->getText();
  if (relation == "=") {
    return make.operator()<EqualExpression>();
  } else if (relation == "!=") {
    return make.operator()<NotEqualExpression>();
  } else if (relation == "<") {
    return make.operator()<LessThanExpression>();
  } else if (relation == ">") {
    return make.operator()<GreaterThanExpression>();
  } else if (relation == "<=") {
    return make.operator()<LessEqualExpression>();
  } else if (relation == ">=") {
    return make.operator()<GreaterEqualExpression>();
  } else {
    AD_FAIL();
  }
}

// ____________________________________________________________________________________
ExpressionPtr Visitor::visit(Parser::NumericExpressionContext* ctx) {
  return visit(ctx->additiveExpression());
}

// ____________________________________________________________________________________
ExpressionPtr Visitor::visit(Parser::AdditiveExpressionContext* ctx) {
  auto result = visit(ctx->multiplicativeExpression());

  for (OperatorAndExpression& signAndExpression :
       visitVector(ctx->multiplicativeExpressionWithSign())) {
    switch (signAndExpression.operator_) {
      case Operator::Plus:
        result = createExpression<sparqlExpression::AddExpression>(
            std::move(result), std::move(signAndExpression.expression_));
        break;
      case Operator::Minus:
        result = createExpression<sparqlExpression::SubtractExpression>(
            std::move(result), std::move(signAndExpression.expression_));
        break;
      default:
        AD_FAIL()
    }
  }
  return result;
}

// ____________________________________________________________________________________
Visitor::OperatorAndExpression Visitor::visit(
    Parser::MultiplicativeExpressionWithSignContext* ctx) {
  return visitAlternative<OperatorAndExpression>(
      ctx->plusSubexpression(), ctx->minusSubexpression(),
      ctx->multiplicativeExpressionWithLeadingSignButNoSpace());
}

// ____________________________________________________________________________________
Visitor::OperatorAndExpression Visitor::visit(
    Parser::PlusSubexpressionContext* ctx) {
  return {Operator::Plus, visit(ctx->multiplicativeExpression())};
}

// ____________________________________________________________________________________
Visitor::OperatorAndExpression Visitor::visit(
    Parser::MinusSubexpressionContext* ctx) {
  return {Operator::Minus, visit(ctx->multiplicativeExpression())};
}

// ____________________________________________________________________________________
Visitor::OperatorAndExpression Visitor::visit(
    Parser::MultiplicativeExpressionWithLeadingSignButNoSpaceContext* ctx) {
  Operator op =
      ctx->numericLiteralPositive() ? Operator::Plus : Operator::Minus;

  // Helper function that inverts a number if  the leading sign of this
  // expression is `-`
  auto invertIfNecessary = [ctx](auto number) {
    return ctx->numericLiteralPositive() ? number : -number;
  };

  // Create the initial expression from a double literal
  auto createFromDouble = [&](double d) -> ExpressionPtr {
    return std::make_unique<sparqlExpression::DoubleExpression>(
        invertIfNecessary(d));
  };
  auto createFromInt = [&](int64_t i) -> ExpressionPtr {
    return std::make_unique<sparqlExpression::IntExpression>(
        invertIfNecessary(i));
  };

  auto literalAsVariant = visitAlternative<IntOrDouble>(
      ctx->numericLiteralPositive(), ctx->numericLiteralNegative());

  auto expression = std::visit(
      ad_utility::OverloadCallOperator{createFromInt, createFromDouble},
      literalAsVariant);

  for (OperatorAndExpression& opAndExp :
       visitVector(ctx->multiplyOrDivideExpression())) {
    switch (opAndExp.operator_) {
      case Operator::Multiply:
        expression = createExpression<sparqlExpression::MultiplyExpression>(
            std::move(expression), std::move(opAndExp.expression_));
        break;
      case Operator::Divide:
        expression = createExpression<sparqlExpression::DivideExpression>(
            std::move(expression), std::move(opAndExp.expression_));
        break;
      default:
        AD_FAIL();
    }
  }
  return {op, std::move(expression)};
}

// ____________________________________________________________________________________
ExpressionPtr Visitor::visit(Parser::MultiplicativeExpressionContext* ctx) {
  auto result = visit(ctx->unaryExpression());

  for (OperatorAndExpression& opAndExp :
       visitVector(ctx->multiplyOrDivideExpression())) {
    switch (opAndExp.operator_) {
      case Operator::Multiply:
        result = createExpression<sparqlExpression::MultiplyExpression>(
            std::move(result), std::move(opAndExp.expression_));
        break;
      case Operator::Divide:
        result = createExpression<sparqlExpression::DivideExpression>(
            std::move(result), std::move(opAndExp.expression_));
        break;
      default:
        AD_FAIL();
    }
  }
  return result;
}

// ____________________________________________________________________________________
Visitor::OperatorAndExpression Visitor::visit(
    Parser::MultiplyOrDivideExpressionContext* ctx) {
  return visitAlternative<OperatorAndExpression>(ctx->multiplyExpression(),
                                                 ctx->divideExpression());
}

// ____________________________________________________________________________________
Visitor::OperatorAndExpression Visitor::visit(
    Parser::MultiplyExpressionContext* ctx) {
  return {Operator::Multiply, visit(ctx->unaryExpression())};
}

// ____________________________________________________________________________________
Visitor::OperatorAndExpression Visitor::visit(
    Parser::DivideExpressionContext* ctx) {
  return {Operator::Divide, visit(ctx->unaryExpression())};
}

// ____________________________________________________________________________________
ExpressionPtr Visitor::visit(Parser::UnaryExpressionContext* ctx) {
  auto child = visit(ctx->primaryExpression());
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
ExpressionPtr Visitor::visit(Parser::PrimaryExpressionContext* ctx) {
  using std::make_unique;
  using namespace sparqlExpression;

  if (ctx->rdfLiteral()) {
    // TODO<joka921> : handle strings with value datatype that are
    // not in the knowledge base correctly.
    return make_unique<StringOrIriExpression>(visit(ctx->rdfLiteral()));
  } else if (ctx->numericLiteral()) {
    auto integralWrapper = [](int64_t x) {
      return ExpressionPtr{make_unique<IntExpression>(x)};
    };
    auto doubleWrapper = [](double x) {
      return ExpressionPtr{make_unique<DoubleExpression>(x)};
    };
    return std::visit(
        ad_utility::OverloadCallOperator{integralWrapper, doubleWrapper},
        visit(ctx->numericLiteral()));
  } else if (ctx->booleanLiteral()) {
    return make_unique<BoolExpression>(visit(ctx->booleanLiteral()));
  } else if (ctx->var()) {
    return make_unique<VariableExpression>(visit(ctx->var()));
  } else {
    return visitAlternative<ExpressionPtr>(
        ctx->builtInCall(), ctx->iriOrFunction(), ctx->brackettedExpression());
  }
}

// ____________________________________________________________________________________
ExpressionPtr Visitor::visit(Parser::BrackettedExpressionContext* ctx) {
  return visit(ctx->expression());
}

// ____________________________________________________________________________________
ExpressionPtr Visitor::visit([[maybe_unused]] Parser::BuiltInCallContext* ctx) {
  if (ctx->aggregate()) {
    return visit(ctx->aggregate());
  } else if (ctx->regexExpression()) {
    return visit(ctx->regexExpression());
  } else if (ctx->langExpression()) {
    return visit(ctx->langExpression());
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
    reportNotSupported(
        ctx, "Built-in functions (other than aggregates and REGEX) are ");
  }
}

// _____________________________________________________________________________
ExpressionPtr Visitor::visit(Parser::RegexExpressionContext* ctx) {
  const auto& exp = ctx->expression();
  const auto& numArgs = exp.size();
  AD_CHECK(numArgs >= 2 && numArgs <= 3);
  auto flags = numArgs == 3 ? visitOptional(exp[2]) : std::nullopt;
  try {
    return std::make_unique<sparqlExpression::RegexExpression>(
        visit(exp[0]), visit(exp[1]), std::move(flags));
  } catch (const std::exception& e) {
    reportError(ctx, e.what());
  }
}

// _____________________________________________________________________________
ExpressionPtr Visitor::visit(Parser::LangExpressionContext* ctx) {
  try {
    return std::make_unique<sparqlExpression::LangExpression>(
        visit(ctx->expression()));
  } catch (const std::exception& e) {
    reportError(ctx, e.what());
  }
}

// ____________________________________________________________________________________
void Visitor::visit(Parser::SubstringExpressionContext* ctx) {
  reportNotSupported(ctx, "The SUBSTR function is");
}

// ____________________________________________________________________________________
void Visitor::visit(Parser::StrReplaceExpressionContext* ctx) {
  reportNotSupported(ctx, "The REPLACE function is");
}

// ____________________________________________________________________________________
void Visitor::visit(Parser::ExistsFuncContext* ctx) {
  reportNotSupported(ctx, "The EXISTS function is");
}

// ____________________________________________________________________________________
void Visitor::visit(Parser::NotExistsFuncContext* ctx) {
  reportNotSupported(ctx, "The NOT EXISTS function is");
}

// ____________________________________________________________________________________
ExpressionPtr Visitor::visit(Parser::AggregateContext* ctx) {
  using namespace sparqlExpression;
  // the only case that there is no child expression is COUNT(*), so we can
  // check this outside the if below.
  if (!ctx->expression()) {
    reportError(ctx,
                "This parser currently doesn't support COUNT(*), please "
                "specify an explicit expression for the COUNT");
  }
  auto childExpression = visit(ctx->expression());
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
      separator = visit(ctx->string());
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
ExpressionPtr Visitor::visit(Parser::IriOrFunctionContext* ctx) {
  // Case 1: Just an IRI.
  if (ctx->argList() == nullptr) {
    return std::make_unique<sparqlExpression::StringOrIriExpression>(
        visit(ctx->iri()));
  }
  // Case 2: Function call, where the function name is an IRI.
  return processIriFunctionCall(visit(ctx->iri()), visit(ctx->argList()));
}

// ____________________________________________________________________________________
std::string Visitor::visit(Parser::RdfLiteralContext* ctx) {
  // TODO: This should really be an RdfLiteral class that stores a unified
  //  version of the string, and the langtag/datatype separately.
  string ret = visit(ctx->string());
  if (ctx->LANGTAG()) {
    ret += ctx->LANGTAG()->getText();
  } else if (ctx->iri()) {
    ret += ("^^" + visit(ctx->iri()));
  }
  return ret;
}

// ____________________________________________________________________________________
std::variant<int64_t, double> Visitor::visit(
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
std::variant<int64_t, double> Visitor::visit(
    Parser::NumericLiteralUnsignedContext* ctx) {
  return parseNumericLiteral(ctx, ctx->INTEGER());
}

// ____________________________________________________________________________________
std::variant<int64_t, double> Visitor::visit(
    Parser::NumericLiteralPositiveContext* ctx) {
  return parseNumericLiteral(ctx, ctx->INTEGER_POSITIVE());
}

// ____________________________________________________________________________________
std::variant<int64_t, double> Visitor::visit(
    Parser::NumericLiteralNegativeContext* ctx) {
  return parseNumericLiteral(ctx, ctx->INTEGER_NEGATIVE());
}

// ____________________________________________________________________________________
bool Visitor::visit(Parser::BooleanLiteralContext* ctx) {
  return ctx->getText() == "true";
}

// ____________________________________________________________________________________
BlankNode Visitor::visit(Parser::BlankNodeContext* ctx) {
  if (ctx->ANON()) {
    return newBlankNode();
  } else if (ctx->BLANK_NODE_LABEL()) {
    // strip _: prefix from string
    constexpr size_t length = std::string_view{"_:"}.length();
    const string label = ctx->BLANK_NODE_LABEL()->getText().substr(length);
    // false means the query explicitly contains a blank node label
    return {false, label};
  } else {
    AD_FAIL();
  }
}

// ____________________________________________________________________________________
template <typename Ctx>
void Visitor::visitVector(const std::vector<Ctx*>& childContexts) requires
    voidWhenVisited<Visitor, Ctx> {
  for (const auto& child : childContexts) {
    visit(child);
  }
}

// ____________________________________________________________________________________
template <typename Ctx>
[[nodiscard]] auto Visitor::visitVector(const std::vector<Ctx*>& childContexts)
    -> std::vector<decltype(visit(childContexts[0]))>
requires(!voidWhenVisited<Visitor, Ctx>) {
  std::vector<decltype(visit(childContexts[0]))> children;
  for (const auto& child : childContexts) {
    children.emplace_back(visit(child));
  }
  return children;
}

// ____________________________________________________________________________________

template <typename Out, typename... Contexts>
Out Visitor::visitAlternative(Contexts*... ctxs) {
  // Check that exactly one of the `ctxs` is not `nullptr`.
  AD_CHECK(1u == (... + static_cast<bool>(ctxs)));
  if constexpr (std::is_void_v<Out>) {
    (..., visitIf(ctxs));
  } else {
    std::optional<Out> out;
    // Visit the one `context` which is not null and write the result to `out`.
    (..., visitIf<std::optional<Out>, Out>(&out, ctxs));
    return std::move(out.value());
  }
}

// ____________________________________________________________________________________
template <typename Ctx>
auto Visitor::visitOptional(Ctx* ctx) -> std::optional<decltype(visit(ctx))> {
  if (ctx) {
    return visit(ctx);
  } else {
    return std::nullopt;
  }
}

// ____________________________________________________________________________________
template <typename Target, typename Intermediate, typename Ctx>
void Visitor::visitIf(Target* target, Ctx* ctx) {
  if (ctx) {
    *target = Intermediate{visit(ctx)};
  }
}

// ____________________________________________________________________________________
template <typename Ctx>
void Visitor::visitIf(Ctx* ctx) requires voidWhenVisited<Visitor, Ctx> {
  if (ctx) {
    visit(ctx);
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

// ____________________________________________________________________________________
void Visitor::reportNotSupported(antlr4::ParserRuleContext* ctx,
                                 const std::string& feature) {
  reportError(ctx, feature + " currently not supported by QLever.");
}
