// Copyright 2021 - 2024, University of Freiburg
// Chair of Algorithms and Data Structures
// Authors:
//   2021 -    Hannah Bast <bast@cs.uni-freiburg.de>
//   2022      Julian Mundhahs <mundhahj@tf.uni-freiburg.de>
//   2022 -    Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include "parser/sparqlParser/SparqlQleverVisitor.h"

#include <absl/strings/str_join.h>
#include <absl/strings/str_split.h>

#include <string>
#include <vector>

#include "absl/time/time.h"
#include "engine/sparqlExpressions/CountStarExpression.h"
#include "engine/sparqlExpressions/GroupConcatExpression.h"
#include "engine/sparqlExpressions/LiteralExpression.h"
#include "engine/sparqlExpressions/NowDatetimeExpression.h"
#include "engine/sparqlExpressions/RandomExpression.h"
#include "engine/sparqlExpressions/RegexExpression.h"
#include "engine/sparqlExpressions/RelationalExpressions.h"
#include "engine/sparqlExpressions/SampleExpression.h"
#include "engine/sparqlExpressions/UuidExpressions.h"
#include "parser/SparqlParser.h"
#include "parser/TokenizerCtre.h"
#include "parser/TurtleParser.h"
#include "parser/data/Variable.h"
#include "util/StringUtils.h"
#include "util/TransparentFunctors.h"
#include "util/antlr/GenerateAntlrExceptionMetadata.h"

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

// _____________________________________________________________________________
std::string Visitor::getOriginalInputForContext(
    const antlr4::ParserRuleContext* context) {
  const auto& fullInput = context->getStart()->getInputStream()->toString();
  size_t posBeg = context->getStart()->getStartIndex();
  size_t posEnd = context->getStop()->getStopIndex();
  // Note that `getUTF8Substring` returns a `std::string_view`. We copy this to
  // a `std::string` because it's not clear whether the original string still
  // exists when the result of this call is used. Not performance-critical.
  return std::string{
      ad_utility::getUTF8Substring(fullInput, posBeg, posEnd - posBeg + 1)};
}

// _____________________________________________________________________________
std::string Visitor::currentTimeAsXsdString() {
  return absl::FormatTime("%Y-%m-%dT%H:%M:%E3S%Ez", absl::Now(),
                          absl::LocalTimeZone());
}

// ___________________________________________________________________________
ExpressionPtr Visitor::processIriFunctionCall(
    const TripleComponent::Iri& iri, std::vector<ExpressionPtr> argList,
    const antlr4::ParserRuleContext* ctx) {
  std::string_view functionName = asStringViewUnsafe(iri.getContent());
  std::string_view prefixName;
  // Helper lambda that checks if `functionName` starts with the given prefix.
  // If yes, remove the prefix from `functionName` and set
  // `prefixName` to the short name of the prefix; see `global/Constants.h`.
  auto checkPrefix = [&functionName, &prefixName](
                         std::pair<std::string_view, std::string_view> prefix) {
    if (functionName.starts_with(prefix.second)) {
      prefixName = prefix.first;
      functionName.remove_prefix(prefix.second.size());
      return true;
    } else {
      return false;
    }
  };
  // Helper lambda that checks the number of arguments and throws an error
  // if it's not right. The `functionName` and `prefixName` are used for the
  // error message.
  auto checkNumArgs = [&argList, &ctx, &functionName,
                       &prefixName](size_t numArgs) {
    static std::array<std::string, 6> wordForNumArgs = {
        "no", "one", "two", "three", "four", "five"};
    if (argList.size() != numArgs) {
      reportError(ctx,
                  absl::StrCat("Function ", prefixName, functionName, " takes ",
                               numArgs < 5 ? wordForNumArgs[numArgs]
                                           : std::to_string(numArgs),
                               numArgs == 1 ? " argument" : " arguments"));
    }
  };
  // Geo functions.
  if (checkPrefix(GEOF_PREFIX)) {
    if (functionName == "distance") {
      checkNumArgs(2);
      return sparqlExpression::makeDistExpression(std::move(argList[0]),
                                                  std::move(argList[1]));
    } else if (functionName == "longitude") {
      checkNumArgs(1);
      return sparqlExpression::makeLongitudeExpression(std::move(argList[0]));
    } else if (functionName == "latitude") {
      checkNumArgs(1);
      return sparqlExpression::makeLatitudeExpression(std::move(argList[0]));
    }
  } else if (checkPrefix(MATH_PREFIX)) {
    if (functionName == "log") {
      checkNumArgs(1);
      return sparqlExpression::makeLogExpression(std::move(argList[0]));
    } else if (functionName == "exp") {
      checkNumArgs(1);
      return sparqlExpression::makeExpExpression(std::move(argList[0]));
    } else if (functionName == "sqrt") {
      checkNumArgs(1);
      return sparqlExpression::makeSqrtExpression(std::move(argList[0]));
    } else if (functionName == "sin") {
      checkNumArgs(1);
      return sparqlExpression::makeSinExpression(std::move(argList[0]));
    } else if (functionName == "cos") {
      checkNumArgs(1);
      return sparqlExpression::makeCosExpression(std::move(argList[0]));
    } else if (functionName == "tan") {
      checkNumArgs(1);
      return sparqlExpression::makeTanExpression(std::move(argList[0]));
    }
  } else if (checkPrefix(XSD_PREFIX)) {
    if (functionName == "integer" || functionName == "int") {
      checkNumArgs(1);
      return sparqlExpression::makeConvertToIntExpression(
          std::move(argList[0]));
    } else if (functionName == "double" || functionName == "decimal") {
      checkNumArgs(1);
      return sparqlExpression::makeConvertToDoubleExpression(
          std::move(argList[0]));
    }
  }
  reportNotSupported(ctx,
                     "Function \""s + iri.toStringRepresentation() + "\" is");
}

void Visitor::addVisibleVariable(Variable var) {
  visibleVariables_.emplace_back(std::move(var));
}

// ___________________________________________________________________________
PathObjectPairs joinPredicateAndObject(const VarOrPath& predicate,
                                       auto objectList) {
  PathObjectPairs tuples;
  tuples.reserve(objectList.first.size());
  for (auto& object : objectList.first) {
    tuples.emplace_back(predicate, std::move(object));
  }
  return tuples;
}

// ___________________________________________________________________________
SparqlExpressionPimpl Visitor::visitExpressionPimpl(
    auto* ctx, [[maybe_unused]] bool allowLanguageFilters) {
  return {visit(ctx), getOriginalInputForContext(ctx)};
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
ParsedQuery Visitor::visit(Parser::QueryOrUpdateContext* ctx) {
  if (ctx->update()) {
    // An empty query currently matches the `update()` rule. We handle this
    // case manually to get a better error message. If an update query doesn't
    // have an `update1()`, then it consists of a (possibly empty) prologue, but
    // has not actual content, see the grammar in `SparqlAutomatic.g4` for
    // details.
    if (!ctx->update()->update1()) {
      reportError(ctx->update(),
                  "Empty query (this includes queries that only consist "
                  "of comments or prefix declarations).");
    }
    reportNotSupported(ctx->update(), "SPARQL 1.1 Update is");
  } else {
    return visit(ctx->query());
  }
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
ParsedQuery Visitor::visit(const Parser::DescribeQueryContext* ctx) {
  reportNotSupported(ctx, "DESCRIBE queries are");
}

// ____________________________________________________________________________________
ParsedQuery Visitor::visit(const Parser::AskQueryContext* ctx) {
  reportNotSupported(ctx, "ASK queries are");
}

// ____________________________________________________________________________________
void Visitor::visit(const Parser::DatasetClauseContext* ctx) {
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
  // `false` for the second argument means: The variable name is already
  // validated by the grammar, no need to check it again (which would lead to an
  // infinite loop here).
  return Variable{ctx->getText(), false};
}

// ____________________________________________________________________________________
GraphPatternOperation Visitor::visit(Parser::BindContext* ctx) {
  Variable target = visit(ctx->var());
  if (ad_utility::contains(visibleVariables_, target)) {
    reportError(
        ctx,
        absl::StrCat(
            "The target variable ", target.name(),
            " of an AS clause was already used before in the query body."));
  }

  auto expression = visitExpressionPimpl(ctx->expression());
  if (disableSomeChecksOnlyForTesting_ ==
      DisableSomeChecksOnlyForTesting::False) {
    for (const auto& var : expression.containedVariables()) {
      if (!ad_utility::contains(visibleVariables_, *var)) {
        reportError(ctx,
                    absl::StrCat("The variable ", var->name(),
                                 " was used in the expression of a BIND clause "
                                 "but was not previously bound in the query."));
      }
    }
  }
  addVisibleVariable(target);
  return GraphPatternOperation{Bind{std::move(expression), std::move(target)}};
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
ParsedQuery Visitor::visit(Parser::UpdateContext* ctx) {
  visit(ctx->prologue());

  auto query = visit(ctx->update1());

  if (ctx->update()) {
    parsedQuery_ = ParsedQuery{};
    reportNotSupported(ctx->update(), "Multiple updates in one query are");
  }

  return query;
}

// ____________________________________________________________________________________
ParsedQuery Visitor::visit(Parser::Update1Context* ctx) {
  if (ctx->modify()) {
    return visit(ctx->modify());
  } else if (ctx->clear()) {
    return visit(ctx->clear());
  }

  parsedQuery_._clause = parsedQuery::UpdateClause();

  if (ctx->insertData() || ctx->deleteData()) {
    // handles insertData and deleteData cases
    visitIf(&parsedQuery_.updateClause().toInsert_, ctx->insertData());
    visitIf(&parsedQuery_.updateClause().toDelete_, ctx->deleteData());
  } else if (ctx->deleteWhere()) {
    auto [toDelete, pattern] = visit(ctx->deleteWhere());
    parsedQuery_.updateClause().toDelete_ = std::move(toDelete);
    parsedQuery_._rootGraphPattern = std::move(pattern);
  } else {
    visitAlternative<void>(ctx->load(), ctx->drop(), ctx->add(), ctx->move(),
                           ctx->copy(), ctx->create());
    AD_FAIL();
  }

  return parsedQuery_;
}

// ____________________________________________________________________________________
void Visitor::visit(const Parser::LoadContext* ctx) const {
  reportNotSupported(ctx, "SPARQL 1.1 Update Load is");
}

// ____________________________________________________________________________________
ParsedQuery Visitor::visit(Parser::ClearContext* ctx) {
  auto graphRef = visit(ctx->graphRefAll());

  if (holds_alternative<DEFAULT>(graphRef)) {
    parsedQuery_._clause = parsedQuery::UpdateClause();
    parsedQuery_.updateClause().toDelete_ = {
        {Variable("?s"), Variable("?p"), Variable("?o")}};
    parsedQuery_._rootGraphPattern._graphPatterns.emplace_back(
        BasicGraphPattern{{{Variable("?s"), "?p", Variable("?o")}}});
    return parsedQuery_;
  } else {
    reportNotSupported(ctx, "Named Graphs are");
  }
}

// ____________________________________________________________________________________
void Visitor::visit(const Parser::DropContext* ctx) const {
  reportNotSupported(ctx, "SPARQL 1.1 Update Drop is");
}

// ____________________________________________________________________________________
void Visitor::visit(const Parser::CreateContext* ctx) const {
  reportNotSupported(ctx, "SPARQL 1.1 Update Create is");
}

// ____________________________________________________________________________________
void Visitor::visit(const Parser::AddContext* ctx) const {
  reportNotSupported(ctx, "SPARQL 1.1 Update Add is");
}

// ____________________________________________________________________________________
void Visitor::visit(const Parser::MoveContext* ctx) const {
  reportNotSupported(ctx, "SPARQL 1.1 Update Move is");
}

// ____________________________________________________________________________________
void Visitor::visit(const Parser::CopyContext* ctx) const {
  reportNotSupported(ctx, "SPARQL 1.1 Update Copy is");
}

// ____________________________________________________________________________________
vector<SparqlTripleSimple> Visitor::visit(Parser::InsertDataContext* ctx) {
  return visit(ctx->quadData());
}

// ____________________________________________________________________________________
vector<SparqlTripleSimple> Visitor::visit(Parser::DeleteDataContext* ctx) {
  return visit(ctx->quadData());
}

// ____________________________________________________________________________________
std::pair<vector<SparqlTripleSimple>, ParsedQuery::GraphPattern> Visitor::visit(
    Parser::DeleteWhereContext* ctx) {
  auto triples = visit(ctx->quadPattern());
  auto registerIfVariable = [this](const TripleComponent& component) {
    if (component.isVariable()) {
      addVisibleVariable(component.getVariable());
    }
  };
  auto transformAndRegisterTriple =
      [registerIfVariable](const SparqlTripleSimple& triple) {
        registerIfVariable(triple.s_);
        registerIfVariable(triple.p_);
        registerIfVariable(triple.o_);

        // The predicate comes from a rule in the grammar (`verb`) which only
        // allows variables and IRIs.
        AD_CORRECTNESS_CHECK(triple.p_.isVariable() || triple.p_.isIri());
        return SparqlTriple::fromSimple(triple);
      };
  GraphPattern pattern;
  pattern._graphPatterns.emplace_back(BasicGraphPattern{
      ad_utility::transform(triples, transformAndRegisterTriple)});

  return {std::move(triples), std::move(pattern)};
}

// ____________________________________________________________________________________
ParsedQuery Visitor::visit(Parser::ModifyContext* ctx) {
  if (ctx->iri()) {
    reportNotSupported(ctx->iri(), "Named graphs are");
  }
  if (!ctx->usingClause().empty()) {
    reportNotSupported(ctx->usingClause(0),
                       "USING inside an DELETE or INSERT is");
  }

  parsedQuery_._rootGraphPattern = visit(ctx->groupGraphPattern());

  parsedQuery_._clause = parsedQuery::UpdateClause();
  visitIf(&parsedQuery_.updateClause().toInsert_, ctx->insertClause());
  visitIf(&parsedQuery_.updateClause().toDelete_, ctx->deleteClause());

  return parsedQuery_;
}

// ____________________________________________________________________________________
vector<SparqlTripleSimple> Visitor::visit(Parser::DeleteClauseContext* ctx) {
  return visit(ctx->quadPattern());
}

// ____________________________________________________________________________________
vector<SparqlTripleSimple> Visitor::visit(Parser::InsertClauseContext* ctx) {
  return visit(ctx->quadPattern());
}

// ____________________________________________________________________________________
GraphOrDefault Visitor::visit(Parser::GraphOrDefaultContext* ctx) {
  if (ctx->iri()) {
    return visit(ctx->iri());
  } else {
    return DEFAULT{};
  }
}

// ____________________________________________________________________________________
GraphRef Visitor::visit(Parser::GraphRefContext* ctx) {
  return visit(ctx->iri());
}

// ____________________________________________________________________________________
GraphRefAll Visitor::visit(Parser::GraphRefAllContext* ctx) {
  if (ctx->graphRef()) {
    return visit(ctx->graphRef());
  } else if (ctx->DEFAULT()) {
    return DEFAULT{};
  } else if (ctx->NAMED()) {
    return NAMED{};
  } else if (ctx->ALL()) {
    return ALL{};
  } else {
    AD_FAIL();
  }
}

// ____________________________________________________________________________________
vector<SparqlTripleSimple> Visitor::visit(Parser::QuadPatternContext* ctx) {
  return visit(ctx->quads());
}

// ____________________________________________________________________________________
vector<SparqlTripleSimple> Visitor::visit(Parser::QuadDataContext* ctx) {
  auto quads = visit(ctx->quads());
  auto checkAndReportVar = [&ctx](const TripleComponent& term) {
    if (term.isVariable()) {
      reportError(ctx->quads(), "Variables (" + term.getVariable().name() +
                                    ") are not allowed here.");
    }
  };

  for (const auto& quad : quads) {
    checkAndReportVar(quad.s_);
    checkAndReportVar(quad.p_);
    checkAndReportVar(quad.o_);
  }

  return quads;
}

// ____________________________________________________________________________________
vector<SparqlTripleSimple> Visitor::visit(Parser::QuadsContext* ctx) {
  if (!ctx->quadsNotTriples().empty()) {
    // Could also be default; disallow completely for now.
    reportNotSupported(ctx->quadsNotTriples(0), "Named graphs are");
  }

  AD_CORRECTNESS_CHECK(ctx->triplesTemplate().size() == 1);

  auto convertTriple =
      [](const std::array<GraphTerm, 3>& triple) -> SparqlTripleSimple {
    return {visitGraphTerm(triple[0]), visitGraphTerm(triple[1]),
            visitGraphTerm(triple[2])};
  };

  return ad_utility::transform(visit(ctx->triplesTemplate(0)), convertTriple);
}

// ____________________________________________________________________________________
GraphPattern Visitor::visit(Parser::GroupGraphPatternContext* ctx) {
  GraphPattern pattern;

  // The following code makes sure that the variables from outside the graph
  // pattern are NOT visible inside the graph pattern, but the variables from
  // the graph pattern are visible outside the graph pattern.
  auto visibleVariablesSoFar = std::move(visibleVariables_);
  visibleVariables_.clear();
  auto mergeVariables =
      ad_utility::makeOnDestructionDontThrowDuringStackUnwinding(
          [this, &visibleVariablesSoFar]() {
            std::swap(visibleVariables_, visibleVariablesSoFar);
            visibleVariables_.insert(visibleVariables_.end(),
                                     visibleVariablesSoFar.begin(),
                                     visibleVariablesSoFar.end());
          });
  if (ctx->subSelect()) {
    auto parsedQuerySoFar = std::exchange(parsedQuery_, ParsedQuery{});
    auto [subquery, valuesOpt] = visit(ctx->subSelect());
    pattern._graphPatterns.emplace_back(std::move(subquery));
    if (valuesOpt.has_value()) {
      pattern._graphPatterns.emplace_back(std::move(valuesOpt.value()));
    }
    parsedQuery_ = std::move(parsedQuerySoFar);
    return pattern;
  } else {
    AD_CORRECTNESS_CHECK(ctx->groupGraphPatternSub());
    auto [subOps, filters] = visit(ctx->groupGraphPatternSub());
    pattern._graphPatterns = std::move(subOps);
    for (auto& filter : filters) {
      if (auto langFilterData =
              filter.expression_.getLanguageFilterExpression();
          langFilterData.has_value()) {
        const auto& [variable, language] = langFilterData.value();
        pattern.addLanguageFilter(variable, language);
      } else {
        pattern._filters.push_back(std::move(filter));
      }
    }
    return pattern;
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
  for (auto& [graphPattern, triples] :
       visitVector(ctx->graphPatternNotTriplesAndMaybeTriples())) {
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
  auto varToPropertyPath = [](const Variable& var) {
    return PropertyPath::fromVariable(var);
  };
  auto propertyPathIdentity = [](const PropertyPath& path) { return path; };
  auto visitVarOrPath =
      [&varToPropertyPath, &propertyPathIdentity](
          const ad_utility::sparql_types::VarOrPath& varOrPath) {
        return std::visit(
            ad_utility::OverloadCallOperator{varToPropertyPath,
                                             propertyPathIdentity},
            varOrPath);
      };
  auto registerIfVariable = [this](const auto& variant) {
    if (holds_alternative<Variable>(variant)) {
      addVisibleVariable(std::get<Variable>(variant));
    }
  };
  auto convertAndRegisterTriple =
      [&visitVarOrPath, &registerIfVariable](
          const TripleWithPropertyPath& triple) -> SparqlTriple {
    registerIfVariable(triple.subject_);
    registerIfVariable(triple.predicate_);
    registerIfVariable(triple.object_);

    return {visitGraphTerm(triple.subject_), visitVarOrPath(triple.predicate_),
            visitGraphTerm(triple.object_)};
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

// Parsing for the `serviceGraphPattern` rule.
parsedQuery::Service Visitor::visit(Parser::ServiceGraphPatternContext* ctx) {
  // If SILENT is specified, report that we do not support it yet.
  //
  // TODO: Support it, it's not hard. The semantics of SILENT is that if no
  // result can be obtained from the remote endpoint, then do as if the SERVICE
  // clause would not be there = the result is the neutral element.
  if (ctx->SILENT()) {
    reportNotSupported(ctx, "SILENT modifier in SERVICE is");
  }
  // Get the IRI and if a variable is specified, report that we do not support
  // it yet.
  //
  // NOTE: According to the grammar, this should either be a `Variable` or an
  // `Iri`, but due to (not very good) technical reasons, the `visit` returns a
  // `std::variant<Variable, GraphTerm>`, where `GraphTerm` is a
  // `std::variant<Literal, BlankNode, Iri>`, hence the `AD_CONTRACT_CHECK`.
  //
  // TODO: Also support variables. The semantics is to make a connection for
  // each IRI matching the variable and take the union of the results.
  GraphTerm varOrIri = visit(ctx->varOrIri());
  if (std::holds_alternative<Variable>(varOrIri)) {
    reportNotSupported(ctx->varOrIri(), "Variable endpoint in SERVICE is");
  }
  AD_CONTRACT_CHECK(std::holds_alternative<Iri>(varOrIri));
  Iri serviceIri = std::get<Iri>(varOrIri);
  // Parse the body of the SERVICE query. Add the visible variables from the
  // SERVICE clause to the visible variables so far, but also remember them
  // separately (with duplicates removed) because we need them in `Service.cpp`
  // when computing the result for this operation.
  std::vector<Variable> visibleVariablesSoFar = std::move(visibleVariables_);
  parsedQuery::GraphPattern graphPattern = visit(ctx->groupGraphPattern());
  // Note: The `visit` call in the line above has filled the `visibleVariables_`
  // member with all the variables visible inside the graph pattern.
  std::vector<Variable> visibleVariablesServiceQuery =
      ad_utility::removeDuplicates(visibleVariables_);
  visibleVariables_ = std::move(visibleVariablesSoFar);
  visibleVariables_.insert(visibleVariables_.end(),
                           visibleVariablesServiceQuery.begin(),
                           visibleVariablesServiceQuery.end());
  // Create suitable `parsedQuery::Service` object and return it.
  return {std::move(visibleVariablesServiceQuery), std::move(serviceIri),
          prologueString_,
          getOriginalInputForContext(ctx->groupGraphPattern())};
}

// ____________________________________________________________________________
parsedQuery::GraphPatternOperation Visitor::visit(
    const Parser::GraphGraphPatternContext* ctx) {
  reportNotSupported(ctx, "Named Graphs (FROM, GRAPH) are");
}

// Parsing for the `expression` rule.
sparqlExpression::SparqlExpression::Ptr Visitor::visit(
    Parser::ExpressionContext* ctx) {
  return visit(ctx->conditionalOrExpression());
}

// Parsing for the `whereClause` rule.
Visitor::PatternAndVisibleVariables Visitor::visit(
    Parser::WhereClauseContext* ctx) {
  // Get the variables visible in this WHERE clause separately from the visible
  // variables so far because they might not all be visible in the outer query.
  // Adding appropriately to the visible variables so far is then taken care of
  // in `visit(SubSelectContext*)`.
  std::vector<Variable> visibleVariablesSoFar = std::move(visibleVariables_);
  auto graphPatternWhereClause = visit(ctx->groupGraphPattern());
  // Using `std::exchange` as per Johannes' suggestion. I am slightly irritated
  // that this calls the move constructor AND the move assignment operator for
  // the second argument, since this is a potential performance issue (not in
  // this case though).
  auto visibleVariablesWhereClause =
      std::exchange(visibleVariables_, std::move(visibleVariablesSoFar));
  return {std::move(graphPatternWhereClause),
          std::move(visibleVariablesWhereClause)};
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
  visitIf(&clause.textLimit_, ctx->textLimitClause());
  return clause;
}

// ____________________________________________________________________________________
vector<SparqlFilter> Visitor::visit(Parser::HavingClauseContext* ctx) {
  return visitVector(ctx->havingCondition());
}

// ____________________________________________________________________________________
SparqlFilter Visitor::visit(Parser::HavingConditionContext* ctx) {
  return {visitExpressionPimpl(ctx->constraint())};
}

// ____________________________________________________________________________________
OrderClause Visitor::visit(Parser::OrderClauseContext* ctx) {
  auto orderKeys = visitVector(ctx->orderCondition());

  if (ctx->internalSortBy) {
    auto isDescending = [](const auto& variant) {
      return std::visit([](const auto& k) { return k.isDescending_; }, variant);
    };
    if (std::ranges::any_of(orderKeys, isDescending)) {
      reportError(ctx,
                  "When using the `INTERNAL SORT BY` modifier, all sorted "
                  "variables have to be ascending");
    }
    return {IsInternalSort::True, std::move(orderKeys)};
  } else {
    AD_CONTRACT_CHECK(ctx->orderBy);
    return {IsInternalSort::False, std::move(orderKeys)};
  }
}

// ____________________________________________________________________________________
vector<GroupKey> Visitor::visit(Parser::GroupClauseContext* ctx) {
  return visitVector(ctx->groupCondition());
}

// ____________________________________________________________________________________
std::optional<parsedQuery::ConstructClause> Visitor::visit(
    Parser::ConstructTemplateContext* ctx) {
  if (ctx->constructTriples()) {
    isInsideConstructTriples_ = true;
    auto cleanup =
        absl::Cleanup{[this]() { isInsideConstructTriples_ = false; }};
    return parsedQuery::ConstructClause{visit(ctx->constructTriples())};
  } else {
    return std::nullopt;
  }
}

// ____________________________________________________________________________________
RdfEscaping::NormalizedRDFString Visitor::visit(Parser::StringContext* ctx) {
  return RdfEscaping::normalizeRDFLiteral(ctx->getText());
}

// ____________________________________________________________________________________
TripleComponent::Iri Visitor::visit(Parser::IriContext* ctx) {
  string langtag =
      ctx->PREFIX_LANGTAG() ? ctx->PREFIX_LANGTAG()->getText() : "";
  return TripleComponent::Iri::fromIriref(
      langtag + visitAlternative<string>(ctx->iriref(), ctx->prefixedName()));
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
  // Remember the whole prologue (we need this when we encounter a SERVICE
  // clause, see `visit(ServiceGraphPatternContext*)` below.
  if (ctx->getStart() && ctx->getStop()) {
    prologueString_ = getOriginalInputForContext(ctx);
  }
}

// ____________________________________________________________________________________
void Visitor::visit(const Parser::BaseDeclContext* ctx) {
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
  parsedQuery_._clause = visit(ctx->selectClause());
  visitVector(ctx->datasetClause());
  auto [pattern, visibleVariables] = visit(ctx->whereClause());
  parsedQuery_._rootGraphPattern = std::move(pattern);
  parsedQuery_.registerVariablesVisibleInQueryBody(visibleVariables);
  parsedQuery_.addSolutionModifiers(visit(ctx->solutionModifier()));
  return parsedQuery_;
}

// ____________________________________________________________________________________
Visitor::SubQueryAndMaybeValues Visitor::visit(Parser::SubSelectContext* ctx) {
  ParsedQuery& query = parsedQuery_;
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
  } else {
    AD_CORRECTNESS_CHECK(ctx->expression());
    auto expr = visitExpressionPimpl(ctx->expression());
    if (ctx->AS() && ctx->var()) {
      return Alias{std::move(expr), visit(ctx->var())};
    } else {
      return expr;
    }
  }
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
  } else {
    AD_CORRECTNESS_CHECK(ctx->brackettedExpression());
    return visitExprOrderKey(ctx->DESC() != nullptr,
                             ctx->brackettedExpression());
  }
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
  values._variables.push_back(visit(ctx->var()));
  for (auto& dataBlockValue : ctx->dataBlockValue()) {
    values._values.push_back({visit(dataBlockValue)});
  }
  return values;
}

// ____________________________________________________________________________________
SparqlValues Visitor::visit(Parser::InlineDataFullContext* ctx) {
  SparqlValues values;
  values._variables = visitVector(ctx->var());
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
vector<TripleComponent> Visitor::visit(Parser::DataBlockSingleContext* ctx) {
  if (ctx->NIL()) {
    return {};
  }
  return visitVector(ctx->dataBlockValue());
}

// ____________________________________________________________________________________
TripleComponent Visitor::visit(Parser::DataBlockValueContext* ctx) {
  // Return a string
  if (ctx->iri()) {
    return visit(ctx->iri());
  } else if (ctx->rdfLiteral()) {
    return TurtleStringParser<TokenizerCtre>::parseTripleObject(
        visit(ctx->rdfLiteral()));
  } else if (ctx->numericLiteral()) {
    return std::visit(
        [](auto intOrDouble) { return TripleComponent{intOrDouble}; },
        visit(ctx->numericLiteral()));
  } else if (ctx->UNDEF()) {
    return TripleComponent::UNDEF{};
  } else {
    AD_CORRECTNESS_CHECK(ctx->booleanLiteral());
    return TripleComponent{visit(ctx->booleanLiteral())};
  }
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
  // The second argument means that the expression `LANG(?var) = "language"` is
  // allowed.
  return SparqlFilter{visitExpressionPimpl(ctx->constraint(), true)};
}

// ____________________________________________________________________________________
ExpressionPtr Visitor::visit(Parser::ConstraintContext* ctx) {
  return visitAlternative<ExpressionPtr>(
      ctx->brackettedExpression(), ctx->builtInCall(), ctx->functionCall());
}

// ____________________________________________________________________________________
ExpressionPtr Visitor::visit(Parser::FunctionCallContext* ctx) {
  return processIriFunctionCall(visit(ctx->iri()), visit(ctx->argList()), ctx);
}

// ____________________________________________________________________________________
vector<Visitor::ExpressionPtr> Visitor::visit(Parser::ArgListContext* ctx) {
  // If no arguments, return empty expression vector.
  if (ctx->NIL()) {
    return std::vector<ExpressionPtr>{};
  }
  // The grammar allows an optional DISTINCT before the argument list (the
  // whole list, not the individual arguments), but we currently don't support
  // it.
  if (ctx->DISTINCT()) {
    reportNotSupported(
        ctx, "DISTINCT for the argument lists of an IRI functions is ");
  }
  // Visit the expression of each argument.
  return visitVector(ctx->expression());
}

// ____________________________________________________________________________________
std::vector<ExpressionPtr> Visitor::visit(Parser::ExpressionListContext* ctx) {
  if (ctx->NIL()) {
    return {};
  }
  return visitVector(ctx->expression());
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
    GraphTerm subject = visit(ctx->varOrTerm());
    AD_CONTRACT_CHECK(ctx->propertyListNotEmpty());
    auto propertyList = visit(ctx->propertyListNotEmpty());
    for (auto& tuple : propertyList.first) {
      triples.push_back({subject, std::move(tuple[0]), std::move(tuple[1])});
    }
    ad_utility::appendVector(triples, std::move(propertyList.second));
  } else {
    AD_CORRECTNESS_CHECK(ctx->triplesNode());
    auto tripleNodes = visit(ctx->triplesNode());
    ad_utility::appendVector(triples, std::move(tripleNodes.second));
    AD_CONTRACT_CHECK(ctx->propertyList());
    auto propertyList = visit(ctx->propertyList());
    for (auto& tuple : propertyList.first) {
      triples.push_back(
          {tripleNodes.first, std::move(tuple[0]), std::move(tuple[1])});
    }
    ad_utility::appendVector(triples, std::move(propertyList.second));
  }
  return triples;
}

// ____________________________________________________________________________________
PredicateObjectPairsAndTriples Visitor::visit(
    Parser::PropertyListContext* ctx) {
  return ctx->propertyListNotEmpty() ? visit(ctx->propertyListNotEmpty())
                                     : PredicateObjectPairsAndTriples{
                                           PredicateObjectPairs{}, Triples{}};
}

// ____________________________________________________________________________________
PredicateObjectPairsAndTriples Visitor::visit(
    Parser::PropertyListNotEmptyContext* ctx) {
  PredicateObjectPairs triplesWithoutSubject;
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
GraphTerm Visitor::visit(Parser::VerbContext* ctx) {
  if (ctx->varOrIri()) {
    return visit(ctx->varOrIri());
  } else {
    // Special keyword 'a'
    AD_CORRECTNESS_CHECK(ctx->getText() == "a");
    return GraphTerm{Iri{"<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>"}};
  }
}

// ____________________________________________________________________________________
ObjectsAndTriples Visitor::visit(Parser::ObjectListContext* ctx) {
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
SubjectOrObjectAndTriples Visitor::visit(Parser::ObjectRContext* ctx) {
  return visit(ctx->graphNode());
}

// ____________________________________________________________________________________
void Visitor::setMatchingWordAndScoreVisibleIfPresent(
    // If a triple `?var ql:contains-word "words"` or `?var ql:contains-entity
    // <entity>` is contained in the query, then the variable
    // `?ql_textscore_var` is implicitly created and visible in the query body.
    // Similarly, if a triple `?var ql:contains-word "words"` is contained in
    // the query, then the variable `ql_matchingword_var` is implicitly created
    // and visible in the query body.
    auto* ctx, const TripleWithPropertyPath& triple) {
  const auto& [subject, predicate, object] = triple;

  auto* var = std::get_if<Variable>(&subject);
  auto* propertyPath = std::get_if<PropertyPath>(&predicate);

  if (!var || !propertyPath) {
    return;
  }

  if (propertyPath->asString() == CONTAINS_WORD_PREDICATE) {
    string name = object.toSparql();
    if (!((name.starts_with('"') && name.ends_with('"')) ||
          (name.starts_with('\'') && name.ends_with('\'')))) {
      reportError(ctx,
                  "ql:contains-word has to be followed by a string in quotes");
    }
    for (std::string_view s : std::vector<std::string>(
             absl::StrSplit(name.substr(1, name.size() - 2), ' '))) {
      if (!s.ends_with('*')) {
        continue;
      }
      addVisibleVariable(var->getMatchingWordVariable(
          ad_utility::utf8ToLower(s.substr(0, s.size() - 1))));
    }
  } else if (propertyPath->asString() == CONTAINS_ENTITY_PREDICATE) {
    if (const auto* entVar = std::get_if<Variable>(&object)) {
      addVisibleVariable(var->getScoreVariable(*entVar));
    } else {
      addVisibleVariable(var->getScoreVariable(object.toSparql()));
    }
  }
}

// ___________________________________________________________________________
vector<TripleWithPropertyPath> Visitor::visit(
    Parser::TriplesSameSubjectPathContext* ctx) {
  /*
  // If a triple `?var ql:contains-word "words"` or `?var ql:contains-entity
  // <entity>` is contained in the query, then the variable `?ql_textscore_var`
  // is implicitly created and visible in the query body.
  // Similarly if a triple `?var ql:contains-word "words"` is contained in the
  // query, then the variable `ql_matchingword_var` is implicitly created and
  // visible in the query body.
  auto setMatchingWordAndScoreVisibleIfPresent =
      [this, ctx](GraphTerm& subject, VarOrPath& predicate, GraphTerm& object) {
        auto* var = std::get_if<Variable>(&subject);
        auto* propertyPath = std::get_if<PropertyPath>(&predicate);

        if (!var || !propertyPath) {
          return;
        }

        if (propertyPath->asString() == CONTAINS_WORD_PREDICATE) {
          string name = object.toSparql();
          if (!((name.starts_with('"') && name.ends_with('"')) ||
                (name.starts_with('\'') && name.ends_with('\'')))) {
            reportError(
                ctx,
                "ql:contains-word has to be followed by a string in quotes");
          }
          for (std::string_view s : std::vector<std::string>(
                   absl::StrSplit(name.substr(1, name.size() - 2), ' '))) {
            if (!s.ends_with('*')) {
              continue;
            }
            addVisibleVariable(var->getMatchingWordVariable(
                ad_utility::utf8ToLower(s.substr(0, s.size() - 1))));
          }
        } else if (propertyPath->asString() == CONTAINS_ENTITY_PREDICATE) {
          if (const auto* entVar = std::get_if<Variable>(&object)) {
            addVisibleVariable(var->getScoreVariable(*entVar));
          } else {
            addVisibleVariable(var->getScoreVariable(object.toSparql()));
          }
        }
      };
      */

  // Assemble the final result from a set of given `triples` and possibly empty
  // `additionalTriples`, the given `subject` and the given pairs of
  // `[predicate, object]`
  using TripleVec = std::vector<TripleWithPropertyPath>;
  auto assembleResult = [this, ctx](TripleVec triples, GraphTerm subject,
                                    PathObjectPairs predicateObjectPairs,
                                    TripleVec additionalTriples) {
    for (auto&& [predicate, object] : std::move(predicateObjectPairs)) {
      triples.emplace_back(subject, std::move(predicate), std::move(object));
    }
    std::ranges::copy(additionalTriples, std::back_inserter(triples));
    for (const auto& triple : triples) {
      setMatchingWordAndScoreVisibleIfPresent(ctx, triple);
    }
    return triples;
  };
  if (ctx->varOrTerm()) {
    auto subject = visit(ctx->varOrTerm());
    auto [tuples, triples] = visit(ctx->propertyListPathNotEmpty());
    return assembleResult(std::move(triples), std::move(subject),
                          std::move(tuples), {});
  } else {
    AD_CORRECTNESS_CHECK(ctx->triplesNodePath());
    auto [subject, result] = visit(ctx->triplesNodePath());
    auto additionalTriples = visit(ctx->propertyListPath());
    if (additionalTriples.has_value()) {
      auto& [tuples, triples] = additionalTriples.value();
      return assembleResult(std::move(result), std::move(subject),
                            std::move(tuples), std::move(triples));
    } else {
      return assembleResult(std::move(result), std::move(subject), {}, {});
    }
  }
}

// ___________________________________________________________________________
std::optional<PathObjectPairsAndTriples> Visitor::visit(
    Parser::PropertyListPathContext* ctx) {
  return visitOptional(ctx->propertyListPathNotEmpty());
}

// ___________________________________________________________________________
PathObjectPairsAndTriples Visitor::visit(
    Parser::PropertyListPathNotEmptyContext* ctx) {
  PathObjectPairsAndTriples result = visit(ctx->tupleWithPath());
  auto& [pairs, triples] = result;
  vector<PathObjectPairsAndTriples> pairsAndTriples =
      visitVector(ctx->tupleWithoutPath());
  for (auto& [newPairs, newTriples] : pairsAndTriples) {
    std::ranges::move(newPairs, std::back_inserter(pairs));
    std::ranges::move(newTriples, std::back_inserter(triples));
  }
  return result;
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
PathObjectPairsAndTriples Visitor::visit(Parser::TupleWithoutPathContext* ctx) {
  VarOrPath predicate = visit(ctx->verbPathOrSimple());
  ObjectsAndTriples objectList = visit(ctx->objectList());
  auto predicateObjectPairs = joinPredicateAndObject(predicate, objectList);
  std::vector<TripleWithPropertyPath> triples;
  auto toVarOrPath = [](GraphTerm term) -> VarOrPath {
    if (std::holds_alternative<Variable>(term)) {
      return std::get<Variable>(term);
    } else {
      return PropertyPath::fromIri(term.toSparql());
    }
  };
  for (auto& triple : objectList.second) {
    triples.emplace_back(triple[0], toVarOrPath(triple[1]), triple[2]);
  }
  return {std::move(predicateObjectPairs), std::move(triples)};
}

// ____________________________________________________________________________________
PathObjectPairsAndTriples Visitor::visit(Parser::TupleWithPathContext* ctx) {
  VarOrPath predicate = visit(ctx->verbPathOrSimple());
  ObjectsAndPathTriples objectList = visit(ctx->objectListPath());
  auto predicateObjectPairs = joinPredicateAndObject(predicate, objectList);
  return {predicateObjectPairs, std::move(objectList.second)};
}

// ____________________________________________________________________________________
VarOrPath Visitor::visit(Parser::VerbPathOrSimpleContext* ctx) {
  return visitAlternative<ad_utility::sparql_types::VarOrPath>(
      ctx->verbPath(), ctx->verbSimple());
}

// ___________________________________________________________________________
ObjectsAndPathTriples Visitor::visit(Parser::ObjectListPathContext* ctx) {
  auto objectAndTriplesVec = visitVector(ctx->objectPath());
  // First collect all the objects.
  std::vector<GraphTerm> objects;
  std::ranges::copy(
      objectAndTriplesVec | std::views::transform(ad_utility::first),
      std::back_inserter(objects));

  // Collect all the triples. Node: `views::join` flattens the input.
  std::vector<TripleWithPropertyPath> triples;
  std::ranges::copy(objectAndTriplesVec |
                        std::views::transform(ad_utility::second) |
                        std::views::join,
                    std::back_inserter(triples));
  return {std::move(objects), std::move(triples)};
}

// ____________________________________________________________________________________
SubjectOrObjectAndPathTriples Visitor::visit(Parser::ObjectPathContext* ctx) {
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
    std::string modifier = ctx->pathMod()->getText();
    p = PropertyPath::makeModified(p, modifier);
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
    return PropertyPath::fromIri(
        std::string{visit(ctx->iri()).toStringRepresentation()});
  } else if (ctx->path()) {
    return visit(ctx->path());
  } else if (ctx->pathNegatedPropertySet()) {
    return visit(ctx->pathNegatedPropertySet());
  } else {
    AD_CORRECTNESS_CHECK(ctx->getText() == "a");
    // Special keyword 'a'
    return PropertyPath::fromIri(
        "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>");
  }
}

// ____________________________________________________________________________________
PropertyPath Visitor::visit(const Parser::PathNegatedPropertySetContext* ctx) {
  reportNotSupported(ctx, "\"!\" inside a property path is ");
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
    reportNotSupported(ctx, "Integer " + ctx->getText() +
                                " does not fit into 64 bits. This is ");
  }
}

// ____________________________________________________________________________________
SubjectOrObjectAndTriples Visitor::visit(Parser::TriplesNodeContext* ctx) {
  return visitAlternative<SubjectOrObjectAndTriples>(
      ctx->collection(), ctx->blankNodePropertyList());
}

// ____________________________________________________________________________________
SubjectOrObjectAndTriples Visitor::visit(
    Parser::BlankNodePropertyListContext* ctx) {
  GraphTerm term = [this]() -> GraphTerm {
    if (isInsideConstructTriples_) {
      return GraphTerm{newBlankNode()};
    } else {
      return parsedQuery_.getNewInternalVariable();
    }
  }();
  Triples triples;
  auto propertyList = visit(ctx->propertyListNotEmpty());
  for (auto& [predicate, object] : propertyList.first) {
    triples.push_back({term, std::move(predicate), std::move(object)});
  }
  ad_utility::appendVector(triples, std::move(propertyList.second));
  return {std::move(term), std::move(triples)};
}

// ____________________________________________________________________________________
SubjectOrObjectAndPathTriples Visitor::visit(
    Parser::TriplesNodePathContext* ctx) {
  return visitAlternative<SubjectOrObjectAndPathTriples>(
      ctx->blankNodePropertyListPath(), ctx->collectionPath());
}

// ____________________________________________________________________________________
SubjectOrObjectAndPathTriples Visitor::visit(
    Parser::BlankNodePropertyListPathContext* ctx) {
  auto subject = parsedQuery_.getNewInternalVariable();
  auto [predicateObjects, triples] = visit(ctx->propertyListPathNotEmpty());
  for (auto& [predicate, object] : predicateObjects) {
    triples.emplace_back(subject, std::move(predicate), std::move(object));
  }
  return {std::move(subject), triples};
}

// ____________________________________________________________________________________
SubjectOrObjectAndTriples Visitor::visit(Parser::CollectionContext* ctx) {
  Triples triples;
  GraphTerm nextTerm{Iri{"<http://www.w3.org/1999/02/22-rdf-syntax-ns#nil>"}};
  auto nodes = ctx->graphNode();
  for (auto context : Reversed{nodes}) {
    GraphTerm currentTerm{newBlankNode()};
    auto graphNode = visit(context);

    triples.push_back(
        {currentTerm,
         GraphTerm{Iri{"<http://www.w3.org/1999/02/22-rdf-syntax-ns#first>"}},
         std::move(graphNode.first)});
    triples.push_back(
        {currentTerm,
         GraphTerm{Iri{"<http://www.w3.org/1999/02/22-rdf-syntax-ns#rest>"}},
         std::move(nextTerm)});
    nextTerm = std::move(currentTerm);

    ad_utility::appendVector(triples, std::move(graphNode.second));
  }
  return {std::move(nextTerm), std::move(triples)};
}

// ____________________________________________________________________________________
SubjectOrObjectAndPathTriples Visitor::visit(
    Parser::CollectionPathContext* ctx) {
  throwCollectionsNotSupported(ctx);
}

// ____________________________________________________________________________________
SubjectOrObjectAndTriples Visitor::visit(Parser::GraphNodeContext* ctx) {
  if (ctx->varOrTerm()) {
    return {visit(ctx->varOrTerm()), Triples{}};
  } else {
    AD_CORRECTNESS_CHECK(ctx->triplesNode());
    return visit(ctx->triplesNode());
  }
}

// ____________________________________________________________________________________
SubjectOrObjectAndPathTriples Visitor::visit(
    Parser::GraphNodePathContext* ctx) {
  if (ctx->varOrTerm()) {
    return {visit(ctx->varOrTerm()), {}};
  } else {
    AD_CORRECTNESS_CHECK(ctx->triplesNodePath());
    return visit(ctx->triplesNodePath());
  }
}

// ____________________________________________________________________________________
GraphTerm Visitor::visit(Parser::VarOrTermContext* ctx) {
  return visitAlternative<GraphTerm>(ctx->var(), ctx->graphTerm());
}

// ____________________________________________________________________________________
GraphTerm Visitor::visit(Parser::VarOrIriContext* ctx) {
  if (ctx->var()) {
    return visit(ctx->var());
  } else {
    AD_CORRECTNESS_CHECK(ctx->iri());
    // TODO<qup42> If `visit` returns an `Iri` and `GraphTerm` can be
    // constructed from an `Iri`, this whole function becomes
    // `visitAlternative`.
    // TODO<joka921> If we unify the two IRI and Literal types (the ones from
    // the parser and from the `TripleComponent`, then this becomes much
    // simpler.
    return GraphTerm{
        Iri{std::string{visit(ctx->iri()).toStringRepresentation()}}};
  }
}

// ____________________________________________________________________________________
GraphTerm Visitor::visit(Parser::GraphTermContext* ctx) {
  if (ctx->blankNode()) {
    return visit(ctx->blankNode());
  } else if (ctx->iri()) {
    // TODO<joka921> Unify.
    return Iri{std::string{visit(ctx->iri()).toStringRepresentation()}};
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
  AD_CONTRACT_CHECK(!children.empty());
  auto result = std::move(children.front());
  std::for_each(children.begin() + 1, children.end(),
                [&result](ExpressionPtr& ptr) {
                  result = sparqlExpression::makeOrExpression(std::move(result),
                                                              std::move(ptr));
                });
  result->descriptor() = ctx->getText();
  return result;
}

// ____________________________________________________________________________________
ExpressionPtr Visitor::visit(Parser::ConditionalAndExpressionContext* ctx) {
  auto children = visitVector(ctx->valueLogical());
  AD_CONTRACT_CHECK(!children.empty());
  auto result = std::move(children.front());
  std::for_each(children.begin() + 1, children.end(),
                [&result](ExpressionPtr& ptr) {
                  result = sparqlExpression::makeAndExpression(
                      std::move(result), std::move(ptr));
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
    auto lhs = visitVector(ctx->numericExpression());
    AD_CORRECTNESS_CHECK(lhs.size() == 1);
    auto expressions = visit(ctx->expressionList());
    auto inExpression = std::make_unique<InExpression>(std::move(lhs.at(0)),
                                                       std::move(expressions));
    if (ctx->notToken) {
      return makeUnaryNegateExpression(std::move(inExpression));
    } else {
      return inExpression;
    }
  }
  AD_CONTRACT_CHECK(children.size() == 1 || children.size() == 2);
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
  } else {
    AD_CORRECTNESS_CHECK(relation == ">=");
    return make.operator()<GreaterEqualExpression>();
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
        result = sparqlExpression::makeAddExpression(
            std::move(result), std::move(signAndExpression.expression_));
        break;
      case Operator::Minus:
        result = sparqlExpression::makeSubtractExpression(
            std::move(result), std::move(signAndExpression.expression_));
        break;
      default:
        AD_FAIL();
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
    return std::make_unique<sparqlExpression::IdExpression>(
        Id::makeFromDouble(invertIfNecessary(d)));
  };
  auto createFromInt = [&](int64_t i) -> ExpressionPtr {
    return std::make_unique<sparqlExpression::IdExpression>(
        Id::makeFromInt(invertIfNecessary(i)));
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
        expression = sparqlExpression::makeMultiplyExpression(
            std::move(expression), std::move(opAndExp.expression_));
        break;
      case Operator::Divide:
        expression = sparqlExpression::makeDivideExpression(
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
        result = sparqlExpression::makeMultiplyExpression(
            std::move(result), std::move(opAndExp.expression_));
        break;
      case Operator::Divide:
        result = sparqlExpression::makeDivideExpression(
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
    return sparqlExpression::makeUnaryMinusExpression(std::move(child));
  } else if (ctx->children[0]->getText() == "!") {
    return sparqlExpression::makeUnaryNegateExpression(std::move(child));
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
    auto tripleComponent = TurtleStringParser<TokenizerCtre>::parseTripleObject(
        visit(ctx->rdfLiteral()));
    AD_CORRECTNESS_CHECK(!tripleComponent.isIri() &&
                         !tripleComponent.isString());
    if (tripleComponent.isLiteral()) {
      return make_unique<StringLiteralExpression>(tripleComponent.getLiteral());
    } else {
      return make_unique<IdExpression>(
          tripleComponent.toValueIdIfNotString().value());
    }
  } else if (ctx->numericLiteral()) {
    auto integralWrapper = [](int64_t x) {
      return ExpressionPtr{make_unique<IdExpression>(Id::makeFromInt(x))};
    };
    auto doubleWrapper = [](double x) {
      return ExpressionPtr{make_unique<IdExpression>(Id::makeFromDouble(x))};
    };
    return std::visit(
        ad_utility::OverloadCallOperator{integralWrapper, doubleWrapper},
        visit(ctx->numericLiteral()));
  } else if (ctx->booleanLiteral()) {
    return make_unique<IdExpression>(
        Id::makeFromBool(visit(ctx->booleanLiteral())));
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
  } else if (ctx->substringExpression()) {
    return visit(ctx->substringExpression());
  } else if (ctx->strReplaceExpression()) {
    return visit(ctx->strReplaceExpression());
  }
  // Get the function name and the arguments. Note that we do not have to check
  // the number of arguments like for `processIriFunctionCall`, since the number
  // of arguments is fixed by the grammar and we wouldn't even get here if the
  // number were wrong. Hence only the `AD_CONTRACT_CHECK`s.
  AD_CONTRACT_CHECK(!ctx->children.empty());
  auto functionName = ad_utility::getLowercase(ctx->children[0]->getText());
  auto argList = visitVector(ctx->expression());
  using namespace sparqlExpression;
  // Create the expression using the matching factory function from
  // `NaryExpression.h`.
  auto createUnary = [&argList]<typename Function>(Function function)
      requires std::is_invocable_r_v<ExpressionPtr, Function, ExpressionPtr> {
    AD_CORRECTNESS_CHECK(argList.size() == 1, argList.size());
    return function(std::move(argList[0]));
  };
  auto createBinary = [&argList]<typename Function>(Function function)
      requires std::is_invocable_r_v<ExpressionPtr, Function, ExpressionPtr,
                                     ExpressionPtr> {
    AD_CORRECTNESS_CHECK(argList.size() == 2);
    return function(std::move(argList[0]), std::move(argList[1]));
  };
  auto createTernary = [&argList]<typename Function>(Function function)
      requires std::is_invocable_r_v<ExpressionPtr, Function, ExpressionPtr,
                                     ExpressionPtr, ExpressionPtr> {
    AD_CORRECTNESS_CHECK(argList.size() == 3);
    return function(std::move(argList[0]), std::move(argList[1]),
                    std::move(argList[2]));
  };
  if (functionName == "str") {
    return createUnary(&makeStrExpression);
  } else if (functionName == "iri" || functionName == "uri") {
    return createUnary(&makeIriOrUriExpression);
  } else if (functionName == "strlang") {
    return createBinary(&makeStrLangTagExpression);
  } else if (functionName == "strdt") {
    return createBinary(&makeStrIriDtExpression);
  } else if (functionName == "strlen") {
    return createUnary(&makeStrlenExpression);
  } else if (functionName == "strbefore") {
    return createBinary(&makeStrBeforeExpression);
  } else if (functionName == "strafter") {
    return createBinary(&makeStrAfterExpression);
  } else if (functionName == "contains") {
    return createBinary(&makeContainsExpression);
  } else if (functionName == "strends") {
    return createBinary(&makeStrEndsExpression);
  } else if (functionName == "strstarts") {
    return createBinary(&makeStrStartsExpression);
  } else if (functionName == "ucase") {
    return createUnary(&makeUppercaseExpression);
  } else if (functionName == "lcase") {
    return createUnary(&makeLowercaseExpression);
  } else if (functionName == "year") {
    return createUnary(&makeYearExpression);
  } else if (functionName == "month") {
    return createUnary(&makeMonthExpression);
  } else if (functionName == "day") {
    return createUnary(&makeDayExpression);
  } else if (functionName == "tz") {
    return createUnary(&makeTimezoneStrExpression);
  } else if (functionName == "timezone") {
    return createUnary(&makeTimezoneExpression);
  } else if (functionName == "now") {
    AD_CONTRACT_CHECK(argList.empty());
    return std::make_unique<NowDatetimeExpression>(startTime_);
  } else if (functionName == "hours") {
    return createUnary(&makeHoursExpression);
  } else if (functionName == "minutes") {
    return createUnary(&makeMinutesExpression);
  } else if (functionName == "seconds") {
    return createUnary(&makeSecondsExpression);
  } else if (functionName == "md5") {
    return createUnary(&makeMD5Expression);
  } else if (functionName == "sha1") {
    return createUnary(&makeSHA1Expression);
  } else if (functionName == "sha256") {
    return createUnary(&makeSHA256Expression);
  } else if (functionName == "sha384") {
    return createUnary(&makeSHA384Expression);
  } else if (functionName == "sha512") {
    return createUnary(&makeSHA512Expression);
  } else if (functionName == "rand") {
    AD_CONTRACT_CHECK(argList.empty());
    return std::make_unique<RandomExpression>();
  } else if (functionName == "uuid") {
    AD_CONTRACT_CHECK(argList.empty());
    return std::make_unique<UuidExpression>();
  } else if (functionName == "struuid") {
    AD_CONTRACT_CHECK(argList.empty());
    return std::make_unique<StrUuidExpression>();
  } else if (functionName == "ceil") {
    return createUnary(&makeCeilExpression);
  } else if (functionName == "abs") {
    return createUnary(&makeAbsExpression);
  } else if (functionName == "round") {
    return createUnary(&makeRoundExpression);
  } else if (functionName == "floor") {
    return createUnary(&makeFloorExpression);
  } else if (functionName == "if") {
    return createTernary(&makeIfExpression);
  } else if (functionName == "coalesce") {
    AD_CORRECTNESS_CHECK(ctx->expressionList());
    return makeCoalesceExpression(visit(ctx->expressionList()));
  } else if (functionName == "encode_for_uri") {
    return createUnary(&makeEncodeForUriExpression);
  } else if (functionName == "concat") {
    AD_CORRECTNESS_CHECK(ctx->expressionList());
    return makeConcatExpression(visit(ctx->expressionList()));
  } else if (functionName == "isiri") {
    return createUnary(&makeIsIriExpression);
  } else if (functionName == "isblank") {
    return createUnary(&makeIsBlankExpression);
  } else if (functionName == "isliteral") {
    return createUnary(&makeIsLiteralExpression);
  } else if (functionName == "isnumeric") {
    return createUnary(&makeIsNumericExpression);
  } else if (functionName == "datatype") {
    return createUnary(&makeDatatypeExpression);
  } else if (functionName == "langmatches") {
    return createBinary(&makeLangMatchesExpression);
  } else if (functionName == "bound") {
    return makeBoundExpression(
        std::make_unique<VariableExpression>(visit(ctx->var())));
  } else {
    reportError(
        ctx,
        absl::StrCat("Built-in function \"", functionName,
                     "\"  not yet implemented; if you need it, just add it to ",
                     "SparqlQleverVisitor.cpp::visitTypesafe(Parser::"
                     "BuiltInCallContext ",
                     "following the already implemented functions there"));
  }
}

// _____________________________________________________________________________
ExpressionPtr Visitor::visit(Parser::RegexExpressionContext* ctx) {
  const auto& exp = ctx->expression();
  const auto& numArgs = exp.size();
  AD_CONTRACT_CHECK(numArgs >= 2 && numArgs <= 3);
  auto flags = numArgs == 3 ? visitOptional(exp[2]) : std::nullopt;
  try {
    return std::make_unique<sparqlExpression::RegexExpression>(
        visit(exp[0]), visit(exp[1]), std::move(flags));
  } catch (const std::exception& e) {
    reportError(ctx, e.what());
  }
}

// ____________________________________________________________________________________
ExpressionPtr Visitor::visit(Parser::LangExpressionContext* ctx) {
  // The number of children for expression LANG() is fixed to one by the
  // grammar (or definition of the parser).
  return sparqlExpression::makeLangExpression(visit(ctx->expression()));
}

// ____________________________________________________________________________________
SparqlExpression::Ptr Visitor::visit(Parser::SubstringExpressionContext* ctx) {
  auto children = visitVector(ctx->expression());
  AD_CORRECTNESS_CHECK(children.size() == 2 || children.size() == 3);
  if (children.size() == 2) {
    children.push_back(
        std::make_unique<IdExpression>(Id::makeFromInt(Id::maxInt)));
  }
  AD_CONTRACT_CHECK(children.size() == 3);
  return sparqlExpression::makeSubstrExpression(std::move(children.at(0)),
                                                std::move(children.at(1)),
                                                std::move(children.at(2)));
}

// ____________________________________________________________________________________
SparqlExpression::Ptr Visitor::visit(Parser::StrReplaceExpressionContext* ctx) {
  auto children = visitVector(ctx->expression());
  AD_CORRECTNESS_CHECK(children.size() == 3 || children.size() == 4);
  if (children.size() == 4) {
    reportError(
        ctx,
        "REPLACE expressions with four arguments (including regex flags) are "
        "currently not supported by QLever. You can however incorporate flags "
        "directly into a regex by prepending `(?<flags>)` to your regex. For "
        "example `(?i)[ei]` will match the regex `[ei]` in a case-insensitive "
        "way.");
  }
  return sparqlExpression::makeReplaceExpression(std::move(children.at(0)),
                                                 std::move(children.at(1)),
                                                 std::move(children.at(2)));
}

// ____________________________________________________________________________________
void Visitor::visit(const Parser::ExistsFuncContext* ctx) {
  reportNotSupported(ctx, "The EXISTS function is");
}

// ____________________________________________________________________________________
void Visitor::visit(const Parser::NotExistsFuncContext* ctx) {
  reportNotSupported(ctx, "The NOT EXISTS function is");
}

// ____________________________________________________________________________________
ExpressionPtr Visitor::visit(Parser::AggregateContext* ctx) {
  using namespace sparqlExpression;
  const auto& children = ctx->children;
  std::string functionName =
      ad_utility::getLowercase(children.at(0)->getText());

  const bool distinct = std::ranges::any_of(children, [](auto* child) {
    return ad_utility::getLowercase(child->getText()) == "distinct";
  });
  // the only case that there is no child expression is COUNT(*), so we can
  // check this outside the if below.
  if (!ctx->expression()) {
    AD_CORRECTNESS_CHECK(functionName == "count");
    return makeCountStarExpression(distinct);
  }
  auto childExpression = visit(ctx->expression());
  auto makePtr = [&]<typename ExpressionType>(auto&&... additionalArgs) {
    ExpressionPtr result{std::make_unique<ExpressionType>(
        distinct, std::move(childExpression), AD_FWD(additionalArgs)...)};
    result->descriptor() = ctx->getText();
    return result;
  };

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
      separator = visit(ctx->string()).get();
      // If there was a separator, we have to strip the quotation marks
      AD_CONTRACT_CHECK(separator.size() >= 2);
      separator = separator.substr(1, separator.size() - 2);
    } else {
      separator = " "s;
    }

    return makePtr.operator()<GroupConcatExpression>(std::move(separator));
  } else {
    AD_CORRECTNESS_CHECK(functionName == "sample");
    return makePtr.operator()<SampleExpression>();
  }
}

// ____________________________________________________________________________________
ExpressionPtr Visitor::visit(Parser::IriOrFunctionContext* ctx) {
  // Case 1: Just an IRI.
  if (ctx->argList() == nullptr) {
    return std::make_unique<sparqlExpression::IriExpression>(visit(ctx->iri()));
  }
  // Case 2: Function call, where the function name is an IRI.
  return processIriFunctionCall(visit(ctx->iri()), visit(ctx->argList()), ctx);
}

// ____________________________________________________________________________________
std::string Visitor::visit(Parser::RdfLiteralContext* ctx) {
  // TODO: This should really be an RdfLiteral class that stores a unified
  //  version of the string, and the langtag/datatype separately.
  string ret = ctx->string()->getText();
  if (ctx->LANGTAG()) {
    ret += ctx->LANGTAG()->getText();
  } else if (ctx->iri()) {
    // TODO<joka921> Also unify the two Literal classes...
    ret += ("^^" + std::string{visit(ctx->iri()).toStringRepresentation()});
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
    SparqlQleverVisitor::reportError(ctx, "Could not parse numeric literal \"" +
                                              ctx->getText() +
                                              "\" because it is out of range.");
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
GraphTerm Visitor::visit(Parser::BlankNodeContext* ctx) {
  if (ctx->ANON()) {
    if (isInsideConstructTriples_) {
      return newBlankNode();
    } else {
      return parsedQuery_.getNewInternalVariable();
    }
  } else {
    AD_CORRECTNESS_CHECK(ctx->BLANK_NODE_LABEL());
    if (isInsideConstructTriples_) {
      // Strip `_:` prefix from string.
      constexpr size_t length = std::string_view{"_:"}.length();
      const string label = ctx->BLANK_NODE_LABEL()->getText().substr(length);
      // `False` means the blank node is not automatically generated, but
      // explicitly specified in the query.
      return BlankNode{false, label};
    } else {
      return ParsedQuery::blankNodeToInternalVariable(
          ctx->BLANK_NODE_LABEL()->getText());
    }
  }
}

// ____________________________________________________________________________________
template <typename Ctx>
void Visitor::visitVector(const std::vector<Ctx*>& childContexts)
    requires voidWhenVisited<Visitor, Ctx> {
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
  AD_CONTRACT_CHECK(1u == (... + static_cast<bool>(ctxs)));
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

// _____________________________________________________________________________
template <typename Ctx>
void Visitor::visitIf(Ctx* ctx) requires voidWhenVisited<Visitor, Ctx> {
  if (ctx) {
    visit(ctx);
  }
}

// _____________________________________________________________________________
void Visitor::reportError(const antlr4::ParserRuleContext* ctx,
                          const std::string& msg) {
  throw InvalidSparqlQueryException{
      msg, ad_utility::antlr_utility::generateAntlrExceptionMetadata(ctx)};
}

// _____________________________________________________________________________
void Visitor::reportNotSupported(const antlr4::ParserRuleContext* ctx,
                                 const std::string& feature) {
  throw NotSupportedException{
      feature + " currently not supported by QLever.",
      ad_utility::antlr_utility::generateAntlrExceptionMetadata(ctx)};
}

// _____________________________________________________________________________
TripleComponent SparqlQleverVisitor::visitGraphTerm(
    const GraphTerm& graphTerm) {
  return graphTerm.visit([]<typename T>(const T& element) -> TripleComponent {
    if constexpr (std::is_same_v<T, Variable>) {
      return element;
    } else if constexpr (std::is_same_v<T, Literal> || std::is_same_v<T, Iri>) {
      return TurtleStringParser<TokenizerCtre>::parseTripleObject(
          element.toSparql());
    } else {
      return element.toSparql();
    }
  });
}
